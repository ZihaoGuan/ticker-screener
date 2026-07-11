from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
import math

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .market_data_access import load_many_ticker_windows, resolve_database_url, resolve_market_data_source
from .market_extension import resample_to_weekly
from .universe import UniverseTicker


POSITION_ACTION_HISTORY_DAYS = 320
ATR_PERIOD = 14
EMA21_PERIOD = 21
SMA50_PERIOD = 50
WEEKLY_SMA_PERIOD = 10
ADD_ACTIONS = {"add_position", "trim_reduce"}


@dataclass(frozen=True)
class PositionActionSnapshot:
    as_of_date: str
    action: str
    action_score: float
    regime_state: str
    trend_state: str
    extension_state: str
    support_reference: str
    atr_dist_21: float | None
    atr_dist_10w: float | None
    atr_pct: float | None
    daily_atr_ratio: float | None
    close_price: float | None
    ema21: float | None
    sma50: float | None
    sma10w: float | None
    danger_signal_count: int
    reason_summary: str
    evidence: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PositionActionDailyHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    as_of_date: str
    action: str
    action_score: float
    trend_state: str
    extension_state: str
    close_price: float
    ema21: float | None
    sma50: float | None
    sma10w: float | None
    atr_dist_21: float | None
    atr_dist_10w: float | None
    danger_signal_count: int
    reason_summary: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PositionActionDailyScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[PositionActionDailyHit]
    decision_rows: list[dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
            "decision_rows": _normalize_json_payload(self.decision_rows),
        }


def _normalize_json_payload(value: object) -> object:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _normalize_json_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_payload(item) for item in value]
    return value


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    for column in required:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _build_price_frame(financials) -> pd.DataFrame:
    rows = financials._get_clean_price_data()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Open": [row.get("open") for row in rows],
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _true_range(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["Close"].shift(1)
    return pd.concat(
        [
            frame["High"] - frame["Low"],
            (frame["High"] - previous_close).abs(),
            (frame["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def build_position_action_snapshot(frame: pd.DataFrame, *, as_of_date: dt.date | None = None) -> PositionActionSnapshot | None:
    bars = _normalize_bars_frame(frame)
    if as_of_date is not None and not bars.empty:
        bars = bars.loc[bars.index.date <= as_of_date]
    if bars.empty or len(bars) < max(SMA50_PERIOD + 5, ATR_PERIOD + 3):
        return None

    close = bars["Close"].astype(float)
    weekly = resample_to_weekly(bars[["Open", "High", "Low", "Close", "Volume"]])
    weekly_close = weekly["Close"].astype(float)
    weekly_sma10 = weekly_close.rolling(WEEKLY_SMA_PERIOD).mean()
    if weekly_sma10.dropna().empty:
        return None

    bars["ema21"] = close.ewm(span=EMA21_PERIOD, adjust=False).mean()
    bars["sma50"] = close.rolling(SMA50_PERIOD).mean()
    bars["atr14"] = _true_range(bars).rolling(ATR_PERIOD).mean()
    bars["prev_close"] = close.shift(1)
    bars["sma50_slope_5"] = bars["sma50"] - bars["sma50"].shift(5)
    bars["ema21_slope_5"] = bars["ema21"] - bars["ema21"].shift(5)
    bars["sma10w"] = weekly_sma10.reindex(bars.index, method="ffill")

    latest = bars.iloc[-1]
    close_price = float(latest["Close"])
    ema21 = _safe_float(latest.get("ema21"))
    sma50 = _safe_float(latest.get("sma50"))
    sma10w = _safe_float(latest.get("sma10w"))
    atr14 = _safe_float(latest.get("atr14"))
    prev_close = _safe_float(latest.get("prev_close"))
    sma50_slope_5 = _safe_float(latest.get("sma50_slope_5")) or 0.0
    ema21_slope_5 = _safe_float(latest.get("ema21_slope_5")) or 0.0
    if close_price <= 0 or atr14 in (None, 0.0) or ema21 is None or sma50 is None or sma10w is None:
        return None

    atr_dist_21 = (close_price - ema21) / atr14
    atr_dist_10w = (close_price - sma10w) / atr14
    atr_pct = (atr14 / close_price) * 100.0
    daily_atr_ratio = ((close_price - prev_close) / atr14) if prev_close is not None else None

    trend_state = "healthy"
    if close_price < sma50 or close_price < sma10w or sma50_slope_5 <= 0:
        trend_state = "broken"
    elif close_price < ema21 or ema21_slope_5 <= 0:
        trend_state = "weakening"

    extension_state = "normal"
    if atr_dist_21 >= 3.0 or atr_dist_10w >= 4.0:
        extension_state = "extreme"
    elif atr_dist_21 >= 1.5 or atr_dist_10w >= 2.5:
        extension_state = "stretched"

    support_reference = "ema21"
    if close_price < ema21:
        support_reference = "sma50" if close_price >= sma50 else "10w_sma"

    danger_signal_count = 0
    danger_flags: list[str] = []
    if extension_state == "extreme":
        danger_signal_count += 1
        danger_flags.append("extreme_extension")
    if daily_atr_ratio is not None and daily_atr_ratio <= -1.0:
        danger_signal_count += 1
        danger_flags.append("heavy_down_day")
    if close_price < ema21 < sma50:
        danger_signal_count += 1
        danger_flags.append("lost_ema21")
    if close_price < sma50 or close_price < sma10w:
        danger_signal_count += 1
        danger_flags.append("lost_support")

    score = 50.0
    if trend_state == "healthy":
        score += 25.0
    elif trend_state == "weakening":
        score += 5.0
    else:
        score -= 30.0

    if extension_state == "normal":
        score += 10.0
    elif extension_state == "stretched":
        score -= 8.0
    else:
        score -= 22.0

    if atr_dist_21 <= 1.2 and close_price >= ema21:
        score += 8.0
    if atr_dist_21 < -0.5:
        score -= 10.0
    if daily_atr_ratio is not None and daily_atr_ratio > 1.0 and extension_state != "normal":
        score -= 8.0
    score -= float(danger_signal_count * 8)
    score = max(0.0, min(100.0, score))

    regime_state = "bullish" if trend_state == "healthy" else "mixed" if trend_state == "weakening" else "risk_off"
    if trend_state == "broken":
        action = "avoid_new"
    elif extension_state == "extreme" or danger_signal_count >= 2:
        action = "trim_reduce"
    elif trend_state == "healthy" and extension_state == "normal" and atr_dist_21 <= 1.2:
        action = "add_position"
    else:
        action = "hold_position"

    reason_bits: list[str] = []
    if action == "add_position":
        reason_bits.append("trend intact")
        reason_bits.append("price still close enough to support")
    elif action == "trim_reduce":
        reason_bits.append("extension is elevated")
        if danger_signal_count > 0:
            reason_bits.append(f"{danger_signal_count} risk flag{'s' if danger_signal_count != 1 else ''} active")
    elif action == "avoid_new":
        reason_bits.append("trend support is compromised")
    else:
        reason_bits.append("trend still works")
        reason_bits.append("setup is no longer early enough for a fresh add")

    if daily_atr_ratio is not None:
        if daily_atr_ratio >= 1.0:
            reason_bits.append("latest day expanded more than 1 ATR")
        elif daily_atr_ratio <= -1.0:
            reason_bits.append("latest day sold off more than 1 ATR")

    resolved_as_of_date = bars.index[-1].date()
    evidence = {
        "danger_flags": danger_flags,
        "close_above_ema21": close_price >= ema21,
        "close_above_sma50": close_price >= sma50,
        "close_above_sma10w": close_price >= sma10w,
        "sma50_slope_5": round(sma50_slope_5, 4),
        "ema21_slope_5": round(ema21_slope_5, 4),
    }
    return PositionActionSnapshot(
        as_of_date=resolved_as_of_date.isoformat(),
        action=action,
        action_score=round(score, 2),
        regime_state=regime_state,
        trend_state=trend_state,
        extension_state=extension_state,
        support_reference=support_reference,
        atr_dist_21=round(atr_dist_21, 3),
        atr_dist_10w=round(atr_dist_10w, 3),
        atr_pct=round(atr_pct, 3),
        daily_atr_ratio=round(daily_atr_ratio, 3) if daily_atr_ratio is not None else None,
        close_price=round(close_price, 3),
        ema21=round(ema21, 3),
        sma50=round(sma50, 3),
        sma10w=round(sma10w, 3),
        danger_signal_count=danger_signal_count,
        reason_summary=". ".join(reason_bits).strip().capitalize() + ".",
        evidence=evidence,
    )


def _snapshot_to_row(ticker: UniverseTicker, snapshot: PositionActionSnapshot) -> dict[str, object]:
    return {
        "as_of_date": dt.date.fromisoformat(snapshot.as_of_date),
        "ticker": ticker.symbol,
        "action": snapshot.action,
        "action_score": snapshot.action_score,
        "regime_state": snapshot.regime_state,
        "trend_state": snapshot.trend_state,
        "extension_state": snapshot.extension_state,
        "support_reference": snapshot.support_reference,
        "atr_dist_21": snapshot.atr_dist_21,
        "atr_dist_10w": snapshot.atr_dist_10w,
        "atr_pct": snapshot.atr_pct,
        "daily_atr_ratio": snapshot.daily_atr_ratio,
        "close_price": snapshot.close_price,
        "ema21": snapshot.ema21,
        "sma50": snapshot.sma50,
        "sma10w": snapshot.sma10w,
        "danger_signal_count": snapshot.danger_signal_count,
        "reason_summary": snapshot.reason_summary,
        "evidence_json": snapshot.evidence,
    }


def _snapshot_to_hit(ticker: UniverseTicker, snapshot: PositionActionSnapshot) -> PositionActionDailyHit:
    return PositionActionDailyHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        as_of_date=snapshot.as_of_date,
        action=snapshot.action,
        action_score=snapshot.action_score,
        trend_state=snapshot.trend_state,
        extension_state=snapshot.extension_state,
        close_price=snapshot.close_price or 0.0,
        ema21=snapshot.ema21,
        sma50=snapshot.sma50,
        sma10w=snapshot.sma10w,
        atr_dist_21=snapshot.atr_dist_21,
        atr_dist_10w=snapshot.atr_dist_10w,
        danger_signal_count=snapshot.danger_signal_count,
        reason_summary=snapshot.reason_summary,
    )


def run_position_action_daily_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str = "",
    market_data_source: str = "database-first",
) -> PositionActionDailyScreenResult:
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    normalized_source = resolve_market_data_source(market_data_source)
    resolved_database_url = resolve_database_url(database_url)
    failures: list[dict[str, str]] = []
    decision_rows: list[dict[str, object]] = []
    actionable_hits: list[PositionActionDailyHit] = []

    print(
        "starting position action daily screen: "
        f"total={total_tickers}, source={normalized_source}, history_days={POSITION_ACTION_HISTORY_DAYS}"
    )

    db_frames = (
        load_many_ticker_windows(
            [ticker.symbol for ticker in tickers],
            run_date,
            POSITION_ACTION_HISTORY_DAYS,
            database_url=resolved_database_url,
        )
        if normalized_source == "database-first"
        else {}
    )
    ticker_map = {ticker.symbol.upper(): ticker for ticker in tickers}
    processed: set[str] = set()

    for symbol, frame in db_frames.items():
        ticker = ticker_map.get(symbol)
        if ticker is None:
            continue
        snapshot = build_position_action_snapshot(frame, as_of_date=run_date)
        if snapshot is None:
            failures.append({"ticker": ticker.symbol, "error": "insufficient_history"})
            continue
        decision_rows.append(_snapshot_to_row(ticker, snapshot))
        if snapshot.action in ADD_ACTIONS:
            actionable_hits.append(_snapshot_to_hit(ticker, snapshot))
        processed.add(symbol)

    remaining = [ticker for ticker in tickers if ticker.symbol.upper() not in processed]
    if remaining:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            position = len(processed)
            for ticker_batch in iter_prefetched_cookstock_batches(
                config,
                remaining,
                as_of_date=as_of_date,
                history_lookback_days=POSITION_ACTION_HISTORY_DAYS,
                benchmark_ticker=config.benchmark_ticker,
            ):
                for ticker in ticker_batch:
                    position += 1
                    print(f"[{position}/{total_tickers}] screening {ticker.symbol} | actionable={len(actionable_hits)}")
                    try:
                        financials = cookstock.cookFinancials(
                            ticker.symbol,
                            benchmarkTicker=config.benchmark_ticker,
                            historyLookbackDays=POSITION_ACTION_HISTORY_DAYS,
                        )
                        frame = _build_price_frame(financials)
                        snapshot = build_position_action_snapshot(frame, as_of_date=run_date)
                        if snapshot is None:
                            failures.append({"ticker": ticker.symbol, "error": "insufficient_history"})
                            continue
                        decision_rows.append(_snapshot_to_row(ticker, snapshot))
                        if snapshot.action in ADD_ACTIONS:
                            actionable_hits.append(_snapshot_to_hit(ticker, snapshot))
                    except Exception as exc:
                        failures.append({"ticker": ticker.symbol, "error": str(exc)})

    actionable_hits.sort(
        key=lambda item: (
            0 if item.action == "trim_reduce" else 1,
            -item.action_score,
            -(item.danger_signal_count or 0),
            item.ticker,
        )
    )
    decision_rows.sort(key=lambda item: (str(item.get("ticker") or ""), str(item.get("action") or "")))

    return PositionActionDailyScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(actionable_hits),
        failed_tickers=failures,
        hits=actionable_hits,
        decision_rows=decision_rows,
    )


def _safe_float(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
