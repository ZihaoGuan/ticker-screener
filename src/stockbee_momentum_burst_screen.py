from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker


STOCKBEE_MOMENTUM_BURST_HISTORY_DAYS = 80
MIN_HISTORY_DAYS = 25
MIN_PRICE = 5.0
MIN_VOLUME = 100_000
FOUR_PCT_THRESHOLD = 4.0
DOLLAR_THRESHOLD = 0.90
NINE_MILLION_VOLUME = 9_000_000
MAX_PREV_DAY_GAIN_FOR_RANGE = 2.0
MIN_BASE_DAYS = 3
MAX_BASE_DAYS = 20
MAX_BASE_WIDTH_PCT = 15.0
MAX_PRIOR_AVG_RANGE_PCT = 5.0
NARROW_PRIOR_DAY_RANGE_PCT = 3.0
RECENT_BREAKDOWN_LOOKBACK = 5
BREAKDOWN_THRESHOLD_PCT = 4.0
MAX_RISK_PCT_TO_STOP = 10.0
DEFAULT_MARKET_GATE = "allowed"
DB_BATCH_SIZE = 400


def _log(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class StockbeeMomentumBurstHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    primary_trigger: str
    trigger_tags: list[str]
    rating: str
    state: str
    current_price: float
    high_price: float
    low_price: float
    day_gain_pct: float
    dollar_gain: float
    current_range_pct: float
    current_range_dollars: float
    volume: int
    volume_ratio_1d: float
    volume_ratio_20d: float
    close_location_pct: float
    prev_day_gain_pct: float
    prior_base_days: int
    base_width_pct: float
    avg_prior_range_pct: float
    volume_dry_up: bool
    entry_reference: float
    stop_reference: float
    risk_pct_to_stop: float
    score: int
    trigger_score: int
    volume_score: int
    setup_score: int
    close_score: int
    risk_score: int
    failure_filter_score: int
    market_gate_score: int
    market_gate: str
    reject_reasons: list[str]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StockbeeMomentumBurstScreenResult:
    run_date: str
    market_gate: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    rejected_tickers: list[dict[str, object]]
    hits: list[StockbeeMomentumBurstHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "market_gate": self.market_gate,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "rejected_tickers": self.rejected_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _safe_pct(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return (numerator / denominator) * 100.0


def _range_pct(row: pd.Series) -> float:
    close_value = float(row["Close"])
    if close_value == 0.0:
        return 0.0
    return float(row["High"] - row["Low"]) / close_value * 100.0


def _close_location_pct(row: pd.Series) -> float:
    day_range = float(row["High"] - row["Low"])
    if day_range <= 0.0:
        return 50.0
    return (float(row["Close"] - row["Low"]) / day_range) * 100.0


def _is_three_day_run_up(closes: pd.Series) -> bool:
    if len(closes) < 4:
        return False
    recent = closes.iloc[-4:]
    return bool(recent.iloc[0] < recent.iloc[1] < recent.iloc[2] < recent.iloc[3])


def _had_recent_breakdown(closes: pd.Series) -> bool:
    if len(closes) < 2:
        return False
    returns = closes.pct_change().tail(RECENT_BREAKDOWN_LOOKBACK)
    return bool((returns <= -(BREAKDOWN_THRESHOLD_PCT / 100.0)).any())


def _find_base_profile(prior: pd.DataFrame) -> tuple[int, float, float, bool]:
    if prior.empty:
        return 0, 0.0, 0.0, False

    best: tuple[int, float, float] | None = None
    fallback: tuple[int, float, float] | None = None

    upper_bound = min(MAX_BASE_DAYS, len(prior))
    for days in range(upper_bound, MIN_BASE_DAYS - 1, -1):
        window = prior.tail(days)
        low_value = float(window["Low"].min())
        high_value = float(window["High"].max())
        base_width_pct = _safe_pct(high_value - low_value, low_value) if low_value > 0 else 0.0
        avg_prior_range_pct = float(window.apply(_range_pct, axis=1).mean())

        candidate = (days, base_width_pct, avg_prior_range_pct)
        if fallback is None or base_width_pct < fallback[1] or (base_width_pct == fallback[1] and days > fallback[0]):
            fallback = candidate

        if base_width_pct <= MAX_BASE_WIDTH_PCT and avg_prior_range_pct <= MAX_PRIOR_AVG_RANGE_PCT:
            best = candidate
            break

    chosen = best or fallback or (0, 0.0, 0.0)

    prior_volume = prior["Volume"].astype(float)
    recent_vol = float(prior_volume.iloc[-1]) if not prior_volume.empty else 0.0
    previous_pool = prior_volume.iloc[:-1].tail(20)
    volume_dry_up = bool(not previous_pool.empty and recent_vol < float(previous_pool.mean()))
    return chosen[0], chosen[1], chosen[2], volume_dry_up


def evaluate_stockbee_momentum_burst_frame(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    market_gate: str = DEFAULT_MARKET_GATE,
) -> dict[str, object]:
    bars = _normalize_bars_frame(frame)
    reject_reasons: list[str] = []
    signal_date = bars.index[-1].date().isoformat() if not bars.empty else ""
    skeleton: dict[str, object] = {
        "ticker": ticker.symbol,
        "sector": ticker.sector,
        "industry": ticker.industry,
        "exchange": ticker.exchange,
        "signal_date": signal_date,
        "primary_trigger": "",
        "trigger_tags": [],
        "rating": "Reject",
        "state": "REJECTED",
        "score": 0,
        "reject_reasons": reject_reasons,
        "reasons": [],
        "market_gate": market_gate,
    }
    if bars.empty or len(bars) < MIN_HISTORY_DAYS:
        reject_reasons.append("insufficient_history")
        return skeleton

    latest = bars.iloc[-1]
    prior = bars.iloc[:-1]
    if prior.empty:
        reject_reasons.append("insufficient_history")
        return skeleton
    previous = prior.iloc[-1]
    price_value = float(latest["Close"])
    volume_value = int(float(latest["Volume"]))
    if price_value < MIN_PRICE:
        reject_reasons.append("price_below_minimum")
        return skeleton
    if volume_value < MIN_VOLUME:
        reject_reasons.append("volume_below_minimum")
        return skeleton

    previous_close = float(previous["Close"])
    previous_volume = float(previous["Volume"])
    prior_close_window = prior["Close"].astype(float)
    prior_volume_window = prior["Volume"].astype(float)
    latest_range_dollars = float(latest["High"] - latest["Low"])
    latest_range_pct = _range_pct(latest)
    day_gain_pct = _safe_pct(float(latest["Close"] - previous_close), previous_close)
    dollar_gain = float(latest["Close"] - latest["Open"])
    volume_ratio_1d = float(volume_value / previous_volume) if previous_volume > 0 else 0.0
    volume_ratio_20d = (
        float(volume_value / float(prior_volume_window.tail(20).mean()))
        if not prior_volume_window.tail(20).empty and float(prior_volume_window.tail(20).mean()) > 0
        else 0.0
    )
    close_location_pct = _close_location_pct(latest)

    prev_day_gain_pct = 0.0
    if len(prior_close_window) >= 2:
        prev_day_gain_pct = _safe_pct(
            float(prior_close_window.iloc[-1] - prior_close_window.iloc[-2]),
            float(prior_close_window.iloc[-2]),
        )

    prior_ranges = prior.apply(_range_pct, axis=1)
    recent_prior_ranges = prior_ranges.tail(3)
    trigger_tags: list[str] = []
    four_pct_breakout = (
        previous_close > 0
        and day_gain_pct >= FOUR_PCT_THRESHOLD
        and volume_value > previous_volume
        and volume_value >= MIN_VOLUME
    )
    if four_pct_breakout:
        trigger_tags.append("4pct_breakout")

    dollar_breakout = dollar_gain >= DOLLAR_THRESHOLD and volume_value >= MIN_VOLUME
    if dollar_breakout:
        trigger_tags.append("dollar_breakout")

    range_expansion = (
        len(recent_prior_ranges) == 3
        and latest_range_pct > float(recent_prior_ranges.max())
        and prev_day_gain_pct <= MAX_PREV_DAY_GAIN_FOR_RANGE
        and volume_ratio_20d >= 1.1
    )
    if range_expansion:
        trigger_tags.append("range_expansion")

    if volume_value >= NINE_MILLION_VOLUME:
        trigger_tags.append("9m_volume")

    if not any(tag in trigger_tags for tag in ("4pct_breakout", "dollar_breakout", "range_expansion")):
        reject_reasons.append("no_momentum_burst_trigger")
        return skeleton

    prior_base_days, base_width_pct, avg_prior_range_pct, volume_dry_up = _find_base_profile(prior)
    entry_reference = price_value
    stop_reference = float(latest["Low"])
    risk_pct_to_stop = _safe_pct(entry_reference - stop_reference, entry_reference)
    if risk_pct_to_stop > MAX_RISK_PCT_TO_STOP:
        reject_reasons.append("risk_too_wide")
        return {
            **skeleton,
            "primary_trigger": trigger_tags[0],
            "trigger_tags": trigger_tags,
            "risk_pct_to_stop": round(risk_pct_to_stop, 2),
            "entry_reference": round(entry_reference, 4),
            "stop_reference": round(stop_reference, 4),
        }

    trigger_score = 0
    if four_pct_breakout:
        trigger_score += 10
    if dollar_breakout:
        trigger_score += 6
    if range_expansion:
        trigger_score += 6
    if volume_value >= NINE_MILLION_VOLUME:
        trigger_score += 4
    trigger_score = min(20, trigger_score)

    volume_score = 0
    if volume_ratio_1d >= 2.0:
        volume_score += 8
    elif volume_ratio_1d >= 1.5:
        volume_score += 6
    elif volume_ratio_1d >= 1.2:
        volume_score += 4
    elif volume_ratio_1d > 1.0:
        volume_score += 2
    if volume_ratio_20d >= 2.5:
        volume_score += 7
    elif volume_ratio_20d >= 1.8:
        volume_score += 5
    elif volume_ratio_20d >= 1.3:
        volume_score += 3
    elif volume_ratio_20d >= 1.0:
        volume_score += 1
    volume_score = min(15, volume_score)

    setup_score = 0
    if prior_base_days >= 10:
        setup_score += 10
    elif prior_base_days >= 6:
        setup_score += 7
    elif prior_base_days >= MIN_BASE_DAYS:
        setup_score += 5
    if base_width_pct <= 6.0:
        setup_score += 7
    elif base_width_pct <= 10.0:
        setup_score += 5
    elif base_width_pct <= MAX_BASE_WIDTH_PCT:
        setup_score += 3
    if prev_day_gain_pct <= 0.0:
        setup_score += 4
    elif _range_pct(previous) <= NARROW_PRIOR_DAY_RANGE_PCT:
        setup_score += 3
    if volume_dry_up:
        setup_score += 4
    setup_score = min(25, setup_score)

    if close_location_pct >= 85.0:
        close_score = 10
    elif close_location_pct >= 75.0:
        close_score = 8
    elif close_location_pct >= 65.0:
        close_score = 6
    elif close_location_pct >= 50.0:
        close_score = 4
    else:
        close_score = 0

    if risk_pct_to_stop <= 3.0:
        risk_score = 15
    elif risk_pct_to_stop <= 5.0:
        risk_score = 12
    elif risk_pct_to_stop <= 7.0:
        risk_score = 9
    else:
        risk_score = 6

    three_day_run_up = _is_three_day_run_up(prior_close_window)
    recent_breakdown = _had_recent_breakdown(prior_close_window)
    failure_filter_score = 10
    if three_day_run_up:
        failure_filter_score -= 4
    if recent_breakdown:
        failure_filter_score -= 4
    if base_width_pct > MAX_BASE_WIDTH_PCT:
        failure_filter_score -= 3
    if close_location_pct < 50.0:
        failure_filter_score -= 3
    if volume_ratio_20d < 1.2:
        failure_filter_score -= 2
    failure_filter_score = max(0, min(10, failure_filter_score))

    normalized_market_gate = str(market_gate or DEFAULT_MARKET_GATE).strip().lower()
    if normalized_market_gate == "allowed":
        market_gate_score = 5
    elif normalized_market_gate == "neutral":
        market_gate_score = 2
    else:
        market_gate_score = 0

    total_score = trigger_score + volume_score + setup_score + close_score + risk_score + failure_filter_score + market_gate_score
    if total_score >= 90:
        rating, state = "A", "ACTIONABLE_DAY1"
    elif total_score >= 80:
        rating, state = "A-", "ACTIONABLE_DAY1"
    elif total_score >= 70:
        rating, state = "B", "MANUAL_REVIEW"
    elif total_score >= 55:
        rating, state = "Watch", "WATCH_ONLY"
    else:
        rating, state = "Reject", "REJECTED"
        reject_reasons.append("score_below_threshold")

    reasons = [
        f"Primary trigger {trigger_tags[0]}",
        f"Day gain {day_gain_pct:.2f}%",
        f"Dollar gain {dollar_gain:.2f}",
        f"Volume {volume_ratio_1d:.2f}x vs 1D and {volume_ratio_20d:.2f}x vs 20D",
        f"Close location {close_location_pct:.1f}%",
        f"Base {prior_base_days}d width {base_width_pct:.2f}%",
        f"Risk to trigger-day low {risk_pct_to_stop:.2f}%",
    ]
    if volume_dry_up:
        reasons.append("Prior volume dry-up present")
    if three_day_run_up:
        reasons.append("Three-day run-up penalty")
    if recent_breakdown:
        reasons.append("Recent breakdown penalty")

    return {
        "ticker": ticker.symbol,
        "sector": ticker.sector,
        "industry": ticker.industry,
        "exchange": ticker.exchange,
        "signal_date": bars.index[-1].date().isoformat(),
        "primary_trigger": trigger_tags[0],
        "trigger_tags": trigger_tags,
        "rating": rating,
        "state": state,
        "current_price": round(price_value, 4),
        "high_price": round(float(latest["High"]), 4),
        "low_price": round(float(latest["Low"]), 4),
        "day_gain_pct": round(day_gain_pct, 2),
        "dollar_gain": round(dollar_gain, 2),
        "current_range_pct": round(latest_range_pct, 2),
        "current_range_dollars": round(latest_range_dollars, 2),
        "volume": volume_value,
        "volume_ratio_1d": round(volume_ratio_1d, 2),
        "volume_ratio_20d": round(volume_ratio_20d, 2),
        "close_location_pct": round(close_location_pct, 2),
        "prev_day_gain_pct": round(prev_day_gain_pct, 2),
        "prior_base_days": prior_base_days,
        "base_width_pct": round(base_width_pct, 2),
        "avg_prior_range_pct": round(avg_prior_range_pct, 2),
        "volume_dry_up": volume_dry_up,
        "entry_reference": round(entry_reference, 4),
        "stop_reference": round(stop_reference, 4),
        "risk_pct_to_stop": round(risk_pct_to_stop, 2),
        "score": int(total_score),
        "trigger_score": trigger_score,
        "volume_score": volume_score,
        "setup_score": setup_score,
        "close_score": close_score,
        "risk_score": risk_score,
        "failure_filter_score": failure_filter_score,
        "market_gate_score": market_gate_score,
        "market_gate": normalized_market_gate,
        "reject_reasons": reject_reasons,
        "reasons": reasons,
    }


def find_recent_stockbee_momentum_burst_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    market_gate: str = DEFAULT_MARKET_GATE,
) -> StockbeeMomentumBurstHit | None:
    evaluation = evaluate_stockbee_momentum_burst_frame(frame, ticker=ticker, market_gate=market_gate)
    if str(evaluation.get("state") or "") == "REJECTED":
        return None
    return StockbeeMomentumBurstHit(**evaluation)


def run_stockbee_momentum_burst_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    market_gate: str = DEFAULT_MARKET_GATE,
    database_url: str | None = None,
) -> StockbeeMomentumBurstScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    total_tickers = len(tickers)
    hits: list[StockbeeMomentumBurstHit] = []
    failures: list[dict[str, str]] = []
    rejected: list[dict[str, object]] = []

    _log(f"starting stockbee momentum burst screen: total={total_tickers} market_gate={market_gate}")

    for batch_start in range(0, total_tickers, DB_BATCH_SIZE):
        batch = tickers[batch_start : batch_start + DB_BATCH_SIZE]
        symbols = [ticker.symbol for ticker in batch]
        frame_map = load_many_ticker_windows(
            symbols,
            run_date,
            STOCKBEE_MOMENTUM_BURST_HISTORY_DAYS,
            database_url=resolved_database_url,
        )
        metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
        for offset, ticker in enumerate(batch, start=1):
            position = batch_start + offset
            metadata = metadata_map.get(ticker.symbol, {})
            runtime_ticker = UniverseTicker(
                symbol=ticker.symbol,
                sector=ticker.sector or str(metadata.get("sector") or "") or None,
                industry=ticker.industry or str(metadata.get("industry") or "") or None,
                exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
            )
            _log(f"[{position}/{total_tickers}] screening {runtime_ticker.symbol} | passed={len(hits)}")
            frame = frame_map.get(runtime_ticker.symbol)
            if frame is None or getattr(frame, "empty", False):
                failures.append({"ticker": runtime_ticker.symbol, "error": "missing_daily_bars"})
                _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: missing daily_bars")
                continue
            try:
                evaluation = evaluate_stockbee_momentum_burst_frame(
                    frame,
                    ticker=runtime_ticker,
                    market_gate=market_gate,
                )
            except Exception as exc:
                failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})
                _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} failed: {exc}")
                continue
            if str(evaluation.get("state") or "") == "REJECTED":
                rejected.append(
                    {
                        "ticker": runtime_ticker.symbol,
                        "signal_date": evaluation.get("signal_date"),
                        "reject_reasons": list(evaluation.get("reject_reasons") or []),
                        "primary_trigger": evaluation.get("primary_trigger"),
                    }
                )
                _log(
                    f"[{position}/{total_tickers}] {runtime_ticker.symbol} filtered: "
                    f"{','.join(str(item) for item in evaluation.get('reject_reasons') or ['rejected'])} | passed={len(hits)}"
                )
                continue
            hit = StockbeeMomentumBurstHit(**evaluation)
            hits.append(hit)
            _log(
                f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed "
                f"{hit.primary_trigger} score={hit.score} rating={hit.rating} | passed={len(hits)}"
            )

    hits.sort(key=lambda item: (item.score, item.day_gain_pct, item.volume_ratio_20d), reverse=True)
    actionable_count = sum(1 for item in hits if item.rating in {"A", "A-"})
    _log(
        "completed stockbee momentum burst screen: "
        f"total={total_tickers} hits={len(hits)} actionable_a_minus_or_better={actionable_count} "
        f"rejected={len(rejected)} failed={len(failures)}"
    )
    return StockbeeMomentumBurstScreenResult(
        run_date=run_date.isoformat(),
        market_gate=str(market_gate or DEFAULT_MARKET_GATE),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        rejected_tickers=rejected,
        hits=hits,
    )
