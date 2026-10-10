from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


EMA9_20_CROSSOVER_TARGETS_STRATEGY_ID = "ema9_20_crossover_targets"
TARGET_LOOKBACK_BARS = 500
TARGET_TRACKING_BARS = 60
# 500 bars for the slow EMA warm-up, then enough history for a signal and targets.
PRICE_HISTORY_DAYS = 1_100


@dataclass(frozen=True)
class Ema9_20CrossoverTargetHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    as_of_date: str
    is_fresh_signal: bool
    current_price: float
    signal_open: float
    signal_close: float
    ema9: float
    ema20: float
    ema150: float
    ema500: float
    rsi14: float | None
    above_ema150: bool
    above_ema500: bool
    bull_target_1: float
    bull_target_2: float
    bear_target_1: float | None
    bear_target_2: float | None
    bull_target_1_hit_date: str | None
    bull_target_2_hit_date: str | None
    bear_target_1_hit_date: str | None
    bear_target_2_hit_date: str | None
    bull_target_1_bars_to_hit: int | None
    bull_target_2_bars_to_hit: int | None
    bear_target_1_bars_to_hit: int | None
    bear_target_2_bars_to_hit: int | None
    target_status: str
    same_bar_target_conflict: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Ema9_20CrossoverTargetsScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    fresh_buy_signals: int
    active_signals: int
    bull_target_2_hits: int
    bear_target_2_hits: int
    failed_tickers: list[dict[str, str]]
    hits: list[Ema9_20CrossoverTargetHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "fresh_buy_signals": self.fresh_buy_signals,
            "active_signals": self.active_signals,
            "bull_target_2_hits": self.bull_target_2_hits,
            "bear_target_2_hits": self.bear_target_2_hits,
            "failed_tickers": self.failed_tickers,
            "hits": [hit.to_dict() for hit in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    aliases = {str(column).lower(): column for column in frame.columns}
    required = ("Open", "High", "Low", "Close")
    if any(name.lower() not in aliases for name in required):
        return pd.DataFrame()
    normalized = frame[[aliases[name.lower()] for name in required]].copy()
    normalized.columns = required
    normalized = normalized.apply(pd.to_numeric, errors="coerce").dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Open": [row.get("open") for row in rows],
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Open", "High", "Low", "Close"]).set_index("Date").sort_index()


def _wilder_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    average_gain = gain.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    average_loss = loss.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()
    rs = average_gain / average_loss.replace(0.0, float("nan"))
    return 100.0 - (100.0 / (1.0 + rs))


def _date_text(index_value: object) -> str:
    timestamp = pd.Timestamp(index_value)
    return timestamp.date().isoformat()


def _first_target_hit(
    bars: pd.DataFrame,
    *,
    signal_index: int,
    target: float | None,
    direction: str,
) -> tuple[str | None, int | None]:
    if target is None:
        return None, None
    # Targets are known only once the signal bar closes; do not use that bar's range.
    for index in range(signal_index + 1, len(bars)):
        reached = float(bars["High"].iloc[index]) >= target if direction == "up" else float(bars["Low"].iloc[index]) <= target
        if reached:
            return _date_text(bars.index[index]), index - signal_index
    return None, None


def _target_status(*, bull_t1: str | None, bull_t2: str | None, bear_t1: str | None, bear_t2: str | None) -> str:
    if bull_t2 and bear_t2:
        return "both_target_2_hit"
    if bull_t2:
        return "bull_target_2_hit"
    if bear_t2:
        return "bear_target_2_hit"
    if bull_t1 and bear_t1:
        return "both_target_1_hit"
    if bull_t1:
        return "bull_target_1_hit"
    if bear_t1:
        return "bear_target_1_hit"
    return "active"


def find_recent_ema9_20_crossover_target_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    as_of_date: dt.date,
) -> Ema9_20CrossoverTargetHit | None:
    bars = _normalize_price_frame(frame)
    minimum_rows = max(500, TARGET_LOOKBACK_BARS + TARGET_TRACKING_BARS + 1)
    if len(bars) < minimum_rows:
        return None

    close = bars["Close"].astype(float)
    ema9 = close.ewm(span=9, adjust=False).mean()
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema150 = close.ewm(span=150, adjust=False).mean()
    ema500 = close.ewm(span=500, adjust=False).mean()
    rsi14 = _wilder_rsi(close)
    bullish_cross = ema9.gt(ema20) & ema9.shift(1).le(ema20.shift(1))
    candidate_indexes = [index for index in range(max(1, len(bars) - TARGET_TRACKING_BARS), len(bars)) if bool(bullish_cross.iloc[index])]
    if not candidate_indexes:
        return None
    signal_index = candidate_indexes[-1]
    # Pine's `for i = 0 to 500` includes the signal bar plus 500 prior bars.
    start_index = max(0, signal_index - TARGET_LOOKBACK_BARS)
    target_slice = slice(start_index, signal_index + 1)
    above_fast = close.ge(ema9) & close.ge(ema20)
    below_fast = close.lt(ema9) & close.le(ema20)
    bullish_closes = close.iloc[target_slice][above_fast.iloc[target_slice]]
    bullish_cross_closes = close.iloc[target_slice][bullish_cross.iloc[target_slice]]
    bearish_cross = ema20.gt(ema9) & ema20.shift(1).le(ema9.shift(1))
    bearish_closes = close.iloc[target_slice][below_fast.iloc[target_slice]]
    bearish_cross_closes = close.iloc[target_slice][bearish_cross.iloc[target_slice]]
    if bullish_closes.empty or bullish_cross_closes.empty:
        return None

    signal_open = float(bars["Open"].iloc[signal_index])
    bull_range = float(bullish_closes.max() - bullish_cross_closes.mean())
    bull_target_1 = signal_open + (bull_range / 2.0)
    bull_target_2 = signal_open + bull_range
    bear_target_1: float | None = None
    bear_target_2: float | None = None
    if not bearish_closes.empty and not bearish_cross_closes.empty:
        bear_range = float(bearish_cross_closes.mean() - bearish_closes.min())
        bear_target_1 = signal_open - (bear_range / 2.0)
        bear_target_2 = signal_open - bear_range

    bull_t1_date, bull_t1_bars = _first_target_hit(bars, signal_index=signal_index, target=bull_target_1, direction="up")
    bull_t2_date, bull_t2_bars = _first_target_hit(bars, signal_index=signal_index, target=bull_target_2, direction="up")
    bear_t1_date, bear_t1_bars = _first_target_hit(bars, signal_index=signal_index, target=bear_target_1, direction="down")
    bear_t2_date, bear_t2_bars = _first_target_hit(bars, signal_index=signal_index, target=bear_target_2, direction="down")
    bull_dates = {date for date in (bull_t1_date, bull_t2_date) if date}
    bear_dates = {date for date in (bear_t1_date, bear_t2_date) if date}
    latest_close = float(close.iloc[-1])
    latest_rsi = rsi14.iloc[-1]
    status = _target_status(bull_t1=bull_t1_date, bull_t2=bull_t2_date, bear_t1=bear_t1_date, bear_t2=bear_t2_date)
    is_fresh = signal_index == len(bars) - 1
    return Ema9_20CrossoverTargetHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=_date_text(bars.index[signal_index]),
        as_of_date=as_of_date.isoformat(),
        is_fresh_signal=is_fresh,
        current_price=latest_close,
        signal_open=signal_open,
        signal_close=float(close.iloc[signal_index]),
        ema9=float(ema9.iloc[-1]),
        ema20=float(ema20.iloc[-1]),
        ema150=float(ema150.iloc[-1]),
        ema500=float(ema500.iloc[-1]),
        rsi14=float(latest_rsi) if pd.notna(latest_rsi) else None,
        above_ema150=latest_close >= float(ema150.iloc[-1]),
        above_ema500=latest_close >= float(ema500.iloc[-1]),
        bull_target_1=bull_target_1,
        bull_target_2=bull_target_2,
        bear_target_1=bear_target_1,
        bear_target_2=bear_target_2,
        bull_target_1_hit_date=bull_t1_date,
        bull_target_2_hit_date=bull_t2_date,
        bear_target_1_hit_date=bear_t1_date,
        bear_target_2_hit_date=bear_t2_date,
        bull_target_1_bars_to_hit=bull_t1_bars,
        bull_target_2_bars_to_hit=bull_t2_bars,
        bear_target_1_bars_to_hit=bear_t1_bars,
        bear_target_2_bars_to_hit=bear_t2_bars,
        target_status=status,
        same_bar_target_conflict=bool(bull_dates & bear_dates),
        reasons=[
            "Confirmed EMA 9 crossed above EMA 20.",
            f"Signal open ${signal_open:.2f}; bull targets ${bull_target_1:.2f} / ${bull_target_2:.2f}.",
            (f"Bear reference targets ${bear_target_1:.2f} / ${bear_target_2:.2f}." if bear_target_1 is not None and bear_target_2 is not None else "Bear reference targets unavailable: no qualifying bearish history in the target window."),
            f"Target status: {status.replace('_', ' ')}.",
        ],
    )


def run_ema9_20_crossover_targets_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str = "",
) -> Ema9_20CrossoverTargetsScreenResult:
    run_date = as_of_date or dt.date.today()
    symbols = [ticker.symbol.upper() for ticker in tickers]
    resolved_database_url = resolve_database_url(database_url)
    frame_map = load_many_ticker_windows(symbols, run_date, PRICE_HISTORY_DAYS, database_url=resolved_database_url)
    hits: list[Ema9_20CrossoverTargetHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[UniverseTicker] = []
    for ticker in tickers:
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < 500:
            fallback_tickers.append(ticker)
            continue
        hit = find_recent_ema9_20_crossover_target_hit(frame, ticker=ticker, as_of_date=run_date)
        if hit is not None:
            hits.append(hit)

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for ticker in fallback_tickers:
                try:
                    financials = cookstock.cookFinancials(ticker.symbol, benchmarkTicker=config.benchmark_ticker, historyLookbackDays=PRICE_HISTORY_DAYS)
                    hit = find_recent_ema9_20_crossover_target_hit(_build_price_frame(financials), ticker=ticker, as_of_date=run_date)
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(key=lambda hit: (0 if hit.is_fresh_signal else 1, -dt.date.fromisoformat(hit.signal_date).toordinal(), hit.ticker))
    return Ema9_20CrossoverTargetsScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        fresh_buy_signals=sum(1 for hit in hits if hit.is_fresh_signal),
        active_signals=sum(1 for hit in hits if hit.target_status == "active"),
        bull_target_2_hits=sum(1 for hit in hits if hit.bull_target_2_hit_date is not None),
        bear_target_2_hits=sum(1 for hit in hits if hit.bear_target_2_hit_date is not None),
        failed_tickers=failures,
        hits=hits,
    )
