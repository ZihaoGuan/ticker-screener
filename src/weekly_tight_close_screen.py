from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


WEEKLY_TIGHT_CLOSE_HISTORY_DAYS = 220
WEEKLY_TIGHT_CLOSE_ATR_PERIOD = 14


@dataclass(frozen=True)
class WeeklyTightCloseHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    first_week_date: str
    second_week_date: str
    current_price: float
    breakout_price: float
    lowest_price: float
    atr_value: float
    threshold_pct: float
    close_spread_pct: float
    high_spread_pct: float
    low_spread_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WeeklyTightCloseScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WeeklyTightCloseHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


@dataclass(frozen=True)
class WeeklyTightCloseBreakoutScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WeeklyTightCloseHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
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


def _to_weekly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    weekly = frame.resample("W-FRI").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    return weekly.dropna(subset=["Open", "High", "Low", "Close"])


def _atr(frame: pd.DataFrame, period: int) -> pd.Series:
    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    close = frame["Close"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(window=period, min_periods=period).mean()


def _within_band(value_a: float, value_b: float, tolerance_ratio: float) -> bool:
    return (value_b * (1.0 - tolerance_ratio)) <= value_a <= (value_b * (1.0 + tolerance_ratio))


def _compute_tight_setup(weekly: pd.DataFrame, atr_series: pd.Series, end_index: int) -> dict[str, float | str] | None:
    if end_index < 2 or end_index >= len(weekly):
        return None
    current = weekly.iloc[end_index]
    prev1 = weekly.iloc[end_index - 1]
    prev2 = weekly.iloc[end_index - 2]
    atr_value = atr_series.iloc[end_index]
    if pd.isna(atr_value) or float(current["Close"]) <= 0:
        return None

    threshold_ratio = float(atr_value) / (float(current["Close"]) * 2.0)

    cond_tight_close = (
        _within_band(float(current["Close"]), float(prev1["Close"]), threshold_ratio)
        and _within_band(float(prev1["Close"]), float(prev2["Close"]), threshold_ratio)
        and _within_band(float(current["Close"]), float(prev2["Close"]), threshold_ratio)
    )
    cond_tight_high = (
        _within_band(float(current["High"]), float(prev1["High"]), threshold_ratio)
        and _within_band(float(prev1["High"]), float(prev2["High"]), threshold_ratio)
    )
    cond_tight_low = (
        _within_band(float(current["Low"]), float(prev1["Low"]), threshold_ratio)
        and _within_band(float(prev1["Low"]), float(prev2["Low"]), threshold_ratio)
    )

    first_open = float(prev2["Open"])
    first_close = float(prev2["Close"])
    first_high = float(prev2["High"])
    first_low = float(prev2["Low"])
    second_range = float(prev1["High"]) - float(prev1["Low"])
    if first_close >= first_open:
        cond_first_candle = ((first_high - first_close) + (first_open - first_low) > 2.0 * (first_close - first_open)) or (
            (first_high - first_low) < second_range
        )
    else:
        cond_first_candle = ((first_high - first_open) + (first_close - first_low) > 2.0 * (first_open - first_close)) or (
            (first_high - first_low) < second_range
        )

    if not (cond_tight_close and (cond_tight_high or cond_tight_low) and cond_first_candle):
        return None

    close_values = [float(prev2["Close"]), float(prev1["Close"]), float(current["Close"])]
    high_values = [float(prev2["High"]), float(prev1["High"]), float(current["High"])]
    low_values = [float(prev2["Low"]), float(prev1["Low"]), float(current["Low"])]
    highest_w = float(max(high_values))
    lowest_w = float(min(low_values))
    close_spread_pct = ((max(close_values) - min(close_values)) / max(max(close_values), 1e-9)) * 100.0
    high_spread_pct = ((max(high_values) - min(high_values)) / max(max(high_values), 1e-9)) * 100.0
    low_spread_pct = ((max(low_values) - min(low_values)) / max(max(low_values), 1e-9)) * 100.0

    return {
        "signal_date": weekly.index[end_index].date().isoformat(),
        "first_week_date": weekly.index[end_index - 2].date().isoformat(),
        "second_week_date": weekly.index[end_index - 1].date().isoformat(),
        "current_price": float(current["Close"]),
        "breakout_price": highest_w,
        "lowest_price": lowest_w,
        "atr_value": float(atr_value),
        "threshold_pct": threshold_ratio * 100.0,
        "close_spread_pct": close_spread_pct,
        "high_spread_pct": high_spread_pct,
        "low_spread_pct": low_spread_pct,
    }


def find_weekly_tight_close_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> WeeklyTightCloseHit | None:
    daily = _normalize_bars_frame(frame)
    if daily.empty:
        return None
    weekly = _to_weekly_frame(daily)
    if len(weekly) < WEEKLY_TIGHT_CLOSE_ATR_PERIOD + 3:
        return None

    atr_series = _atr(weekly, WEEKLY_TIGHT_CLOSE_ATR_PERIOD)
    setup = _compute_tight_setup(weekly, atr_series, len(weekly) - 1)
    if setup is None:
        return None

    reasons = [
        "3 weekly tight closes active now",
        f"ATR14 {float(setup['atr_value']):.2f}, threshold {float(setup['threshold_pct']):.2f}%",
        f"close spread {float(setup['close_spread_pct']):.2f}%, high spread {float(setup['high_spread_pct']):.2f}%, low spread {float(setup['low_spread_pct']):.2f}%",
        "first weekly candle passed wick/range filter",
    ]

    return WeeklyTightCloseHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=str(setup["signal_date"]),
        first_week_date=str(setup["first_week_date"]),
        second_week_date=str(setup["second_week_date"]),
        current_price=float(setup["current_price"]),
        breakout_price=float(setup["breakout_price"]),
        lowest_price=float(setup["lowest_price"]),
        atr_value=float(setup["atr_value"]),
        threshold_pct=float(setup["threshold_pct"]),
        close_spread_pct=float(setup["close_spread_pct"]),
        high_spread_pct=float(setup["high_spread_pct"]),
        low_spread_pct=float(setup["low_spread_pct"]),
        reasons=reasons,
    )


def find_weekly_tight_close_breakout_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> WeeklyTightCloseHit | None:
    daily = _normalize_bars_frame(frame)
    if daily.empty:
        return None
    weekly = _to_weekly_frame(daily)
    if len(weekly) < WEEKLY_TIGHT_CLOSE_ATR_PERIOD + 4:
        return None

    atr_series = _atr(weekly, WEEKLY_TIGHT_CLOSE_ATR_PERIOD)
    setup = _compute_tight_setup(weekly, atr_series, len(weekly) - 2)
    if setup is None:
        return None

    breakout_week = weekly.iloc[-1]
    breakout_price = float(setup["breakout_price"])
    prior_week = weekly.iloc[-2]
    if float(breakout_week["High"]) <= breakout_price:
        return None
    if float(prior_week["High"]) > breakout_price:
        return None

    reasons = [
        "weekly tight close breakout fired",
        f"prior 3-week box high {breakout_price:.2f}, box low {float(setup['lowest_price']):.2f}",
        f"breakout week high {float(breakout_week['High']):.2f}, close {float(breakout_week['Close']):.2f}",
        f"setup threshold was {float(setup['threshold_pct']):.2f}%",
    ]

    return WeeklyTightCloseHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=weekly.index[-1].date().isoformat(),
        first_week_date=str(setup["first_week_date"]),
        second_week_date=str(setup["second_week_date"]),
        current_price=float(breakout_week["Close"]),
        breakout_price=breakout_price,
        lowest_price=float(setup["lowest_price"]),
        atr_value=float(setup["atr_value"]),
        threshold_pct=float(setup["threshold_pct"]),
        close_spread_pct=float(setup["close_spread_pct"]),
        high_spread_pct=float(setup["high_spread_pct"]),
        low_spread_pct=float(setup["low_spread_pct"]),
        reasons=reasons,
    )


def run_weekly_tight_close_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> WeeklyTightCloseScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[WeeklyTightCloseHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting weekly tight close screen: total={total_tickers}, atr_period={WEEKLY_TIGHT_CLOSE_ATR_PERIOD}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=WEEKLY_TIGHT_CLOSE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=WEEKLY_TIGHT_CLOSE_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_weekly_tight_close_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no weekly tight close | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed weekly tight close | threshold={hit.threshold_pct:.2f}% passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished weekly tight close screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return WeeklyTightCloseScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def run_weekly_tight_close_breakout_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> WeeklyTightCloseBreakoutScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[WeeklyTightCloseHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting weekly tight close breakout screen: total={total_tickers}, atr_period={WEEKLY_TIGHT_CLOSE_ATR_PERIOD}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=WEEKLY_TIGHT_CLOSE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=WEEKLY_TIGHT_CLOSE_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_weekly_tight_close_breakout_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no weekly tight close breakout | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed weekly tight close breakout | breakout={hit.breakout_price:.2f} passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished weekly tight close breakout screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return WeeklyTightCloseBreakoutScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
