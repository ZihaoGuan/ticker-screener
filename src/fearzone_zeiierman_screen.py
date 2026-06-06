from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
import math

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


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


def _wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    weight_sum = float(weights.sum())
    return series.rolling(length).apply(lambda values: float(np.dot(values, weights) / weight_sum), raw=True)


def _rma(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(alpha=1.0 / float(length), adjust=False).mean()


def _hma(series: pd.Series, length: int) -> pd.Series:
    half_length = max(1, int(length) // 2)
    root_length = max(1, int(math.sqrt(int(length))))
    return _wma((2.0 * _wma(series, half_length)) - _wma(series, int(length)), root_length)


def _moving_average(series: pd.Series, length: int, ma_type: str) -> pd.Series:
    normalized = str(ma_type or "WMA").strip().upper()
    if normalized == "SMA":
        return series.rolling(length).mean()
    if normalized == "EMA":
        return series.ewm(span=length, adjust=False).mean()
    if normalized == "HMA":
        return _hma(series, length)
    if normalized == "RMA":
        return _rma(series, length)
    return _wma(series, length)


@dataclass(frozen=True)
class FearzoneZeiiermanHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_age_bars: int
    current_price: float
    signal_close: float
    signal_low: float
    fz1_value: float
    fz1_limit: float
    fz2_value: float
    fz2_limit: float
    high_period: int
    stdev_period: int
    ma_type: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FearzoneZeiiermanScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[FearzoneZeiiermanHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def find_recent_fearzone_zeiierman_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    config: AppConfig,
) -> FearzoneZeiiermanHit | None:
    bars = _normalize_bars_frame(frame)
    high_period = int(config.fearzone_zeiierman_high_period)
    stdev_period = int(config.fearzone_zeiierman_stdev_period)
    recent_signal_window = int(config.fearzone_zeiierman_recent_signal_lookback_days)
    ma_type = str(config.fearzone_zeiierman_ma_type or "WMA").strip().upper()
    minimum_bars = max(high_period + stdev_period + 5, 120)
    if bars.empty or len(bars) < minimum_bars:
        return None

    source = bars[["Open", "High", "Low", "Close"]].mean(axis=1)
    highest_source = source.rolling(high_period).max()
    fz1_value = (highest_source - source) / highest_source.replace(0, np.nan)
    fz1_limit = _moving_average(fz1_value, stdev_period, ma_type) + fz1_value.rolling(stdev_period).std(ddof=0)
    in_fz1 = (fz1_value > fz1_limit).fillna(False)

    fz2_value = _moving_average(source, high_period, ma_type)
    fz2_limit = _moving_average(fz2_value, stdev_period, ma_type) - fz2_value.rolling(stdev_period).std(ddof=0)
    in_fz2 = (fz2_value < fz2_limit).fillna(False)

    fearzone_condition = in_fz1 & in_fz2
    signal_bars = fearzone_condition & ~fearzone_condition.shift(1, fill_value=False)
    recent_signals = signal_bars[signal_bars].tail(max(1, recent_signal_window))
    if recent_signals.empty:
        return None

    signal_date = recent_signals.index[-1]
    signal_position = int(bars.index.get_loc(signal_date))
    signal_age_bars = len(bars) - 1 - signal_position
    if signal_age_bars >= max(1, recent_signal_window):
        return None

    reasons = [
        f"Fearzone condition turned on via {ma_type} average",
        f"FZ1 {fz1_value.iloc[signal_position]:.4f} > limit {fz1_limit.iloc[signal_position]:.4f}",
        f"FZ2 {fz2_value.iloc[signal_position]:.4f} < limit {fz2_limit.iloc[signal_position]:.4f}",
    ]
    if signal_age_bars == 0:
        reasons.append("signal fired on latest bar")
    else:
        reasons.append(f"signal is {signal_age_bars} bar(s) old")

    return FearzoneZeiiermanHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.date().isoformat(),
        signal_age_bars=signal_age_bars,
        current_price=float(bars["Close"].iloc[-1]),
        signal_close=float(bars["Close"].iloc[signal_position]),
        signal_low=float(bars["Low"].iloc[signal_position]),
        fz1_value=float(fz1_value.iloc[signal_position]),
        fz1_limit=float(fz1_limit.iloc[signal_position]),
        fz2_value=float(fz2_value.iloc[signal_position]),
        fz2_limit=float(fz2_limit.iloc[signal_position]),
        high_period=high_period,
        stdev_period=stdev_period,
        ma_type=ma_type,
        reasons=reasons,
    )


def run_fearzone_zeiierman_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> FearzoneZeiiermanScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[FearzoneZeiiermanHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()
    history_days = max(
        int(config.fearzone_zeiierman_high_period) + int(config.fearzone_zeiierman_stdev_period) + 60,
        int(config.fearzone_zeiierman_stdev_period) * 3,
        220,
    )

    print(
        "starting fearzone zeiierman screen: "
        f"total={total_tickers}, "
        f"high_period={config.fearzone_zeiierman_high_period}, "
        f"stdev_period={config.fearzone_zeiierman_stdev_period}, "
        f"ma_type={config.fearzone_zeiierman_ma_type}, "
        f"recent_window={config.fearzone_zeiierman_recent_signal_lookback_days}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=history_days,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        history_lookback_days=history_days,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_fearzone_zeiierman_hit(frame, ticker=ticker, config=config)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no Zeiierman fearzone setup | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(f"[{position}/{total_tickers}] {ticker.symbol} passed | passed={len(hits)}")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished fearzone zeiierman screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return FearzoneZeiiermanScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
