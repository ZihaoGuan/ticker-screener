from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


@dataclass(frozen=True)
class FearzoneHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    benchmark_ticker: str
    signal_date: str
    signal_age_bars: int
    current_price: float
    signal_close: float
    signal_high: float
    signal_low: float
    fz1_value: float
    fz1_upper: float
    fz2_value: float
    fz2_lower: float
    ma200: float
    slow_k: float
    impulse_pct: float
    close_in_range_pct: float
    avg_volume_20: float
    avg_dollar_volume_20: float
    trigger_negative_impulse: bool
    trigger_ricochet_zone: bool
    trigger_magic_k1: bool
    above_ma200: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FearzoneScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[FearzoneHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


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


def _compute_slow_k(frame: pd.DataFrame, k_length: int, d_length: int) -> pd.Series:
    lowest_low = frame["Low"].rolling(k_length).min()
    highest_high = frame["High"].rolling(k_length).max()
    range_values = highest_high - lowest_low
    raw_k = pd.Series(np.where(range_values > 0, (frame["Close"] - lowest_low) * 100.0 / range_values, np.nan), index=frame.index)
    fast_k = raw_k.rolling(d_length).mean()
    fast_d = fast_k.rolling(d_length).mean()
    return fast_d


def find_recent_fearzone_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    config: AppConfig,
) -> FearzoneHit | None:
    bars = _normalize_bars_frame(frame)
    high_period = int(config.fearzone_high_period)
    band_period = int(config.fearzone_band_period)
    ma_long_period = int(config.fearzone_ma_long_period)
    impulse_lookback = int(config.fearzone_negative_impulse_lookback_days)
    recent_signal_window = int(config.fearzone_recent_signal_lookback_days)
    if bars.empty or len(bars) < max(high_period, band_period, ma_long_period, impulse_lookback + 5):
        return None

    source = bars[["Open", "High", "Low", "Close"]].mean(axis=1)
    highest_source = source.rolling(high_period).max()
    fz1_value = (highest_source - source) / highest_source.replace(0, np.nan)
    fz1_basis = fz1_value.rolling(band_period).mean()
    fz1_std = fz1_value.rolling(band_period).std(ddof=0)
    fz1_upper = fz1_basis + (fz1_std * float(config.fearzone_band_std_multiplier))
    in_fz1 = fz1_value > fz1_upper

    source_ma = source.rolling(high_period).mean()
    fz2_value = source - source_ma
    fz2_basis = fz2_value.rolling(band_period).mean()
    fz2_std = fz2_value.rolling(band_period).std(ddof=0)
    fz2_lower = fz2_basis - (fz2_std * float(config.fearzone_band_std_multiplier))
    in_fz2 = fz2_value < fz2_lower

    impulse_pct = ((bars["Close"] / bars["Close"].shift(impulse_lookback)) - 1.0) * 100.0
    negative_impulse = impulse_pct < (-abs(float(config.fearzone_negative_impulse_pct)))

    bar_range = bars["High"] - bars["Low"]
    range_floor = bars["Low"] + (bar_range * float(config.fearzone_ricochet_zone_pct))
    close_in_range_pct = np.where(bar_range > 0, ((bars["Close"] - bars["Low"]) / bar_range) * 100.0, np.nan)
    in_ricochet_zone = bars["Close"] < range_floor

    slow_k = _compute_slow_k(frame=bars, k_length=int(config.fearzone_stochastic_k), d_length=int(config.fearzone_stochastic_d))
    magic_k1 = slow_k < float(config.fearzone_magic_k1_threshold)

    ma200 = bars["Close"].rolling(ma_long_period).mean()
    above_ma200 = bars["Close"] > ma200

    buy_signal = in_fz1 & in_fz2 & above_ma200 & (negative_impulse | in_ricochet_zone | magic_k1)
    recent_signals = buy_signal[buy_signal].tail(max(1, recent_signal_window))
    if recent_signals.empty:
        return None

    signal_date = recent_signals.index[-1]
    signal_position = int(bars.index.get_loc(signal_date))
    signal_age_bars = len(bars) - 1 - signal_position
    if signal_age_bars >= max(1, recent_signal_window):
        return None

    latest_close = float(bars["Close"].iloc[-1])
    signal_low = float(bars["Low"].iloc[signal_position])

    signal_triggers: list[str] = []
    if bool(negative_impulse.iloc[signal_position]):
        signal_triggers.append(f"negative impulse {impulse_pct.iloc[signal_position]:.1f}%")
    if bool(in_ricochet_zone.iloc[signal_position]):
        signal_triggers.append(f"ricochet close in bottom {close_in_range_pct[signal_position]:.1f}% of range")
    if bool(magic_k1.iloc[signal_position]):
        signal_triggers.append(f"Magic-K1 {slow_k.iloc[signal_position]:.1f}")

    reasons = [
        f"FZ1 {fz1_value.iloc[signal_position]:.3f} > upper band {fz1_upper.iloc[signal_position]:.3f}",
        f"FZ2 {fz2_value.iloc[signal_position]:.2f} < lower band {fz2_lower.iloc[signal_position]:.2f}",
        f"close above MA200 {ma200.iloc[signal_position]:.2f}",
        " | ".join(signal_triggers) if signal_triggers else "Fearzone panel trigger fired",
    ]
    if signal_age_bars == 0:
        reasons.append("signal fired on latest bar")
    else:
        reasons.append(f"signal is {signal_age_bars} bar(s) old")

    avg_volume_20 = float(bars["Volume"].tail(20).mean())
    avg_dollar_volume_20 = float((bars["Close"] * bars["Volume"]).tail(20).mean())

    return FearzoneHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        benchmark_ticker=benchmark_ticker,
        signal_date=signal_date.date().isoformat(),
        signal_age_bars=signal_age_bars,
        current_price=latest_close,
        signal_close=float(bars["Close"].iloc[signal_position]),
        signal_high=float(bars["High"].iloc[signal_position]),
        signal_low=signal_low,
        fz1_value=float(fz1_value.iloc[signal_position]),
        fz1_upper=float(fz1_upper.iloc[signal_position]),
        fz2_value=float(fz2_value.iloc[signal_position]),
        fz2_lower=float(fz2_lower.iloc[signal_position]),
        ma200=float(ma200.iloc[signal_position]),
        slow_k=float(slow_k.iloc[signal_position]),
        impulse_pct=float(impulse_pct.iloc[signal_position]),
        close_in_range_pct=float(close_in_range_pct[signal_position]),
        avg_volume_20=avg_volume_20,
        avg_dollar_volume_20=avg_dollar_volume_20,
        trigger_negative_impulse=bool(negative_impulse.iloc[signal_position]),
        trigger_ricochet_zone=bool(in_ricochet_zone.iloc[signal_position]),
        trigger_magic_k1=bool(magic_k1.iloc[signal_position]),
        above_ma200=bool(above_ma200.iloc[signal_position]),
        reasons=reasons,
    )


def run_fearzone_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> FearzoneScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[FearzoneHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()
    history_days = max(int(config.fearzone_band_period) + 80, int(config.fearzone_ma_long_period) + 40, 320)

    print(
        "starting fearzone screen: "
        f"total={total_tickers}, "
        f"high_period={config.fearzone_high_period}, "
        f"band_period={config.fearzone_band_period}, "
        f"recent_window={config.fearzone_recent_signal_lookback_days}"
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
                        historyLookbackDays=history_days,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_fearzone_hit(
                        frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                        config=config,
                    )
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no actionable fearzone setup | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"signal {hit.signal_date} age={hit.signal_age_bars} close={hit.current_price:.2f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            item.signal_age_bars,
            -(int(item.trigger_negative_impulse) + int(item.trigger_ricochet_zone) + int(item.trigger_magic_k1)),
            -item.avg_dollar_volume_20,
            item.ticker,
        )
    )

    print(f"finished fearzone screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")

    return FearzoneScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
