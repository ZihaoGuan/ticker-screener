from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker


MA_PULLBACK_RETEST_STRATEGY_ID = "ma_pullback_retest"
MA_PULLBACK_RETEST_HISTORY_DAYS = 1_300
SIGNAL_LOOKBACK_DAYS = 3
MAX_READY_DISTANCE_ATR = 2.0


@dataclass(frozen=True)
class PullbackProfile:
    id: str
    label: str
    timeframe: str
    ma_type: str
    period: int
    tolerance_atr: float


PULLBACK_PROFILES: tuple[PullbackProfile, ...] = (
    PullbackProfile("daily_ema8", "D EMA8", "daily", "ema", 8, 0.35),
    PullbackProfile("daily_ema21", "D EMA21", "daily", "ema", 21, 0.50),
    PullbackProfile("daily_ema200", "D EMA200", "daily", "ema", 200, 1.00),
    PullbackProfile("daily_sma50", "D SMA50", "daily", "sma", 50, 0.75),
    PullbackProfile("daily_sma120", "D SMA120", "daily", "sma", 120, 0.90),
    PullbackProfile("daily_sma200", "D SMA200", "daily", "sma", 200, 1.00),
    PullbackProfile("weekly_ema8", "W EMA8", "weekly", "ema", 8, 0.60),
    PullbackProfile("weekly_ema200", "W EMA200", "weekly", "ema", 200, 1.00),
)


@dataclass(frozen=True)
class MaPullbackRetestHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_state: str
    current_price: float
    support_price: float
    atr14: float
    distance_atr: float
    matched_profiles: list[str]
    active_profiles: list[str]
    ready_profiles: list[str]
    stop_price: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MaPullbackRetestResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[MaPullbackRetestHit]

    def to_dict(self) -> dict[str, object]:
        return {"run_date": self.run_date, "total_tickers": self.total_tickers, "passed_tickers": self.passed_tickers, "failed_tickers": self.failed_tickers, "hits": [item.to_dict() for item in self.hits]}


def _bars(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("Open", "High", "Low", "Close", "Volume")
    columns = {str(column).lower(): column for column in frame.columns}
    if any(name.lower() not in columns for name in required):
        return pd.DataFrame()
    result = frame[[columns[name.lower()] for name in required]].copy()
    result.columns = required
    result = result.dropna(subset=required).sort_index()
    if not isinstance(result.index, pd.DatetimeIndex):
        result.index = pd.to_datetime(result.index)
    return result


def _atr14(bars: pd.DataFrame) -> pd.Series:
    previous_close = bars["Close"].shift(1)
    true_range = pd.concat([bars["High"] - bars["Low"], (bars["High"] - previous_close).abs(), (bars["Low"] - previous_close).abs()], axis=1).max(axis=1)
    return true_range.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()


def _weekly_bars(bars: pd.DataFrame) -> pd.DataFrame:
    return bars.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()


def _moving_average(series: pd.Series, profile: PullbackProfile) -> pd.Series:
    return series.ewm(span=profile.period, adjust=False).mean() if profile.ma_type == "ema" else series.rolling(profile.period).mean()


def _profile_signal(bars: pd.DataFrame, profile: PullbackProfile, atr14: pd.Series) -> tuple[str, float, float] | None:
    source = bars if profile.timeframe == "daily" else _weekly_bars(bars)
    if len(source) < profile.period + 5:
        return None
    support = _moving_average(source["Close"].astype(float), profile)
    support_price = float(support.iloc[-1])
    if pd.isna(support_price):
        return None
    slope_lookback = 3 if profile.timeframe == "weekly" else 5
    prior_support = support.iloc[-1 - slope_lookback]
    if pd.isna(prior_support) or support_price <= float(prior_support):
        return None
    close = float(bars["Close"].iloc[-1])
    low = float(bars["Low"].iloc[-1])
    open_ = float(bars["Open"].iloc[-1])
    atr = float(atr14.iloc[-1])
    if atr <= 0:
        return None
    distance_atr = (close - support_price) / atr
    if distance_atr > MAX_READY_DISTANCE_ATR:
        return None
    recent_low = float(bars["Low"].tail(SIGNAL_LOOKBACK_DAYS).min())
    touched = recent_low <= support_price + profile.tolerance_atr * atr
    held = close >= support_price - profile.tolerance_atr * atr
    if not touched or not held:
        return None
    reclaimed = low <= support_price + profile.tolerance_atr * atr and close > support_price and close > open_ and close >= float(bars["Close"].iloc[-2])
    return ("active" if reclaimed else "ready", support_price, distance_atr)


def find_ma_pullback_retest_hit(frame: pd.DataFrame, *, ticker: UniverseTicker) -> MaPullbackRetestHit | None:
    bars = _bars(frame)
    if len(bars) < 220:
        return None
    atr14 = _atr14(bars)
    if pd.isna(atr14.iloc[-1]):
        return None
    matches: list[tuple[PullbackProfile, str, float, float]] = []
    for profile in PULLBACK_PROFILES:
        signal = _profile_signal(bars, profile, atr14)
        if signal is not None:
            matches.append((profile, *signal))
    if not matches:
        return None
    active = [profile.label for profile, state, _, _ in matches if state == "active"]
    ready = [profile.label for profile, state, _, _ in matches if state == "ready"]
    profile, _, support, distance = min(matches, key=lambda item: abs(item[3]))
    current = float(bars["Close"].iloc[-1])
    stop = min(float(bars["Low"].tail(SIGNAL_LOOKBACK_DAYS).min()), support - float(atr14.iloc[-1]))
    labels = [item[0].label for item in matches]
    state = "active" if active else "ready"
    return MaPullbackRetestHit(
        ticker=ticker.symbol, sector=ticker.sector, industry=ticker.industry, exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(), signal_state=state, current_price=current,
        support_price=support, atr14=float(atr14.iloc[-1]), distance_atr=distance, matched_profiles=labels,
        active_profiles=active, ready_profiles=ready, stop_price=stop,
        reasons=[f"{state.title()} pullback/retest: {', '.join(labels)}", f"Nearest support {profile.label} {support:.2f}; {distance:+.2f} ATR", f"Signal is within the last {SIGNAL_LOOKBACK_DAYS} sessions"],
    )


def run_ma_pullback_retest_screen(config: AppConfig, tickers: list[UniverseTicker], *, as_of_date: dt.date | None = None, database_url: str | None = None) -> MaPullbackRetestResult:
    del config
    run_date = as_of_date or dt.date.today()
    database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol.upper() for ticker in tickers]
    frames = load_many_ticker_windows(symbols, run_date, MA_PULLBACK_RETEST_HISTORY_DAYS, database_url=database_url)
    metadata = load_ticker_metadata_map(symbols, database_url=database_url)
    hits: list[MaPullbackRetestHit] = []
    failures: list[dict[str, str]] = []
    for ticker in tickers:
        symbol = ticker.symbol.upper()
        frame = frames.get(symbol)
        if frame is None or not db_frame_has_recent_coverage(frame, run_date):
            continue
        meta = metadata.get(symbol, {})
        runtime_ticker = UniverseTicker(symbol=symbol, sector=ticker.sector or meta.get("sector"), industry=ticker.industry or meta.get("industry"), exchange=ticker.exchange or meta.get("exchange"))
        try:
            hit = find_ma_pullback_retest_hit(frame, ticker=runtime_ticker)
            if hit:
                hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": symbol, "error": str(exc)})
    hits.sort(key=lambda item: (item.signal_state != "active", abs(item.distance_atr), item.ticker))
    return MaPullbackRetestResult(run_date.isoformat(), len(tickers), len(hits), failures, hits)
