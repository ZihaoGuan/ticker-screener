from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    resolve_database_url,
)
from .ratings.repository import RatingsRepository
from .trend_template_screen import TrendTemplateSnapshot, evaluate_trend_template
from .universe import UniverseTicker


MINERVINI_VCP_HISTORY_DAYS = 540
MIN_MARKET_CAP = 2_000_000_000.0
DAILY_RANGE_DAYS = 100
DAILY_RESISTANCE_STABILITY_OFFSET = 10
DAILY_HIGH_PROXIMITY_PCT = 7.0
WEEKLY_RANGE_WEEKS = 100
WEEKLY_HIGH_PROXIMITY_PCT = 20.0
VOLUME_SMA_DAYS = 20
VOLUME_LOOKBACK_OFFSETS = (5, 10, 15, 20, 25, 30)


@dataclass(frozen=True)
class MinerviniVcpDetectorSnapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    trend_template: TrendTemplateSnapshot
    daily_100d_high: float
    weekly_100w_high: float
    distance_from_daily_100d_high_pct: float
    distance_from_weekly_100w_high_pct: float
    higher_low_10d: bool
    higher_low_20d: bool
    higher_low_30d: bool
    volume_contracting_comparisons: int
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["trend_template"] = self.trend_template.to_dict()
        return payload


@dataclass(frozen=True)
class MinerviniVcpDetectorHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float
    daily_100d_high: float
    weekly_100w_high: float
    distance_from_daily_100d_high_pct: float
    distance_from_weekly_100w_high_pct: float
    volume_contracting_comparisons: int
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MinerviniVcpDetectorScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[MinerviniVcpDetectorHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("High", "Low", "Close", "Volume")
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    bars = frame[[available[column.lower()] for column in required]].copy()
    bars.columns = list(required)
    bars = bars.dropna(subset=list(required)).sort_index()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    return bars


def _higher_low(low: pd.Series, window: int) -> bool:
    if len(low) < window * 2:
        return False
    current_low = float(low.iloc[-window:].min())
    prior_low = float(low.iloc[-(window * 2) : -window].min())
    return current_low > prior_low


def _safe_float(value: object) -> float | None:
    try:
        return float(value) if value not in (None, "", "NA", "N/A", "n/a") else None
    except (TypeError, ValueError):
        return None


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    ).dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def evaluate_minervini_vcp_detector(
    frame: pd.DataFrame,
    *,
    market_cap: float | None,
) -> MinerviniVcpDetectorSnapshot | None:
    bars = _normalize_price_frame(frame)
    required_daily_bars = DAILY_RANGE_DAYS + DAILY_RESISTANCE_STABILITY_OFFSET + 1
    if bars.empty or len(bars) < max(MINERVINI_VCP_HISTORY_DAYS, required_daily_bars):
        return None

    trend_template = evaluate_trend_template(bars)
    if trend_template is None:
        return None

    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])

    daily_100d_high = float(high.iloc[-(DAILY_RANGE_DAYS + 1) : -1].max())
    daily_100d_high_10d_ago = float(
        high.iloc[-(DAILY_RANGE_DAYS + DAILY_RESISTANCE_STABILITY_OFFSET + 1) : -(DAILY_RESISTANCE_STABILITY_OFFSET + 1)].max()
    )
    weekly = bars.resample("W-FRI").agg({"High": "max", "Close": "last"}).dropna()
    if len(weekly) < WEEKLY_RANGE_WEEKS:
        return None
    weekly_100w_high = float(weekly["High"].astype(float).iloc[-WEEKLY_RANGE_WEEKS:].max())

    distance_from_daily = ((current_price / daily_100d_high) - 1.0) * 100.0 if daily_100d_high > 0 else 0.0
    distance_from_weekly = ((current_price / weekly_100w_high) - 1.0) * 100.0 if weekly_100w_high > 0 else 0.0

    higher_low_10d = _higher_low(low, 10)
    higher_low_20d = _higher_low(low, 20)
    higher_low_30d = _higher_low(low, 30)

    volume_sma20 = volume.rolling(VOLUME_SMA_DAYS).mean()
    latest_completed_volume_sma20 = float(volume_sma20.iloc[-2])
    volume_contracting_comparisons = sum(
        1
        for offset in VOLUME_LOOKBACK_OFFSETS
        if latest_completed_volume_sma20 < float(volume_sma20.iloc[-(offset + 2)])
    )

    criteria = {
        "trend_template_met": trend_template.matched,
        "market_cap_gt_2b": market_cap is not None and market_cap > MIN_MARKET_CAP,
        "daily_100d_resistance_stable_10d": daily_100d_high == daily_100d_high_10d_ago,
        "within_7pct_below_daily_100d_high": -DAILY_HIGH_PROXIMITY_PCT <= distance_from_daily <= 0.0,
        "within_20pct_of_weekly_100w_high": abs(distance_from_weekly) <= WEEKLY_HIGH_PROXIMITY_PCT,
        "price_at_or_below_daily_100d_high": current_price <= daily_100d_high,
        "higher_lows_10_20_30d": higher_low_10d and higher_low_20d and higher_low_30d,
        "volume_contracting": volume_contracting_comparisons >= 1,
    }
    reasons = [
        f"Trend Template {trend_template.criteria_passed}/{trend_template.criteria_total}",
        f"market cap ${(market_cap or 0.0) / 1_000_000_000:.1f}B",
        f"close {distance_from_daily:+.1f}% vs prior 100D high ${daily_100d_high:.2f}",
        f"close {distance_from_weekly:+.1f}% vs 100W high ${weekly_100w_high:.2f}",
        f"higher lows 10D/20D/30D: {higher_low_10d}/{higher_low_20d}/{higher_low_30d}",
        f"20D volume SMA contracting in {volume_contracting_comparisons}/{len(VOLUME_LOOKBACK_OFFSETS)} comparisons",
    ]
    return MinerviniVcpDetectorSnapshot(
        matched=all(criteria.values()),
        current_price=current_price,
        market_cap=market_cap,
        trend_template=trend_template,
        daily_100d_high=daily_100d_high,
        weekly_100w_high=weekly_100w_high,
        distance_from_daily_100d_high_pct=distance_from_daily,
        distance_from_weekly_100w_high_pct=distance_from_weekly,
        higher_low_10d=higher_low_10d,
        higher_low_20d=higher_low_20d,
        higher_low_30d=higher_low_30d,
        volume_contracting_comparisons=volume_contracting_comparisons,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
    )


def find_minervini_vcp_detector_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    market_cap: float | None,
    signal_date: dt.date,
) -> MinerviniVcpDetectorHit | None:
    snapshot = evaluate_minervini_vcp_detector(frame, market_cap=market_cap)
    if snapshot is None or not snapshot.matched or snapshot.market_cap is None:
        return None
    return MinerviniVcpDetectorHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        market_cap=snapshot.market_cap,
        daily_100d_high=snapshot.daily_100d_high,
        weekly_100w_high=snapshot.weekly_100w_high,
        distance_from_daily_100d_high_pct=snapshot.distance_from_daily_100d_high_pct,
        distance_from_weekly_100w_high_pct=snapshot.distance_from_weekly_100w_high_pct,
        volume_contracting_comparisons=snapshot.volume_contracting_comparisons,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        criteria=snapshot.criteria,
        reasons=snapshot.reasons,
    )


def run_minervini_vcp_detector_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> MinerviniVcpDetectorScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [item.symbol.upper() for item in tickers]
    frames = load_many_ticker_windows(
        symbols,
        run_date,
        MINERVINI_VCP_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    metadata = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    fundamentals = RatingsRepository(resolved_database_url).load_latest_fundamentals_snapshots_for_tickers(
        symbols,
        as_of_date=run_date,
    )
    hits: list[MinerviniVcpDetectorHit] = []
    failures: list[dict[str, str]] = []
    fallback: list[UniverseTicker] = []

    def screen(ticker: UniverseTicker, frame: pd.DataFrame) -> None:
        symbol = ticker.symbol.upper()
        meta = metadata.get(symbol, {})
        runtime_ticker = UniverseTicker(
            symbol=symbol,
            sector=ticker.sector or meta.get("sector"),
            industry=ticker.industry or meta.get("industry"),
            exchange=ticker.exchange or meta.get("exchange"),
        )
        market_cap = _safe_float(fundamentals.get(symbol, {}).get("market_cap"))
        hit = find_minervini_vcp_detector_hit(
            frame,
            ticker=runtime_ticker,
            market_cap=market_cap,
            signal_date=run_date,
        )
        if hit is not None:
            hits.append(hit)

    for ticker in tickers:
        frame = frames.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < MINERVINI_VCP_HISTORY_DAYS:
            fallback.append(ticker)
            continue
        try:
            screen(ticker, frame)
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})

    if fallback:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for ticker in fallback:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=MINERVINI_VCP_HISTORY_DAYS,
                    )
                    screen(ticker, _build_price_frame(financials))
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(
        key=lambda item: (
            abs(item.distance_from_daily_100d_high_pct),
            -item.volume_contracting_comparisons,
            -item.market_cap,
            item.ticker,
        )
    )
    return MinerviniVcpDetectorScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
