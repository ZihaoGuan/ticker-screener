from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any, Protocol, Sequence

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .earnings_growth_screen import YFinanceGrowthClient
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_active_universe_from_db,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    resolve_database_url,
)
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


LIQUID_GROWTH_STRATEGY_ID = "liquid_growth"
LIQUID_GROWTH_HISTORY_DAYS = 320
ONE_YEAR_SESSIONS = 252
MA_SLOPE_SESSIONS = 20
MIN_PRICE = 20.0
MIN_MARKET_CAP = 2_000_000_000.0
MIN_AVG_VOLUME_50 = 500_000.0
MIN_AVG_DOLLAR_VOLUME_50 = 30_000_000.0
MIN_ROE_PCT = 17.0
MIN_GROSS_MARGIN_PCT = 40.0
MIN_OPERATING_MARGIN_PCT = 15.0
MIN_QUARTERLY_GROWTH_PCT = 25.0
MIN_DAILY_RS_RATING = 85.0


class QuarterlyFundamentalsClient(Protocol):
    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class LiquidGrowthSnapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    avg_volume_50: float
    avg_dollar_volume_50: float
    roe_pct: float | None
    gross_margin_pct: float | None
    operating_margin_pct: float | None
    daily_rs_rating: float | None
    quarterly_revenue_yoy_pct: float | None
    quarterly_eps_yoy_pct: float | None
    sma50: float
    sma200: float
    sma50_20_sessions_ago: float
    sma200_20_sessions_ago: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]


@dataclass(frozen=True)
class LiquidGrowthHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float
    avg_volume_50: float
    avg_dollar_volume_50: float
    roe_pct: float
    gross_margin_pct: float | None
    operating_margin_pct: float | None
    daily_rs_rating: float
    quarterly_revenue_yoy_pct: float
    quarterly_eps_yoy_pct: float
    sma50: float
    sma200: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class LiquidGrowthScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[LiquidGrowthHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    available = {str(column).lower(): column for column in frame.columns}
    if "close" not in available or "volume" not in available:
        return pd.DataFrame()
    bars = frame[[available["close"], available["volume"]]].copy()
    bars.columns = ["Close", "Volume"]
    bars = bars.dropna(subset=["Close", "Volume"]).sort_index()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    return bars


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    ).dropna(subset=["Date", "Close", "Volume"]).set_index("Date").sort_index()


def _safe_float(value: object) -> float | None:
    try:
        return float(value) if value not in (None, "", "NA", "n/a") else None
    except (TypeError, ValueError):
        return None


def _parse_date(value: object) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _quarterly_yoy(rows: Sequence[dict[str, Any]], *, key: str, as_of_date: dt.date) -> float | None:
    values: list[float] = []
    ordered = sorted(
        (row for row in rows if isinstance(row, dict) and _parse_date(row.get("date")) is not None),
        key=lambda row: str(row.get("date") or ""),
        reverse=True,
    )
    for row in ordered:
        reported_date = _parse_date(row.get("date"))
        if reported_date is None or reported_date > as_of_date:
            continue
        value = _safe_float(row.get(key))
        if value is None and key == "diluted_eps":
            value = _safe_float(row.get("eps") or row.get("epsActual") or row.get("actualEps"))
        if value is None:
            return None
        values.append(value)
        if len(values) == 5:
            break
    if len(values) < 5 or values[4] == 0:
        return None
    return ((values[0] - values[4]) / abs(values[4])) * 100.0


def evaluate_liquid_growth(
    frame: pd.DataFrame,
    *,
    market_cap: float | None,
    roe_pct: float | None,
    gross_margin_pct: float | None,
    operating_margin_pct: float | None,
    daily_rs_rating: float | None,
    quarterly_income_rows: Sequence[dict[str, Any]],
    as_of_date: dt.date,
) -> LiquidGrowthSnapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < ONE_YEAR_SESSIONS + MA_SLOPE_SESSIONS:
        return None
    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)
    sma50_series = close.rolling(50).mean()
    sma200_series = close.rolling(200).mean()
    sma50 = float(sma50_series.iloc[-1])
    sma200 = float(sma200_series.iloc[-1])
    sma50_prior = float(sma50_series.iloc[-(MA_SLOPE_SESSIONS + 1)])
    sma200_prior = float(sma200_series.iloc[-(MA_SLOPE_SESSIONS + 1)])
    current_price = float(close.iloc[-1])
    avg_volume_50 = float(volume.tail(50).mean())
    avg_dollar_volume_50 = current_price * avg_volume_50
    revenue_yoy = _quarterly_yoy(quarterly_income_rows, key="revenue", as_of_date=as_of_date)
    eps_yoy = _quarterly_yoy(quarterly_income_rows, key="diluted_eps", as_of_date=as_of_date)
    margin_ok = (gross_margin_pct is not None and gross_margin_pct >= MIN_GROSS_MARGIN_PCT) or (
        operating_margin_pct is not None and operating_margin_pct >= MIN_OPERATING_MARGIN_PCT
    )
    criteria = {
        "price_gte_20": current_price >= MIN_PRICE,
        "market_cap_gte_2b": market_cap is not None and market_cap >= MIN_MARKET_CAP,
        "avg_volume_50_gte_500k": avg_volume_50 >= MIN_AVG_VOLUME_50,
        "avg_dollar_volume_50_gte_30m": avg_dollar_volume_50 >= MIN_AVG_DOLLAR_VOLUME_50,
        "roe_gte_17pct": roe_pct is not None and roe_pct >= MIN_ROE_PCT,
        "quality_margin": margin_ok,
        "quarterly_revenue_yoy_gte_25pct": revenue_yoy is not None and revenue_yoy >= MIN_QUARTERLY_GROWTH_PCT,
        "quarterly_eps_yoy_gte_25pct": eps_yoy is not None and eps_yoy >= MIN_QUARTERLY_GROWTH_PCT,
        "price_above_sma50": current_price > sma50,
        "price_above_sma200": current_price > sma200,
        "sma50_rising_20_sessions": sma50 > sma50_prior,
        "sma200_rising_20_sessions": sma200 > sma200_prior,
        "daily_rs_gte_85": daily_rs_rating is not None and daily_rs_rating >= MIN_DAILY_RS_RATING,
    }
    reasons = [
        f"close ${current_price:.2f}; 50/200 SMA ${sma50:.2f}/${sma200:.2f}",
        f"50D volume {avg_volume_50:,.0f}; dollar volume ${avg_dollar_volume_50 / 1_000_000:.1f}M",
        f"market cap ${(market_cap or 0.0) / 1_000_000_000:.1f}B; ROE {(roe_pct if roe_pct is not None else 0.0):.1f}%",
        f"reported quarterly revenue/EPS YoY {(revenue_yoy if revenue_yoy is not None else 0.0):.1f}% / {(eps_yoy if eps_yoy is not None else 0.0):.1f}%",
        f"Daily RS {(daily_rs_rating if daily_rs_rating is not None else 0.0):.0f}; 50/200 SMA rising over {MA_SLOPE_SESSIONS} sessions",
        "Leadership candidate only: use a separate base or Strike Zone signal to time an entry.",
    ]
    return LiquidGrowthSnapshot(
        matched=all(criteria.values()), current_price=current_price, market_cap=market_cap,
        avg_volume_50=avg_volume_50, avg_dollar_volume_50=avg_dollar_volume_50, roe_pct=roe_pct,
        gross_margin_pct=gross_margin_pct, operating_margin_pct=operating_margin_pct,
        daily_rs_rating=daily_rs_rating, quarterly_revenue_yoy_pct=revenue_yoy, quarterly_eps_yoy_pct=eps_yoy,
        sma50=sma50, sma200=sma200, sma50_20_sessions_ago=sma50_prior, sma200_20_sessions_ago=sma200_prior,
        criteria_passed=sum(criteria.values()), criteria_total=len(criteria), criteria=criteria, reasons=reasons,
    )


def find_liquid_growth_hit(
    frame: pd.DataFrame, *, ticker: UniverseTicker, market_cap: float | None, roe_pct: float | None,
    gross_margin_pct: float | None, operating_margin_pct: float | None, daily_rs_rating: float | None,
    quarterly_income_rows: Sequence[dict[str, Any]], signal_date: dt.date,
) -> LiquidGrowthHit | None:
    snapshot = evaluate_liquid_growth(
        frame, market_cap=market_cap, roe_pct=roe_pct, gross_margin_pct=gross_margin_pct,
        operating_margin_pct=operating_margin_pct, daily_rs_rating=daily_rs_rating,
        quarterly_income_rows=quarterly_income_rows, as_of_date=signal_date,
    )
    if snapshot is None or not snapshot.matched or None in (snapshot.market_cap, snapshot.roe_pct, snapshot.daily_rs_rating, snapshot.quarterly_revenue_yoy_pct, snapshot.quarterly_eps_yoy_pct):
        return None
    return LiquidGrowthHit(
        ticker=ticker.symbol, sector=ticker.sector, industry=ticker.industry, exchange=ticker.exchange,
        signal_date=signal_date.isoformat(), current_price=snapshot.current_price, market_cap=snapshot.market_cap,
        avg_volume_50=snapshot.avg_volume_50, avg_dollar_volume_50=snapshot.avg_dollar_volume_50,
        roe_pct=snapshot.roe_pct, gross_margin_pct=snapshot.gross_margin_pct, operating_margin_pct=snapshot.operating_margin_pct,
        daily_rs_rating=snapshot.daily_rs_rating, quarterly_revenue_yoy_pct=snapshot.quarterly_revenue_yoy_pct,
        quarterly_eps_yoy_pct=snapshot.quarterly_eps_yoy_pct, sma50=snapshot.sma50, sma200=snapshot.sma200,
        criteria_passed=snapshot.criteria_passed, criteria_total=snapshot.criteria_total, reasons=snapshot.reasons,
    )


def _passes_snapshot_prefilter(fundamental: dict[str, Any], technical: dict[str, Any]) -> bool:
    return (
        _safe_float(fundamental.get("market_cap")) is not None and float(fundamental["market_cap"]) >= MIN_MARKET_CAP
        and _safe_float(fundamental.get("roe_pct")) is not None and float(fundamental["roe_pct"]) >= MIN_ROE_PCT
        and (( _safe_float(fundamental.get("gross_margin_pct")) or float("-inf")) >= MIN_GROSS_MARGIN_PCT
             or ((_safe_float(fundamental.get("operating_margin_pct")) or float("-inf")) >= MIN_OPERATING_MARGIN_PCT))
        and _safe_float(technical.get("daily_rs_rating")) is not None and float(technical["daily_rs_rating"]) >= MIN_DAILY_RS_RATING
    )


def run_liquid_growth_screen(
    config: AppConfig, tickers: list[UniverseTicker], *, as_of_date: dt.date | None = None,
    database_url: str | None = None, quarterly_client: QuarterlyFundamentalsClient | None = None,
) -> LiquidGrowthScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [item.symbol.upper() for item in tickers]
    frames = load_many_ticker_windows(symbols, run_date, LIQUID_GROWTH_HISTORY_DAYS, database_url=resolved_database_url)
    metadata = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    ratings = RatingsRepository(resolved_database_url)
    fundamentals = ratings.load_latest_fundamentals_snapshots_for_tickers(symbols, as_of_date=run_date)
    technicals = ratings.load_latest_technical_rating_snapshots_for_tickers(symbols, as_of_date=run_date, allow_older_as_of_date=True)
    client = quarterly_client or YFinanceGrowthClient()
    hits: list[LiquidGrowthHit] = []
    failures: list[dict[str, str]] = []
    fallback: list[UniverseTicker] = []

    def screen(ticker: UniverseTicker, frame: pd.DataFrame, source: str) -> None:
        symbol = ticker.symbol.upper()
        fundamental, technical = fundamentals.get(symbol, {}), technicals.get(symbol, {})
        if not _passes_snapshot_prefilter(fundamental, technical):
            return
        rows = client.get_income_statements(symbol, limit=8)
        meta = metadata.get(symbol, {})
        hit = find_liquid_growth_hit(
            frame, ticker=UniverseTicker(symbol=symbol, sector=ticker.sector or meta.get("sector") or fundamental.get("sector"), industry=ticker.industry or meta.get("industry") or fundamental.get("industry"), exchange=ticker.exchange or meta.get("exchange")),
            market_cap=_safe_float(fundamental.get("market_cap")), roe_pct=_safe_float(fundamental.get("roe_pct")),
            gross_margin_pct=_safe_float(fundamental.get("gross_margin_pct")), operating_margin_pct=_safe_float(fundamental.get("operating_margin_pct")),
            daily_rs_rating=_safe_float(technical.get("daily_rs_rating")), quarterly_income_rows=rows, signal_date=run_date,
        )
        if hit is not None:
            hits.append(hit)

    for ticker in tickers:
        frame = frames.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < ONE_YEAR_SESSIONS + MA_SLOPE_SESSIONS:
            fallback.append(ticker)
            continue
        try:
            screen(ticker, frame, "DB")
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
    if fallback:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for ticker in fallback:
                try:
                    screen(ticker, _build_price_frame(cookstock.cookFinancials(ticker.symbol, benchmarkTicker=config.benchmark_ticker, historyLookbackDays=LIQUID_GROWTH_HISTORY_DAYS)), "internet fallback")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
    hits.sort(key=lambda item: (-item.daily_rs_rating, -item.quarterly_revenue_yoy_pct, -item.quarterly_eps_yoy_pct, item.ticker))
    return LiquidGrowthScreenResult(run_date=run_date.isoformat(), total_tickers=len(tickers), passed_tickers=len(hits), failed_tickers=failures, hits=hits)


def load_liquid_growth_universe(*, as_of_date: dt.date | None = None, limit: int | None = None, database_url: str | None = None) -> list[UniverseTicker]:
    return load_active_universe_from_db(as_of_date=as_of_date, limit=limit, database_url=resolve_database_url(database_url))
