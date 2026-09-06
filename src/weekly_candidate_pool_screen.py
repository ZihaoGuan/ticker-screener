from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


WEEKLY_CANDIDATE_POOL_STRATEGY_ID = "weekly_candidate_pool"
PRICE_HISTORY_DAYS = 320
ADR_PERIOD_DAYS = 20
EMA_FAST_DAYS = 10
EMA_MID_DAYS = 20
EMA_TREND_DAYS = 50
HIGH_LOW_LOOKBACK_DAYS = 260
MIN_PRICE = 10.0
MIN_ADR_PCT = 4.0
MIN_DAILY_RS_RATING = 90.0
MIN_DISTANCE_FROM_52W_LOW_PCT = 70.0


@dataclass(frozen=True)
class WeeklyCandidatePoolSnapshot:
    matched: bool
    current_price: float
    adr_pct_20: float
    daily_rs_rating: float | None
    ema10: float
    ema20: float
    ema50: float
    low_52wk: float
    distance_from_52wk_low_pct: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]


@dataclass(frozen=True)
class WeeklyCandidatePoolHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    adr_pct_20: float
    daily_rs_rating: float
    ema10: float
    ema20: float
    ema50: float
    low_52wk: float
    distance_from_52wk_low_pct: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WeeklyCandidatePoolScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WeeklyCandidatePoolHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("High", "Low", "Close")
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
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
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close"]).set_index("Date").sort_index()


def evaluate_weekly_candidate_pool(
    frame: pd.DataFrame,
    *,
    daily_rs_rating: float | None,
) -> WeeklyCandidatePoolSnapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < HIGH_LOW_LOOKBACK_DAYS:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    ema10 = close.ewm(span=EMA_FAST_DAYS, adjust=False).mean()
    ema20 = close.ewm(span=EMA_MID_DAYS, adjust=False).mean()
    ema50 = close.ewm(span=EMA_TREND_DAYS, adjust=False).mean()
    adr_pct_20 = (((high - low) / close) * 100.0).rolling(ADR_PERIOD_DAYS).mean()

    latest_close = float(close.iloc[-1])
    latest_ema10 = float(ema10.iloc[-1])
    latest_ema20 = float(ema20.iloc[-1])
    latest_ema50 = float(ema50.iloc[-1])
    latest_adr_pct_20 = float(adr_pct_20.iloc[-1])
    low_52wk = float(low.tail(HIGH_LOW_LOOKBACK_DAYS).min())
    distance_from_52wk_low_pct = ((latest_close / low_52wk) - 1.0) * 100.0 if low_52wk > 0 else 0.0
    normalized_daily_rs = float(daily_rs_rating) if daily_rs_rating is not None else None

    criteria = {
        "price_above_10": latest_close > MIN_PRICE,
        "adr20_above_4pct": latest_adr_pct_20 > MIN_ADR_PCT,
        "daily_rs_above_90": normalized_daily_rs is not None and normalized_daily_rs > MIN_DAILY_RS_RATING,
        "price_above_50ema": latest_close > latest_ema50,
        "ema10_above_ema20": latest_ema10 > latest_ema20,
        "price_70pct_above_52w_low": distance_from_52wk_low_pct >= MIN_DISTANCE_FROM_52W_LOW_PCT,
    }
    reasons = [
        f"close ${latest_close:.2f} above ${MIN_PRICE:.2f}",
        f"ADR{ADR_PERIOD_DAYS} {latest_adr_pct_20:.2f}% above {MIN_ADR_PCT:.2f}%",
        f"Daily RS {normalized_daily_rs:.1f} above {MIN_DAILY_RS_RATING:.0f}" if normalized_daily_rs is not None else "Daily RS rating unavailable",
        f"close ${latest_close:.2f} above 50 EMA ${latest_ema50:.2f}",
        f"10 EMA ${latest_ema10:.2f} above 20 EMA ${latest_ema20:.2f}",
        f"{distance_from_52wk_low_pct:.1f}% above 52-week low ${low_52wk:.2f}",
        "Candidate pool only: review chart structure and wait for a setup before taking action.",
    ]
    return WeeklyCandidatePoolSnapshot(
        matched=all(criteria.values()),
        current_price=latest_close,
        adr_pct_20=latest_adr_pct_20,
        daily_rs_rating=normalized_daily_rs,
        ema10=latest_ema10,
        ema20=latest_ema20,
        ema50=latest_ema50,
        low_52wk=low_52wk,
        distance_from_52wk_low_pct=distance_from_52wk_low_pct,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
    )


def find_weekly_candidate_pool_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    daily_rs_rating: float | None,
    signal_date: dt.date,
) -> WeeklyCandidatePoolHit | None:
    snapshot = evaluate_weekly_candidate_pool(frame, daily_rs_rating=daily_rs_rating)
    if snapshot is None or not snapshot.matched or snapshot.daily_rs_rating is None:
        return None
    return WeeklyCandidatePoolHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        adr_pct_20=snapshot.adr_pct_20,
        daily_rs_rating=snapshot.daily_rs_rating,
        ema10=snapshot.ema10,
        ema20=snapshot.ema20,
        ema50=snapshot.ema50,
        low_52wk=snapshot.low_52wk,
        distance_from_52wk_low_pct=snapshot.distance_from_52wk_low_pct,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def run_weekly_candidate_pool_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str = "",
) -> WeeklyCandidatePoolScreenResult:
    run_date = as_of_date or dt.date.today()
    symbols = [item.symbol.upper() for item in tickers]
    resolved_database_url = resolve_database_url(database_url)
    technical_map = RatingsRepository(resolved_database_url).load_latest_technical_rating_snapshots_for_tickers(
        symbols,
        as_of_date=run_date,
        allow_older_as_of_date=True,
    )
    frame_map = load_many_ticker_windows(symbols, run_date, PRICE_HISTORY_DAYS, database_url=resolved_database_url)
    hits: list[WeeklyCandidatePoolHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < HIGH_LOW_LOOKBACK_DAYS:
            fallback_tickers.append((position, ticker))
            continue
        daily_rs_rating = technical_map.get(ticker.symbol.upper(), {}).get("daily_rs_rating")
        hit = find_weekly_candidate_pool_hit(frame, ticker=ticker, daily_rs_rating=daily_rs_rating, signal_date=run_date)
        if hit is not None:
            hits.append(hit)

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for _position, ticker in fallback_tickers:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=PRICE_HISTORY_DAYS,
                    )
                    daily_rs_rating = technical_map.get(ticker.symbol.upper(), {}).get("daily_rs_rating")
                    hit = find_weekly_candidate_pool_hit(
                        _build_price_frame(financials),
                        ticker=ticker,
                        daily_rs_rating=daily_rs_rating,
                        signal_date=run_date,
                    )
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(key=lambda item: (-item.daily_rs_rating, -item.adr_pct_20, -item.distance_from_52wk_low_pct, item.ticker))
    return WeeklyCandidatePoolScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
