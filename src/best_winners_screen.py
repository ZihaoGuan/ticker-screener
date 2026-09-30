from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker


BEST_WINNERS_STRATEGY_ID = "best_winners"
BEST_WINNERS_HISTORY_DAYS = 380
ONE_YEAR_SESSIONS = 252
SIX_MONTH_SESSIONS = 126
THREE_MONTH_SESSIONS = 63
ADR_PERIOD_DAYS = 20
AVG_VOLUME_PERIOD_DAYS = 30
MIN_PRICE = 1.0
MIN_ADR_PCT = 4.5
MIN_DISTANCE_FROM_52W_LOW_PCT = 70.0
MIN_AVG_DOLLAR_VOLUME_30 = 50_000_000.0
MIN_SESSION_DOLLAR_VOLUME = 20_000_000.0


@dataclass(frozen=True)
class BestWinnersSnapshot:
    matched: bool
    current_price: float
    adr_pct_20: float
    low_52wk: float
    distance_from_52wk_low_pct: float
    return_3m_pct: float
    return_6m_pct: float
    return_1y_pct: float
    avg_volume_30: float
    avg_dollar_volume_30: float
    session_volume: float
    session_dollar_volume: float
    ema8: float
    ema21: float
    ema60: float
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]


@dataclass(frozen=True)
class BestWinnersHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    adr_pct_20: float
    low_52wk: float
    distance_from_52wk_low_pct: float
    return_3m_pct: float
    return_6m_pct: float
    return_1y_pct: float
    avg_volume_30: float
    avg_dollar_volume_30: float
    session_volume: float
    session_dollar_volume: float
    ema8: float
    ema21: float
    ema60: float
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BestWinnersScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[BestWinnersHit]

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
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _period_return_pct(close: pd.Series, sessions: int) -> float:
    prior = float(close.iloc[-(sessions + 1)])
    return ((float(close.iloc[-1]) / prior) - 1.0) * 100.0 if prior > 0 else 0.0


def evaluate_best_winners(frame: pd.DataFrame) -> BestWinnersSnapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < ONE_YEAR_SESSIONS + 1:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])
    session_volume = float(volume.iloc[-1])
    avg_volume_30 = float(volume.tail(AVG_VOLUME_PERIOD_DAYS).mean())
    avg_dollar_volume_30 = current_price * avg_volume_30
    session_dollar_volume = current_price * session_volume
    adr_pct_20 = float((((high - low) / close) * 100.0).rolling(ADR_PERIOD_DAYS).mean().iloc[-1])
    low_52wk = float(low.tail(ONE_YEAR_SESSIONS).min())
    distance_from_52wk_low_pct = ((current_price / low_52wk) - 1.0) * 100.0 if low_52wk > 0 else 0.0
    return_3m_pct = _period_return_pct(close, THREE_MONTH_SESSIONS)
    return_6m_pct = _period_return_pct(close, SIX_MONTH_SESSIONS)
    return_1y_pct = _period_return_pct(close, ONE_YEAR_SESSIONS)
    ema8 = float(close.ewm(span=8, adjust=False).mean().iloc[-1])
    ema21 = float(close.ewm(span=21, adjust=False).mean().iloc[-1])
    ema60 = float(close.ewm(span=60, adjust=False).mean().iloc[-1])

    if any(
        pd.isna(value)
        for value in (adr_pct_20, avg_volume_30, ema8, ema21, ema60)
    ):
        return None

    criteria = {
        "price_gt_1": current_price > MIN_PRICE,
        "adr20_gt_4_5pct": adr_pct_20 > MIN_ADR_PCT,
        "price_at_least_70pct_above_52w_low": distance_from_52wk_low_pct >= MIN_DISTANCE_FROM_52W_LOW_PCT,
        "return_3m_gt_0": return_3m_pct > 0.0,
        "avg_dollar_volume_30_gt_50m": avg_dollar_volume_30 > MIN_AVG_DOLLAR_VOLUME_30,
        "session_dollar_volume_gt_20m": session_dollar_volume > MIN_SESSION_DOLLAR_VOLUME,
        "ema8_gt_ema21": ema8 > ema21,
        "price_gt_ema60": current_price > ema60,
        "return_6m_gt_0": return_6m_pct > 0.0,
        "return_1y_gt_0": return_1y_pct > 0.0,
    }
    reasons = [
        f"close ${current_price:.2f} above $1",
        f"ADR{ADR_PERIOD_DAYS} {adr_pct_20:.2f}% above 4.5%",
        f"close is {distance_from_52wk_low_pct:.1f}% above 52-week low ${low_52wk:.2f}",
        f"3M / 6M / 1Y returns {return_3m_pct:.1f}% / {return_6m_pct:.1f}% / {return_1y_pct:.1f}%",
        f"close x 30D average volume ${avg_dollar_volume_30 / 1_000_000:.1f}M above $50M",
        f"close x session volume ${session_dollar_volume / 1_000_000:.1f}M above $20M",
        f"8 EMA ${ema8:.2f} above 21 EMA ${ema21:.2f}",
        f"close ${current_price:.2f} above 60 EMA ${ema60:.2f}",
    ]
    return BestWinnersSnapshot(
        matched=all(criteria.values()),
        current_price=current_price,
        adr_pct_20=adr_pct_20,
        low_52wk=low_52wk,
        distance_from_52wk_low_pct=distance_from_52wk_low_pct,
        return_3m_pct=return_3m_pct,
        return_6m_pct=return_6m_pct,
        return_1y_pct=return_1y_pct,
        avg_volume_30=avg_volume_30,
        avg_dollar_volume_30=avg_dollar_volume_30,
        session_volume=session_volume,
        session_dollar_volume=session_dollar_volume,
        ema8=ema8,
        ema21=ema21,
        ema60=ema60,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
    )


def find_best_winners_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    signal_date: dt.date,
) -> BestWinnersHit | None:
    snapshot = evaluate_best_winners(frame)
    if snapshot is None or not snapshot.matched:
        return None
    return BestWinnersHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        adr_pct_20=snapshot.adr_pct_20,
        low_52wk=snapshot.low_52wk,
        distance_from_52wk_low_pct=snapshot.distance_from_52wk_low_pct,
        return_3m_pct=snapshot.return_3m_pct,
        return_6m_pct=snapshot.return_6m_pct,
        return_1y_pct=snapshot.return_1y_pct,
        avg_volume_30=snapshot.avg_volume_30,
        avg_dollar_volume_30=snapshot.avg_dollar_volume_30,
        session_volume=snapshot.session_volume,
        session_dollar_volume=snapshot.session_dollar_volume,
        ema8=snapshot.ema8,
        ema21=snapshot.ema21,
        ema60=snapshot.ema60,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def run_best_winners_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> BestWinnersScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol.upper() for ticker in tickers]
    frame_map = load_many_ticker_windows(
        symbols,
        run_date,
        BEST_WINNERS_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    hits: list[BestWinnersHit] = []
    failures: list[dict[str, str]] = []
    fallback_tickers: list[UniverseTicker] = []

    for ticker in tickers:
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < ONE_YEAR_SESSIONS + 1:
            fallback_tickers.append(ticker)
            continue
        try:
            hit = find_best_winners_hit(frame, ticker=ticker, signal_date=run_date)
            if hit is not None:
                hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for ticker in fallback_tickers:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=BEST_WINNERS_HISTORY_DAYS,
                    )
                    hit = find_best_winners_hit(
                        _build_price_frame(financials),
                        ticker=ticker,
                        signal_date=run_date,
                    )
                    if hit is not None:
                        hits.append(hit)
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(
        key=lambda item: (
            -item.return_1y_pct,
            -item.return_6m_pct,
            -item.avg_dollar_volume_30,
            item.ticker,
        )
    )
    return BestWinnersScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
