from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any, Protocol, Sequence

from .earnings_growth_screen import AKShareGrowthClient, YFinanceGrowthClient
from .universe import UniverseTicker

ANNUAL_GROWTH_MIN_PCT = 15.0
QUARTERLY_GROWTH_MIN_PCT = 25.0


class AnnualFundamentalsClient(Protocol):
    def get_annual_fundamentals(self, ticker: str, limit: int = 4) -> list[dict[str, Any]]:
        ...


class QuarterlyFundamentalsClient(Protocol):
    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, Any]]:
        ...

    def get_earnings(self, ticker: str, limit: int = 12) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class MinerviniGrowthAccelerationHit:
    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    signal_date: str
    annual_eps_growth_latest_pct: float
    annual_eps_growth_prev_pct: float
    annual_eps_growth_oldest_pct: float
    quarterly_eps_growth_latest_pct: float
    quarterly_eps_growth_prev_pct: float
    annual_revenue_growth_latest_pct: float
    annual_revenue_growth_prev_pct: float
    annual_revenue_growth_oldest_pct: float
    quarterly_revenue_growth_latest_pct: float
    quarterly_revenue_growth_prev_pct: float
    acceleration_score: float
    acceleration_label: str
    screen_pass_count: int
    annual_fundamentals_source: str
    quarterly_fundamentals_source: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MinerviniGrowthAccelerationScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    annual_fundamentals_provider: str
    quarterly_fundamentals_provider: str
    failed_tickers: list[dict[str, str]]
    hits: list[MinerviniGrowthAccelerationHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "annual_fundamentals_provider": self.annual_fundamentals_provider,
            "quarterly_fundamentals_provider": self.quarterly_fundamentals_provider,
            "failed_tickers": list(self.failed_tickers),
            "hits": [item.to_dict() for item in self.hits],
        }


def _safe_float(value: object) -> float | None:
    if value in (None, "", "NA", "n/a", "None"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_date(value: object) -> dt.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _sort_rows_desc(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [row for row in rows if isinstance(row, dict) and _parse_date(row.get("date")) is not None]
    normalized.sort(key=lambda item: str(item.get("date") or ""), reverse=True)
    return normalized


def _growth_pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return ((current - previous) / abs(previous)) * 100.0


def _extract_annual_metric_rows(rows: list[dict[str, Any]], key: str) -> list[float]:
    ordered = _sort_rows_desc(rows)
    values: list[float] = []
    for row in ordered:
        value = _safe_float(row.get(key))
        if value is None:
            return []
        values.append(value)
        if len(values) >= 4:
            break
    return values


def _extract_quarterly_revenue_rows(rows: list[dict[str, Any]], *, as_of_date: dt.date) -> list[float]:
    ordered = _sort_rows_desc(rows)
    values: list[float] = []
    for row in ordered:
        date_value = _parse_date(row.get("date"))
        if date_value is None or date_value > as_of_date:
            continue
        revenue = _safe_float(row.get("revenue"))
        if revenue is None:
            return []
        values.append(revenue)
        if len(values) >= 6:
            break
    return values


def _extract_quarterly_eps_rows(rows: list[dict[str, Any]], *, as_of_date: dt.date) -> list[float]:
    ordered = _sort_rows_desc(rows)
    values: list[float] = []
    for row in ordered:
        date_value = _parse_date(row.get("date"))
        if date_value is None or date_value > as_of_date:
            continue
        eps = _safe_float(row.get("eps") or row.get("epsActual") or row.get("actualEps") or row.get("actualEPS"))
        if eps is None:
            continue
        values.append(eps)
        if len(values) >= 6:
            break
    return values


def _annual_growth_triplet(values: list[float]) -> tuple[float, float, float] | None:
    if len(values) < 4:
        return None
    latest = _growth_pct(values[0], values[1])
    prev = _growth_pct(values[1], values[2])
    oldest = _growth_pct(values[2], values[3])
    if latest is None or prev is None or oldest is None:
        return None
    return latest, prev, oldest


def _quarterly_yoy_pair(values: list[float]) -> tuple[float, float] | None:
    if len(values) < 6:
        return None
    latest = _growth_pct(values[0], values[4])
    prev = _growth_pct(values[1], values[5])
    if latest is None or prev is None:
        return None
    return latest, prev


def _passes_annual_growth(triplet: tuple[float, float, float] | None) -> bool:
    if triplet is None:
        return False
    latest, prev, oldest = triplet
    return latest >= ANNUAL_GROWTH_MIN_PCT and prev >= ANNUAL_GROWTH_MIN_PCT and oldest >= ANNUAL_GROWTH_MIN_PCT and latest > prev


def _passes_quarterly_growth(pair: tuple[float, float] | None) -> bool:
    if pair is None:
        return False
    latest, prev = pair
    return latest >= QUARTERLY_GROWTH_MIN_PCT and prev >= QUARTERLY_GROWTH_MIN_PCT and latest > prev


def _load_annual_rows(
    ticker: str,
    *,
    primary_client: AnnualFundamentalsClient,
    fallback_client: AnnualFundamentalsClient | None,
) -> tuple[list[dict[str, Any]], str]:
    primary_rows = primary_client.get_annual_fundamentals(ticker, limit=4)
    revenue_values = _extract_annual_metric_rows(primary_rows, "revenue")
    eps_values = _extract_annual_metric_rows(primary_rows, "diluted_eps")
    if len(revenue_values) >= 4 and len(eps_values) >= 4:
        return primary_rows, "yfinance"
    if fallback_client is None:
        return primary_rows, "yfinance"
    fallback_rows = fallback_client.get_annual_fundamentals(ticker, limit=4)
    merged_by_date: dict[str, dict[str, Any]] = {}
    for row in fallback_rows:
        date_text = str(row.get("date") or "").strip()
        if date_text:
            merged_by_date[date_text] = dict(row)
    for row in primary_rows:
        date_text = str(row.get("date") or "").strip()
        if not date_text:
            continue
        payload = merged_by_date.setdefault(date_text, {})
        payload.update({key: value for key, value in row.items() if key != "date"})
        payload["date"] = date_text
    return list(merged_by_date.values()), "yfinance+akshare"


def _load_quarterly_revenue_rows(
    ticker: str,
    *,
    primary_client: QuarterlyFundamentalsClient,
    fallback_client: QuarterlyFundamentalsClient | None,
) -> tuple[list[dict[str, Any]], str]:
    primary_rows = primary_client.get_income_statements(ticker, limit=8)
    if len(primary_rows) >= 6:
        return primary_rows, "yfinance"
    if fallback_client is None:
        return primary_rows, "yfinance"
    fallback_rows = fallback_client.get_income_statements(ticker, limit=8)
    return (fallback_rows or primary_rows), ("akshare" if fallback_rows else "yfinance")


def _build_reasons(
    annual_eps: tuple[float, float, float],
    quarterly_eps: tuple[float, float],
    annual_revenue: tuple[float, float, float],
    quarterly_revenue: tuple[float, float],
) -> list[str]:
    return [
        f"Annual EPS growth {annual_eps[2]:.1f}% -> {annual_eps[1]:.1f}% -> {annual_eps[0]:.1f}%",
        f"Quarterly EPS YoY {quarterly_eps[1]:.1f}% -> {quarterly_eps[0]:.1f}%",
        f"Annual revenue growth {annual_revenue[2]:.1f}% -> {annual_revenue[1]:.1f}% -> {annual_revenue[0]:.1f}%",
        f"Quarterly revenue YoY {quarterly_revenue[1]:.1f}% -> {quarterly_revenue[0]:.1f}%",
    ]


def run_minervini_growth_acceleration_screen(
    tickers: Sequence[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    annual_client: AnnualFundamentalsClient | None = None,
    quarterly_client: QuarterlyFundamentalsClient | None = None,
    fallback_client: Any | None | object = ...,
) -> MinerviniGrowthAccelerationScreenResult:
    tickers = list(tickers)
    as_of_date = as_of_date or dt.date.today()
    primary_annual_client = annual_client or YFinanceGrowthClient()
    primary_quarterly_client = quarterly_client or primary_annual_client
    if fallback_client is ...:
        try:
            fallback_provider: Any | None = AKShareGrowthClient()
        except Exception:
            fallback_provider = None
    else:
        fallback_provider = fallback_client

    failed: list[dict[str, str]] = []
    hits: list[MinerviniGrowthAccelerationHit] = []

    for item in tickers:
        ticker = str(getattr(item, "symbol", "") or "").strip().upper()
        if not ticker:
            continue
        try:
            annual_rows, annual_provider = _load_annual_rows(ticker, primary_client=primary_annual_client, fallback_client=fallback_provider)
            quarterly_revenue_rows, quarterly_provider = _load_quarterly_revenue_rows(ticker, primary_client=primary_quarterly_client, fallback_client=fallback_provider)
            earnings_rows = primary_quarterly_client.get_earnings(ticker, limit=12)

            annual_eps_triplet = _annual_growth_triplet(_extract_annual_metric_rows(annual_rows, "diluted_eps"))
            annual_revenue_triplet = _annual_growth_triplet(_extract_annual_metric_rows(annual_rows, "revenue"))
            quarterly_eps_pair = _quarterly_yoy_pair(_extract_quarterly_eps_rows(earnings_rows, as_of_date=as_of_date))
            quarterly_revenue_pair = _quarterly_yoy_pair(_extract_quarterly_revenue_rows(quarterly_revenue_rows, as_of_date=as_of_date))

            pass_count = sum(int(value) for value in (
                _passes_annual_growth(annual_eps_triplet),
                _passes_quarterly_growth(quarterly_eps_pair),
                _passes_annual_growth(annual_revenue_triplet),
                _passes_quarterly_growth(quarterly_revenue_pair),
            ))
            if pass_count < 4:
                continue
            assert annual_eps_triplet is not None and annual_revenue_triplet is not None and quarterly_eps_pair is not None and quarterly_revenue_pair is not None
            hits.append(MinerviniGrowthAccelerationHit(
                ticker=ticker,
                company_name=None,
                sector=getattr(item, "sector", None),
                industry=getattr(item, "industry", None),
                signal_date=as_of_date.isoformat(),
                annual_eps_growth_latest_pct=round(annual_eps_triplet[0], 1),
                annual_eps_growth_prev_pct=round(annual_eps_triplet[1], 1),
                annual_eps_growth_oldest_pct=round(annual_eps_triplet[2], 1),
                quarterly_eps_growth_latest_pct=round(quarterly_eps_pair[0], 1),
                quarterly_eps_growth_prev_pct=round(quarterly_eps_pair[1], 1),
                annual_revenue_growth_latest_pct=round(annual_revenue_triplet[0], 1),
                annual_revenue_growth_prev_pct=round(annual_revenue_triplet[1], 1),
                annual_revenue_growth_oldest_pct=round(annual_revenue_triplet[2], 1),
                quarterly_revenue_growth_latest_pct=round(quarterly_revenue_pair[0], 1),
                quarterly_revenue_growth_prev_pct=round(quarterly_revenue_pair[1], 1),
                acceleration_score=100.0,
                acceleration_label="4/4",
                screen_pass_count=4,
                annual_fundamentals_source=annual_provider,
                quarterly_fundamentals_source=quarterly_provider,
                reasons=_build_reasons(annual_eps_triplet, quarterly_eps_pair, annual_revenue_triplet, quarterly_revenue_pair),
            ))
        except Exception as exc:
            failed.append({"ticker": ticker, "reason": str(exc)})

    hits.sort(key=lambda item: (item.quarterly_eps_growth_latest_pct, item.quarterly_revenue_growth_latest_pct, item.annual_eps_growth_latest_pct, item.annual_revenue_growth_latest_pct, item.ticker), reverse=True)
    return MinerviniGrowthAccelerationScreenResult(
        run_date=as_of_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        annual_fundamentals_provider="yfinance+akshare-fallback" if fallback_provider is not None else "yfinance",
        quarterly_fundamentals_provider="yfinance+akshare-fallback" if fallback_provider is not None else "yfinance",
        failed_tickers=failed,
        hits=hits,
    )
