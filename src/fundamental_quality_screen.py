from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from pathlib import Path
import sys
from typing import Any, Protocol, Sequence

from .earnings_growth_screen import AKShareGrowthClient, YFinanceGrowthClient
from .ratings.finviz_parser import _coerce_number


FUNDAMENTAL_QUALITY_FILTERS: tuple[str, ...] = (
    "cap_midover",
    "fa_roe_o15",
    "fa_opermargin_o15",
    "fa_grossmargin_o40",
    "ind_stocksonly",
)


class AnnualFundamentalsClient(Protocol):
    def get_annual_fundamentals(self, ticker: str, limit: int = 4) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class FundamentalQualityHit:
    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    market_cap: float | None
    roe_pct: float | None
    operating_margin_pct: float | None
    gross_margin_pct: float | None
    revenue_3y_cagr_pct: float
    diluted_eps_1y_growth_pct: float
    annual_fundamentals_source: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class FundamentalQualityScreenResult:
    run_date: str
    prefilter_source: str
    annual_fundamentals_provider: str
    total_prefilter_candidates: int
    evaluated_candidates: int
    passed_tickers: int
    filters: list[str]
    failed_tickers: list[dict[str, str]]
    hits: list[FundamentalQualityHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "prefilter_source": self.prefilter_source,
            "annual_fundamentals_provider": self.annual_fundamentals_provider,
            "total_prefilter_candidates": self.total_prefilter_candidates,
            "evaluated_candidates": self.evaluated_candidates,
            "passed_tickers": self.passed_tickers,
            "filters": list(self.filters),
            "failed_tickers": list(self.failed_tickers),
            "hits": [item.to_dict() for item in self.hits],
        }


def _load_finviz_screener() -> type[Any]:
    try:
        from finviz.screener import Screener
    except ImportError:
        project_root = Path(__file__).resolve().parents[1]
        vendored_root = project_root / "finviz"
        if vendored_root.exists():
            vendored_path = str(vendored_root)
            if vendored_path not in sys.path:
                sys.path.insert(0, vendored_path)
            sys.modules.pop("finviz", None)
            try:
                from finviz.screener import Screener
            except ImportError as exc:
                raise RuntimeError(
                    "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running screen."
                ) from exc
        else:
            raise RuntimeError(
                "finviz dependency missing. Install requirements-finviz.txt or requirements.txt before running screen."
            )
    return Screener


def _normalize_ticker_list(tickers: Sequence[str] | None) -> set[str]:
    normalized: set[str] = set()
    for item in tickers or ():
        ticker = str(item or "").strip().upper()
        if ticker:
            normalized.add(ticker)
    return normalized


def _parse_metric(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        parsed = _coerce_number(str(value))
        if parsed is not None:
            return parsed
    return None


def _sorted_annual_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date_text = str(row.get("date") or "").strip()
        if not date_text:
            continue
        normalized.append(row)
    normalized.sort(key=lambda item: str(item.get("date") or ""), reverse=True)
    return normalized


def compute_revenue_3y_cagr_pct(rows: list[dict[str, Any]]) -> float | None:
    ordered = _sorted_annual_rows(rows)
    if len(ordered) < 4:
        return None
    ending_revenue = _parse_metric(ordered[0], "revenue")
    starting_revenue = _parse_metric(ordered[3], "revenue")
    if ending_revenue is None or starting_revenue in (None, 0) or ending_revenue <= 0 or starting_revenue <= 0:
        return None
    return (((ending_revenue / starting_revenue) ** (1.0 / 3.0)) - 1.0) * 100.0


def compute_diluted_eps_1y_growth_pct(rows: list[dict[str, Any]]) -> float | None:
    ordered = _sorted_annual_rows(rows)
    if len(ordered) < 2:
        return None
    current_eps = _parse_metric(ordered[0], "diluted_eps")
    prior_eps = _parse_metric(ordered[1], "diluted_eps")
    if current_eps is None or prior_eps in (None, 0):
        return None
    return ((current_eps - prior_eps) / abs(prior_eps)) * 100.0


def evaluate_fundamental_quality_ticker(
    ticker: str,
    *,
    as_of_date: dt.date,
) -> FundamentalQualityHit | None:
    result = run_fundamental_quality_screen(
        as_of_date=as_of_date,
        limit=1,
        tickers=[ticker],
    )
    hits = list(result.hits)
    return hits[0] if hits else None


def _normalize_hit(row: dict[str, Any], *, revenue_3y_cagr_pct: float, diluted_eps_1y_growth_pct: float, provider: str) -> FundamentalQualityHit:
    ticker = str(row.get("Ticker") or "").strip().upper()
    return FundamentalQualityHit(
        ticker=ticker,
        company_name=str(row.get("Company") or "").strip() or None,
        sector=str(row.get("Sector") or "").strip() or None,
        industry=str(row.get("Industry") or "").strip() or None,
        market_cap=_parse_metric(row, "Market Cap"),
        roe_pct=_parse_metric(row, "ROE"),
        operating_margin_pct=_parse_metric(row, "Oper. Margin", "Oper. Marg"),
        gross_margin_pct=_parse_metric(row, "Gross Margin", "Gross Marg"),
        revenue_3y_cagr_pct=revenue_3y_cagr_pct,
        diluted_eps_1y_growth_pct=diluted_eps_1y_growth_pct,
        annual_fundamentals_source=provider,
    )


def _prefilter_rows(*, limit: int | None, tickers: Sequence[str] | None) -> list[dict[str, Any]]:
    screener_cls = _load_finviz_screener()
    screener = screener_cls(filters=list(FUNDAMENTAL_QUALITY_FILTERS), table="Financial", order="-marketcap")
    requested_tickers = _normalize_ticker_list(tickers)
    rows: list[dict[str, Any]] = []
    for raw in screener:
        row = dict(raw)
        ticker = str(row.get("Ticker") or "").strip().upper()
        if requested_tickers and ticker not in requested_tickers:
            continue
        rows.append(row)
        if limit is not None and len(rows) >= limit:
            break
    return rows


def _load_annual_rows(
    ticker: str,
    *,
    primary_client: AnnualFundamentalsClient,
    fallback_client: AnnualFundamentalsClient | None,
) -> tuple[list[dict[str, Any]], str]:
    primary_rows = primary_client.get_annual_fundamentals(ticker, limit=4)
    provider = "yfinance"
    revenue_cagr = compute_revenue_3y_cagr_pct(primary_rows)
    eps_growth = compute_diluted_eps_1y_growth_pct(primary_rows)
    if revenue_cagr is not None and eps_growth is not None:
        return primary_rows, provider

    if fallback_client is None:
        return primary_rows, provider

    fallback_rows = fallback_client.get_annual_fundamentals(ticker, limit=4)
    fallback_revenue_cagr = compute_revenue_3y_cagr_pct(fallback_rows)
    if fallback_revenue_cagr is None:
        return primary_rows, provider

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
    merged_rows = list(merged_by_date.values())
    return merged_rows, "yfinance+akshare"


def run_fundamental_quality_screen(
    *,
    as_of_date: dt.date | None = None,
    limit: int | None = None,
    tickers: Sequence[str] | None = None,
    prefilter_rows: list[dict[str, Any]] | None = None,
    primary_client: AnnualFundamentalsClient | None = None,
    fallback_client: AnnualFundamentalsClient | None | object = ...,
) -> FundamentalQualityScreenResult:
    as_of_date = as_of_date or dt.date.today()
    rows = list(prefilter_rows) if prefilter_rows is not None else _prefilter_rows(limit=limit, tickers=tickers)

    yfinance_client = primary_client or YFinanceGrowthClient()
    if fallback_client is ...:
        try:
            akshare_client: AnnualFundamentalsClient | None = AKShareGrowthClient()
        except Exception:
            akshare_client = None
    else:
        akshare_client = fallback_client  # type: ignore[assignment]

    failed: list[dict[str, str]] = []
    hits: list[FundamentalQualityHit] = []

    for row in rows:
        ticker = str(row.get("Ticker") or "").strip().upper()
        if not ticker:
            continue
        try:
            annual_rows, provider = _load_annual_rows(
                ticker,
                primary_client=yfinance_client,
                fallback_client=akshare_client,
            )
            revenue_cagr = compute_revenue_3y_cagr_pct(annual_rows)
            diluted_eps_growth = compute_diluted_eps_1y_growth_pct(annual_rows)
            if revenue_cagr is None:
                failed.append({"ticker": ticker, "reason": "missing_revenue_3y_cagr_inputs"})
                continue
            if diluted_eps_growth is None:
                failed.append({"ticker": ticker, "reason": "missing_diluted_eps_1y_growth_inputs"})
                continue
            if diluted_eps_growth <= 30.0:
                continue
            if revenue_cagr <= 20.0:
                continue
            hits.append(
                _normalize_hit(
                    row,
                    revenue_3y_cagr_pct=revenue_cagr,
                    diluted_eps_1y_growth_pct=diluted_eps_growth,
                    provider=provider,
                )
            )
        except Exception as exc:
            failed.append({"ticker": ticker, "reason": str(exc)})

    hits.sort(
        key=lambda item: (item.revenue_3y_cagr_pct, item.diluted_eps_1y_growth_pct, item.market_cap or 0.0),
        reverse=True,
    )
    return FundamentalQualityScreenResult(
        run_date=as_of_date.isoformat(),
        prefilter_source="finviz.screener",
        annual_fundamentals_provider="yfinance+akshare-fallback" if akshare_client is not None else "yfinance",
        total_prefilter_candidates=len(rows),
        evaluated_candidates=len(rows),
        passed_tickers=len(hits),
        filters=list(FUNDAMENTAL_QUALITY_FILTERS),
        failed_tickers=failed,
        hits=hits,
    )
