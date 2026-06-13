from __future__ import annotations

from dataclasses import dataclass, field
import datetime as dt
from typing import Any


@dataclass(frozen=True)
class FinvizProbeResult:
    ticker: str
    source_url: str
    status_code: int | None
    final_url: str
    title: str
    body_excerpt: str
    company_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    market_cap_class: str | None = None
    exchange: str | None = None
    metric_pairs: tuple[tuple[str, str], ...] = ()


@dataclass
class FundamentalsSnapshot:
    ticker: str
    as_of_date: dt.date
    sector: str | None
    industry: str | None
    source: str = "finviz"
    source_url: str = ""
    parse_status: str = "ok"
    parse_error: str | None = None
    scraped_at: dt.datetime | None = None
    updated_at: dt.datetime | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None
    forward_pe: float | None = None
    peg_ratio_5y: float | None = None
    price_to_sales: float | None = None
    price_to_book: float | None = None
    price_to_fcf: float | None = None
    profit_margin_pct: float | None = None
    operating_margin_pct: float | None = None
    gross_margin_pct: float | None = None
    roa_pct: float | None = None
    roe_pct: float | None = None
    eps_this_y_pct: float | None = None
    eps_next_y_pct: float | None = None
    eps_next_5y_pct: float | None = None
    sales_qq_pct: float | None = None
    eps_qq_pct: float | None = None
    perf_month_pct: float | None = None
    perf_quarter_pct: float | None = None
    perf_half_pct: float | None = None
    perf_year_pct: float | None = None
    perf_ytd_pct: float | None = None
    volatility_week_pct: float | None = None
    volatility_month_pct: float | None = None

    def to_record(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class SectorMetricBaseline:
    as_of_date: dt.date
    sector: str
    metric_name: str
    sample_size: int
    filtered_sample_size: int
    median_value: float | None
    pct10_value: float | None
    pct90_value: float | None
    std_value: float | None
    std_step_value: float | None


@dataclass
class RatingSnapshot:
    ticker: str
    as_of_date: dt.date
    sector: str | None
    valuation_score: float | None = None
    profitability_score: float | None = None
    growth_score: float | None = None
    performance_score: float | None = None
    overall_rating: float | None = None
    valuation_grade: str | None = None
    profitability_grade: str | None = None
    growth_grade: str | None = None
    performance_grade: str | None = None
    rating_status: str = "ok"
    rating_status_reason: str | None = None
    missing_metric_names: list[str] = field(default_factory=list)
    insufficient_baseline_metrics: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return dict(self.__dict__)
