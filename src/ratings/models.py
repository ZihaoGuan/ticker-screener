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
    roic_pct: float | None = None
    institutional_ownership_pct: float | None = None
    institutional_transactions_pct: float | None = None
    insider_ownership_pct: float | None = None
    insider_transactions_pct: float | None = None
    shares_float: float | None = None
    shares_outstanding: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None
    debt_to_equity: float | None = None
    lt_debt_to_equity: float | None = None
    eps_next_q: float | None = None
    eps_this_y_pct: float | None = None
    eps_next_y_pct: float | None = None
    eps_next_5y_pct: float | None = None
    sales_qq_pct: float | None = None
    sales_yoy_ttm_pct: float | None = None
    eps_qq_pct: float | None = None
    eps_yoy_ttm_pct: float | None = None
    eps_surprise_pct: float | None = None
    sales_surprise_pct: float | None = None
    analyst_recommendation: float | None = None
    target_price: float | None = None
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


@dataclass
class TechnicalSnapshotInput:
    ticker: str
    as_of_date: dt.date
    close: float | None = None
    atr20: float | None = None
    sma20: float | None = None
    sma50: float | None = None
    sma100: float | None = None
    sma200: float | None = None
    sma20_5d_ago: float | None = None
    sma50_10d_ago: float | None = None
    sma100_10d_ago: float | None = None
    sma50_20d_ago: float | None = None
    sma200_20d_ago: float | None = None
    daily_rs_rating: float | None = None
    weekly_rs_rating: float | None = None
    rs_line: float | None = None
    rs_line_sma50: float | None = None
    rs_line_3m_high: float | None = None
    rs_line_12m_high: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    tr_10d_avg: float | None = None
    tr_20d_avg: float | None = None
    close_above_bar_midpoint_count_10d: int | None = None
    up_down_volume_ratio_20d: float | None = None
    breakout_volume_ratio: float | None = None
    distribution_day_count_20d: int | None = None

    def to_record(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class TechnicalRatingSnapshot:
    ticker: str
    as_of_date: dt.date
    trend_regime_score: float | None = None
    dma_speed_score: float | None = None
    divergence_health_score: float | None = None
    leadership_score: float | None = None
    structure_volume_score: float | None = None
    overall_rating: float | None = None
    rating_band: str | None = None
    technical_status: str = "ok"
    technical_status_reason: str | None = None
    flags: list[str] = field(default_factory=list)
    missing_metric_names: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class TechnicalIndicatorRatingSnapshot:
    ticker: str
    as_of_date: dt.date
    timeframe: str
    moving_average_score: float | None = None
    oscillator_score: float | None = None
    overall_score: float | None = None
    rating_label: str | None = None
    technical_status: str = "ok"
    technical_status_reason: str | None = None
    missing_metric_names: list[str] = field(default_factory=list)

    def to_record(self) -> dict[str, Any]:
        return dict(self.__dict__)
