from __future__ import annotations

from collections.abc import Mapping

RATING_STATUS_OK = "ok"
RATING_STATUS_MISSING_SECTOR = "missing_sector"
RATING_STATUS_MISSING_METRICS = "missing_metrics"
RATING_STATUS_INSUFFICIENT_SECTOR_PEERS = "insufficient_sector_peers"
RATING_STATUS_SCRAPE_FAILED = "scrape_failed"
TECHNICAL_RATING_STATUS_MISSING_METRICS = "missing_metrics"

VALUATION_METRICS = (
    "forward_pe",
    "peg_ratio_5y",
    "price_to_sales",
    "price_to_book",
    "price_to_fcf",
)
PROFITABILITY_METRICS = (
    "profit_margin_pct",
    "operating_margin_pct",
    "gross_margin_pct",
    "roe_pct",
    "roa_pct",
)
GROWTH_METRICS = (
    "eps_this_y_pct",
    "eps_next_y_pct",
    "eps_next_5y_pct",
    "sales_qq_pct",
    "eps_qq_pct",
)
PERFORMANCE_METRICS = (
    "perf_month_pct",
    "perf_quarter_pct",
    "perf_half_pct",
    "perf_year_pct",
    "perf_ytd_pct",
    "volatility_month_pct",
)

CATEGORY_METRICS: Mapping[str, tuple[str, ...]] = {
    "valuation": VALUATION_METRICS,
    "profitability": PROFITABILITY_METRICS,
    "growth": GROWTH_METRICS,
    "performance": PERFORMANCE_METRICS,
}

ALL_RATING_METRICS = (
    *VALUATION_METRICS,
    *PROFITABILITY_METRICS,
    *GROWTH_METRICS,
    *PERFORMANCE_METRICS,
)

LESS_IS_BETTER_METRICS = {
    "forward_pe",
    "peg_ratio_5y",
    "price_to_sales",
    "price_to_book",
    "price_to_fcf",
    "volatility_month_pct",
}

GRADE_SCORES: Mapping[str, float] = {
    "A+": 4.3,
    "A": 4.0,
    "A-": 3.7,
    "B+": 3.3,
    "B": 3.0,
    "B-": 2.7,
    "C+": 2.3,
    "C": 2.0,
    "C-": 1.7,
    "D+": 1.3,
    "D": 1.0,
    "D-": 0.7,
    "F": 0.0,
}

GRADE_ORDER = tuple(GRADE_SCORES.keys())

METRIC_LABEL_TO_FIELD: Mapping[str, str] = {
    "Market Cap": "market_cap",
    "Enterprise Value": "enterprise_value",
    "Forward P/E": "forward_pe",
    "PEG": "peg_ratio_5y",
    "P/S": "price_to_sales",
    "P/B": "price_to_book",
    "P/FCF": "price_to_fcf",
    "Profit Margin": "profit_margin_pct",
    "Profit Marg": "profit_margin_pct",
    "Oper. Margin": "operating_margin_pct",
    "Oper. Marg": "operating_margin_pct",
    "Gross Margin": "gross_margin_pct",
    "Gross Marg": "gross_margin_pct",
    "ROA": "roa_pct",
    "ROE": "roe_pct",
    "Inst Own": "institutional_ownership_pct",
    "Inst Trans": "institutional_transactions_pct",
    "Insider Own": "insider_ownership_pct",
    "Insider Trans": "insider_transactions_pct",
    "Shs Float": "shares_float",
    "Shs Outstand": "shares_outstanding",
    "EPS this Y": "eps_this_y_pct",
    "EPS next 5Y": "eps_next_5y_pct",
    "Sales Q/Q": "sales_qq_pct",
    "EPS Q/Q": "eps_qq_pct",
    "Perf Month": "perf_month_pct",
    "Perf Quarter": "perf_quarter_pct",
    "Perf Quart": "perf_quarter_pct",
    "Perf Half Y": "perf_half_pct",
    "Perf Year": "perf_year_pct",
    "Perf YTD": "perf_ytd_pct",
}

MIN_SECTOR_PEERS_DEFAULT = 20
MIN_CATEGORY_METRICS_DEFAULT = 1.0

TECHNICAL_REQUIRED_METRICS = (
    "close",
    "atr20",
    "sma20",
    "sma50",
    "sma100",
    "sma200",
    "sma20_5d_ago",
    "sma50_10d_ago",
    "sma100_10d_ago",
    "sma200_20d_ago",
    "sma50_20d_ago",
    "sma200_20d_ago",
    "daily_rs_rating",
    "weekly_rs_rating",
    "high_52w",
    "low_52w",
)

TECHNICAL_INDICATOR_TIMEFRAMES = ("1d", "1w", "1m")

TECHNICAL_INDICATOR_STATUS_OK = "ok"
TECHNICAL_INDICATOR_STATUS_MISSING_METRICS = "missing_metrics"

TECHNICAL_INDICATOR_LABEL_STRONG_SELL = "Strong Sell"
TECHNICAL_INDICATOR_LABEL_SELL = "Sell"
TECHNICAL_INDICATOR_LABEL_NEUTRAL = "Neutral"
TECHNICAL_INDICATOR_LABEL_BUY = "Buy"
TECHNICAL_INDICATOR_LABEL_STRONG_BUY = "Strong Buy"
