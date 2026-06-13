from __future__ import annotations

import datetime as dt
import unittest

from src.ratings.calculator import build_ticker_rating
from src.ratings.constants import ALL_RATING_METRICS
from src.ratings.models import FundamentalsSnapshot, SectorMetricBaseline


def _baseline(metric_name: str, *, less_is_better: bool) -> SectorMetricBaseline:
    return SectorMetricBaseline(
        as_of_date=dt.date(2026, 6, 13),
        sector="Technology",
        metric_name=metric_name,
        sample_size=30,
        filtered_sample_size=30,
        median_value=10.0,
        pct10_value=1.0 if less_is_better else 5.0,
        pct90_value=20.0 if less_is_better else 100.0,
        std_value=5.0,
        std_step_value=1.0,
    )


class RatingsCalculatorTests(unittest.TestCase):
    def test_missing_sector_returns_unrated(self) -> None:
        snapshot = FundamentalsSnapshot(ticker="NVDA", as_of_date=dt.date(2026, 6, 13), sector=None, industry="Semiconductors")
        rating = build_ticker_rating(snapshot, {})
        self.assertEqual(rating.rating_status, "missing_sector")
        self.assertIsNone(rating.overall_rating)

    def test_missing_metric_returns_missing_metrics(self) -> None:
        snapshot = FundamentalsSnapshot(
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
            sector="Technology",
            industry="Semiconductors",
        )
        rating = build_ticker_rating(snapshot, {})
        self.assertEqual(rating.rating_status, "missing_metrics")

    def test_complete_inputs_produce_rating(self) -> None:
        snapshot = FundamentalsSnapshot(
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
            sector="Technology",
            industry="Semiconductors",
            forward_pe=0.5,
            peg_ratio_5y=0.5,
            price_to_sales=0.5,
            price_to_book=0.5,
            price_to_fcf=0.5,
            profit_margin_pct=120.0,
            operating_margin_pct=120.0,
            gross_margin_pct=120.0,
            roe_pct=120.0,
            roa_pct=120.0,
            eps_this_y_pct=120.0,
            eps_next_y_pct=120.0,
            eps_next_5y_pct=120.0,
            sales_qq_pct=120.0,
            eps_qq_pct=120.0,
            perf_month_pct=120.0,
            perf_quarter_pct=120.0,
            perf_half_pct=120.0,
            perf_year_pct=120.0,
            perf_ytd_pct=120.0,
            volatility_month_pct=0.5,
        )
        less_is_better = {"forward_pe", "peg_ratio_5y", "price_to_sales", "price_to_book", "price_to_fcf", "volatility_month_pct"}
        baselines = {metric_name: _baseline(metric_name, less_is_better=metric_name in less_is_better) for metric_name in ALL_RATING_METRICS}
        rating = build_ticker_rating(snapshot, baselines)
        self.assertEqual(rating.rating_status, "ok")
        self.assertEqual(rating.valuation_grade, "A+")
        self.assertEqual(rating.profitability_grade, "A+")
        self.assertEqual(rating.growth_grade, "A+")
        self.assertEqual(rating.performance_grade, "A+")
        self.assertEqual(rating.overall_rating, 106.64)


if __name__ == "__main__":
    unittest.main()
