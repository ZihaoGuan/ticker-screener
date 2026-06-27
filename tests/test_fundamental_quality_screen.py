from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.earnings_growth_screen import _extract_annual_fundamentals_from_frame
from src.fundamental_quality_screen import (
    FUNDAMENTAL_QUALITY_FILTERS,
    compute_diluted_eps_1y_growth_pct,
    compute_revenue_3y_cagr_pct,
    run_fundamental_quality_screen,
)


class _FakeAnnualClient:
    def __init__(self, rows_by_ticker: dict[str, list[dict[str, object]]]) -> None:
        self.rows_by_ticker = rows_by_ticker

    def get_annual_fundamentals(self, ticker: str, limit: int = 4) -> list[dict[str, object]]:
        return list(self.rows_by_ticker.get(ticker, []))[:limit]


class _FakeScreener(list):
    def __init__(self, *, filters: list[str], table: str, order: str) -> None:
        self.filters = filters
        self.table = table
        self.order = order
        super().__init__(
            [
                {
                    "Ticker": "NVDA",
                    "Company": "NVIDIA",
                    "Sector": "Technology",
                    "Industry": "Semiconductors",
                    "Market Cap": "3.5T",
                    "ROE": "114.2%",
                    "Oper. Margin": "64.0%",
                    "Gross Margin": "74.1%",
                },
                {
                    "Ticker": "SLOW",
                    "Company": "Slow Co",
                    "Sector": "Technology",
                    "Industry": "Software",
                    "Market Cap": "20B",
                    "ROE": "18.0%",
                    "Oper. Margin": "18.0%",
                    "Gross Margin": "50.0%",
                },
            ]
        )


class FundamentalQualityScreenTests(unittest.TestCase):
    def test_extract_annual_fundamentals_from_frame_keeps_revenue_and_diluted_eps(self) -> None:
        frame = pd.DataFrame(
            {
                pd.Timestamp("2025-12-31"): [120.0, 3.2],
                pd.Timestamp("2024-12-31"): [90.0, 2.0],
                pd.Timestamp("2023-12-31"): [70.0, 1.6],
                pd.Timestamp("2022-12-31"): [50.0, 1.1],
            },
            index=["Total Revenue", "Diluted EPS"],
        )

        rows = _extract_annual_fundamentals_from_frame(frame, limit=4)

        self.assertEqual(rows[0]["date"], "2025-12-31")
        self.assertEqual(rows[0]["revenue"], 120.0)
        self.assertEqual(rows[0]["diluted_eps"], 3.2)
        self.assertEqual(len(rows), 4)

    def test_compute_growth_metrics_from_annual_rows(self) -> None:
        rows = [
            {"date": "2025-12-31", "revenue": 120.0, "diluted_eps": 3.2},
            {"date": "2024-12-31", "revenue": 90.0, "diluted_eps": 2.0},
            {"date": "2023-12-31", "revenue": 70.0, "diluted_eps": 1.6},
            {"date": "2022-12-31", "revenue": 50.0, "diluted_eps": 1.1},
        ]

        revenue_cagr = compute_revenue_3y_cagr_pct(rows)
        eps_growth = compute_diluted_eps_1y_growth_pct(rows)

        self.assertAlmostEqual(revenue_cagr or 0.0, 33.8869, places=3)
        self.assertAlmostEqual(eps_growth or 0.0, 60.0, places=3)

    def test_run_screen_prefilters_and_applies_local_growth_checks(self) -> None:
        primary_client = _FakeAnnualClient(
            {
                "NVDA": [
                    {"date": "2025-12-31", "revenue": 120.0, "diluted_eps": 3.2},
                    {"date": "2024-12-31", "revenue": 90.0, "diluted_eps": 2.0},
                    {"date": "2023-12-31", "revenue": 70.0, "diluted_eps": 1.6},
                    {"date": "2022-12-31", "revenue": 50.0, "diluted_eps": 1.1},
                ],
                "SLOW": [
                    {"date": "2025-12-31", "revenue": 120.0, "diluted_eps": 1.2},
                    {"date": "2024-12-31", "revenue": 110.0, "diluted_eps": 1.0},
                    {"date": "2023-12-31", "revenue": 105.0, "diluted_eps": 0.9},
                    {"date": "2022-12-31", "revenue": 100.0, "diluted_eps": 0.8},
                ],
            }
        )

        with patch("src.fundamental_quality_screen._load_finviz_screener", return_value=_FakeScreener):
            result = run_fundamental_quality_screen(
                as_of_date=dt.date(2026, 6, 28),
                primary_client=primary_client,
                fallback_client=None,
            )

        self.assertEqual(result.prefilter_source, "finviz.screener")
        self.assertEqual(result.filters, list(FUNDAMENTAL_QUALITY_FILTERS))
        self.assertEqual(result.total_prefilter_candidates, 2)
        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertGreater(result.hits[0].revenue_3y_cagr_pct, 20.0)
        self.assertGreater(result.hits[0].diluted_eps_1y_growth_pct, 30.0)


if __name__ == "__main__":
    unittest.main()
