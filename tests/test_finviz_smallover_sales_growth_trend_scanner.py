from __future__ import annotations

import requests
import unittest
from unittest.mock import patch

from src.finviz_smallover_sales_growth_trend_scanner import (
    FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS,
    FINVIZ_SMALLOVER_SALES_GROWTH_TREND_STRATEGY_ID,
    MIN_DAILY_RS_RATING,
    run_finviz_smallover_sales_growth_trend_scanner,
)


class _FakeScreener(list):
    def __init__(self, *, filters: list[str], table: str, order: str) -> None:
        self.filters = list(filters)
        self.table = table
        self.order = order
        super().__init__(
            [
                {"Ticker": "NVDA", "Company": "NVIDIA"},
                {"Ticker": "PLTR", "Company": "Palantir"},
                {"Ticker": "APP", "Company": "AppLovin"},
            ]
        )


class _RateLimitedFakeScreener(_FakeScreener):
    attempts = 0

    def __init__(self, *, filters: list[str], table: str, order: str) -> None:
        type(self).attempts += 1
        if type(self).attempts == 1:
            response = requests.Response()
            response.status_code = 429
            response.url = "https://finviz.com/screener?v=111"
            raise requests.exceptions.HTTPError("429 Client Error: Too Many Requests", response=response)
        super().__init__(filters=filters, table=table, order=order)


class FinvizSmalloverSalesGrowthTrendScannerTests(unittest.TestCase):
    def test_run_scanner_filters_requested_tickers_and_limit(self) -> None:
        rs_snapshots = {
            "NVDA": {"daily_rs_rating": 98.0},
            "PLTR": {"daily_rs_rating": 82.0},
            "APP": {"daily_rs_rating": 74.0},
        }
        with patch("src.finviz_smallover_sales_growth_trend_scanner._load_finviz_screener", return_value=_FakeScreener), patch(
            "src.finviz_smallover_sales_growth_trend_scanner.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value=rs_snapshots,
        ):
            payload = run_finviz_smallover_sales_growth_trend_scanner(limit=1, tickers=["pltr", "meta"])

        self.assertEqual(payload["strategy_id"], FINVIZ_SMALLOVER_SALES_GROWTH_TREND_STRATEGY_ID)
        self.assertEqual(payload["filters"], list(FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS))
        self.assertEqual(payload["min_daily_rs_rating"], float(MIN_DAILY_RS_RATING))
        self.assertEqual(payload["requested_tickers"], ["PLTR", "META"])
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["hits"][0]["ticker"], "PLTR")
        self.assertEqual(payload["hits"][0]["company_name"], "Palantir")
        self.assertEqual(payload["hits"][0]["source"], "finviz")
        self.assertEqual(payload["hits"][0]["finviz_filter_set"], "smallover_sales_growth_trend")
        self.assertEqual(payload["hits"][0]["daily_rs_rating"], 82.0)
        self.assertEqual(payload["hits"][0]["min_daily_rs_rating"], float(MIN_DAILY_RS_RATING))

    def test_filter_tokens_match_expected_user_pasted_selector(self) -> None:
        self.assertEqual(
            list(FINVIZ_SMALLOVER_SALES_GROWTH_TREND_FILTERS),
            [
                "cap_smallover",
                "fa_salesqoq_o5",
                "sh_curvol_o50",
                "sh_instown_o10",
                "sh_price_o20",
                "ta_highlow52w_b0to10h",
                "ta_sma200_sb50",
                "ta_sma50_pa",
            ],
        )

    def test_run_scanner_retries_rate_limited_fetch(self) -> None:
        _RateLimitedFakeScreener.attempts = 0
        with patch(
            "src.finviz_smallover_sales_growth_trend_scanner._load_finviz_screener",
            return_value=_RateLimitedFakeScreener,
        ), patch(
            "src.finviz_smallover_sales_growth_trend_scanner.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"NVDA": {"daily_rs_rating": 98.0}, "PLTR": {"daily_rs_rating": 82.0}, "APP": {"daily_rs_rating": 79.0}},
        ), patch("src.finviz_smallover_sales_growth_trend_scanner.time.sleep") as sleep_mock:
            payload = run_finviz_smallover_sales_growth_trend_scanner()

        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(_RateLimitedFakeScreener.attempts, 2)
        sleep_mock.assert_called_once()

    def test_run_scanner_drops_tickers_without_required_rs_rating(self) -> None:
        with patch("src.finviz_smallover_sales_growth_trend_scanner._load_finviz_screener", return_value=_FakeScreener), patch(
            "src.finviz_smallover_sales_growth_trend_scanner.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={
                "NVDA": {"daily_rs_rating": 75.0},
                "PLTR": {"daily_rs_rating": 75.1},
                "APP": {},
            },
        ):
            payload = run_finviz_smallover_sales_growth_trend_scanner()

        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual([item["ticker"] for item in payload["hits"]], ["PLTR"])


if __name__ == "__main__":
    unittest.main()
