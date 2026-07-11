from __future__ import annotations

import requests
import unittest
from unittest.mock import patch

from src.finviz_analyst_recom_scanner import (
    FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS,
    FINVIZ_ANALYST_RECOM_STRONGBUY_STRATEGY_ID,
    run_finviz_analyst_recom_strongbuy_scanner,
)


class _FakeScreener(list):
    def __init__(self, *, filters: list[str], table: str, order: str) -> None:
        self.filters = list(filters)
        self.table = table
        self.order = order
        super().__init__(
            [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Recom": "1.00"},
                {"Ticker": "PLTR", "Company": "Palantir", "Recom": "1.00"},
                {"Ticker": "APP", "Company": "AppLovin", "Recom": "1.00"},
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


class FinvizAnalystRecomScannerTests(unittest.TestCase):
    def test_run_scanner_filters_requested_tickers_and_limit(self) -> None:
        with patch("src.finviz_analyst_recom_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_analyst_recom_strongbuy_scanner(limit=1, tickers=["pltr", "meta"])

        self.assertEqual(payload["strategy_id"], FINVIZ_ANALYST_RECOM_STRONGBUY_STRATEGY_ID)
        self.assertEqual(payload["filters"], list(FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS))
        self.assertEqual(payload["analyst_recom_filter"], "strongbuy")
        self.assertEqual(payload["requested_tickers"], ["PLTR", "META"])
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["hits"][0]["ticker"], "PLTR")
        self.assertEqual(payload["hits"][0]["company_name"], "Palantir")
        self.assertEqual(payload["hits"][0]["source"], "finviz")
        self.assertEqual(payload["hits"][0]["analyst_recom_label"], "Strong Buy (1)")

    def test_filter_tokens_match_expected_user_pasted_selector(self) -> None:
        self.assertEqual(list(FINVIZ_ANALYST_RECOM_STRONGBUY_FILTERS), ["ind_stocksonly", "an_recom_strongbuy"])

    def test_run_scanner_retries_rate_limited_fetch(self) -> None:
        _RateLimitedFakeScreener.attempts = 0
        with patch("src.finviz_analyst_recom_scanner._load_finviz_screener", return_value=_RateLimitedFakeScreener), patch(
            "src.finviz_analyst_recom_scanner.time.sleep"
        ) as sleep_mock:
            payload = run_finviz_analyst_recom_strongbuy_scanner()

        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(_RateLimitedFakeScreener.attempts, 2)
        sleep_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
