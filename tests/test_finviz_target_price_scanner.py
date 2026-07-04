from __future__ import annotations

import unittest
from unittest.mock import patch

from src.finviz_target_price_scanner import (
    FINVIZ_TARGET_PRICE_SCANNER_FILTERS,
    FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID,
    TARGET_PRICE_UPSIDE_RATIO,
    run_finviz_target_price_scanner,
)


class _FakeScreener(list):
    def __init__(self, *, tickers=None, filters=None, table: str, order: str, custom=None, request_method=None) -> None:
        self.tickers = list(tickers or [])
        self.filters = list(filters or [])
        self.table = table
        self.order = order
        self.custom = list(custom or [])
        self.request_method = request_method
        if table == "Overview":
            rows = [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Price": "100"},
                {"Ticker": "PLTR", "Company": "Palantir", "Price": "100"},
                {"Ticker": "APP", "Company": "AppLovin", "Price": "50"},
            ]
        else:
            rows = [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Price": "100", "Target Price": "160"},
                {"Ticker": "PLTR", "Company": "Palantir", "Price": "100", "Target Price": "140"},
                {"Ticker": "APP", "Company": "AppLovin", "Price": "50", "Target Price": "75"},
            ]
        if self.tickers:
            ticker_set = {item.upper() for item in self.tickers}
            rows = [row for row in rows if str(row.get("Ticker") or "").upper() in ticker_set]
        super().__init__(rows)

    def get_ticker_details(self):
        return list(self)


class _AsyncFailingFakeScreener(_FakeScreener):
    def __init__(self, *, tickers=None, filters=None, table: str, order: str, custom=None, request_method=None) -> None:
        if request_method == "async":
            raise RuntimeError("async fetch failed")
        super().__init__(
            tickers=tickers,
            filters=filters,
            table=table,
            order=order,
            custom=custom,
            request_method=request_method,
        )


class FinvizTargetPriceScannerTests(unittest.TestCase):
    def test_run_scanner_filters_by_target_price_upside_and_limit(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_target_price_scanner(limit=1, tickers=["nvda", "pltr", "app"])

        self.assertEqual(payload["strategy_id"], FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID)
        self.assertEqual(payload["filters"], list(FINVIZ_TARGET_PRICE_SCANNER_FILTERS))
        self.assertEqual(payload["minimum_upside_ratio"], TARGET_PRICE_UPSIDE_RATIO)
        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:async")
        self.assertEqual(payload["requested_tickers"], ["NVDA", "PLTR", "APP"])
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["hits"][0]["ticker"], "NVDA")
        self.assertEqual(payload["hits"][0]["target_price_upside_pct"], 60.0)
        self.assertEqual(payload["total_candidates"], 3)

    def test_run_scanner_uses_direct_filters_when_no_tickers_provided(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:async")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual([hit["ticker"] for hit in payload["hits"]], ["NVDA", "APP"])

    def test_run_scanner_falls_back_to_sync_when_async_fetch_fails(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_AsyncFailingFakeScreener):
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:sync")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual([hit["ticker"] for hit in payload["hits"]], ["NVDA", "APP"])


if __name__ == "__main__":
    unittest.main()
