from __future__ import annotations

import unittest
from unittest.mock import patch
import requests

from src.finviz_target_price_scanner import (
    FINVIZ_TARGET_PRICE_SCANNER_FILTERS,
    FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID,
    TARGET_PRICE_UPSIDE_RATIO,
    run_finviz_target_price_scanner,
)


class _FakeScreener(list):
    def __init__(
        self,
        *,
        tickers=None,
        filters=None,
        rows=None,
        table: str,
        order: str,
        custom=None,
        request_method=None,
    ) -> None:
        self.tickers = list(tickers or [])
        self.filters = list(filters or [])
        self.table = table
        self.order = order
        self.custom = list(custom or [])
        self.request_method = request_method
        if table == "Overview":
            data = [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Price": "100"},
                {"Ticker": "PLTR", "Company": "Palantir", "Price": "100"},
                {"Ticker": "APP", "Company": "AppLovin", "Price": "50"},
            ]
        else:
            data = [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Price": "100", "Target Price": "160"},
                {"Ticker": "PLTR", "Company": "Palantir", "Price": "100", "Target Price": "140"},
                {"Ticker": "APP", "Company": "AppLovin", "Price": "50", "Target Price": "75"},
            ]
        if self.tickers:
            ticker_set = {item.upper() for item in self.tickers}
            data = [row for row in data if str(row.get("Ticker") or "").upper() in ticker_set]
        self.total_rows = len(data)
        super().__init__(data[:rows] if rows is not None else data)

    def get_ticker_details(self):
        return list(self)


class _AsyncFailingFakeScreener(_FakeScreener):
    def __init__(self, *, tickers=None, filters=None, rows=None, table: str, order: str, custom=None, request_method=None) -> None:
        if request_method == "async":
            raise RuntimeError("async fetch failed")
        super().__init__(
            tickers=tickers,
            filters=filters,
            rows=rows,
            table=table,
            order=order,
            custom=custom,
            request_method=request_method,
        )


class _RateLimitedFakeScreener(_FakeScreener):
    attempts: dict[tuple[str, str], int] = {}

    def __init__(self, *, tickers=None, filters=None, rows=None, table: str, order: str, custom=None, request_method=None) -> None:
        key = (table, request_method or "")
        count = self.attempts.get(key, 0)
        self.attempts[key] = count + 1
        if table == "Custom" and request_method == "async" and count == 0:
            response = requests.Response()
            response.status_code = 429
            response.url = "https://finviz.com/screener?v=111"
            raise requests.exceptions.HTTPError("429 Client Error: Too Many Requests", response=response)
        super().__init__(
            tickers=tickers,
            filters=filters,
            rows=rows,
            table=table,
            order=order,
            custom=custom,
            request_method=request_method,
        )


class _OversizedExchangeFakeScreener(list):
    def __init__(self, *, filters=None, rows=None, table: str, order: str, custom=None, request_method=None) -> None:
        del table, order, custom, request_method
        applied = set(filters or [])
        if not any(item.startswith("exch_") for item in applied):
            response = requests.Response()
            response.status_code = 403
            response.url = "https://finviz.com/screener.ashx?r=1001"
            raise requests.exceptions.HTTPError("403 Client Error: Forbidden", response=response)

        exchange = next(item for item in applied if item.startswith("exch_"))
        cap_filter = next((item for item in applied if item.startswith("cap_")), "")
        if exchange == "exch_nasd" and not cap_filter:
            self.total_rows = 1_428
            data = [{"Ticker": "PROBE", "Company": "Probe", "Price": "1", "Target Price": "2"}]
        else:
            self.total_rows = 1
            suffix = cap_filter.removeprefix("cap_") or exchange.removeprefix("exch_")
            ticker = suffix[:5].upper()
            data = [{"Ticker": ticker, "Company": ticker, "Price": "10", "Target Price": "20"}]
        super().__init__(data[:rows] if rows is not None else data)


class FinvizTargetPriceScannerTests(unittest.TestCase):
    def test_run_scanner_filters_by_target_price_upside_and_limit(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_target_price_scanner(limit=1, tickers=["nvda", "pltr", "app"])

        self.assertEqual(payload["strategy_id"], FINVIZ_TARGET_PRICE_SCANNER_STRATEGY_ID)
        self.assertEqual(payload["filters"], list(FINVIZ_TARGET_PRICE_SCANNER_FILTERS))
        self.assertEqual(payload["minimum_upside_ratio"], TARGET_PRICE_UPSIDE_RATIO)
        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:partitioned:async")
        self.assertEqual(payload["requested_tickers"], ["NVDA", "PLTR", "APP"])
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["hits"][0]["ticker"], "NVDA")
        self.assertEqual(payload["hits"][0]["target_price_upside_pct"], 60.0)
        self.assertEqual(payload["total_candidates"], 3)

    def test_run_scanner_uses_direct_filters_when_no_tickers_provided(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:partitioned:async")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual([hit["ticker"] for hit in payload["hits"]], ["NVDA", "APP"])

    def test_run_scanner_falls_back_to_sync_when_async_fetch_fails(self) -> None:
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_AsyncFailingFakeScreener):
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:partitioned:sync")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual([hit["ticker"] for hit in payload["hits"]], ["NVDA", "APP"])

    def test_run_scanner_retries_rate_limited_fetch(self) -> None:
        _RateLimitedFakeScreener.attempts = {}
        with patch("src.finviz_target_price_scanner._load_finviz_screener", return_value=_RateLimitedFakeScreener), patch(
            "src.finviz_target_price_scanner.time.sleep"
        ) as sleep_mock:
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["scan_mode"], "filters")
        self.assertEqual(payload["row_source"], "custom:partitioned:async")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertGreaterEqual(_RateLimitedFakeScreener.attempts[("Custom", "async")], 2)
        sleep_mock.assert_called_once()

    def test_run_scanner_partitions_exchange_larger_than_finviz_page_limit(self) -> None:
        with patch(
            "src.finviz_target_price_scanner._load_finviz_screener",
            return_value=_OversizedExchangeFakeScreener,
        ):
            payload = run_finviz_target_price_scanner()

        self.assertEqual(payload["row_source"], "custom:partitioned:async")
        self.assertEqual(payload["total_candidates"], 8)
        self.assertEqual(payload["returned_candidates"], 8)


if __name__ == "__main__":
    unittest.main()
