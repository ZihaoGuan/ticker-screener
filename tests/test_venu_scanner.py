from __future__ import annotations

import unittest
from unittest.mock import patch

from src.venu_scanner import VENU_SCANNER_FILTERS, run_venu_scanner


class _FakeScreener(list):
    def __init__(self, *, filters: list[str], table: str, order: str) -> None:
        self.filters = filters
        self.table = table
        self.order = order
        super().__init__(
            [
                {"Ticker": "NVDA", "Company": "NVIDIA", "Market Cap": "3.50T"},
                {"Ticker": "PLTR", "Company": "Palantir", "Market Cap": "300B"},
                {"Ticker": "APP", "Company": "AppLovin", "Market Cap": "120B"},
            ]
        )


class VenuScannerTests(unittest.TestCase):
    def test_run_venu_scanner_filters_requested_tickers_and_limit(self) -> None:
        with patch("src.venu_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_venu_scanner(limit=1, tickers=["pltr", "meta"])

        self.assertEqual(payload["strategy_id"], "venu_scanner")
        self.assertEqual(payload["filters"], list(VENU_SCANNER_FILTERS))
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["requested_tickers"], ["META", "PLTR"])
        self.assertEqual(payload["hits"][0]["ticker"], "PLTR")
        self.assertEqual(payload["hits"][0]["company_name"], "Palantir")
        self.assertEqual(payload["hits"][0]["source"], "finviz")

    def test_run_venu_scanner_drops_adjacent_ticker_company_values(self) -> None:
        class _ShiftedScreener(list):
            def __init__(self, *, filters: list[str], table: str, order: str) -> None:
                self.filters = filters
                self.table = table
                self.order = order
                super().__init__([{"Ticker": "S", "Company": "SGHC", "Market Cap": "6B"}])

        with patch("src.venu_scanner._load_finviz_screener", return_value=_ShiftedScreener):
            payload = run_venu_scanner()

        self.assertEqual(payload["hits"][0]["ticker"], "S")
        self.assertEqual(payload["hits"][0]["company_name"], "")


if __name__ == "__main__":
    unittest.main()
