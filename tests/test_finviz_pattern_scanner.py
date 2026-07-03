from __future__ import annotations

import unittest
from unittest.mock import patch

from src.finviz_pattern_scanner import (
    FINVIZ_PATTERN_OPTIONS,
    build_finviz_pattern_strategy_id,
    run_finviz_pattern_scanner,
)


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


class FinvizPatternScannerTests(unittest.TestCase):
    def test_run_finviz_pattern_scanner_filters_requested_tickers_and_limit(self) -> None:
        with patch("src.finviz_pattern_scanner._load_finviz_screener", return_value=_FakeScreener):
            payload = run_finviz_pattern_scanner(pattern="horizontal2", limit=1, tickers=["pltr", "meta"])

        self.assertEqual(payload["strategy_id"], build_finviz_pattern_strategy_id("horizontal2"))
        self.assertEqual(payload["pattern"], "horizontal2")
        self.assertEqual(payload["pattern_label"], "Horizontal S/R (Strong)")
        self.assertEqual(payload["finviz_filter"], "ta_pattern_horizontal2")
        self.assertEqual(payload["total_candidates"], 3)
        self.assertEqual(payload["returned_candidates"], 1)
        self.assertEqual(payload["requested_tickers"], ["META", "PLTR"])
        self.assertEqual(payload["hits"][0]["ticker"], "PLTR")
        self.assertEqual(payload["hits"][0]["company_name"], "Palantir")
        self.assertEqual(payload["hits"][0]["source"], "finviz")
        self.assertEqual(payload["hits"][0]["pattern"], "horizontal2")
        self.assertEqual(payload["hits"][0]["strategy_id"], build_finviz_pattern_strategy_id("horizontal2"))

    def test_pattern_options_match_expected_user_pasted_values(self) -> None:
        values = [value for value, _label in FINVIZ_PATTERN_OPTIONS]

        self.assertEqual(
            values,
            [
                "horizontal",
                "horizontal2",
                "tlresistance",
                "tlresistance2",
                "tlsupport",
                "tlsupport2",
                "wedgeup",
                "wedgeup2",
                "wedgedown",
                "wedgedown2",
                "wedgeresistance",
                "wedgeresistance2",
                "wedgesupport",
                "wedgesupport2",
                "wedge",
                "wedge2",
                "channelup",
                "channelup2",
                "channeldown",
                "channeldown2",
                "channel",
                "channel2",
                "doubletop",
                "doublebottom",
                "multipletop",
                "multiplebottom",
                "headandshoulders",
                "headandshouldersinv",
            ],
        )


if __name__ == "__main__":
    unittest.main()
