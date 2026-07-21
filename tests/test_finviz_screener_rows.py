from __future__ import annotations

import unittest

from src.finviz_screener_rows import normalize_finviz_ticker, sanitize_finviz_company_name


class FinvizScreenerRowsTests(unittest.TestCase):
    def test_sanitize_company_name_keeps_real_company_names(self) -> None:
        row = {"Ticker": "PLTR", "Company": "Palantir"}

        self.assertEqual(normalize_finviz_ticker(row), "PLTR")
        self.assertEqual(sanitize_finviz_company_name(row, ticker="PLTR"), "Palantir")

    def test_sanitize_company_name_drops_adjacent_ticker_values(self) -> None:
        row = {"Ticker": "S", "Company": "SGHC"}

        self.assertEqual(normalize_finviz_ticker(row), "S")
        self.assertEqual(sanitize_finviz_company_name(row, ticker="S"), "")


if __name__ == "__main__":
    unittest.main()
