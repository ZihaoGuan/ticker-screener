from __future__ import annotations

import datetime as dt
import unittest

from src.ratings.finviz_api import parse_finviz_stock_data, snapshot_needs_fallback


class FinvizApiParserTests(unittest.TestCase):
    def test_parse_stock_data_maps_required_rating_metrics(self) -> None:
        snapshot = parse_finviz_stock_data(
            {
                "Ticker": "NVDA",
                "Sector": "Technology",
                "Industry": "Semiconductors",
                "Forward P/E": "16.44",
                "PEG": "0.36",
                "P/S": "19.59",
                "P/B": "25.42",
                "P/FCF": "46.81",
                "Profit Marg": "62.97%",
                "Oper. Marg": "64.02%",
                "Gross Marg": "74.15%",
                "ROA": "82.97%",
                "ROE": "114.29%",
                "EPS this Y": "87.17%",
                "EPS growth next Y": "39.83%",
                "EPS next 5Y": "45.51%",
                "Sales Q/Q": "85.23%",
                "EPS Q/Q": "213.42%",
                "Perf Month": "-9.14%",
                "Perf Quart": "13.84%",
                "Perf Half Y": "11.65%",
                "Perf Year": "41.51%",
                "Perf YTD": "10.02%",
                "Volatility (Week)": "3.34%",
                "Volatility (Month)": "3.58%",
            },
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
        )

        self.assertEqual(snapshot.eps_next_y_pct, 39.83)
        self.assertEqual(snapshot.profit_margin_pct, 62.97)
        self.assertEqual(snapshot.operating_margin_pct, 64.02)
        self.assertEqual(snapshot.gross_margin_pct, 74.15)
        self.assertEqual(snapshot.perf_quarter_pct, 13.84)
        self.assertEqual(snapshot.volatility_month_pct, 3.58)
        self.assertFalse(snapshot_needs_fallback(snapshot))

    def test_missing_required_metric_triggers_fallback(self) -> None:
        snapshot = parse_finviz_stock_data(
            {"Ticker": "NVDA", "Sector": "Technology", "Industry": "Semiconductors"},
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
        )

        self.assertTrue(snapshot_needs_fallback(snapshot))


if __name__ == "__main__":
    unittest.main()
