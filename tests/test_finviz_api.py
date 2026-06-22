from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

from src.ratings.finviz_api import FinvizApiError, fetch_finviz_api_snapshot, parse_finviz_stock_data, snapshot_needs_fallback


class FinvizApiParserTests(unittest.TestCase):
    def tearDown(self) -> None:
        from src.ratings import finviz_api

        finviz_api._load_finviz_get_stock.cache_clear()
        finviz_api._select_finviz_python.cache_clear()

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
                "Inst Own": "42.50%",
                "Inst Trans": "6.20%",
                "Insider Own": "3.10%",
                "Insider Trans": "-1.40%",
                "Shs Float": "2.41B",
                "Shs Outstand": "2.46B",
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
        self.assertEqual(snapshot.institutional_ownership_pct, 42.5)
        self.assertEqual(snapshot.institutional_transactions_pct, 6.2)
        self.assertEqual(snapshot.insider_ownership_pct, 3.1)
        self.assertEqual(snapshot.insider_transactions_pct, -1.4)
        self.assertEqual(snapshot.shares_float, 2_410_000_000.0)
        self.assertEqual(snapshot.shares_outstanding, 2_460_000_000.0)
        self.assertFalse(snapshot_needs_fallback(snapshot))

    def test_missing_required_metric_does_not_trigger_fallback_when_classification_exists(self) -> None:
        snapshot = parse_finviz_stock_data(
            {"Ticker": "NVDA", "Sector": "Technology", "Industry": "Semiconductors"},
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
        )

        self.assertFalse(snapshot_needs_fallback(snapshot))

    def test_missing_sector_or_industry_triggers_fallback(self) -> None:
        snapshot = parse_finviz_stock_data(
            {"Ticker": "NVDA", "Sector": "Technology"},
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
        )

        self.assertTrue(snapshot_needs_fallback(snapshot))

    def test_fetch_snapshot_uses_in_process_finviz_when_available(self) -> None:
        with patch("src.ratings.finviz_api._load_finviz_get_stock", return_value=lambda ticker: {"Ticker": ticker, "Sector": "Technology", "Forward P/E": "16.44"}), patch(
            "src.ratings.finviz_api._fetch_finviz_api_snapshot_via_subprocess"
        ) as subprocess_fetch:
            snapshot = fetch_finviz_api_snapshot("NVDA", as_of_date=dt.date(2026, 6, 13))

        self.assertEqual(snapshot.ticker, "NVDA")
        self.assertEqual(snapshot.source, "finviz-api")
        self.assertEqual(snapshot.forward_pe, 16.44)
        subprocess_fetch.assert_not_called()

    def test_fetch_snapshot_falls_back_to_subprocess_when_in_process_import_fails(self) -> None:
        with patch("src.ratings.finviz_api._load_finviz_get_stock", side_effect=FinvizApiError("import failed")), patch(
            "src.ratings.finviz_api._fetch_finviz_api_snapshot_via_subprocess",
            return_value={"Ticker": "ARM", "Sector": "Technology", "Forward P/E": "55.10"},
        ) as subprocess_fetch:
            snapshot = fetch_finviz_api_snapshot("ARM", as_of_date=dt.date(2026, 6, 13))

        self.assertEqual(snapshot.ticker, "ARM")
        self.assertEqual(snapshot.forward_pe, 55.10)
        subprocess_fetch.assert_called_once_with("ARM")


if __name__ == "__main__":
    unittest.main()
