from __future__ import annotations

import argparse
import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

import scripts.run_earnings_trade_analyzer_screen as script


class RunEarningsTradeAnalyzerScreenTests(unittest.TestCase):
    def test_resolve_price_provider_uses_yfinance_when_no_fmp_key(self) -> None:
        args = argparse.Namespace(api_key=None, price_provider="auto")
        with patch.dict("os.environ", {}, clear=False):
            self.assertEqual(script._resolve_price_provider(args), "yfinance")

    def test_resolve_price_provider_prefers_fmp_when_key_present(self) -> None:
        args = argparse.Namespace(api_key=None, price_provider="auto")
        with patch.dict("os.environ", {"FMP_API_KEY": "test-key"}, clear=False):
            self.assertEqual(script._resolve_price_provider(args), "fmp")

    def test_normalize_yfinance_history_returns_descending_bars(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [10.0, 11.0],
                "High": [12.0, 13.0],
                "Low": [9.0, 10.0],
                "Close": [11.0, 12.0],
                "Volume": [1000, 2000],
            },
            index=pd.to_datetime(["2026-06-26", "2026-06-27"]),
        )
        rows = script._normalize_yfinance_history(frame)
        self.assertEqual(rows[0]["date"], "2026-06-27")
        self.assertEqual(rows[1]["date"], "2026-06-26")
        self.assertEqual(rows[0]["close"], 12.0)

    def test_fetch_yfinance_daily_prices_limits_rows(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [10.0, 11.0, 12.0],
                "High": [12.0, 13.0, 14.0],
                "Low": [9.0, 10.0, 11.0],
                "Close": [11.0, 12.0, 13.0],
                "Volume": [1000, 2000, 3000],
            },
            index=pd.to_datetime(["2026-06-25", "2026-06-26", "2026-06-27"]),
        )

        class FakeYF:
            @staticmethod
            def download(**_: object) -> pd.DataFrame:
                return frame

        with patch.object(script, "_load_yfinance", return_value=FakeYF()):
            rows = script._fetch_yfinance_daily_prices("TEST", run_date=dt.date(2026, 6, 27), days=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["date"], "2026-06-27")


if __name__ == "__main__":
    unittest.main()
