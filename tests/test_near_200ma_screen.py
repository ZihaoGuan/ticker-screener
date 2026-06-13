from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.near_200ma_screen import run_near_200ma_screen
from src.universe import UniverseTicker


class Near200MaScreenTests(unittest.TestCase):
    def test_run_near_200ma_screen_prefers_trendline_snapshots_when_available(self) -> None:
        ticker = UniverseTicker(symbol="AAPL", sector="Technology", industry="Consumer Electronics", exchange="nasdaq")
        index = pd.date_range("2026-05-18", periods=20, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [101.0 + value for value in range(20)],
                "High": [102.0 + value for value in range(20)],
                "Low": [100.0 + value for value in range(20)],
                "Close": [101.0 + value for value in range(20)],
                "Adj Close": [101.0 + value for value in range(20)],
                "Volume": [1_500_000 for _ in range(20)],
            },
            index=index,
        )

        with patch("src.near_200ma_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.near_200ma_screen.load_latest_trendline_snapshot_map",
            return_value={
                "AAPL": {
                    "ticker": "AAPL",
                    "trade_date": dt.date(2026, 6, 12),
                    "close": 120.0,
                    "daily_sma50": 100.0,
                    "daily_sma200": 123.0,
                }
            },
        ), patch(
            "src.near_200ma_screen.load_many_ticker_windows",
            return_value={"AAPL": frame},
        ), patch("src.near_200ma_screen.load_configured_cookstock") as load_cookstock:
            result = run_near_200ma_screen(AppConfig(), [ticker], as_of_date=dt.date(2026, 6, 12))

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "AAPL")
        self.assertEqual(result.hits[0].case_group, "bull")
        load_cookstock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
