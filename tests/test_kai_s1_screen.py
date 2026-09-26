from __future__ import annotations

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.kai_s1_screen import evaluate_kai_s1, run_kai_s1_screen
from src.universe import UniverseTicker


def _frame(*, volume: float = 1_000_000.0) -> pd.DataFrame:
    index = pd.bdate_range("2025-01-02", periods=260)
    close = pd.Series(np.linspace(40.0, 180.0, len(index)), index=index)
    return pd.DataFrame({"High": close * 1.025, "Low": close * 0.975, "Close": close, "Volume": volume}, index=index)


class KaiS1ScreenTests(unittest.TestCase):
    def test_matches_each_screenshot_filter(self) -> None:
        snapshot = evaluate_kai_s1(_frame(), market_cap=11_000_000_000.0)

        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertEqual(snapshot.criteria_passed, 8)
        self.assertGreater(snapshot.current_price, snapshot.weekly_sma30)
        self.assertGreater(snapshot.current_price, snapshot.weekly_sma40)
        self.assertGreater(snapshot.weekly_sma30, snapshot.weekly_sma40)
        self.assertGreater(snapshot.current_price, 5.0)
        self.assertGreater(snapshot.market_cap or 0.0, 10_000_000_000.0)
        self.assertGreater(snapshot.current_price, snapshot.sma50)
        self.assertGreater(snapshot.dollar_volume, 100_000_000.0)
        self.assertGreater(snapshot.adr_pct_20, 2.0)

    def test_requires_strictly_more_than_ten_billion_market_cap(self) -> None:
        snapshot = evaluate_kai_s1(_frame(), market_cap=10_000_000_000.0)

        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["market_cap_gt_10b"])
        self.assertTrue(all(passed for key, passed in snapshot.criteria.items() if key != "market_cap_gt_10b"))

    def test_run_uses_db_data_and_market_cap(self) -> None:
        frame = _frame()
        ticker = UniverseTicker(symbol="NVDA", sector="Technology")

        with patch("src.kai_s1_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.kai_s1_screen.load_many_ticker_windows", return_value={"NVDA": frame}
        ), patch(
            "src.kai_s1_screen.load_ticker_metadata_map", return_value={"NVDA": {"industry": "Semiconductors", "exchange": "NASDAQ"}}
        ), patch(
            "src.kai_s1_screen.load_latest_market_caps", return_value={"NVDA": 11_000_000_000.0}
        ), patch("src.kai_s1_screen.load_configured_cookstock") as load_cookstock:
            result = run_kai_s1_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertEqual(result.hits[0].industry, "Semiconductors")
        load_cookstock.assert_not_called()
