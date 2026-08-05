from __future__ import annotations

import datetime as dt
import math
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.kai_s2_screen import evaluate_kai_s2, run_kai_s2_screen
from src.universe import UniverseTicker


def _close_series(*, start: float, periods: int, alpha: float = 1.0) -> list[float]:
    closes = [start]
    for idx in range(1, periods):
        benchmark_return = 0.001 + (0.004 * math.sin(idx / 7.0))
        stock_return = 0.0015 + (alpha * (benchmark_return - 0.001))
        closes.append(closes[-1] * (1.0 + stock_return))
    return closes


def _price_frame(closes: list[float], *, volume: float = 3_000_000.0) -> pd.DataFrame:
    index = pd.date_range("2025-01-02", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "Open": [value - 0.5 for value in closes],
            "High": [value + 1.0 for value in closes],
            "Low": [value - 1.0 for value in closes],
            "Close": closes,
            "Adj Close": closes,
            "Volume": [volume for _ in closes],
        },
        index=index,
    )


def _kai_s2_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    benchmark = _price_frame(_close_series(start=100.0, periods=320, alpha=1.0), volume=10_000_000.0)
    stock = _price_frame(_close_series(start=50.0, periods=320, alpha=1.6), volume=3_000_000.0)
    return stock, benchmark


class KaiS2ScreenTests(unittest.TestCase):
    def test_evaluate_kai_s2_matches_literal_filters(self) -> None:
        stock, benchmark = _kai_s2_frames()

        snapshot = evaluate_kai_s2(stock, benchmark, market_cap=1_500_000_000.0)

        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertEqual(snapshot.criteria_passed, snapshot.criteria_total)
        self.assertGreater(snapshot.current_price, 5.0)
        self.assertGreater(snapshot.market_cap or 0.0, 1_000_000_000.0)
        self.assertGreater(snapshot.sma150, snapshot.sma200)
        self.assertGreater(snapshot.current_price, snapshot.sma200)
        self.assertGreater(snapshot.avg_volume_30, 2_000_000.0)
        self.assertGreater(snapshot.price_times_avg_volume_30, 100_000_000.0)
        self.assertGreater(snapshot.beta_1y or 0.0, 1.0)
        self.assertGreater(snapshot.current_price, snapshot.sma50)
        self.assertGreater(snapshot.current_price, snapshot.sma20)

    def test_evaluate_kai_s2_requires_market_cap(self) -> None:
        stock, benchmark = _kai_s2_frames()

        snapshot = evaluate_kai_s2(stock, benchmark, market_cap=None)

        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["market_cap_gt_1b"])
        self.assertTrue(all(passed for key, passed in snapshot.criteria.items() if key != "market_cap_gt_1b"))

    def test_run_kai_s2_screen_uses_db_frames_market_caps_and_benchmark(self) -> None:
        stock, benchmark = _kai_s2_frames()
        ticker = UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors", exchange="NASDAQ")
        as_of_date = stock.index[-1].date()

        with patch("src.kai_s2_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.kai_s2_screen.load_many_ticker_windows",
            return_value={"NVDA": stock},
        ), patch("src.kai_s2_screen.load_ticker_window", return_value=benchmark), patch(
            "src.kai_s2_screen.load_ticker_metadata_map",
            return_value={"NVDA": {"sector": "Technology", "industry": "Semiconductors", "exchange": "NASDAQ"}},
        ), patch("src.kai_s2_screen.load_latest_market_caps", return_value={"NVDA": 1_500_000_000.0}), patch(
            "src.kai_s2_screen.load_configured_cookstock"
        ) as load_cookstock:
            result = run_kai_s2_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertEqual(result.hits[0].criteria_passed, result.hits[0].criteria_total)
        load_cookstock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
