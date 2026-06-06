from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.rs_rating_screen import find_recent_rs_rating_hit
from src.universe import UniverseTicker


def _strong_rs_frame() -> tuple[pd.DataFrame, pd.DataFrame]:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    benchmark_close = np.linspace(100.0, 105.0, 320)
    stock_close = 50.0 + (150.0 * (np.linspace(0.0, 1.0, 320) ** 1.2))
    benchmark = pd.DataFrame({"Close": benchmark_close}, index=index)
    stock = pd.DataFrame(
        {
            "Open": stock_close * 0.998,
            "High": stock_close + 1.0,
            "Low": stock_close - 1.0,
            "Close": stock_close,
            "Volume": np.full(shape=320, fill_value=1_250_000.0),
        },
        index=index,
    )
    return stock, benchmark


def _weak_rs_frame() -> tuple[pd.DataFrame, pd.DataFrame]:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    benchmark_close = np.linspace(100.0, 140.0, 320)
    stock_close = np.linspace(100.0, 110.0, 320)
    benchmark = pd.DataFrame({"Close": benchmark_close}, index=index)
    stock = pd.DataFrame(
        {
            "Open": stock_close * 0.998,
            "High": stock_close + 1.0,
            "Low": stock_close - 1.0,
            "Close": stock_close,
            "Volume": np.full(shape=320, fill_value=900_000.0),
        },
        index=index,
    )
    return stock, benchmark


class RsRatingScreenTests(unittest.TestCase):
    def test_find_recent_rs_rating_hit_returns_hit_when_rating_at_least_90(self) -> None:
        stock, benchmark = _strong_rs_frame()
        hit = find_recent_rs_rating_hit(
            stock,
            benchmark,
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            benchmark_ticker="SPY",
            min_rating=90.0,
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreaterEqual(hit.rs_rating, 90.0)
        self.assertEqual(hit.benchmark_ticker, "SPY")

    def test_find_recent_rs_rating_hit_returns_none_below_threshold(self) -> None:
        stock, benchmark = _weak_rs_frame()
        hit = find_recent_rs_rating_hit(
            stock,
            benchmark,
            ticker=UniverseTicker(symbol="MSFT"),
            benchmark_ticker="SPY",
            min_rating=90.0,
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
