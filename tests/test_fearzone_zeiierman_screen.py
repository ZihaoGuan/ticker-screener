from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.fearzone_zeiierman_screen import find_recent_fearzone_zeiierman_hit
from src.universe import UniverseTicker


def _fearzone_zeiierman_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-01", periods=220, freq="B")
    close = np.linspace(100.0, 180.0, 220)
    close[-10:] -= np.linspace(0.0, 55.0, 10)
    open_values = close * 1.002
    high = np.maximum(open_values, close) + 1.0
    low = np.minimum(open_values, close) - 1.0
    volume = np.full(shape=220, fill_value=1_250_000.0)
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        },
        index=index,
    )


class FearzoneZeiiermanScreenTests(unittest.TestCase):
    def test_find_recent_hit_returns_recent_transition(self) -> None:
        hit = find_recent_fearzone_zeiierman_hit(
            _fearzone_zeiierman_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            config=AppConfig(),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "AAPL")
        self.assertLessEqual(hit.signal_age_bars, 4)
        self.assertEqual(hit.ma_type, "WMA")

    def test_find_recent_hit_returns_none_for_flat_series(self) -> None:
        index = pd.date_range(start="2025-01-01", periods=220, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [100.0] * 220,
                "High": [101.0] * 220,
                "Low": [99.0] * 220,
                "Close": [100.0] * 220,
                "Volume": [1_000_000.0] * 220,
            },
            index=index,
        )

        hit = find_recent_fearzone_zeiierman_hit(
            frame,
            ticker=UniverseTicker(symbol="MSFT"),
            config=AppConfig(),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
