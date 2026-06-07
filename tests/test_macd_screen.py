from __future__ import annotations

import unittest

import pandas as pd

from src.macd_screen import find_recent_macd_hit
from src.universe import UniverseTicker


def _golden_cross_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=60, freq="B")
    close = [100.0 - (idx * 0.35) for idx in range(55)]
    close.extend([80.6, 80.1, 79.8, 80.0, 81.8])
    return pd.DataFrame(
        {
            "Open": [value - 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _dead_cross_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=60, freq="B")
    close = [100.0 + (idx * 0.35) for idx in range(55)]
    close.extend([119.4, 119.8, 120.0, 119.5, 117.5])
    return pd.DataFrame(
        {
            "Open": [value + 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


class MacdScreenTests(unittest.TestCase):
    def test_find_recent_macd_golden_cross_hit_returns_recent_signal(self) -> None:
        hit = find_recent_macd_hit(
            _golden_cross_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            direction="golden_cross",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "golden_cross")
        self.assertLessEqual(hit.signal_age_bars, 1)

    def test_find_recent_macd_dead_cross_hit_returns_recent_signal(self) -> None:
        hit = find_recent_macd_hit(
            _dead_cross_frame(),
            ticker=UniverseTicker(symbol="TSLA"),
            direction="dead_cross",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "dead_cross")
        self.assertLessEqual(hit.signal_age_bars, 1)

    def test_find_recent_macd_hit_returns_none_without_recent_cross(self) -> None:
        frame = _golden_cross_frame().iloc[:-10]
        hit = find_recent_macd_hit(
            frame,
            ticker=UniverseTicker(symbol="MSFT"),
            direction="golden_cross",
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
