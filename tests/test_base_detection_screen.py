from __future__ import annotations

import unittest

import pandas as pd

from src.base_detection_screen import find_active_base_detection_hit
from src.universe import UniverseTicker


def _active_base_frame(*, breakout: bool = False) -> pd.DataFrame:
    index = pd.date_range(start="2025-10-01", periods=160, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)
    close.extend(
        [
            115.0,
            114.4,
            114.8,
            113.7,
            114.2,
            113.8,
            114.3,
            113.9,
            114.5,
            114.1,
            113.6,
            114.0,
            113.5,
            114.2,
            113.9,
            114.4,
            114.1,
            113.7,
            114.0,
            113.8,
            114.2,
            114.0,
            113.9,
            114.1,
            114.0,
        ]
    )
    while len(close) < len(index):
        step = len(close) - 126
        close.append(114.0 + ((step % 6) * 0.18))

    open_values = [value - 0.25 for value in close]
    high = [value + 0.65 for value in close]
    low = [value - 0.65 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.2
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.0

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 118.9)
        low[idx] = max(low[idx], 108.0)
        close[idx] = min(close[idx], 118.0)

    if breakout:
        high[-1] = 120.8
        close[-1] = 119.6
        low[-1] = 118.8

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


class BaseDetectionScreenTests(unittest.TestCase):
    def test_find_active_base_detection_hit_returns_flat_base_hit(self) -> None:
        hit = find_active_base_detection_hit(
            _active_base_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.base_type, "Flat Base")
        self.assertGreaterEqual(hit.base_weeks, 5)
        self.assertLessEqual(hit.base_depth_pct, 15.0)
        self.assertGreater(hit.breakout_price, hit.base_low)

    def test_find_active_base_detection_hit_returns_none_after_breakout(self) -> None:
        hit = find_active_base_detection_hit(
            _active_base_frame(breakout=True),
            ticker=UniverseTicker(symbol="TSLA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
