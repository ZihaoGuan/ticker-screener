from __future__ import annotations

import unittest

import pandas as pd

from src.double_bottom_detection_screen import find_active_double_bottom_detection_hit
from src.universe import UniverseTicker


def _active_double_bottom_frame(*, breakout: bool = False) -> pd.DataFrame:
    index = pd.date_range(start="2025-09-01", periods=135, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)
    close.extend(
        [
            114.0,
            110.0,
            106.0,
            102.0,
            99.0,
            96.0,
            94.0,
            95.0,
            98.0,
            101.0,
            104.0,
            107.0,
            110.0,
            111.5,
            112.0,
            112.6,
            111.8,
            110.0,
            107.0,
            104.0,
            100.0,
            96.0,
            91.0,
            92.0,
            95.0,
            98.0,
            101.0,
            104.0,
            107.0,
            109.0,
            110.0,
            110.5,
            111.0,
            111.2,
        ]
    )

    open_values = [value - 0.25 for value in close]
    high = [value + 0.85 for value in close]
    low = [value - 0.85 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.3
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.5

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 112.85)
        low[idx] = max(low[idx], 89.8)
        close[idx] = min(close[idx], 112.1)

    if breakout:
        high[-1] = 120.8
        close[-1] = 119.2
        low[-1] = 118.6

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


class DoubleBottomDetectionScreenTests(unittest.TestCase):
    def test_find_active_double_bottom_detection_hit_returns_hit(self) -> None:
        hit = find_active_double_bottom_detection_hit(
            _active_double_bottom_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreaterEqual(hit.pattern_weeks, 6)
        self.assertGreaterEqual(hit.depth_pct, 10.0)
        self.assertLess(hit.current_price, hit.breakout_price)

    def test_find_active_double_bottom_detection_hit_returns_none_after_breakout(self) -> None:
        hit = find_active_double_bottom_detection_hit(
            _active_double_bottom_frame(breakout=True),
            ticker=UniverseTicker(symbol="TSLA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
