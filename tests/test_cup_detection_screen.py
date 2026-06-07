from __future__ import annotations

import unittest

import pandas as pd

from src.cup_detection_screen import find_active_cup_detection_hit
from src.universe import UniverseTicker


def _active_cup_frame(*, breakout: bool = False) -> pd.DataFrame:
    index = pd.date_range(start="2025-09-01", periods=145, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)

    cup_segment = [
        116.0,
        114.0,
        112.0,
        110.0,
        108.0,
        106.0,
        104.0,
        102.0,
        100.0,
        98.0,
        96.0,
        95.0,
        94.5,
        94.0,
        93.5,
        93.5,
        93.8,
        94.0,
        94.2,
        94.5,
        95.0,
        95.5,
        96.0,
        97.0,
        98.0,
        99.0,
        100.0,
        101.0,
        102.0,
        103.0,
        104.5,
        106.0,
        108.0,
        110.0,
        111.5,
        112.5,
        113.5,
        114.2,
        114.8,
        115.2,
        115.5,
        115.8,
        116.0,
        116.2,
    ]
    close.extend(cup_segment)

    open_values = [value - 0.25 for value in close]
    high = [value + 0.75 for value in close]
    low = [value - 0.75 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.3
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.5

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 118.9)
        close[idx] = min(close[idx], 117.0)
        low[idx] = max(low[idx], 92.0)

    if breakout:
        high[-1] = 120.7
        close[-1] = 119.3
        low[-1] = 118.5

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


class CupDetectionScreenTests(unittest.TestCase):
    def test_find_active_cup_detection_hit_returns_hit(self) -> None:
        hit = find_active_cup_detection_hit(
            _active_cup_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreaterEqual(hit.cup_weeks, 6)
        self.assertGreaterEqual(hit.cup_depth_pct, 8.0)
        self.assertLessEqual(hit.cup_depth_pct, 50.0)
        self.assertIn(hit.shape_mode, {"thirds", "quarters"})

    def test_find_active_cup_detection_hit_returns_none_after_breakout(self) -> None:
        hit = find_active_cup_detection_hit(
            _active_cup_frame(breakout=True),
            ticker=UniverseTicker(symbol="TSLA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
