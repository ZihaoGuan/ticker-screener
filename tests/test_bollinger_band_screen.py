from __future__ import annotations

import unittest

import pandas as pd

from src.bollinger_band_screen import compute_latest_bollinger_snapshot, find_recent_bollinger_band_breakout_hit


def _build_frame(close_values: list[float]) -> pd.DataFrame:
    index = pd.bdate_range(end="2026-06-24", periods=len(close_values))
    return pd.DataFrame(
        {
            "Open": [value - 0.5 for value in close_values],
            "High": [value + 1.0 for value in close_values],
            "Low": [value - 1.0 for value in close_values],
            "Close": close_values,
            "Volume": [1_000_000.0 for _ in close_values],
        },
        index=index,
    )


class BollingerBandScreenTests(unittest.TestCase):
    def test_compute_latest_snapshot_marks_above_upper_band(self) -> None:
        frame = _build_frame([100.0] * 24 + [120.0])

        snapshot = compute_latest_bollinger_snapshot(frame)

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.status, "above_upper_band")

    def test_find_recent_breakout_returns_none_when_price_within_bands(self) -> None:
        frame = _build_frame([100.0 + (index * 0.2) for index in range(30)])

        hit = find_recent_bollinger_band_breakout_hit(frame, ticker="MSFT")

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
