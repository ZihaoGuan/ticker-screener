from __future__ import annotations

import unittest

import pandas as pd

from src.rti_screen import find_recent_rti_hit
from src.universe import UniverseTicker


def _rti_frame(*, mode: str) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=8, freq="B")
    close_values = [100.0 + (idx * 0.4) for idx in range(8)]
    open_values = [value - 0.2 for value in close_values]
    volume_values = [1_000_000.0 for _ in close_values]

    if mode == "below_20":
        ranges = [6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.7, 1.2]
    elif mode == "orange_dot":
        ranges = [6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.15, 1.2]
    elif mode == "range_expansion":
        ranges = [6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.2, 1.8]
    else:
        ranges = [6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.5, 1.6]

    high_values = [close + (bar_range / 2.0) for close, bar_range in zip(close_values, ranges)]
    low_values = [close - (bar_range / 2.0) for close, bar_range in zip(close_values, ranges)]

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


class RtiScreenTests(unittest.TestCase):
    def test_find_recent_rti_hit_returns_below_20_signal(self) -> None:
        hit = find_recent_rti_hit(_rti_frame(mode="below_20"), ticker=UniverseTicker(symbol="AAPL"))

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "below_20")
        self.assertTrue(hit.below_20)

    def test_find_recent_rti_hit_returns_orange_dot_signal(self) -> None:
        hit = find_recent_rti_hit(_rti_frame(mode="orange_dot"), ticker=UniverseTicker(symbol="AAPL"))

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "orange_dot")
        self.assertTrue(hit.dot_condition)

    def test_find_recent_rti_hit_returns_range_expansion_signal(self) -> None:
        hit = find_recent_rti_hit(_rti_frame(mode="range_expansion"), ticker=UniverseTicker(symbol="AAPL"))

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "range_expansion")
        self.assertTrue(hit.range_expansion_condition)

    def test_find_recent_rti_hit_returns_none_without_signal(self) -> None:
        hit = find_recent_rti_hit(_rti_frame(mode="none"), ticker=UniverseTicker(symbol="AAPL"))

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
