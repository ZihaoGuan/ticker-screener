from __future__ import annotations

import unittest

import pandas as pd

from src.td_sequential_screen import find_recent_td_sequential_hit
from src.universe import UniverseTicker


def _bearish_td9_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=20, freq="B")
    close = [
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        99.0,
        101.0,
        102.0,
        103.0,
        104.0,
        105.0,
        106.0,
        107.0,
        108.0,
        109.0,
    ]
    open_values = [value - 0.2 for value in close]
    high = [value + 0.8 for value in close]
    low = [value - 0.8 for value in close]
    volume = [1_000_000.0 for _ in close]
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


def _bullish_td9_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=20, freq="B")
    close = [
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        100.0,
        101.0,
        99.0,
        98.0,
        97.0,
        96.0,
        95.0,
        94.0,
        93.0,
        92.0,
        91.0,
    ]
    open_values = [value + 0.2 for value in close]
    high = [value + 0.8 for value in close]
    low = [value - 0.8 for value in close]
    volume = [1_000_000.0 for _ in close]
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


class TdSequentialScreenTests(unittest.TestCase):
    def test_find_recent_bullish_td9_hit_returns_latest_signal(self) -> None:
        hit = find_recent_td_sequential_hit(
            _bullish_td9_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            direction="bullish",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "bullish")
        self.assertEqual(hit.setup_count, 9)
        self.assertEqual(hit.signal_date, "2026-01-29")

    def test_find_recent_bearish_td9_hit_returns_latest_signal(self) -> None:
        hit = find_recent_td_sequential_hit(
            _bearish_td9_frame(),
            ticker=UniverseTicker(symbol="TSLA"),
            direction="bearish",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.direction, "bearish")
        self.assertEqual(hit.setup_count, 9)
        self.assertEqual(hit.signal_date, "2026-01-29")

    def test_find_recent_td9_hit_returns_none_when_latest_bar_not_nine(self) -> None:
        frame = _bullish_td9_frame().iloc[:-1]
        hit = find_recent_td_sequential_hit(
            frame,
            ticker=UniverseTicker(symbol="MSFT"),
            direction="bullish",
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
