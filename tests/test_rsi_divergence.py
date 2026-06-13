from __future__ import annotations

import unittest

import pandas as pd

from src.rsi_divergence import find_latest_bearish_rsi_divergence_top


def _bearish_divergence_top_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=80, freq="B")
    close_values = [
        100, 102, 104, 106, 108, 110, 112, 114, 116, 118,
        120, 122, 124, 126, 128, 130, 132, 134, 136, 138,
        140, 138, 136, 134, 132, 130, 128, 126, 124, 122,
        124, 126, 128, 130, 132, 134, 136, 138, 140, 142,
        144, 146, 148, 150, 152, 154, 156, 158, 160, 162,
        161, 160, 159, 158, 157, 156, 155, 154, 153, 152,
        153, 154, 155, 156, 157, 158, 159, 160, 161, 162,
        163, 164, 165, 166, 167, 168, 167, 166, 165, 164,
    ]
    return pd.DataFrame(
        {
            "Open": [value - 1.0 for value in close_values],
            "High": [value + 2.0 for value in close_values],
            "Low": [value - 2.0 for value in close_values],
            "Close": close_values,
            "Volume": [1_000_000 for _ in close_values],
        },
        index=index,
    )


class RsiDivergenceTests(unittest.TestCase):
    def test_find_latest_bearish_rsi_divergence_top_returns_active_signal(self) -> None:
        signal = find_latest_bearish_rsi_divergence_top(_bearish_divergence_top_frame())

        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertGreater(signal.signal_price, signal.previous_signal_price)
        self.assertLess(signal.signal_rsi, signal.previous_signal_rsi)
        self.assertIn(signal.state, {"fresh_top_warning", "active_top_warning"})
        self.assertGreaterEqual(signal.bars_apart, 1)

    def test_find_latest_bearish_rsi_divergence_top_returns_none_without_pattern(self) -> None:
        index = pd.date_range(start="2026-01-02", periods=80, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [100.0 + idx for idx in range(80)],
                "High": [101.0 + idx for idx in range(80)],
                "Low": [99.0 + idx for idx in range(80)],
                "Close": [100.5 + idx for idx in range(80)],
                "Volume": [1_000_000 for _ in range(80)],
            },
            index=index,
        )

        signal = find_latest_bearish_rsi_divergence_top(frame)

        self.assertIsNone(signal)


if __name__ == "__main__":
    unittest.main()
