from __future__ import annotations

import unittest

import pandas as pd

from src.ema21_pullback_buy_screen import find_recent_ema21_pullback_buy_hit
from src.universe import UniverseTicker


def _ema21_pullback_frame(*, breakout: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=72, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx in range(70):
        center = 100.0 + (idx * 0.75)
        open_value = center - 0.25
        close_value = center + 0.2
        high_value = close_value + 0.75
        low_value = open_value - 0.7
        close_values.append(close_value)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(1_000_000.0)

    test_center = close_values[-1] - 0.4
    open_values.append(test_center - 0.1)
    close_values.append(test_center + 0.25)
    high_values.append(test_center + 0.8)
    low_values.append(test_center - 7.4)
    volume_values.append(1_100_000.0)
    test_high = high_values[-1]

    breakout_center = close_values[-1] + (1.25 if breakout else 0.15)
    breakout_open = breakout_center - 0.2
    breakout_close = breakout_center + (0.55 if breakout else 0.1)
    breakout_high = (test_high + 0.3) if breakout else (test_high - 0.05)
    breakout_low = breakout_open - 0.35
    open_values.append(breakout_open)
    close_values.append(breakout_close)
    high_values.append(breakout_high if breakout else max(breakout_close + 0.02, breakout_high - 0.02))
    low_values.append(breakout_low)
    volume_values.append(1_250_000.0)

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


class Ema21PullbackBuyScreenTests(unittest.TestCase):
    def test_find_recent_ema21_pullback_buy_hit_returns_latest_breakout(self) -> None:
        hit = find_recent_ema21_pullback_buy_hit(
            _ema21_pullback_frame(breakout=True),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "NVDA")
        self.assertEqual(hit.test_count, 1)
        self.assertLess(hit.test_low, hit.ema21)
        self.assertGreater(hit.breakout_high, hit.test_high)

    def test_find_recent_ema21_pullback_buy_hit_returns_none_without_breakout(self) -> None:
        hit = find_recent_ema21_pullback_buy_hit(
            _ema21_pullback_frame(breakout=False),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
