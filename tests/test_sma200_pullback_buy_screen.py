from __future__ import annotations

import unittest

import pandas as pd

from src.sma200_pullback_buy_screen import find_recent_sma200_pullback_buy_hit
from src.universe import UniverseTicker


def _sma200_pullback_frame(*, breakout: bool, breakout_delay_bars: int = 0) -> pd.DataFrame:
    period_count = 242 + breakout_delay_bars
    index = pd.date_range(start="2025-01-02", periods=period_count, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx in range(240):
        center = 100.0 + (idx * 0.35)
        open_value = center - 0.25
        close_value = center + 0.2
        high_value = close_value + 0.75
        low_value = open_value - 0.7
        close_values.append(close_value)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(1_000_000.0)

    test_center = close_values[-1] - 0.1
    open_values.append(test_center - 0.1)
    close_values.append(test_center + 0.1)
    high_values.append(test_center + 0.8)
    low_values.append(test_center - 45.0)
    volume_values.append(1_100_000.0)
    test_high = high_values[-1]

    for step in range(breakout_delay_bars):
        filler_center = close_values[-1] + 0.28 + (step * 0.08)
        filler_open = filler_center - 0.14
        filler_close = filler_center + 0.1
        filler_high = min(test_high - 0.15, filler_close + 0.18)
        filler_low = filler_open - 0.1
        open_values.append(filler_open)
        close_values.append(filler_close)
        high_values.append(filler_high)
        low_values.append(filler_low)
        volume_values.append(1_050_000.0)

    breakout_center = close_values[-1] + (0.95 if breakout else 0.1)
    breakout_open = breakout_center - 0.2
    breakout_close = breakout_center + (0.55 if breakout else 0.08)
    breakout_high = (test_high + 0.4) if breakout else (test_high - 0.05)
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


class Sma200PullbackBuyScreenTests(unittest.TestCase):
    def test_find_recent_sma200_pullback_buy_hit_returns_latest_breakout(self) -> None:
        hit = find_recent_sma200_pullback_buy_hit(
            _sma200_pullback_frame(breakout=True),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "NVDA")
        self.assertEqual(hit.test_count, 1)
        self.assertLess(hit.test_low, hit.sma200)
        self.assertGreater(hit.breakout_high, hit.test_high)

    def test_find_recent_sma200_pullback_buy_hit_returns_none_without_breakout(self) -> None:
        hit = find_recent_sma200_pullback_buy_hit(
            _sma200_pullback_frame(breakout=False),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNone(hit)

    def test_find_recent_sma200_pullback_buy_hit_rejects_test_older_than_one_week(self) -> None:
        hit = find_recent_sma200_pullback_buy_hit(
            _sma200_pullback_frame(breakout=True, breakout_delay_bars=6),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
