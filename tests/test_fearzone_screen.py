from __future__ import annotations

import unittest

import pandas as pd

from src.config import AppConfig
from src.fearzone_screen import find_recent_fearzone_hit
from src.universe import UniverseTicker


def _fearzone_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    close = [90.0 + (idx * 0.16) for idx in range(240)]
    close.extend(
        [
            129.0,
            129.6,
            128.8,
            130.0,
            131.2,
            132.0,
            131.0,
            132.4,
            131.6,
            132.2,
            131.8,
            132.0,
            131.4,
            119.0,
            121.4,
            123.7,
            124.9,
            125.8,
            126.7,
            127.5,
        ]
    )
    open_values = [value * 1.003 for value in close]
    high = [max(op, cl) + 1.0 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 1.0 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_100_000.0 for _ in close]
    signal_index = len(close) - 5
    open_values[signal_index] = 126.5
    close[signal_index] = 119.0
    high[signal_index] = 127.0
    low[signal_index] = 118.2
    volume[signal_index] = 1_900_000.0
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


class FearzoneScreenTests(unittest.TestCase):
    def test_find_recent_fearzone_hit_returns_actionable_signal(self) -> None:
        hit = find_recent_fearzone_hit(
            _fearzone_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            benchmark_ticker="SPY",
            config=AppConfig(),
        )
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "AAPL")
        self.assertLessEqual(hit.signal_age_bars, 4)
        self.assertTrue(hit.above_ma200)
        self.assertTrue(hit.trigger_negative_impulse or hit.trigger_ricochet_zone or hit.trigger_magic_k1)

    def test_find_recent_fearzone_hit_returns_none_for_flat_series(self) -> None:
        index = pd.date_range(start="2025-01-02", periods=260, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [100.0] * 260,
                "High": [101.0] * 260,
                "Low": [99.0] * 260,
                "Close": [100.0] * 260,
                "Volume": [1_000_000.0] * 260,
            },
            index=index,
        )
        hit = find_recent_fearzone_hit(
            frame,
            ticker=UniverseTicker(symbol="MSFT"),
            benchmark_ticker="SPY",
            config=AppConfig(),
        )
        self.assertIsNone(hit)
