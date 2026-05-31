from __future__ import annotations

import unittest

import pandas as pd

from src.config import AppConfig
from src.ftd_sweep_screen import find_recent_ftd_sweep_hit
from src.universe import UniverseTicker


def _ftd_sweep_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-05", periods=80, freq="B")
    close = [100.0 - (idx * 0.18) for idx in range(80)]
    open_values = [value + 0.45 for value in close]
    high = [max(op, cl) + 0.55 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.55 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 for _ in close]

    overrides = {
        21: (90.5, 89.8, 91.0, 89.0, 1_050_000.0),
        22: (90.2, 91.0, 91.4, 89.5, 1_050_000.0),
        23: (91.1, 90.8, 91.6, 90.2, 1_000_000.0),
        24: (90.7, 90.2, 91.0, 89.8, 1_000_000.0),
        25: (90.0, 89.7, 90.3, 89.3, 1_000_000.0),
        26: (89.6, 89.3, 89.9, 88.9, 1_000_000.0),
        27: (89.1, 88.8, 89.4, 88.4, 1_000_000.0),
        28: (88.5, 88.2, 88.8, 87.8, 1_000_000.0),
        29: (88.1, 87.6, 88.5, 87.2, 1_000_000.0),
        30: (85.4, 83.8, 85.8, 82.8, 1_050_000.0),
        31: (84.2, 86.0, 86.5, 84.0, 1_000_000.0),
        32: (85.4, 89.4, 90.0, 85.2, 1_900_000.0),
        33: (89.2, 89.9, 90.3, 88.9, 1_250_000.0),
        34: (90.3, 89.6, 90.5, 89.2, 700_000.0),
        35: (89.9, 89.2, 90.1, 88.9, 650_000.0),
        36: (89.4, 88.8, 89.6, 88.5, 600_000.0),
        37: (89.0, 91.6, 92.4, 88.9, 1_400_000.0),
        38: (91.8, 91.0, 92.0, 90.6, 1_000_000.0),
        39: (91.2, 90.8, 91.4, 90.2, 950_000.0),
        40: (90.9, 93.2, 93.6, 90.7, 1_300_000.0),
        41: (93.0, 93.6, 94.0, 92.8, 1_100_000.0),
        42: (93.5, 93.9, 94.3, 93.3, 1_100_000.0),
    }
    for idx in range(43, len(close)):
        close[idx] = 94.0 + ((idx - 43) * 0.12)
        open_values[idx] = close[idx] - 0.3
        high[idx] = close[idx] + 0.45
        low[idx] = open_values[idx] - 0.35
        volume[idx] = 1_050_000.0

    for idx, row in overrides.items():
        open_values[idx], close[idx], high[idx], low[idx], volume[idx] = row

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


class FtdSweepScreenTests(unittest.TestCase):
    def test_detects_recent_ftd_sweep_breakout(self) -> None:
        frame = _ftd_sweep_frame().iloc[:43]
        config = AppConfig(
            ftd_sweep_history_days=90,
            ftd_sweep_min_avg_volume=0,
            ftd_sweep_min_avg_dollar_volume=0.0,
        )

        hit = find_recent_ftd_sweep_hit(
            frame,
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", exchange="NASDAQ"),
            benchmark_ticker="SPY",
            config=config,
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ftd_date, frame.index[37].date().isoformat())
        self.assertEqual(hit.sweep_breakout_date, frame.index[40].date().isoformat())
        self.assertEqual(hit.bars_since_breakout, 2)
        self.assertGreater(hit.breakout_distance_pct, 0.0)

    def test_rejects_breakout_when_too_old_for_recent_window(self) -> None:
        frame = _ftd_sweep_frame().iloc[:47]
        config = AppConfig(
            ftd_sweep_history_days=90,
            ftd_sweep_min_avg_volume=0,
            ftd_sweep_min_avg_dollar_volume=0.0,
            ftd_sweep_recent_breakout_lookback_days=2,
        )

        hit = find_recent_ftd_sweep_hit(
            frame,
            ticker=UniverseTicker(symbol="AAPL"),
            benchmark_ticker="SPY",
            config=config,
        )

        self.assertIsNone(hit)

    def test_detects_same_bar_wick_sweep_and_close_reclaim(self) -> None:
        frame = _ftd_sweep_frame().iloc[:43].copy()
        frame.iloc[39, frame.columns.get_loc("Open")] = 92.0
        frame.iloc[39, frame.columns.get_loc("High")] = 93.3
        frame.iloc[39, frame.columns.get_loc("Low")] = 90.2
        frame.iloc[39, frame.columns.get_loc("Close")] = 92.8
        frame.iloc[40, frame.columns.get_loc("Open")] = 92.7
        frame.iloc[40, frame.columns.get_loc("High")] = 93.6
        frame.iloc[40, frame.columns.get_loc("Low")] = 92.4
        frame.iloc[40, frame.columns.get_loc("Close")] = 93.2
        config = AppConfig(
            ftd_sweep_history_days=90,
            ftd_sweep_min_avg_volume=0,
            ftd_sweep_min_avg_dollar_volume=0.0,
        )

        hit = find_recent_ftd_sweep_hit(
            frame,
            ticker=UniverseTicker(symbol="AAPL"),
            benchmark_ticker="SPY",
            config=config,
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.sweep_breakout_date, frame.index[39].date().isoformat())
