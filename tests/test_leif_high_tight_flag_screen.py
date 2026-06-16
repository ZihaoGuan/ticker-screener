from __future__ import annotations

import unittest

import pandas as pd

from src.leif_high_tight_flag_screen import find_leif_high_tight_flag_hit
from src.universe import UniverseTicker


def _benchmark_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    close_values = [100.0 + (index_value * 0.18) for index_value in range(len(index))]
    return pd.DataFrame({"Close": close_values}, index=index)


def _leif_high_tight_flag_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 220:
            close_value = 40.0 + (idx * 0.12)
            volume_value = 1_000_000.0 + ((idx % 5) * 15_000.0)
        elif idx < 240:
            close_value = 66.4 + ((idx - 220) * 3.2)
            volume_value = 2_200_000.0 + ((idx - 220) * 12_000.0)
        elif idx < 259:
            flag_closes = [
                126.8,
                125.9,
                125.2,
                123.8,
                122.1,
                120.6,
                118.4,
                116.5,
                114.2,
                112.4,
                110.8,
                111.6,
                112.7,
                114.0,
                115.4,
                117.1,
                118.9,
                120.5,
                122.4,
            ]
            close_value = flag_closes[idx - 240]
            volume_value = 1_050_000.0 - ((idx - 240) * 10_000.0)
        else:
            close_value = 127.2 if passes else 126.7
            volume_value = 2_450_000.0 if passes else 1_300_000.0
        open_value = close_value * 0.992
        high_value = close_value * 1.01
        low_value = close_value * 0.99
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
        volume_values.append(volume_value)

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


class LeifHighTightFlagScreenTests(unittest.TestCase):
    def test_find_leif_high_tight_flag_hit_returns_hit_when_rules_pass(self) -> None:
        hit = find_leif_high_tight_flag_hit(
            _leif_high_tight_flag_frame(passes=True),
            _benchmark_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
            benchmark_ticker="SPY",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreaterEqual(hit.rs_rating, 80.0)
        self.assertGreaterEqual(hit.score, 5.0)
        self.assertGreaterEqual(hit.pole_gain_pct, 90.0)
        self.assertGreaterEqual(hit.breakout_volume_ratio, 1.5)
        self.assertGreaterEqual(hit.flag_drawdown_pct, 10.0)
        self.assertLessEqual(hit.flag_drawdown_pct, 25.0)

    def test_find_leif_high_tight_flag_hit_returns_none_when_breakout_not_confirmed(self) -> None:
        hit = find_leif_high_tight_flag_hit(
            _leif_high_tight_flag_frame(passes=False),
            _benchmark_frame(),
            ticker=UniverseTicker(symbol="AAPL"),
            benchmark_ticker="SPY",
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
