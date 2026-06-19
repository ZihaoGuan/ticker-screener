from __future__ import annotations

import unittest

import pandas as pd

from src.high_tight_flag_setup_screen import find_high_tight_flag_setup_hit
from src.universe import UniverseTicker


def _high_tight_flag_setup_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    flag_closes_pass = [
        125.0,
        123.4,
        122.2,
        121.1,
        120.6,
        121.3,
        122.0,
        122.8,
        123.7,
        124.1,
        124.6,
        125.1,
        125.3,
        125.7,
        126.0,
    ]
    flag_closes_fail = [
        125.0,
        121.0,
        117.5,
        113.0,
        109.0,
        106.0,
        108.0,
        110.0,
        111.0,
        112.0,
        113.0,
        114.0,
        115.0,
        115.5,
        116.0,
    ]
    flag_closes = flag_closes_pass if passes else flag_closes_fail

    for idx, _date in enumerate(index):
        if idx < 220:
            close_value = 40.0 + (idx * 0.12)
            bar_range = 3.2 - min(idx * 0.004, 1.0)
            volume_value = 1_000_000.0 + ((idx % 5) * 12_000.0)
        elif idx < 245:
            close_value = 66.4 + ((idx - 220) * 2.35)
            bar_range = 4.8 - ((idx - 220) * 0.10)
            volume_value = 2_300_000.0 + ((idx - 220) * 18_000.0)
        else:
            close_value = flag_closes[idx - 245]
            bar_range = 2.4 - ((idx - 245) * 0.06)
            volume_value = 1_050_000.0 - ((idx - 245) * 12_000.0)
        open_value = close_value - (bar_range * 0.18)
        high_value = close_value + (bar_range * 0.45)
        low_value = close_value - (bar_range * 0.45)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
        volume_values.append(max(volume_value, 550_000.0))

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


class HighTightFlagSetupScreenTests(unittest.TestCase):
    def test_find_high_tight_flag_setup_hit_returns_hit_when_rules_pass(self) -> None:
        hit = find_high_tight_flag_setup_hit(
            _high_tight_flag_setup_frame(passes=True),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreaterEqual(hit.pole_gain_ratio, 1.9)
        self.assertGreaterEqual(hit.flag_days, 5)
        self.assertLessEqual(hit.flag_days, 20)
        self.assertLess(hit.distance_to_pivot_pct, 0.08)
        self.assertLess(hit.atr_ratio, 0.08)
        self.assertGreater(hit.runup_60_ratio, 1.5)

    def test_find_high_tight_flag_setup_hit_returns_none_when_flag_breaks_too_deep(self) -> None:
        hit = find_high_tight_flag_setup_hit(
            _high_tight_flag_setup_frame(passes=False),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
