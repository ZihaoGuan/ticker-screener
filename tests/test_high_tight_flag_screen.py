from __future__ import annotations

import unittest

import pandas as pd

from src.high_tight_flag_screen import find_high_tight_flag_hit
from src.universe import UniverseTicker


def _high_tight_flag_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 200:
            close_value = 45.0 + (idx * 0.4)
            bar_range = 8.5 - min(idx * 0.01, 2.0)
            volume_value = 1_700_000.0 + (idx * 2_000.0)
        elif idx < 220:
            close_value = 100.0 + ((idx - 200) * 1.0)
            bar_range = 6.0 - ((idx - 200) * 0.08)
            volume_value = 2_000_000.0 - ((idx - 200) * 12_000.0)
        else:
            close_value = 145.0 + ((idx - 220) * 2.2)
            if not passes:
                close_value = 145.0 + ((idx - 220) * 0.8)
            bar_range = 4.2 - ((idx - 220) * 0.05)
            volume_value = 1_760_000.0 - ((idx - 220) * 20_000.0)
        open_value = close_value - (bar_range * 0.15)
        high_value = close_value + (bar_range * 0.5)
        low_value = close_value - (bar_range * 0.5)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
        volume_values.append(max(volume_value, 500_000.0))

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


class HighTightFlagScreenTests(unittest.TestCase):
    def test_find_high_tight_flag_hit_returns_hit_when_all_rules_pass(self) -> None:
        hit = find_high_tight_flag_hit(
            _high_tight_flag_frame(passes=True),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.runup_40_ratio, 1.9)
        self.assertGreater(hit.runup_60_ratio, 1.5)
        self.assertLess(hit.atr_ratio, 0.08)
        self.assertGreater(hit.sma_200_slope_10, 0.0)
        self.assertLess(hit.avg_volume_50_slope_10, 0.0)
        self.assertLess(hit.atr_14_slope_10, 0.0)

    def test_find_high_tight_flag_hit_returns_none_when_runup_is_not_tight_enough(self) -> None:
        hit = find_high_tight_flag_hit(
            _high_tight_flag_frame(passes=False),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
