from __future__ import annotations

import unittest

import pandas as pd

from src.three_weeks_tight_screen import find_three_weeks_tight_hit
from src.universe import UniverseTicker


def _three_weeks_tight_frame(*, broken: bool = False) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-06", periods=30, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    weekly_closes = [100.0, 108.0, 114.0, 120.0, 120.8, 121.4 if not broken else 123.8]
    weekly_highs = [101.0, 109.0, 115.0, 121.0, 121.7, 121.9 if not broken else 124.2]
    weekly_lows = [98.5, 106.5, 112.5, 118.0, 119.8, 120.2]

    for week in range(6):
        week_dates = index[week * 5 : (week + 1) * 5]
        week_open = weekly_closes[week] - 1.0
        week_close = weekly_closes[week]
        week_high = weekly_highs[week]
        week_low = weekly_lows[week]
        for day_index, _date in enumerate(week_dates):
            open_values.append(week_open + (day_index * 0.1))
            high_values.append(week_high - (0.1 * (4 - day_index)))
            low_values.append(week_low + (0.1 * day_index))
            close_values.append(week_close if day_index == 4 else week_open + (day_index * 0.2))
            volume_values.append(1_000_000.0)

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


class ThreeWeeksTightScreenTests(unittest.TestCase):
    def test_find_three_weeks_tight_hit_returns_hit(self) -> None:
        hit = find_three_weeks_tight_hit(
            _three_weeks_tight_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.buy_price, hit.range_high)
        self.assertLessEqual(abs(hit.close_change_1_pct), hit.threshold_pct)
        self.assertLessEqual(abs(hit.close_change_2_pct), hit.threshold_pct)

    def test_find_three_weeks_tight_hit_returns_none_when_threshold_breaks(self) -> None:
        hit = find_three_weeks_tight_hit(
            _three_weeks_tight_frame(broken=True),
            ticker=UniverseTicker(symbol="TSLA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
