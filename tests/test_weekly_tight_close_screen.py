from __future__ import annotations

import unittest

import pandas as pd

from src.universe import UniverseTicker
from src.weekly_tight_close_screen import find_weekly_tight_close_breakout_hit, find_weekly_tight_close_hit


def _weekly_tight_close_frame(*, broken: bool = False, breakout: bool = False) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-06", periods=105, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for week in range(21):
        week_dates = index[week * 5 : (week + 1) * 5]
        if week < 17:
            week_open = 100.0 + (week * 1.2)
            week_high = week_open + 3.0
            week_low = week_open - 3.0
            week_close = week_open + 1.0
        elif week == 17:
            week_open = 120.5
            week_high = 122.0
            week_low = 119.0
            week_close = 121.0
        elif week == 18:
            week_open = 120.8
            week_high = 122.2
            week_low = 119.4
            week_close = 121.2
        elif week == 19:
            week_open = 121.0
            week_high = 122.1 if not broken else 126.0
            week_low = 119.5
            week_close = 121.1 if not broken else 124.8
        else:
            week_open = 121.8
            week_high = 123.8 if breakout else 122.0
            week_low = 120.9
            week_close = 123.1 if breakout else 121.6

        for day_index, _date in enumerate(week_dates):
            open_values.append(week_open + (day_index * 0.02))
            high_values.append(week_high - (0.1 * (4 - day_index)))
            low_values.append(week_low + (0.1 * day_index))
            close_values.append(week_close if day_index == 4 else week_open + (day_index * 0.15))
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


class WeeklyTightCloseScreenTests(unittest.TestCase):
    def test_find_weekly_tight_close_hit_returns_hit(self) -> None:
        hit = find_weekly_tight_close_hit(
            _weekly_tight_close_frame(),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.breakout_price, hit.lowest_price)
        self.assertGreater(hit.threshold_pct, 0.0)

    def test_find_weekly_tight_close_hit_returns_none_when_not_tight(self) -> None:
        hit = find_weekly_tight_close_hit(
            _weekly_tight_close_frame(broken=True),
            ticker=UniverseTicker(symbol="TSLA"),
        )

        self.assertIsNone(hit)

    def test_find_weekly_tight_close_breakout_hit_returns_hit(self) -> None:
        hit = find_weekly_tight_close_breakout_hit(
            _weekly_tight_close_frame(breakout=True),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.current_price, hit.breakout_price)

    def test_find_weekly_tight_close_breakout_hit_returns_none_without_breakout(self) -> None:
        hit = find_weekly_tight_close_breakout_hit(
            _weekly_tight_close_frame(),
            ticker=UniverseTicker(symbol="AMD"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
