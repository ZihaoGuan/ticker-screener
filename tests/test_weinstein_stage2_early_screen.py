from __future__ import annotations

import unittest

import pandas as pd

from src.universe import UniverseTicker
from src.weinstein_stage2_early_screen import find_weinstein_stage2_early_hit


def _weinstein_stage2_frame(*, early: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-06", periods=45 * 5, freq="B")
    weekly_closes = [100.0 for _ in range(39)]
    weekly_closes.extend([100.8, 106.0, 108.0, 110.0] if early else [104.0, 106.0, 108.0, 110.0, 112.0, 114.0])

    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for week, week_close in enumerate(weekly_closes):
        week_dates = index[week * 5 : (week + 1) * 5]
        week_open = week_close - 0.8
        week_high = week_close + 1.2
        week_low = week_close - 1.4
        for day_index, _date in enumerate(week_dates):
            open_values.append(week_open + (day_index * 0.05))
            high_values.append(week_high - (0.08 * (4 - day_index)))
            low_values.append(week_low + (0.08 * day_index))
            close_values.append(week_close if day_index == 4 else week_open + (day_index * 0.12))
            volume_values.append(1_200_000.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index[: len(close_values)],
    )


class WeinsteinStage2EarlyScreenTests(unittest.TestCase):
    def test_find_weinstein_stage2_early_hit_returns_hit(self) -> None:
        hit = find_weinstein_stage2_early_hit(
            _weinstein_stage2_frame(early=True),
            ticker=UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.previous_stage, "Stage 1 - Base")
        self.assertEqual(hit.current_stage, "Stage 2 - Advance")
        self.assertEqual(hit.maturity, "Early")
        self.assertLess(hit.run_length_weeks, 5)

    def test_find_weinstein_stage2_early_hit_returns_none_when_stage2_is_mature(self) -> None:
        hit = find_weinstein_stage2_early_hit(
            _weinstein_stage2_frame(early=False),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
