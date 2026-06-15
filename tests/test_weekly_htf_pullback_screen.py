from __future__ import annotations

import unittest

import pandas as pd

from src.weekly_htf_pullback_screen import _build_price_frame


class _FakeFinancials:
    def __init__(self, rows):
        self._rows = rows

    def _get_clean_price_data(self):
        return self._rows


class WeeklyHtfPullbackScreenTests(unittest.TestCase):
    def test_build_price_frame_accepts_dataframe_rows(self) -> None:
        rows = pd.DataFrame(
            {
                "Open": [10.0, 10.5],
                "High": [10.8, 10.9],
                "Low": [9.9, 10.2],
                "Close": [10.7, 10.8],
                "Volume": [1000, 1100],
            },
            index=pd.to_datetime(["2026-06-12", "2026-06-15"]),
        )

        frame = _build_price_frame(_FakeFinancials(rows))

        self.assertEqual(list(frame.columns), ["Open", "High", "Low", "Close", "Volume"])
        self.assertEqual(len(frame), 2)
        self.assertFalse(frame.empty)


if __name__ == "__main__":
    unittest.main()
