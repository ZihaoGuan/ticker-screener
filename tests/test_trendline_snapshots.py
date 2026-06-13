from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.trendline_snapshots import build_snapshot_rows_for_range, build_trendline_snapshot_frame


class TrendlineSnapshotTests(unittest.TestCase):
    def test_build_trendline_snapshot_frame_computes_daily_and_weekly_levels(self) -> None:
        index = pd.date_range("2026-01-02", periods=80, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [float(value) for value in range(1, 81)],
                "High": [float(value) + 1.0 for value in range(1, 81)],
                "Low": [float(value) - 1.0 for value in range(1, 81)],
                "Close": [float(value) for value in range(1, 81)],
                "Adj Close": [float(value) for value in range(1, 81)],
                "Volume": [1_000_000 for _ in range(80)],
            },
            index=index,
        )

        snapshot = build_trendline_snapshot_frame(frame)

        self.assertEqual(len(snapshot), 80)
        self.assertAlmostEqual(float(snapshot.iloc[-1]["daily_sma50"]), 55.5)
        self.assertGreater(float(snapshot.iloc[-1]["daily_ema9"]), 0.0)
        self.assertGreater(float(snapshot.iloc[-1]["daily_ema21"]), 0.0)
        self.assertGreater(float(snapshot.iloc[-1]["weekly_ema8"]), 0.0)
        self.assertTrue(pd.isna(snapshot.iloc[-1]["weekly_sma200"]))

    def test_build_snapshot_rows_for_range_filters_dates_and_preserves_nulls(self) -> None:
        index = pd.date_range("2026-06-01", periods=10, freq="B")
        frame = pd.DataFrame(
            {
                "Open": [10.0 + value for value in range(10)],
                "High": [11.0 + value for value in range(10)],
                "Low": [9.0 + value for value in range(10)],
                "Close": [10.0 + value for value in range(10)],
                "Adj Close": [10.0 + value for value in range(10)],
                "Volume": [500_000 for _ in range(10)],
            },
            index=index,
        )

        rows = build_snapshot_rows_for_range(
            "aapl",
            frame,
            start_date=dt.date(2026, 6, 5),
            end_date=dt.date(2026, 6, 10),
        )

        self.assertEqual([row[0] for row in rows], ["AAPL", "AAPL", "AAPL", "AAPL"])
        self.assertEqual(rows[0][1].isoformat(), "2026-06-05")
        self.assertIsNone(rows[0][5])
        self.assertIsNone(rows[0][6])


if __name__ == "__main__":
    unittest.main()
