from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.rts_validation import build_rts_validation_report


def _frame(start: float, daily_step: float) -> pd.DataFrame:
    index = pd.bdate_range(start=dt.date(2025, 1, 2), periods=330)
    close = [start + daily_step * position for position in range(len(index))]
    return pd.DataFrame({"Close": close, "Low": [value * 0.99 for value in close]}, index=index)


class RtsValidationTests(unittest.TestCase):
    def test_report_groups_persisted_snapshots_without_lookahead_rebuild(self) -> None:
        snapshot_date = pd.bdate_range(start=dt.date(2025, 1, 2), periods=220)[-1].date()
        report = build_rts_validation_report(
            [{"ticker": "LEAD", "as_of_date": snapshot_date, "rts_score": 88, "rts_state": "expanding", "sector": "Technology"}],
            {"LEAD": _frame(100, 0.8), "SPY": _frame(100, 0.2)},
        )

        self.assertEqual(report["observation_count"], 1)
        self.assertIn("80-100", report["by_score_bucket"])
        self.assertGreater(report["overall"]["horizons"]["20"]["average_excess_return_pct"], 0)
        self.assertIn("above_spy_sma200", report["by_market_regime"])
