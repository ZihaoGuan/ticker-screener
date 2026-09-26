from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.rmv_screen import evaluate_rmv, find_rmv_hit
from src.universe import UniverseTicker


def _bars() -> pd.DataFrame:
    index = pd.bdate_range("2025-01-01", periods=230)
    close = pd.Series(range(100, 330), index=index, dtype=float)
    return pd.DataFrame({"Open": close, "High": close + 1, "Low": close - 1, "Close": close, "Volume": 1_000_000}, index=index)


class RmvScreenTests(unittest.TestCase):
    def test_quiet_latest_bar_uses_prior_loud_baseline_and_forms_trough(self) -> None:
        frame = _bars()
        frame.iloc[-1, frame.columns.get_loc("Open")] = frame.iloc[-1]["Close"] - 0.1
        frame.iloc[-1, frame.columns.get_loc("High")] = frame.iloc[-1]["Close"] + 0.2
        frame.iloc[-1, frame.columns.get_loc("Low")] = frame.iloc[-1]["Close"] - 0.2

        snapshot = evaluate_rmv(frame)
        hit = find_rmv_hit(frame, ticker=UniverseTicker(symbol="RMV"), signal_date=dt.date(2025, 12, 1))

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.rmv, 2.5, places=3)
        self.assertEqual(snapshot.rank_tier, 1)
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "confluence")


if __name__ == "__main__":
    unittest.main()
