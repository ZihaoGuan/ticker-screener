import datetime as dt
import unittest

import numpy as np
import pandas as pd

from src.martin_bar_watchlist_screen import build_rank_context, evaluate_martin_bar_watchlist


class MartinBarWatchlistTests(unittest.TestCase):
    def _bars(self, closes=None, volume=None):
        dates = pd.bdate_range("2026-01-01", periods=150)
        closes = np.full(150, 20.0) if closes is None else closes
        volume = np.full(150, 1_000_000.0) if volume is None else volume
        return pd.DataFrame({"Open": closes * .99, "High": closes * 1.02, "Low": closes * .98,
                             "Close": closes, "Volume": volume}, index=dates)

    def test_mover_records_volume_baseline_excluding_event_bar(self):
        closes = np.full(150, 20.0)
        closes[-1] = 22.0
        volume = np.full(150, 1_000_000.0)
        volume[-1] = 1_500_000.0
        row = evaluate_martin_bar_watchlist(self._bars(closes, volume), self._bars(), {}, ticker="ABC")
        self.assertIsNotNone(row)
        self.assertEqual(row["discovery_categories"], ["potent_mover", "gap_mover"])
        self.assertEqual(row["volume_ratio_20_excluding_T"], 1.5)

    def test_cross_sectional_ties_are_alphabetical(self):
        a, b = self._bars(), self._bars()
        context = build_rank_context({"ZZZ": b, "AAA": a}, a.index[-1].date())
        self.assertEqual(context["AAA"]["rank"], 1)
        self.assertEqual(context["ZZZ"]["rank"], 2)

    def test_rank_context_requires_current_session_bar(self):
        current, stale = self._bars(), self._bars().iloc[:-1]
        context = build_rank_context({"CURRENT": current, "STALE": stale}, current.index[-1].date())
        self.assertEqual(list(context), ["CURRENT"])

    def test_missing_market_session_blocks_rank_and_mover(self):
        bars = self._bars()
        expected = [value.date() for value in bars.index[-120:]]
        missing = bars.drop(bars.index[-10])
        volume = missing["Volume"].copy()
        volume.iloc[-1] = 2_000_000
        missing.loc[missing.index[-1], "Volume"] = volume.iloc[-1]
        missing.loc[missing.index[-1], "Close"] = 22.0
        missing.loc[missing.index[-1], "High"] = 22.5
        missing.loc[missing.index[-1], "Open"] = 21.0
        self.assertEqual(build_rank_context({"ABC": missing}, bars.index[-1].date(), expected), {})
        self.assertIsNone(evaluate_martin_bar_watchlist(
            missing, bars, {}, ticker="ABC", as_of_date=bars.index[-1].date(), expected_sessions=expected))

    def test_missing_current_bar_is_not_evaluated(self):
        bars = self._bars().iloc[:-1]
        row = evaluate_martin_bar_watchlist(bars, self._bars(), {}, ticker="ABC",
                                            as_of_date=dt.date(2026, 12, 31))
        self.assertIsNone(row)


if __name__ == "__main__":
    unittest.main()
