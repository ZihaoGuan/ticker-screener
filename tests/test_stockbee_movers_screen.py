from __future__ import annotations

import unittest

import pandas as pd

from src.stockbee_movers_screen import evaluate_stockbee_mover_frame
from src.stockbee_movers_watchlist_builder import build_stockbee_movers_watchlist
from src.universe import UniverseTicker


class StockbeeMoversScreenTests(unittest.TestCase):
    def setUp(self) -> None:
        dates = pd.bdate_range("2026-06-01", periods=6)
        self.frame = pd.DataFrame(
            {"Close": [10, 10.5, 11, 11.5, 12, 12.5], "Volume": [1_000_000, 1_200_000, 1_100_000, 1_300_000, 1_400_000, 9_100_000]},
            index=dates,
        )
        self.ticker = UniverseTicker(symbol="TEST", sector="Technology", industry="Software")

    def test_profiles_apply_their_named_thresholds(self) -> None:
        volume_hit = evaluate_stockbee_mover_frame(self.frame, ticker=self.ticker, profile="stockbee_9m_movers")
        weekly_hit = evaluate_stockbee_mover_frame(self.frame, ticker=self.ticker, profile="stockbee_20pct_weekly_movers")
        daily_hit = evaluate_stockbee_mover_frame(self.frame, ticker=self.ticker, profile="stockbee_4pct_daily_movers")

        self.assertIsNotNone(volume_hit)
        self.assertIsNotNone(weekly_hit)
        self.assertIsNotNone(daily_hit)
        assert volume_hit is not None and weekly_hit is not None and daily_hit is not None
        self.assertEqual(volume_hit.volume, 9_100_000)
        self.assertEqual(weekly_hit.weekly_change_pct, 25.0)
        self.assertEqual(daily_hit.daily_change_pct, 4.17)
        self.assertEqual(build_stockbee_movers_watchlist([daily_hit])[0]["setup_label"], "Stockbee 4% Daily Movers")

    def test_profile_returns_no_hit_when_threshold_is_not_met(self) -> None:
        frame = self.frame.copy()
        frame.iloc[-1, frame.columns.get_loc("Volume")] = 8_999_999
        self.assertIsNone(evaluate_stockbee_mover_frame(frame, ticker=self.ticker, profile="stockbee_9m_movers"))


if __name__ == "__main__":
    unittest.main()
