from __future__ import annotations

import datetime as dt
import unittest

from src.ratings.calculator import build_technical_rating
from src.ratings.models import TechnicalSnapshotInput


class TechnicalRatingsCalculatorTests(unittest.TestCase):
    def test_missing_required_metrics_returns_missing_status(self) -> None:
        snapshot = TechnicalSnapshotInput(ticker="NVDA", as_of_date=dt.date(2026, 6, 14))
        rating = build_technical_rating(snapshot)
        self.assertEqual(rating.technical_status, "missing_metrics")
        self.assertIsNone(rating.overall_rating)
        self.assertIn("close", rating.missing_metric_names)

    def test_constructive_leader_scores_strong(self) -> None:
        snapshot = TechnicalSnapshotInput(
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 14),
            close=141.0,
            atr20=4.5,
            sma20=136.0,
            sma50=129.0,
            sma100=118.0,
            sma200=103.0,
            sma20_5d_ago=133.0,
            sma50_10d_ago=126.0,
            sma100_10d_ago=116.0,
            sma200_20d_ago=100.0,
            sma50_20d_ago=124.0,
            daily_rs_rating=96.0,
            weekly_rs_rating=93.0,
            rs_line=1.35,
            rs_line_sma50=1.23,
            rs_line_3m_high=1.35,
            rs_line_12m_high=1.35,
            high_52w=145.0,
            low_52w=72.0,
            tr_10d_avg=4.2,
            tr_20d_avg=5.0,
            close_above_bar_midpoint_count_10d=8,
            up_down_volume_ratio_20d=1.45,
            breakout_volume_ratio=1.7,
            distribution_day_count_20d=1,
        )
        rating = build_technical_rating(snapshot)
        self.assertEqual(rating.technical_status, "ok")
        self.assertGreaterEqual(rating.overall_rating or 0.0, 95.0)
        self.assertEqual(rating.rating_band, "elite")
        self.assertIn("ma_stack_bullish", rating.flags)
        self.assertIn("rs_leader", rating.flags)

    def test_escape_extension_penalizes_even_when_trend_is_up(self) -> None:
        snapshot = TechnicalSnapshotInput(
            ticker="APP",
            as_of_date=dt.date(2026, 6, 14),
            close=220.0,
            atr20=8.0,
            sma20=175.0,
            sma50=150.0,
            sma100=130.0,
            sma200=110.0,
            sma20_5d_ago=165.0,
            sma50_10d_ago=145.0,
            sma100_10d_ago=128.0,
            sma200_20d_ago=108.0,
            sma50_20d_ago=142.0,
            daily_rs_rating=97.0,
            weekly_rs_rating=94.0,
            rs_line=1.5,
            rs_line_sma50=1.32,
            rs_line_3m_high=1.5,
            rs_line_12m_high=1.5,
            high_52w=222.0,
            low_52w=60.0,
            tr_10d_avg=7.8,
            tr_20d_avg=7.0,
            close_above_bar_midpoint_count_10d=8,
            up_down_volume_ratio_20d=1.6,
            breakout_volume_ratio=2.0,
            distribution_day_count_20d=0,
        )
        rating = build_technical_rating(snapshot)
        self.assertEqual(rating.technical_status, "ok")
        self.assertEqual(rating.divergence_health_score, 0.0)
        self.assertLess(rating.overall_rating or 100.0, 60.0)
        self.assertIn("extended", rating.flags)
        self.assertIn("escape_risk", rating.flags)


if __name__ == "__main__":
    unittest.main()
