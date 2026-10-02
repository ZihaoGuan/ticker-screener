from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.relative_trend_strength import build_leadership_health, build_relative_trend_strength_snapshot


def _frame(start: float, step: float) -> pd.DataFrame:
    index = pd.bdate_range(end=dt.date(2026, 10, 2), periods=100)
    close = [start + (index_value * step) for index_value in range(len(index))]
    return pd.DataFrame({"Close": close}, index=index)


def _accelerating_frame() -> pd.DataFrame:
    index = pd.bdate_range(end=dt.date(2026, 10, 2), periods=100)
    close = [100.0] * 79 + [105.0 + (position * 5.0) for position in range(21)]
    return pd.DataFrame({"Close": close}, index=index)


class RelativeTrendStrengthTests(unittest.TestCase):
    def test_expanding_leader_scores_high_with_sector_context(self) -> None:
        snapshot = build_relative_trend_strength_snapshot(
            _accelerating_frame(),
            _frame(100.0, 0.25),
            ticker="LEAD",
            sector="Technology",
            sector_etf="XLK",
            sector_frame=_frame(100.0, 0.35),
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.rts_state, "expanding")
        self.assertEqual(snapshot.confidence, "high")
        self.assertGreaterEqual(snapshot.rts_score, 80.0)
        self.assertGreater(snapshot.stock_vs_spy_63d_pct or 0.0, 0.0)

    def test_lagging_stock_is_not_mislabeled_as_leadership(self) -> None:
        snapshot = build_relative_trend_strength_snapshot(
            _frame(100.0, -0.15),
            _frame(100.0, 0.3),
            ticker="LAG",
            sector="Technology",
            sector_etf="XLK",
            sector_frame=_frame(100.0, 0.25),
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.rts_state, "lagging")
        self.assertLess(snapshot.rts_score, 40.0)

    def test_sectorless_snapshot_remains_available_with_medium_confidence(self) -> None:
        snapshot = build_relative_trend_strength_snapshot(
            _frame(100.0, 0.8),
            _frame(100.0, 0.2),
            ticker="NOSECTOR",
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.confidence, "medium")
        self.assertIsNone(snapshot.stock_vs_sector_63d_pct)

    def test_leadership_health_requires_three_weak_sessions_for_warning(self) -> None:
        history = [
            {"as_of_date": "2026-09-28", "rts_score": 82, "rts_state": "expanding", "close_price": 100},
            {"as_of_date": "2026-09-29", "rts_score": 70, "rts_state": "contracting", "close_price": 101},
            {"as_of_date": "2026-09-30", "rts_score": 65, "rts_state": "contracting", "close_price": 102},
            {"as_of_date": "2026-10-01", "rts_score": 60, "rts_state": "lagging", "close_price": 103},
        ]

        health = build_leadership_health(history)

        self.assertIsNotNone(health)
        assert health is not None
        self.assertTrue(health["persistent_warning"])
        self.assertEqual(health["deterioration_sessions"], 3)
        self.assertTrue(health["price_high_rts_divergence"])
