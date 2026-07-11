from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.position_action_daily_screen import (
    PositionActionDailyScreenResult,
    PositionActionDailyHit,
    build_position_action_snapshot,
)


def _frame(close_values: list[float]) -> pd.DataFrame:
    index = pd.bdate_range(end=dt.date(2026, 7, 10), periods=len(close_values))
    return pd.DataFrame(
        {
            "Open": [value - 0.5 for value in close_values],
            "High": [value + 1.0 for value in close_values],
            "Low": [value - 1.1 for value in close_values],
            "Close": close_values,
            "Volume": [1_500_000 for _ in close_values],
        },
        index=index,
    )


class PositionActionDailyScreenTests(unittest.TestCase):
    def test_result_to_dict_serializes_decision_row_dates(self) -> None:
        result = PositionActionDailyScreenResult(
            run_date="2026-07-10",
            total_tickers=1,
            passed_tickers=1,
            failed_tickers=[],
            hits=[
                PositionActionDailyHit(
                    ticker="NVDA",
                    sector="Technology",
                    industry="Semiconductors",
                    exchange="NASDAQ",
                    as_of_date="2026-07-10",
                    action="add_position",
                    action_score=78.0,
                    trend_state="healthy",
                    extension_state="normal",
                    close_price=121.7,
                    ema21=121.0,
                    sma50=117.2,
                    sma10w=116.8,
                    atr_dist_21=0.2,
                    atr_dist_10w=1.9,
                    danger_signal_count=0,
                    reason_summary="Trend intact.",
                )
            ],
            decision_rows=[
                {
                    "as_of_date": dt.date(2026, 7, 10),
                    "ticker": "NVDA",
                    "action": "add_position",
                    "evidence_json": {"danger_flags": []},
                }
            ],
        )

        payload = result.to_dict()

        self.assertEqual(payload["decision_rows"][0]["as_of_date"], "2026-07-10")

    def test_snapshot_marks_healthy_pullback_as_add_position(self) -> None:
        close_values = [100.0 + (idx * 0.45) for idx in range(58)] + [121.0, 121.2, 121.1, 121.3, 121.5, 121.7]
        snapshot = build_position_action_snapshot(_frame(close_values), as_of_date=dt.date(2026, 7, 10))

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.action, "add_position")
        self.assertEqual(snapshot.trend_state, "healthy")
        self.assertEqual(snapshot.extension_state, "normal")

    def test_snapshot_marks_extreme_extension_as_trim_reduce(self) -> None:
        close_values = [100.0 + (idx * 0.25) for idx in range(58)] + [118.0, 120.0, 121.0, 122.0, 136.0, 145.0]
        snapshot = build_position_action_snapshot(_frame(close_values), as_of_date=dt.date(2026, 7, 10))

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.action, "trim_reduce")
        self.assertEqual(snapshot.extension_state, "extreme")
        self.assertGreaterEqual(snapshot.danger_signal_count, 1)
