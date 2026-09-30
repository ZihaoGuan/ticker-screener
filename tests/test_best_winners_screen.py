from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.best_winners_screen import evaluate_best_winners, find_best_winners_hit, run_best_winners_screen
from src.config import AppConfig
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService


def _winner_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-02", periods=300, freq="B")
    close = [4.0]
    for _ in index[1:]:
        close.append(close[-1] * 1.005)
    return pd.DataFrame(
        {
            "High": [value * 1.03 for value in close],
            "Low": [value * 0.97 for value in close],
            "Close": close,
            "Volume": [4_000_000.0] * len(index),
        },
        index=index,
    )


class BestWinnersScreenTests(unittest.TestCase):
    def test_matches_only_when_all_ten_filters_pass(self) -> None:
        snapshot = evaluate_best_winners(_winner_frame())

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertEqual(snapshot.criteria_passed, 10)
        self.assertGreater(snapshot.current_price, 1.0)
        self.assertGreater(snapshot.adr_pct_20, 4.5)
        self.assertGreaterEqual(snapshot.distance_from_52wk_low_pct, 70.0)
        self.assertGreater(snapshot.return_3m_pct, 0.0)
        self.assertGreater(snapshot.return_6m_pct, 0.0)
        self.assertGreater(snapshot.return_1y_pct, 0.0)
        self.assertGreater(snapshot.avg_dollar_volume_30, 50_000_000.0)
        self.assertGreater(snapshot.session_dollar_volume, 20_000_000.0)
        self.assertGreater(snapshot.ema8, snapshot.ema21)
        self.assertGreater(snapshot.current_price, snapshot.ema60)

    def test_rejects_non_positive_three_month_performance(self) -> None:
        frame = _winner_frame()
        frame.loc[frame.index[-63]:, "Close"] = frame.loc[frame.index[-64], "Close"]
        frame.loc[frame.index[-63]:, "High"] = frame.loc[frame.index[-63]:, "Close"] * 1.03
        frame.loc[frame.index[-63]:, "Low"] = frame.loc[frame.index[-63]:, "Close"] * 0.97

        snapshot = evaluate_best_winners(frame)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["return_3m_gt_0"])

    def test_find_hit_exposes_momentum_and_liquidity_context(self) -> None:
        frame = _winner_frame()
        hit = find_best_winners_hit(
            frame,
            ticker=UniverseTicker(symbol="NVDA", sector="Technology"),
            signal_date=frame.index[-1].date(),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.return_1y_pct, hit.return_3m_pct)
        self.assertGreater(hit.avg_dollar_volume_30, 50_000_000.0)

    def test_run_uses_database_prices_without_fallback(self) -> None:
        frame = _winner_frame()
        ticker = UniverseTicker(symbol="NVDA", sector="Technology")
        with patch("src.best_winners_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.best_winners_screen.load_many_ticker_windows", return_value={"NVDA": frame}
        ), patch("src.best_winners_screen.load_configured_cookstock") as load_cookstock:
            result = run_best_winners_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        self.assertIn("best_winners", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            run_service = RunService(project_root=Path(directory))
            action_ids = {item["id"] for item in run_service.list_actions()}
            schedule_action_ids = {
                item["id"]
                for item in ScheduledJobService(
                    project_root=Path(directory),
                    run_service=run_service,
                ).get_context()["available_actions"]
            }

        self.assertIn("best_winners", action_ids)
        self.assertIn("best_winners", schedule_action_ids)


if __name__ == "__main__":
    unittest.main()
