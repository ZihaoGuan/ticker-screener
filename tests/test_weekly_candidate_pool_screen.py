from __future__ import annotations

import unittest
from unittest.mock import patch
from pathlib import Path
import tempfile

import pandas as pd

from src.config import AppConfig
from src.universe import UniverseTicker
from src.screener_catalog import build_screener_catalog
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService
from src.weekly_candidate_pool_screen import (
    evaluate_weekly_candidate_pool,
    find_weekly_candidate_pool_hit,
    run_weekly_candidate_pool_screen,
)


def _candidate_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-02", periods=300, freq="B")
    close = [12.0 + (index_value * (13.0 / 299.0)) for index_value in range(len(index))]
    return pd.DataFrame(
        {
            "High": [value + 0.65 for value in close],
            "Low": [value - 0.65 for value in close],
            "Close": close,
        },
        index=index,
    )


class WeeklyCandidatePoolScreenTests(unittest.TestCase):
    def test_returns_candidate_only_when_all_six_filters_pass(self) -> None:
        frame = _candidate_frame()
        hit = find_weekly_candidate_pool_hit(
            frame,
            ticker=UniverseTicker(symbol="NVDA"),
            daily_rs_rating=91.0,
            signal_date=frame.index[-1].date(),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.current_price, 10.0)
        self.assertGreater(hit.adr_pct_20, 4.0)
        self.assertGreater(hit.daily_rs_rating, 90.0)
        self.assertGreater(hit.current_price, hit.ema50)
        self.assertGreater(hit.ema10, hit.ema20)
        self.assertGreaterEqual(hit.distance_from_52wk_low_pct, 70.0)

    def test_daily_rs_must_be_strictly_above_90(self) -> None:
        snapshot = evaluate_weekly_candidate_pool(_candidate_frame(), daily_rs_rating=90.0)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["daily_rs_above_90"])

    def test_run_uses_persisted_daily_rs_rating_with_database_prices(self) -> None:
        frame = _candidate_frame()
        ticker = UniverseTicker(symbol="NVDA", sector="Technology")
        with patch("src.weekly_candidate_pool_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.weekly_candidate_pool_screen.load_many_ticker_windows", return_value={"NVDA": frame}
        ), patch(
            "src.weekly_candidate_pool_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"NVDA": {"daily_rs_rating": 91.0}},
        ), patch("src.weekly_candidate_pool_screen.load_configured_cookstock") as load_cookstock:
            result = run_weekly_candidate_pool_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        self.assertIn("weekly_candidate_pool", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            run_service = RunService(project_root=Path(directory))
            action_ids = {item["id"] for item in run_service.list_actions()}
            schedule_action_ids = {
                item["id"]
                for item in ScheduledJobService(project_root=Path(directory), run_service=run_service).get_context()["available_actions"]
            }

        self.assertIn("weekly_candidate_pool", action_ids)
        self.assertIn("weekly_candidate_pool", schedule_action_ids)


if __name__ == "__main__":
    unittest.main()
