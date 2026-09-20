from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.qullamaggie_screen import evaluate_qullamaggie, find_qullamaggie_hit, run_qullamaggie_screen
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService


def _qullamaggie_frame(*, avg_volume: float = 1_500_000.0) -> pd.DataFrame:
    dates = pd.bdate_range("2025-06-02", periods=280)
    early = np.linspace(42.0, 54.0, 150)
    advance = np.linspace(54.0, 98.0, 120)
    tight_flag = np.array([98.0, 99.0, 98.5, 100.0, 99.5, 101.0, 100.5, 101.5, 100.8, 102.0])
    close = np.concatenate([early, advance, tight_flag])
    return pd.DataFrame(
        {
            "Open": close * 0.995,
            "High": close * 1.025,
            "Low": close * 0.975,
            "Close": close,
            "Volume": np.full(len(close), avg_volume),
        },
        index=dates,
    )


class QullamaggieScreenTest(unittest.TestCase):
    def test_candidate_passes_momentum_trend_liquidity_adr_and_tight_setup(self) -> None:
        snapshot = evaluate_qullamaggie(
            _qullamaggie_frame(),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertGreaterEqual(snapshot.recent_return_pct, 30.0)
        self.assertGreaterEqual(snapshot.avg_volume_20, 1_000_000.0)
        self.assertGreaterEqual(snapshot.adr_pct_20, 4.0)
        self.assertTrue(snapshot.tight_flag or snapshot.pullback_near_fast_ma)

        hit = find_qullamaggie_hit(
            _qullamaggie_frame(),
            ticker=UniverseTicker(symbol="TEST", sector="Technology"),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
            signal_date=dt.date(2026, 6, 26),
        )
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "TEST")
        self.assertEqual(hit.criteria_passed, hit.criteria_total)

    def test_candidate_fails_when_share_liquidity_is_below_one_million(self) -> None:
        snapshot = evaluate_qullamaggie(
            _qullamaggie_frame(avg_volume=900_000.0),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["avg_volume_20_gt_1m"])

    def test_run_uses_database_prices_fundamentals_and_technical_rating(self) -> None:
        frame = _qullamaggie_frame()
        ticker = UniverseTicker(symbol="TEST", sector="Technology")
        with patch("src.qullamaggie_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.qullamaggie_screen.load_many_ticker_windows", return_value={"TEST": frame}
        ), patch(
            "src.qullamaggie_screen.load_ticker_metadata_map", return_value={"TEST": {"exchange": "NASDAQ"}}
        ), patch(
            "src.qullamaggie_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"TEST": {"market_cap": 500_000_000.0}},
        ), patch(
            "src.qullamaggie_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"TEST": {"daily_rs_rating": 92.0}},
        ), patch("src.qullamaggie_screen.load_configured_cookstock") as load_cookstock:
            result = run_qullamaggie_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        self.assertIn("qullamaggie", build_screener_catalog(AppConfig()))
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

        self.assertIn("qullamaggie", action_ids)
        self.assertIn("qullamaggie", schedule_action_ids)

    def test_ad_hoc_service_returns_qullamaggie_hit(self) -> None:
        frame = _qullamaggie_frame()
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"TEST": frame, "SPY": frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"TEST": {"ticker": "TEST", "sector": "Technology", "exchange": "NASDAQ"}},
        ), patch(
            "src.screener_catalog.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"TEST": {"market_cap": 500_000_000.0}},
        ), patch(
            "src.screener_catalog.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"TEST": {"daily_rs_rating": 92.0}},
        ):
            payload = service.run(
                ticker="TEST",
                as_of_date=frame.index[-1].date(),
                screener_ids=["qullamaggie"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "qullamaggie")
        self.assertTrue(payload["screeners"][0]["passed"])


if __name__ == "__main__":
    unittest.main()
