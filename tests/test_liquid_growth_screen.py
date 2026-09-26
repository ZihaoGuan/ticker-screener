from __future__ import annotations

import datetime as dt
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.liquid_growth_screen import evaluate_liquid_growth, run_liquid_growth_screen
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService


def _frame(*, avg_volume: float = 1_000_000.0) -> pd.DataFrame:
    dates = pd.bdate_range("2025-06-02", periods=280)
    close = np.concatenate([np.linspace(50.0, 72.0, 220), np.linspace(72.3, 90.0, 60)])
    return pd.DataFrame(
        {"Close": close, "Volume": np.full(len(close), avg_volume)},
        index=dates,
    )


def _quarterly_income_rows() -> list[dict[str, object]]:
    return [
        {"date": "2026-06-30", "revenue": 160.0, "diluted_eps": 1.60},
        {"date": "2026-03-31", "revenue": 150.0, "diluted_eps": 1.50},
        {"date": "2025-12-31", "revenue": 140.0, "diluted_eps": 1.40},
        {"date": "2025-09-30", "revenue": 130.0, "diluted_eps": 1.30},
        {"date": "2025-06-30", "revenue": 120.0, "diluted_eps": 1.20},
        {"date": "2025-03-31", "revenue": 110.0, "diluted_eps": 1.10},
    ]


class _QuarterlyClient:
    def get_income_statements(self, ticker: str, limit: int = 8) -> list[dict[str, object]]:
        return _quarterly_income_rows()


class LiquidGrowthScreenTests(unittest.TestCase):
    def test_matches_liquid_reported_growth_leader(self) -> None:
        snapshot = evaluate_liquid_growth(
            _frame(),
            market_cap=5_000_000_000.0,
            roe_pct=22.0,
            gross_margin_pct=55.0,
            operating_margin_pct=20.0,
            daily_rs_rating=92.0,
            quarterly_income_rows=_quarterly_income_rows(),
            as_of_date=dt.date(2026, 7, 1),
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertGreaterEqual(snapshot.quarterly_revenue_yoy_pct, 25.0)
        self.assertGreaterEqual(snapshot.quarterly_eps_yoy_pct, 25.0)
        self.assertTrue(snapshot.criteria["sma50_rising_20_sessions"])
        self.assertTrue(snapshot.criteria["sma200_rising_20_sessions"])

    def test_fails_when_reported_quarterly_revenue_growth_is_below_threshold(self) -> None:
        rows = _quarterly_income_rows()
        rows[0] = {"date": "2026-06-30", "revenue": 140.0, "diluted_eps": 1.60}
        snapshot = evaluate_liquid_growth(
            _frame(),
            market_cap=5_000_000_000.0,
            roe_pct=22.0,
            gross_margin_pct=55.0,
            operating_margin_pct=20.0,
            daily_rs_rating=92.0,
            quarterly_income_rows=rows,
            as_of_date=dt.date(2026, 7, 1),
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["quarterly_revenue_yoy_gte_25pct"])

    def test_run_prefilters_persisted_fundamentals_before_yahoo_quarterly_lookup(self) -> None:
        ticker = UniverseTicker(symbol="TEST", sector="Technology")
        with patch("src.liquid_growth_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.liquid_growth_screen.load_many_ticker_windows", return_value={"TEST": _frame()}
        ), patch(
            "src.liquid_growth_screen.load_ticker_metadata_map", return_value={"TEST": {"exchange": "NASDAQ"}}
        ), patch(
            "src.liquid_growth_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"TEST": {"market_cap": 5_000_000_000.0, "roe_pct": 22.0, "gross_margin_pct": 55.0, "operating_margin_pct": 20.0}},
        ), patch(
            "src.liquid_growth_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"TEST": {"daily_rs_rating": 92.0}},
        ), patch("src.liquid_growth_screen.load_configured_cookstock") as load_cookstock:
            result = run_liquid_growth_screen(
                AppConfig(),
                [ticker],
                as_of_date=dt.date(2026, 7, 1),
                quarterly_client=_QuarterlyClient(),
            )

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        self.assertIn("liquid_growth", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            service = RunService(project_root=Path(directory))
            action_ids = {item["id"] for item in service.list_actions()}
            scheduled_action_ids = {
                item["id"]
                for item in ScheduledJobService(project_root=Path(directory), run_service=service).get_context()["available_actions"]
            }
        self.assertIn("liquid_growth", action_ids)
        self.assertIn("liquid_growth", scheduled_action_ids)


if __name__ == "__main__":
    unittest.main()
