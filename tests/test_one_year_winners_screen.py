from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.one_year_winners_screen import (
    evaluate_one_year_winners,
    find_one_year_winners_hit,
    run_one_year_winners_screen,
)
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    index = pd.date_range("2025-01-02", periods=300, freq="B")
    benchmark_returns = [0.001 + ((position % 7) - 3) * 0.001 for position in range(len(index))]
    stock_returns = [0.003 + 1.5 * (value - 0.001) for value in benchmark_returns]
    benchmark_close = [100.0]
    stock_close = [25.0]
    for benchmark_return, stock_return in zip(benchmark_returns[1:], stock_returns[1:]):
        benchmark_close.append(benchmark_close[-1] * (1.0 + benchmark_return))
        stock_close.append(stock_close[-1] * (1.0 + stock_return))
    benchmark = pd.DataFrame(
        {"Close": benchmark_close, "Volume": [20_000_000.0] * len(index)},
        index=index,
    )
    stock = pd.DataFrame(
        {"Close": stock_close, "Volume": [20_000_000.0] * len(index)},
        index=index,
    )
    return stock, benchmark


class OneYearWinnersScreenTests(unittest.TestCase):
    def test_matches_only_when_all_screenshot_filters_pass(self) -> None:
        stock, benchmark = _frames()

        snapshot = evaluate_one_year_winners(
            stock,
            benchmark,
            market_cap=25_000_000_000.0,
            revenue_growth_ttm_yoy_pct=12.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertEqual(snapshot.criteria_passed, 8)
        self.assertGreater(snapshot.one_year_return_pct, 30.0)
        self.assertGreater(snapshot.monthly_dollar_volume, 900_000_000.0)
        self.assertGreater(snapshot.current_price, snapshot.ema100)
        self.assertGreater(snapshot.ema21, snapshot.sma50)

    def test_market_cap_and_revenue_thresholds_are_strict(self) -> None:
        stock, benchmark = _frames()

        snapshot = evaluate_one_year_winners(
            stock,
            benchmark,
            market_cap=10_000_000_000.0,
            revenue_growth_ttm_yoy_pct=0.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["market_cap_gt_10b"])
        self.assertFalse(snapshot.criteria["revenue_growth_ttm_yoy_gt_0"])

    def test_find_hit_exposes_short_and_long_performance_context(self) -> None:
        stock, benchmark = _frames()

        hit = find_one_year_winners_hit(
            stock,
            benchmark,
            ticker=UniverseTicker(symbol="NVDA", sector="Technology"),
            market_cap=25_000_000_000.0,
            revenue_growth_ttm_yoy_pct=12.0,
            signal_date=stock.index[-1].date(),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.one_week_return_pct, 0.0)
        self.assertGreater(hit.one_month_return_pct, 0.0)
        self.assertGreater(hit.one_year_return_pct, hit.one_month_return_pct)

    def test_run_uses_database_prices_benchmark_and_fundamentals(self) -> None:
        stock, benchmark = _frames()
        ticker = UniverseTicker(symbol="NVDA", sector="Technology")
        with patch("src.one_year_winners_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.one_year_winners_screen.load_many_ticker_windows", return_value={"NVDA": stock}
        ), patch(
            "src.one_year_winners_screen.load_ticker_window", return_value=benchmark
        ), patch(
            "src.one_year_winners_screen.load_ticker_metadata_map", return_value={"NVDA": {"exchange": "NASDAQ"}}
        ), patch(
            "src.one_year_winners_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"NVDA": {"market_cap": 25_000_000_000.0, "sales_yoy_ttm_pct": 12.0}},
        ), patch("src.one_year_winners_screen.load_configured_cookstock") as load_cookstock:
            result = run_one_year_winners_screen(
                AppConfig(),
                [ticker],
                as_of_date=stock.index[-1].date(),
            )

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        self.assertIn("one_year_winners", build_screener_catalog(AppConfig()))
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

        self.assertIn("one_year_winners", action_ids)
        self.assertIn("one_year_winners", schedule_action_ids)


if __name__ == "__main__":
    unittest.main()
