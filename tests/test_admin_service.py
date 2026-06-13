from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

from src.webapp.services.admin_service import AdminService, _build_missing_ranges


class AdminServiceTests(unittest.TestCase):
    def test_get_context_reports_missing_database_url(self) -> None:
        service = AdminService(database_url="")

        payload = service.get_context()

        self.assertFalse(payload["database_status"]["database_configured"])
        self.assertIn("DATABASE_URL", payload["database_status"]["notes"][0])

    def test_get_context_reports_coverage_gaps(self) -> None:
        service = AdminService(database_url="postgres://example")
        with patch.object(service, "_load_exclusions", return_value=["ZZZ"]), patch.object(
            service,
            "_load_target_tickers",
            return_value=["AAPL", "MSFT", "NVDA"],
        ), patch.object(
            service,
            "_query_db_stats",
            return_value=(
                {
                    "AAPL": {
                        "first_trade_date": "2020-01-01",
                        "last_trade_date": "2026-05-31",
                        "bar_count": 1600,
                    },
                    "MSFT": {
                        "first_trade_date": "2021-02-01",
                        "last_trade_date": "2026-05-31",
                        "bar_count": 1200,
                    },
                },
                {
                    "overall_first_trade_date": "2020-01-01",
                    "overall_last_trade_date": "2026-05-31",
                    "total_bar_rows": 2800,
                    "latest_metadata_update_at": "2026-05-31T12:00:00+00:00",
                },
            ),
        ):
            payload = service.get_context(coverage_start="2020-01-01")

        self.assertEqual(payload["excluded_count"], 1)
        db = payload["database_status"]
        self.assertEqual(db["target_universe_count"], 3)
        self.assertEqual(db["covered_ticker_count"], 1)
        self.assertEqual(db["partial_ticker_count"], 1)
        self.assertEqual(db["missing_ticker_count"], 1)
        self.assertEqual(db["coverage_percent"], 33.3)
        self.assertEqual(db["sample_missing_tickers"], [{"ticker": "NVDA"}])
        self.assertEqual(db["sample_partial_tickers"], [{"ticker": "MSFT"}])

    def test_get_ratings_status_reports_missing_database_url(self) -> None:
        service = AdminService(database_url="")

        payload = service.get_ratings_status()

        self.assertFalse(payload["database_configured"])
        self.assertIn("DATABASE_URL", payload["notes"][0])

    def test_get_ratings_status_summarizes_non_ok_tickers(self) -> None:
        service = AdminService(database_url="postgres://example")
        with patch.object(service, "_load_target_tickers", return_value=["AAPL", "MSFT", "NVDA"]), patch.object(
            service,
            "_query_ratings_status",
            return_value={
                "latest_fundamentals_as_of_date": "2026-06-13",
                "latest_fundamentals_updated_at": "2026-06-13T10:00:00+00:00",
                "latest_baselines_as_of_date": "2026-06-13",
                "latest_baselines_updated_at": "2026-06-13T10:05:00+00:00",
                "latest_ratings_as_of_date": "2026-06-13",
                "latest_ratings_updated_at": "2026-06-13T10:06:00+00:00",
                "latest_fundamentals_snapshot_count": 3,
                "latest_rating_snapshot_count": 2,
                "latest_fundamentals_parse_status_counts": {"ok": 2, "scrape_failed": 1},
                "latest_rating_status_counts": {"missing_metrics": 1, "ok": 1},
                "tickers_with_any_fundamentals": 3,
                "tickers_with_latest_ok_rating": 1,
                "diagnostics_count": 2,
                "diagnostic_category_counts": {"missing_metrics": 1, "scrape_failed": 1},
                "diagnostics": [
                    {"ticker": "MSFT", "category": "missing_metrics", "reason": "One or more required rating metrics are missing."},
                    {"ticker": "NVDA", "category": "scrape_failed", "reason": "Finviz block/captcha detected."},
                ],
            },
        ), patch.object(
            service.history_repository,
            "list_remote_workers",
            return_value=[
                {
                    "worker_name": "worker-a",
                    "status": "idle",
                    "is_healthy": True,
                    "current_job_run_id": None,
                    "last_heartbeat_at": "2026-06-13T10:10:00+00:00",
                    "updated_at": "2026-06-13T10:10:00+00:00",
                }
            ],
        ):
            payload = service.get_ratings_status()

        self.assertTrue(payload["database_configured"])
        self.assertEqual(payload["target_universe_count"], 3)
        self.assertEqual(payload["tickers_with_latest_ok_rating"], 1)
        self.assertEqual(payload["diagnostics_count"], 2)
        self.assertEqual(payload["diagnostic_category_counts"], {"missing_metrics": 1, "scrape_failed": 1})
        self.assertEqual(payload["diagnostics"][0]["ticker"], "MSFT")
        self.assertEqual(payload["healthy_remote_worker_count"], 1)
        self.assertEqual(payload["remote_workers"][0]["worker_name"], "worker-a")

    def test_build_missing_ranges_combines_edge_windows_and_internal_gaps(self) -> None:
        payload = _build_missing_ranges(
            coverage_start=dt.date(2020, 1, 1),
            coverage_end=dt.date(2020, 1, 31),
            first_trade_date=dt.date(2020, 1, 10),
            last_trade_date=dt.date(2020, 1, 24),
            missing_dates=[dt.date(2020, 1, 15), dt.date(2020, 1, 16), dt.date(2020, 1, 21)],
        )

        self.assertEqual(
            payload,
            [
                {"start": "2020-01-01", "end": "2020-01-09", "days": 9},
                {"start": "2020-01-25", "end": "2020-01-31", "days": 7},
                {"start": "2020-01-15", "end": "2020-01-16", "days": 2},
                {"start": "2020-01-21", "end": "2020-01-21", "days": 1},
            ],
        )


if __name__ == "__main__":
    unittest.main()
