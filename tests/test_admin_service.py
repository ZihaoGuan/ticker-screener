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
