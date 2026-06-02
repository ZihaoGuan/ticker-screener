from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.webapp.services.earnings_calendar_service import EarningsCalendarService, IMPLIED_MOVE_CRITERIA_KEY


class _FakeCookstock:
    def __init__(self, events: list[dict[str, object]]) -> None:
        self._events = events

    def fetch_earnings_calendar_watchlist(self, start_date: dt.date, end_date: dt.date):
        _ = (start_date, end_date)
        return list(self._events)


class EarningsCalendarServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        self.service = EarningsCalendarService(
            project_root=Path("/tmp/project"),
            artifacts_dir=self.artifacts_dir,
        )
        self.service._get_universe_index = lambda: {}  # type: ignore[method-assign]
        self.service._load_latest_criteria_meta = lambda: {  # type: ignore[method-assign]
            "available": True,
            "strategy_id": "earnings_weekly_criteria",
            "run_id": 7,
            "run_date": "2026-06-01",
            "matched_tickers": ["AAA"],
            "ticker_details": {
                "AAA": {
                    "passed": True,
                    "criteria": {
                        "institutional_ownership_ge_10": True,
                        "bullish_ma_stack": True,
                        "revenue_yoy_ge_100": True,
                        "latest_eps_negative": True,
                        "eps_improving_last_4": True,
                    },
                    "matched_criteria": [],
                    "not_matched_criteria": [],
                    "pass_mode": "strict",
                    "error": "",
                }
            },
        }

    def test_next_week_calendar_adds_iv_criterion_and_badge(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service._load_implied_move_signals = lambda tickers: {  # type: ignore[method-assign]
            "AAA": {
                "threshold_pct": 7.0,
                "near_earnings": True,
                "matched": True,
                "percent_move": 8.4,
                "status": "ok",
            }
        }

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2))

        entry = payload["days"][1]["before_market"][0]
        self.assertEqual(entry["ticker"], "AAA")
        self.assertEqual(entry["implied_move_signal"]["percent_move"], 8.4)
        self.assertTrue(entry["criteria"]["criteria"][IMPLIED_MOVE_CRITERIA_KEY])
        self.assertTrue(entry["criteria"]["passed"])
        self.assertEqual(payload["criteria_filter"]["matched_count"], 1)

    def test_only_criteria_filters_out_entry_when_iv_rule_fails(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service._load_implied_move_signals = lambda tickers: {  # type: ignore[method-assign]
            "AAA": {
                "threshold_pct": 7.0,
                "near_earnings": True,
                "matched": False,
                "percent_move": 5.2,
                "status": "ok",
            }
        }

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2), only_criteria=True)

        self.assertEqual(payload["days"][1]["before_market"], [])
        self.assertEqual(payload["criteria_filter"]["matched_count"], 0)


if __name__ == "__main__":
    unittest.main()
