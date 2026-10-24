from __future__ import annotations

import datetime as dt
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.refresh_split_adjusted_history import _calendar_start, _merge_events, _parse_calendar_rows
from scripts import refresh_split_adjusted_history as split_refresh


class RefreshSplitAdjustedHistoryTests(unittest.TestCase):
    def test_excluded_events_are_skipped_before_refresh_even_when_tracked(self) -> None:
        excluded = {"LU", "NIVF", "PASW", "HCAI"}
        events = [{"ticker": ticker, "execution_date": "2026-10-23", "ratio": "1:8", "status": "failed"}
                  for ticker in [*sorted(excluded), "GOOD"]]
        args = SimpleNamespace(as_of_date="2026-10-23", lookback_days=0, start_date="2020-01-01",
                               database_url="postgresql://unused", state_path="unused.json")
        with patch.object(split_refresh, "parse_args", return_value=args), \
             patch.object(split_refresh, "_load_state", return_value={"events": events}), \
             patch.object(split_refresh, "_fetch_calendar", return_value=[]), \
             patch.object(split_refresh, "_known_tickers", return_value=excluded | {"GOOD"}), \
             patch.object(split_refresh, "load_app_config"), \
             patch.object(split_refresh, "load_excluded_tickers", return_value=excluded), \
             patch.object(split_refresh, "_write_state") as write_state, \
             patch.object(split_refresh, "_refresh_ticker", return_value={"ticker": "GOOD", "status": "synced", "rows_persisted": 1}) as refresh:
            self.assertEqual(split_refresh.main(), 0)
        self.assertEqual([call.args[0] for call in refresh.call_args_list], ["GOOD"])
        saved = {event["ticker"]: event for event in write_state.call_args.args[1]}
        for ticker in excluded:
            self.assertEqual(saved[ticker]["status"], "ignored_excluded")
            self.assertIn("excluded", saved[ticker]["message"].lower())
        self.assertEqual(saved["GOOD"]["status"], "completed")

    def test_refresh_child_command_does_not_override_exclusions(self) -> None:
        with patch.object(split_refresh.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run, \
             patch.object(Path, "exists", return_value=True), \
             patch.object(Path, "read_text", return_value='{"ticker_results": [{"ticker": "GOOD", "status": "synced", "rows_persisted": 1}]}'):
            self.assertEqual(split_refresh._refresh_ticker("GOOD", "2020-01-01", "2026-10-23", Path("state.json"))["rows_persisted"], 1)
        self.assertNotIn("--include-excluded-tickers", run.call_args.args[0])

    def test_calendar_start_supports_historical_backfill(self) -> None:
        self.assertEqual(_calendar_start(dt.date(2026, 9, 19), 90).isoformat(), "2026-06-21")
        with self.assertRaises(ValueError):
            _calendar_start(dt.date(2026, 9, 19), -1)

    def test_calendar_announcements_are_normalized_and_ratio_corrections_requeue(self) -> None:
        announcements = _parse_calendar_rows(
            {
                "data": {
                    "rows": [
                        {
                            "symbol": "abcd",
                            "name": "Example Corp",
                            "ratio": "1 : 10",
                            "executionDate": "09/22/2026",
                        }
                    ]
                }
            }
        )
        state = {
            "events": [
                {
                    "ticker": "ABCD",
                    "execution_date": "2026-09-22",
                    "ratio": "1 : 5",
                    "status": "completed",
                    "processed_at": "2026-09-21T00:00:00+00:00",
                }
            ]
        }

        events = _merge_events(state, announcements, "2026-09-19T00:00:00+00:00")

        self.assertEqual(events[0]["ticker"], "ABCD")
        self.assertEqual(events[0]["execution_date"], "2026-09-22")
        self.assertEqual(events[0]["ratio"], "1 : 10")
        self.assertEqual(events[0]["status"], "pending")
        self.assertNotIn("processed_at", events[0])


if __name__ == "__main__":
    unittest.main()
