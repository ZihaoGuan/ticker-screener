from __future__ import annotations

import datetime as dt
from contextlib import contextmanager, nullcontext
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
             patch.object(split_refresh, "_known_tickers", return_value={**{ticker: "NASDAQ" for ticker in excluded}, "GOOD": "NASDAQ"}), \
             patch.object(split_refresh, "_latest_completed_session", return_value=dt.date(2026, 10, 23)), \
             patch.object(split_refresh, "load_app_config"), \
             patch.object(split_refresh, "load_excluded_tickers", return_value=excluded), \
             patch.object(split_refresh, "_write_state") as write_state, \
             patch.object(split_refresh, "_refresh_ticker", return_value={
                 "ticker": "GOOD", "status": "failed", "rows_persisted": 0,
                 "reason": "post_split_price_basis_mismatch",
             }) as refresh:
            self.assertEqual(split_refresh.main(), 1)
        self.assertEqual([call.args[0] for call in refresh.call_args_list], ["GOOD"])
        self.assertEqual(refresh.call_args.kwargs["exchange"], "NASDAQ")
        saved = {event["ticker"]: event for event in write_state.call_args.args[1]}
        for ticker in excluded:
            self.assertEqual(saved[ticker]["status"], "ignored_excluded")
            self.assertIn("excluded", saved[ticker]["message"].lower())
        self.assertEqual(saved["GOOD"]["status"], "failed")
        self.assertEqual(saved["GOOD"]["message"], "post_split_price_basis_mismatch")

    def test_split_rebase_requires_full_coverage_latest_session_and_post_split_basis(self) -> None:
        rows = [
            {"trade_date": "2026-10-22", "close": 100},
            {"trade_date": "2026-10-23", "close": 100},
        ]
        candles = {day: {"close": 100} for day in ("2026-10-22", "2026-10-23")}
        self.assertEqual(split_refresh._plan_split_rebase(rows, candles, "2026-10-23", "2026-10-23"),
                         ["2026-10-22", "2026-10-23"])
        with self.assertRaisesRegex(ValueError, "history_coverage_loss"):
            split_refresh._plan_split_rebase(rows, {"2026-10-23": candles["2026-10-23"]}, "2026-10-23", "2026-10-23")
        with self.assertRaisesRegex(ValueError, "latest_completed_session_missing"):
            split_refresh._plan_split_rebase(rows[:1], {"2026-10-22": candles["2026-10-22"]}, "2026-10-22", "2026-10-23")
        with self.assertRaisesRegex(ValueError, "post_split_price_basis_mismatch"):
            split_refresh._plan_split_rebase(rows, {**candles, "2026-10-23": {"close": 80}}, "2026-10-23", "2026-10-23")

    def test_split_rebase_writer_preserves_actions_and_verifies_exact_rows(self) -> None:
        baseline = [{
            "ticker": "GOOD", "trade_date": "2026-10-22", "open": 99, "high": 102, "low": 98,
            "close": 100, "adj_close": 96, "volume": 1000, "dividend": 0.25, "split_factor": 1,
            "source": "yfinance", "updated_at": None,
        }]
        candles = {
            "2026-10-22": {"open": 99, "high": 102, "low": 98, "close": 100, "volume": 1000},
            "2026-10-23": {"open": 100, "high": 103, "low": 99, "close": 101, "volume": 1200},
        }
        after = [
            {**baseline[0], "adj_close": 100, "source": "tradingview"},
            {"ticker": "GOOD", "trade_date": "2026-10-23", **candles["2026-10-23"],
             "adj_close": 101, "dividend": 0, "split_factor": 1, "source": "tradingview", "updated_at": None},
        ]

        class Cursor:
            rowcount = 0

            def __init__(self):
                self.daily_reads = 0

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, sql, params=()):
                self.sql = sql
                if sql.startswith("SELECT ticker,trade_date"):
                    self.daily_reads += 1

            def fetchone(self):
                return {"is_active": True}

            def fetchall(self):
                return baseline if self.daily_reads == 1 else after

            def executemany(self, sql, rows):
                self.upsert_sql = sql
                self.rows = rows
                self.rowcount = len(rows)

        class Connection:
            def __init__(self):
                self.cursor_obj = Cursor()

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def transaction(self):
                return nullcontext()

            @contextmanager
            def cursor(self):
                yield self.cursor_obj

        connection = Connection()
        with patch.object(split_refresh, "_connect_rows", return_value=connection), \
             patch.object(split_refresh, "load_excluded_tickers", return_value=set()), \
             patch.object(split_refresh, "is_excluded_ticker", return_value=False):
            persisted, verified = split_refresh._apply_split_rebase(
                "postgresql://unused", "GOOD", baseline, candles, object(), "2026-10-22", "2026-10-23")

        self.assertEqual(persisted, 2)
        self.assertEqual(verified, after)
        self.assertEqual(after[0]["adj_close"], after[0]["close"])
        self.assertEqual(after[0]["dividend"], baseline[0]["dividend"])
        self.assertEqual(after[0]["split_factor"], baseline[0]["split_factor"])
        update_clause = connection.cursor_obj.upsert_sql.split("DO UPDATE SET", 1)[1]
        self.assertIn("adj_close=EXCLUDED.adj_close", update_clause)
        self.assertNotIn("dividend", update_clause)
        self.assertNotIn("split_factor", update_clause)

    def test_nonzero_tradingview_client_exit_is_recorded_without_writing(self) -> None:
        baseline = [{"trade_date": "2026-10-22", "close": 100}]
        with patch.object(split_refresh, "_connect_rows", return_value=nullcontext(object())), \
             patch.object(split_refresh, "_load_daily_rows", return_value=baseline), \
             patch.object(split_refresh.subprocess, "run", return_value=SimpleNamespace(
                 returncode=2, stdout='{"error_code":"PROVIDER_ERROR"}',
             )) as run, \
             patch.object(split_refresh, "_apply_split_rebase", side_effect=AssertionError("must not write")):
            outcome = split_refresh._refresh_ticker(
                "GOOD", "2026-10-23", "2026-10-23", "postgresql://unused", Path("unused.json"),
                exchange="NASDAQ", config=object(), client_dir=Path("/client"),
            )
        self.assertEqual(outcome["reason"], "tradingview_client_exit_2")
        self.assertEqual(outcome["rows_persisted"], 0)
        self.assertEqual(run.call_args.args[0][3], "NASDAQ:GOOD")

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
