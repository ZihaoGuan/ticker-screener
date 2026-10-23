from __future__ import annotations

import copy
import datetime as dt
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import pandas as pd

from src.webapp.services.martin_rs_replay import (
    _BorrowedConnection, choose_snapshot, prepare_replay, apply_replay, ensure_empty_book,
)
from src.webapp.services.martin_forward_test_service import MartinForwardTestService, RS_EXPANDED_STRATEGY_ID
from tests.test_martin_forward_test_service import _ForwardRepo


class Calendar:
    # Test-only deterministic sessions; production uses the XNYS exchange calendar.
    def sessions_in_range(self, start, end):
        return pd.bdate_range(start, end)

    def session_close(self, day):
        return pd.Timestamp(day).tz_localize("UTC") + pd.Timedelta(hours=20)

    def next_session(self, day):
        return pd.Timestamp(day) + pd.offsets.BDay()

    def session_open(self, day):
        return pd.Timestamp(day).tz_localize("UTC") + pd.Timedelta(hours=13, minutes=30)


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.start, self.end = dt.date(2026, 10, 19), dt.date(2026, 10, 20)
        self.now = dt.datetime(2026, 10, 23, tzinfo=dt.timezone.utc)
        self.calendar = Calendar()
        index = pd.bdate_range(end=self.end, periods=300)
        self.frame = pd.DataFrame({"Open": 100., "High": 102., "Low": 99., "Close": 101., "Volume": 1e6}, index=index)
        self.frames = {"ABC": self.frame, "SPY": self.frame.copy()}
        self.records = [self.record(self.start, 1), self.record(self.end, 2)]

    def record(self, day, run_id):
        generated = f"{day}T21:00:00+00:00"
        return {"id": run_id, "created_at": generated, "result_summary_json": {
            "target_trading_date": str(day), "generated_at": generated, "source_fingerprint": f"original{run_id}",
            "guru_board": {"rows": [{"ticker": "ABC", "scanner_ids": ["rs"], "daily_rs_rating": 95,
                "market_cap": 2e9, "close": 101, "adr14_pct": 4, "trade_readiness": "defined_risk",
                "pullback_quality": {"state": "neutral"}, "trade_plan": {"source_strategy_id": "ema21_pullback_buy",
                    "state": "active", "signal_date": str(day), "trigger_price": 100, "stop_price": 97,
                    "max_entry_price": 97 / .95}}]}}}

    def prepare(self, records=None, frames=None):
        return prepare_replay(self.start, self.end, records if records is not None else self.records,
                              frames if frames is not None else self.frames, self.calendar, self.now)

    def test_chronological_copy_policy_and_provenance(self):
        original = copy.deepcopy(self.records)
        snapshots, report = self.prepare(list(reversed(self.records)))
        self.assertTrue(report["ready"])
        self.assertEqual([item["target_trading_date"] for item in snapshots], [str(self.start), str(self.end)])
        row = snapshots[0]["guru_board"]["rows"][0]
        self.assertEqual(row["trade_plans"]["martin_daily_structural_v1"]["trade_plan"]["stop_price"], 97)
        self.assertEqual(snapshots[0]["replay_provenance"]["origin"], "historical_replay")
        self.assertNotEqual(snapshots[0]["source_fingerprint"], "original1")
        self.assertEqual(self.records, original)
        self.assertEqual(self.prepare()[0], snapshots)

    def test_missing_snapshot_is_not_silently_skipped(self):
        _, report = self.prepare(self.records[:1])
        self.assertFalse(report["ready"])
        self.assertEqual(report["errors"][0]["date"], str(self.end))

    def test_missing_adr_derived_only_for_structural_copy(self):
        records = copy.deepcopy(self.records)
        records[0]["result_summary_json"]["guru_board"]["rows"][0]["adr14_pct"] = None
        snapshots, report = self.prepare(records)
        self.assertTrue(report["ready"])
        row = snapshots[0]["guru_board"]["rows"][0]
        self.assertIsNone(row["adr14_pct"])
        self.assertIsNone(row["trade_plans"]["current_v1"]["adr14_pct"])
        self.assertGreater(row["trade_plans"]["martin_daily_structural_v1"]["adr14_pct"], 0)
        self.assertEqual(report["candidate_status_counts"][str(self.start)]["pending"], 1)

    def test_overwritten_payload_old_created_at_is_rejected(self):
        record = copy.deepcopy(self.records[0])
        record["result_summary_json"]["generated_at"] = "2026-10-22T21:00:00Z"
        self.assertIsNone(choose_snapshot([record], self.start, self.calendar.session_close(pd.Timestamp(self.start)),
            self.calendar.session_open(pd.Timestamp(self.end)), self.now))

    def test_timestamp_timezone_future_and_intraday_are_rejected(self):
        for stamp in (None, "2026-10-19T21:00:00", "2026-10-19T19:00:00Z", "2026-10-20T14:00:00Z"):
            record = copy.deepcopy(self.records[0])
            record["result_summary_json"]["generated_at"] = stamp
            _, report = self.prepare([record, self.records[1]])
            self.assertFalse(report["ready"])

    def test_missing_or_invalid_bars_fail_preflight(self):
        for value in (float("nan"), 0, 103):
            broken = self.frame.copy()
            broken.loc[str(self.end), "Low"] = value
            _, report = self.prepare(frames={"ABC": broken, "SPY": self.frame})
            self.assertFalse(report["ready"])
            self.assertTrue(any(item["ticker"] == "ABC" for item in report["errors"]))

    def test_first_day_selects_then_next_session_fills_with_frozen_provenance(self):
        snapshots, _ = self.prepare()
        repo = _ForwardRepo()
        repo.connection = object()
        with patch("src.webapp.services.martin_rs_replay.ensure_empty_book"), \
             patch("src.webapp.services.martin_rs_replay.ReplayRepository", return_value=repo):
            results = apply_replay(repo.connection, snapshots, self.frames)
        self.assertEqual(results[0]["filled"], 0)
        self.assertEqual(results[1]["filled"], 1)
        self.assertEqual(repo.positions[0]["payload_json"]["replay_provenance"]["source_run_id"], 1)
        self.assertEqual({key[0] for key in repo.runs}, {RS_EXPANDED_STRATEGY_ID})

    def test_existing_book_refused_and_borrowed_context_never_commits(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, *_args): pass
            def fetchone(self): return (1,)
        class Connection:
            def cursor(self): return Cursor()
            def commit(self): raise AssertionError("inner commit")
            def rollback(self): raise AssertionError("inner rollback")
        connection = Connection()
        with self.assertRaisesRegex(ValueError, "already has history"):
            ensure_empty_book(connection)
        with _BorrowedConnection(connection) as borrowed:
            self.assertIs(borrowed.cursor().__class__, Cursor)

    def test_partial_apply_raises_for_outer_rollback(self):
        snapshots, _ = self.prepare()
        with patch("src.webapp.services.martin_rs_replay.ensure_empty_book"), \
             patch("src.webapp.services.martin_rs_replay.MartinForwardTestService") as service:
            service.return_value.run_session.return_value = {"run_id": 1, "partial": True}
            with self.assertRaisesRegex(ValueError, "rolled back"):
                apply_replay(object(), snapshots, self.frames)

    def test_daily_job_does_not_reprocess_a_replayed_session(self):
        repo = _ForwardRepo()
        repo.replay_covers_date = lambda *_args: True
        result = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: self.fail("must not load bars")).run_session(
            as_of_date=self.start, snapshot=None, strategy_id=RS_EXPANDED_STRATEGY_ID)
        self.assertTrue(result["skipped"])

    def test_cli_defaults_to_read_only_and_apply_is_explicit(self):
        from scripts import replay_martin_rs_expanded as cli
        for args, apply in (([], False), (["--dry-run"], False), (["--apply"], True)):
            with patch.object(cli, "load_webapp_config") as config, patch.object(cli, "run_database_replay") as run, patch("builtins.print"):
                config.return_value.database_url = "postgres://test"
                run.return_value = {"ready": True}
                self.assertEqual(cli.main(["--from-date", str(self.start), "--to-date", str(self.end), *args]), 0)
                self.assertEqual(run.call_args.kwargs["apply"], apply)

    def test_database_readonly_mode_and_atomic_rollback_contract(self):
        from src.webapp.services.martin_rs_replay import run_database_replay
        class Connection:
            def __init__(self):
                self.sql, self.committed, self.rolled_back = [], False, False
            def __enter__(self): return self
            def __exit__(self, kind, *_args):
                self.committed, self.rolled_back = kind is None, kind is not None
                return False
            def cursor(self): return self
            def execute(self, sql, *_args): self.sql.append(sql)
            def fetchone(self): return (True,) if "advisory" in self.sql[-1] else (0,)
        for apply in (False, True):
            connection = Connection()
            modules = {"psycopg": SimpleNamespace(connect=lambda *_args: connection),
                       "exchange_calendars": SimpleNamespace(get_calendar=lambda *_args, **_kwargs: self.calendar)}
            with patch.dict("sys.modules", modules), \
                 patch("src.webapp.services.martin_rs_replay.MartinForwardTestRepository._fetch", side_effect=[[], []]), \
                 patch("src.webapp.services.martin_rs_replay.prepare_replay", return_value=([{}], {"ready": True})), \
                 patch("src.webapp.services.martin_rs_replay.apply_replay", side_effect=ValueError("rollback required")) as write:
                if apply:
                    with self.assertRaisesRegex(ValueError, "rollback required"):
                        run_database_replay("unused", self.start, self.end, apply=True)
                    self.assertTrue(connection.rolled_back)
                else:
                    result = run_database_replay("unused", self.start, self.end)
                    self.assertFalse(result["saved"])
                    write.assert_not_called()
                    self.assertIn("READ ONLY", connection.sql[0])
                    self.assertFalse(any("CREATE" in sql or "INSERT" in sql or "UPDATE" in sql for sql in connection.sql))


if __name__ == "__main__":
    unittest.main()
