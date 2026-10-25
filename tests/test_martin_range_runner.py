from __future__ import annotations

import datetime as dt
import sys
import types
import unittest
from unittest import mock

from scripts import replay_martin_range as runner
from scripts.replay_martin_gap_retest import _history_window
from src.webapp.services.martin_forward_test_service import FOCUS_SCANNERS


class MartinRangeRunnerTests(unittest.TestCase):
    def test_calibration_report_matches_and_flags_material_snapshot_mismatch(self):
        focus_id = sorted(FOCUS_SCANNERS)[0]
        saved_row = {"ticker": "ABC", "market_cap": 2_000_000_000, "day_close": 100,
            "daily_rs_rating": 95, "scanner_ids": [focus_id], "scanners": [{"id": focus_id}],
            "scanner_count": 1, "rank": 1, "trade_readiness": "defined_risk",
            "adr14_pct": 6, "adr_suitability": "suitable", "strike_score": 10,
            "strike_zone": {"state": "ready", "score": 10}, "stop_to_adr": .3,
            "trade_plan": {"source_strategy_id": "ma_pullback_retest", "state": "active",
                "trigger_price": 100, "stop_price": 95, "max_entry_price": 100,
                "reference_risk_pct": 5, "risk_valid": True}}
        saved = {"_snapshot_run_id": 44, "guru_board": {"rows": [saved_row]}}
        matching = {"session_date": "2026-10-23", "ready": True, "rows": [dict(saved_row)]}
        match = runner.compare_reconstructed_snapshot(saved, matching, policy_id="martin_daily_v1")
        self.assertEqual(match["focus_membership"]["mismatch_count"], 0)
        self.assertEqual(match["decision_field_mismatches"]["ticker_mismatch_count"], 0)
        self.assertEqual(match["orders_fills"]["reason"], "calibration_scope_input_parity_only")

        changed = dict(saved_row)
        changed.update(rank=2, trade_readiness="avoid", adr14_pct=7, strike_score=25,
                       strike_zone={"state": "avoid", "score": 25}, scanner_count=2)
        changed["trade_plan"] = {**saved_row["trade_plan"], "trigger_price": 102,
            "stop_price": 96, "max_entry_price": 101}
        changed["scanners"] = [*saved_row["scanners"], {"id": "rmv_tightness"}]
        extra = {"ticker": "XYZ", "scanners": [{"id": "other_focus"}], "rank": 3}
        blocked = {"session_date": "2026-10-23", "ready": False, "rows": [changed, extra]}
        mismatch = runner.compare_reconstructed_snapshot(saved, blocked, policy_id="martin_daily_v1")
        self.assertEqual(mismatch["focus_membership"]["mismatch_count"], 1)
        diff = mismatch["decision_field_mismatches"]["differences"][0]
        self.assertEqual(diff["ticker"], "ABC")
        self.assertTrue({"eligibility", "avoid", "trade_readiness", "entry", "stop", "max_entry",
                         "adr14_pct", "strike_score", "scanner_count", "rank"}.issubset(diff["fields"]))
        self.assertIn("rmv_tightness", diff["source_ids"]["reconstructed"])
        self.assertEqual(mismatch["orders_fills"]["reason"], "reconstruction_blocked")

    def test_reconstructed_ready_session_reaches_dry_run(self):
        day = dt.date(2026, 10, 23)
        class Label:
            def date(self): return day
        class Calendar:
            def sessions_in_range(self, _start, _end): return [Label()]
            def next_session(self, _day): return "2026-10-26"
            def session_open(self, _day): return "2026-10-26T13:30:00Z"
        cursor = mock.MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.return_value = (True,)
        connection = mock.MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value = cursor
        psycopg = types.SimpleNamespace(connect=mock.Mock(return_value=connection),
                                        rows=types.SimpleNamespace(dict_row=object()))
        reconstructed = {"ready": True, "rows": [{"ticker": "ABC"}],
                         "source_fingerprint": "source", "manifest_version": "v1",
                         "manifest_digest": "manifest", "code_fingerprint": "code",
                         "config_fingerprint": "config", "data_cutoff": "2026-10-26T13:30:00Z"}
        with mock.patch.dict(sys.modules, {"psycopg": psycopg}), \
             mock.patch("src.config.load_app_config", return_value=object()), \
             mock.patch("src.webapp.config.load_webapp_config", return_value=types.SimpleNamespace(database_url="test")), \
             mock.patch.object(runner, "xnys_calendar", return_value=Calendar()), \
             mock.patch("src.webapp.services.martin_reconstructed_inputs.reconstruct_price_focus_session", return_value=reconstructed), \
             mock.patch.object(runner, "load_saved_top_hits", return_value={"blocked": "no valid snapshot"}), \
             mock.patch.object(runner, "_check_book_empty"), \
             mock.patch.object(runner, "_replay_sessions", return_value=[{"date": day.isoformat()}]) as replay:
            result = runner.main(["--policy", "account", "--from-date", day.isoformat(),
                                  "--to-date", day.isoformat(), "--input-source", "reconstructed",
                                  "--book-id", "martin_range_review"])
        self.assertEqual(result, 0)
        self.assertEqual(replay.call_args.args[2][day]["snapshot"]["guru_board"]["rows"], [{"ticker": "ABC"}])
        connection.rollback.assert_called_once()

    def test_gap_history_warms_up_from_first_replayed_session(self):
        days = [dt.date(2025, 1, 1) + dt.timedelta(days=index) for index in range(390)]
        class Label:
            def __init__(self, day): self.day = day
            def date(self): return self.day
        class Calendar:
            def sessions_in_range(self, _start, _end): return [Label(day) for day in days]
        first, expected = _history_window(Calendar(), days[325], days[-1])
        self.assertEqual(first, days[0])
        self.assertEqual(expected[-1], days[-1])
        self.assertEqual(len(expected), 390)

    def test_sessions_walk_every_xnys_date_inclusive(self):
        class Label:
            def __init__(self, day): self.day = day
            def date(self): return self.day
        class Calendar:
            def sessions_in_range(self, start, end):
                return [Label(dt.date(2026, 10, 23)), Label(dt.date(2026, 10, 26)), Label(dt.date(2026, 10, 27))]
        with mock.patch.object(runner, "xnys_calendar", return_value=Calendar()):
            sessions = runner.xnys_sessions(dt.date(2026, 10, 23), dt.date(2026, 10, 27))
        self.assertEqual(sessions, [dt.date(2026, 10, 23), dt.date(2026, 10, 26), dt.date(2026, 10, 27)])

    def test_saved_top_hits_requires_exact_date_record_and_returns_provenance(self):
        day = dt.date(2026, 10, 23)
        class Cursor:
            def __init__(self, rows): self.rows = rows
            def execute(self, _sql, params): self.params = params
            def fetchall(self): return self.rows
        class Calendar:
            def session_close(self, _day): return "2026-10-23T20:00:00Z"
            def next_session(self, _day): return "2026-10-26"
            def session_open(self, _day): return "2026-10-26T13:30:00Z"
        calendar = Calendar()
        now = dt.datetime(2026, 10, 27, tzinfo=dt.timezone.utc)
        empty_cursor = Cursor([])
        self.assertEqual(runner.load_saved_top_hits(empty_cursor, day, calendar=calendar, now=now)["blocked"],
                         "no_valid_point_in_time_top_hits_for_exact_session")
        record = {"id": 41, "run_date": day, "created_at": "2026-10-23T21:00:00Z",
                  "result_summary_json": {"target_trading_date": day.isoformat(), "generated_at": "2026-10-23T21:00:00Z",
                    "guru_board": {"rows": [{"ticker": "ABC"}]}}}
        cursor = Cursor([record])
        loaded = runner.load_saved_top_hits(cursor, day, calendar=calendar, now=now)
        self.assertEqual(cursor.params, (day,))
        self.assertEqual(loaded["snapshot"]["target_trading_date"], day.isoformat())
        self.assertEqual(loaded["provenance"]["screen_run_id"], 41)
        self.assertEqual(loaded["snapshot"]["guru_board"]["rows"][0]["ticker"], "ABC")
        early = {**record, "created_at": "2026-10-23T19:59:00Z"}
        self.assertEqual(runner.load_saved_top_hits(Cursor([early]), day, calendar=calendar, now=now)["blocked"],
                         "no_valid_point_in_time_top_hits_for_exact_session")

    def test_forward_range_evaluates_each_session_in_one_book(self):
        days = [dt.date(2026, 10, 23), dt.date(2026, 10, 26)]
        dated = {day: {"snapshot": {"target_trading_date": day.isoformat(), "_snapshot_run_id": index + 1}}
                 for index, day in enumerate(days)}
        service = mock.Mock()
        service.run_session.side_effect = [{"run_id": 10, "partial": False}, {"run_id": 11, "partial": False}]
        with mock.patch("src.webapp.services.martin_rs_replay.ReplayRepository"), \
             mock.patch("src.webapp.services.martin_forward_test_service.MartinForwardTestService", return_value=service):
            result = runner._replay_sessions(object(), days, dated, {}, policy="account",
                book_id="martin_range_account_20261023", database_url="test", from_date=days[0], to_date=days[-1])
        self.assertEqual([item["date"] for item in result], [day.isoformat() for day in days])
        self.assertEqual([call.kwargs["book_id"] for call in service.run_session.call_args_list],
                         ["martin_range_account_20261023"] * 2)
        self.assertEqual([call.kwargs["policy_id"] for call in service.run_session.call_args_list],
                         [runner.POLICIES["account"]] * 2)


if __name__ == "__main__":
    unittest.main()
