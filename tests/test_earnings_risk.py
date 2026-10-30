import datetime as dt
import json
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.webapp.services.earnings_risk import entry_decision, holdings_decision, weak_cushion_trim


class EarningsRiskTests(unittest.TestCase):
    def test_holdings_policy_exits_below_fifteen_or_adr_and_allows_loss_exit_without_adr(self):
        day = dt.date(2026, 10, 28)
        event = {"date": dt.date(2026, 10, 30), "timing": "before_market"}
        with patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=2):
            loser = holdings_decision(event, day, 100, 99, None)
            missing_adr = holdings_decision(event, day, 100, 105, None)
            below_minimum = holdings_decision(event, day, 100, 114.99, None)
            at_minimum_unknown_adr = holdings_decision(event, day, 100, 115, None)
            weak = holdings_decision(event, day, 100, 114, 12)
            sufficient = holdings_decision(event, day, 100, 115, 12)
            high_adr_exit = holdings_decision(event, day, 100, 120, 25)
            high_adr_exact = holdings_decision(event, day, 100, 120, 20)
            unknown = holdings_decision(None, day, 100, 99, None)
        self.assertEqual(loser["action"], "exit_all")
        self.assertFalse(loser["adr14_known"])
        self.assertEqual(missing_adr["status"], "unassessable_adr14")
        self.assertEqual(missing_adr["action"], "exit_all")
        self.assertEqual(below_minimum["action"], "exit_all")
        self.assertEqual(at_minimum_unknown_adr["status"], "unassessable_adr14")
        self.assertEqual(at_minimum_unknown_adr["action"], "normal_exits")
        self.assertEqual(weak["action"], "exit_all")
        self.assertEqual(weak["cushion_threshold_pct"], 15)
        self.assertEqual(sufficient["action"], "normal_exits")
        self.assertEqual(high_adr_exit["action"], "exit_all")
        self.assertEqual(high_adr_exit["cushion_threshold_pct"], 25)
        self.assertEqual(high_adr_exact["action"], "normal_exits")
        self.assertEqual(unknown["status"], "unknown_earnings")
        self.assertFalse(unknown["earnings_known"])

    def test_unknown_date_blocks_entry_in_retrospective_mode(self):
        decision = entry_decision(None, dt.date(2026, 10, 26))
        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["status"], "unknown")
        self.assertEqual(decision["reason"], "earnings_date_unknown")
        self.assertTrue(decision["retrospective_lookup"])

    def test_three_trading_session_window_blocks_but_four_does_not(self):
        day = dt.date(2026, 10, 26)  # Monday
        with patch("src.webapp.services.earnings_risk.sessions_to_event", side_effect=lambda _day, event: (event - day).days):
            within = entry_decision({"date": dt.date(2026, 10, 29), "timing": "before_market"}, day)
            outside = entry_decision({"date": dt.date(2026, 10, 30), "timing": "after_market"}, day)
        self.assertFalse(within["allowed"])
        self.assertEqual(within["trading_sessions_to_earnings"], 3)
        self.assertTrue(outside["allowed"])
        self.assertEqual(outside["trading_sessions_to_earnings"], 4)

    def test_holdings_v2_entry_decision_uses_its_own_evidence_mode(self):
        day = dt.date(2026, 10, 26)
        with patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=3):
            blocked = entry_decision({"date": dt.date(2026, 10, 29)}, day,
                evidence_mode="earnings_holdings_retrospective_v2")
            unknown = entry_decision(None, day, evidence_mode="earnings_holdings_retrospective_v2")
        self.assertFalse(blocked["allowed"])
        self.assertEqual(blocked["reason"], "within_three_sessions_of_earnings")
        self.assertEqual(blocked["evidence_mode"], "earnings_holdings_retrospective_v2")
        self.assertTrue(unknown["allowed"])
        self.assertEqual(unknown["status"], "unknown")
        self.assertEqual(unknown["evidence_mode"], "earnings_holdings_retrospective_v2")

    def test_weekend_report_date_counts_to_next_xnys_session(self):
        import pandas as pd
        class Calendar:
            sessions = pd.date_range("2026-10-20", "2026-11-10", freq="B", tz="UTC")
        fake = SimpleNamespace(get_calendar=lambda *_args, **_kwargs: Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake}):
            from src.webapp.services.earnings_risk import sessions_to_event
            self.assertEqual(sessions_to_event(dt.date(2026, 10, 29), dt.date(2026, 10, 31)), 2)

    def test_holiday_endpoint_uses_available_sessions_and_marks_out_of_range_unknown(self):
        import pandas as pd
        from src.webapp.services.earnings_risk import entry_decision
        class Calendar:
            sessions = pd.date_range("2026-01-16", "2026-02-13", freq="B", tz="UTC")
        fake = SimpleNamespace(get_calendar=lambda *_args, **_kwargs: Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake}):
            from src.webapp.services.earnings_risk import sessions_to_event
            self.assertGreater(sessions_to_event(dt.date(2026, 1, 23), dt.date(2026, 2, 9)), 3)
            decision = entry_decision({"date": dt.date(2026, 3, 2)}, dt.date(2026, 1, 23))
        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["status"], "unassessable")

    def test_weak_trim_is_scheduled_early_enough_for_release_timing(self):
        day = dt.date(2026, 10, 28)
        with patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=2):
            bmo = weak_cushion_trim({"date": dt.date(2026, 10, 30), "timing": "before_market"}, day, 100, 111, 12)
        self.assertTrue(bmo["weak"])
        self.assertEqual(bmo["cushion_threshold_pct"], 12)
        self.assertEqual(bmo["execution"], "next_session_open")
        self.assertTrue(bmo["retrospective_lookup"])
        with patch("src.webapp.services.earnings_risk.sessions_to_event", side_effect=lambda date, _event: 1 if date.day == 29 else 2):
            amc = weak_cushion_trim({"date": dt.date(2026, 10, 30), "timing": "after_market"}, dt.date(2026, 10, 29), 100, 111, 12)
            too_early = weak_cushion_trim({"date": dt.date(2026, 10, 30), "timing": "after_market"},
                dt.date(2026, 10, 28), 100, 111, 12)
        self.assertTrue(amc["weak"])
        self.assertIsNone(too_early)
        with patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=2):
            unknown_timing = weak_cushion_trim({"date": dt.date(2026, 10, 30)}, day, 100, 111, 12)
        self.assertIsNotNone(unknown_timing)  # Unknown timing uses the conservative BMO lead.

    def test_missing_current_adr14_is_explicitly_unassessable(self):
        with patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=2):
            result = weak_cushion_trim({"date": dt.date(2026, 10, 30), "timing": "before_market"},
                dt.date(2026, 10, 28), 100, 105, None)
        self.assertEqual(result["status"], "unassessable_adr14")
        self.assertFalse(result["weak"])
        self.assertIsNone(result["cushion_threshold_pct"])

    def test_cushion_threshold_uses_current_adr14(self):
        import pandas as pd
        from src.webapp.services.earnings_risk import adr14_pct
        index = pd.date_range("2026-10-01", periods=14, freq="B")
        frame = pd.DataFrame({"High": 11.0, "Low": 9.0, "Close": 10.0}, index=index)
        self.assertEqual(adr14_pct(frame, index[-1].date()), 20.0)

    def test_bar_retest_executor_flags_unknown_without_blocking(self):
        from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService

        class Repo:
            def __init__(self): self.updated = []
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return []
            def pending_orders(self, **_kwargs): return []
            def update_candidate(self, _run_id, _ticker, candidate):
                self.updated.append(json.loads(json.dumps(candidate)))

        repo = Repo()
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.earnings_policy = "retrospective_v1"
        with patch("src.webapp.services.earnings_risk.load_earnings", return_value={}):
            service.run_session(day=dt.date(2026, 10, 26), run_id=1,
                candidates=[{"ticker": "ABC", "status": "confirmed", "rank": 1,
                    "source_date": dt.date(2026, 10, 25)}], source_on_time=False)
        self.assertEqual(repo.updated[0]["status"], "confirmed")
        self.assertEqual(repo.updated[0]["earnings_risk"]["status"], "unknown")
        self.assertEqual(repo.updated[0]["source_date"], "2026-10-25")

    def test_bar_retest_v2_blocks_new_candidate_and_pending_order_near_earnings(self):
        from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService

        class CandidateRepo:
            def __init__(self): self.updated = []
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return []
            def pending_orders(self, **_kwargs): return []
            def update_candidate(self, _run_id, _ticker, payload): self.updated.append(payload)

        candidate_repo = CandidateRepo()
        candidate_service = MartinBarRetestExecutionService(candidate_repo, bar_loader=lambda *_a, **_k: {})
        candidate_service.earnings_policy = "retrospective_holdings_v2"
        event = {"date": dt.date(2026, 10, 29), "timing": "before_market"}
        with patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": event}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=3):
            candidate_service.run_session(day=dt.date(2026, 10, 26), run_id=1,
                candidates=[{"ticker": "ABC", "status": "confirmed", "rank": 1}], source_on_time=True)
        self.assertEqual(candidate_repo.updated[0]["status"], "blocked_earnings")
        self.assertEqual(candidate_repo.updated[0]["earnings_risk"]["evidence_mode"],
            "earnings_holdings_retrospective_v2")

        class PendingRepo(CandidateRepo):
            def __init__(self): super().__init__(); self.events = []
            def pending_candidates(self, **_kwargs): return [{"id": 7, "ticker": "ABC", "run_id": 1, "payload_json": {}}]
            def apply_event(self, **kwargs): self.events.append(kwargs)

        pending_repo = PendingRepo()
        pending_service = MartinBarRetestExecutionService(pending_repo, bar_loader=lambda *_a, **_k: {})
        pending_service.earnings_policy = "retrospective_holdings_v2"
        with patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": event}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=3):
            pending_service.run_session(day=dt.date(2026, 10, 26), run_id=2, candidates=[], source_on_time=True)
        self.assertEqual(pending_repo.events[0]["event_type"], "order_blocked_earnings")
        self.assertEqual(pending_repo.events[0]["payload"]["evidence_mode"],
            "earnings_holdings_retrospective_v2")

    def test_bar_retest_decides_on_completed_bar_and_fills_half_at_next_open(self):
        from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService

        class Repo:
            def __init__(self, position): self.position = position; self.events = []
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def pending_orders(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return [self.position]
            def set_position_payload(self, _id, payload): self.position = {**self.position, "payload_json": payload}
            def apply_event(self, **kwargs): self.events.append(kwargs)

        event = {"date": dt.date(2026, 10, 30), "timing": "before_market"}
        position = {"id": 1, "ticker": "ABC", "payload_json": {"entry_price": 100,
            "remaining_shares": 10, "initial_shares": 10, "initial_r_per_share": 5,
            "stop": 90, "last_managed_date": "2026-10-27"}}
        repo = Repo(position)
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.earnings_policy = "retrospective_v1"
        bars = [{"open": 101.0, "high": 106.0, "low": 100.0, "close": 105.0, "volume": 1000}]
        with patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": event}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", side_effect=lambda date, _event: 2 if date.day == 28 else 1), \
                patch("src.webapp.services.earnings_risk.adr14_pct", return_value=12.0), \
                patch("src.webapp.services.martin_bar_retest_execution.bar_for", return_value=bars[0]), \
                patch("src.webapp.services.martin_bar_retest_execution.manage_position", return_value=None):
            service.run_session(day=dt.date(2026, 10, 28), run_id=1, candidates=[], source_on_time=True)
            scheduled = repo.position["payload_json"]
            self.assertTrue(scheduled["earnings_half_trim_scheduled"]["weak"])
            self.assertNotIn("earnings_half_trim_on", scheduled)
            repo.position["payload_json"]["last_managed_date"] = "2026-10-28"
            service.run_session(day=dt.date(2026, 10, 29), run_id=2, candidates=[], source_on_time=True)
        trim = next(item for item in repo.events if item["event_type"] == "earnings_half_trim")
        self.assertEqual(trim["payload"]["fill_basis"], "next_session_open")
        self.assertEqual(trim["payload"]["shares"], 5)
        self.assertEqual(trim["payload"]["price"], 101.0 * .999)

    def test_holdings_v2_schedules_loss_exit_and_closes_position_at_next_open(self):
        from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService

        class Repo:
            def __init__(self):
                self.position = {"id": 3, "ticker": "ABC", "payload_json": {"entry_price": 100,
                    "remaining_shares": 10, "initial_shares": 10, "initial_r_per_share": 5,
                    "stop": 90, "last_managed_date": "2026-10-27"}}
                self.events = []
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def pending_orders(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return [self.position] if self.position.get("status") != "closed" else []
            def set_position_payload(self, _id, payload): self.position = {**self.position, "payload_json": payload}
            def apply_event(self, **kwargs):
                self.events.append(kwargs)
                if kwargs.get("position_id") is not None:
                    self.position = {**self.position, "payload_json": kwargs["position_payload"],
                        "status": "closed" if kwargs.get("close_position") else "open"}

        repo = Repo()
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.earnings_policy = "retrospective_holdings_v2"
        service.execution_version = "martin_bar_retest_execution_v5"
        event = {"date": dt.date(2026, 10, 30), "timing": "before_market"}
        # The low breaches the stop after the open; the scheduled open sale must fill first.
        completed_bar = {"open": 99.0, "high": 101.0, "low": 80.0, "close": 99.0, "volume": 1000}
        with patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": event}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", side_effect=lambda date, _event: 2 if date.day == 28 else 1), \
                patch("src.webapp.services.earnings_risk.adr14_pct", return_value=None), \
                patch("src.webapp.services.martin_bar_retest_execution.bar_for", return_value=completed_bar), \
                patch("src.webapp.services.martin_bar_retest_execution.manage_position", return_value=None):
            service.run_session(day=dt.date(2026, 10, 28), run_id=1, candidates=[], source_on_time=True)
            self.assertEqual(repo.position["payload_json"]["earnings_holdings_scheduled"]["action"], "exit_all")
            repo.position["payload_json"]["last_managed_date"] = "2026-10-28"
            service.run_session(day=dt.date(2026, 10, 29), run_id=2, candidates=[], source_on_time=True)
        exit_event = next(item for item in repo.events if item["event_type"] == "earnings_exit")
        self.assertTrue(exit_event["close_position"])
        self.assertEqual(exit_event["position_payload"]["remaining_shares"], 0)
        self.assertEqual(exit_event["position_payload"]["status"], "closed")
        self.assertEqual(exit_event["payload"]["shares"], 10)

    def test_rs_pivot_replay_flags_unknown_without_blocking(self):
        from pathlib import Path
        from src.webapp.services.rs_leader_30m_pivot import execute_pivot_range

        class Repo:
            def __init__(self, _connection): self.candidates = []
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return []
            def daily_bar_replay_provenance(self, *_args): return {}
            def save_session(self, *, candidates, **_kwargs): self.candidates = candidates; return 1
            def set_run_metadata(self, *_args): pass

        repo_type = Repo
        candidate = {"ticker": "ABC", "status": "pending", "setup": {}, "reasons": []}
        coverage = {"mark_coverage": {"status": "complete"}}
        with patch("src.webapp.services.martin_rs_replay.ReplayRepository", repo_type), \
                patch("src.webapp.services.martin_rotation.require_completed_session"), \
                patch("src.market_data_access.load_many_ticker_windows", return_value={}), \
                patch("src.webapp.services.rs_leader_30m_pivot.candidates_from_saved_run", return_value=[candidate]), \
                patch("src.webapp.services.earnings_risk.load_earnings", return_value={}), \
                patch("scripts.replay_martin_bar_retest_range.continuation_mark", return_value=coverage):
            # ReplayRepository is instantiated inside the function; inspect the saved candidate via patched save_session.
            saved = []
            original = Repo.save_session
            def save_session(self, *, candidates, **kwargs):
                saved.extend(candidates)
                return original(self, candidates=candidates, **kwargs)
            with patch.object(Repo, "save_session", save_session):
                execute_pivot_range(object(), sessions=[dt.date(2026, 10, 26)],
                    sources={dt.date(2026, 10, 26): {"source_run_id": 1, "hits": [],
                        "source_fingerprint": "x", "replay_provenance": {}}},
                    book_id="rs_leader_30m_pivot_replay_test", database_url="", intraday_dir=None,
                    auto_freeze_missing=True, artifacts_dir=Path("."), earnings_policy="retrospective_v1")
        self.assertEqual(saved[0]["status"], "pending")
        self.assertEqual(saved[0]["earnings_risk"]["status"], "unknown")

    def test_rs_pivot_v2_blocks_new_candidate_and_pending_order_near_earnings(self):
        from pathlib import Path
        from src.webapp.services.rs_leader_30m_pivot import execute_pivot_range

        class Repo:
            def __init__(self, _connection):
                self.saved_candidates = []
                self.events = []
                repositories.append(self)
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs):
                return [{"id": 9, "ticker": "XYZ", "run_id": 1, "payload_json": {}}]
            def open_positions(self, **_kwargs): return []
            def daily_bar_replay_provenance(self, *_args): return {}
            def save_session(self, *, candidates, **_kwargs): self.saved_candidates = candidates; return 1
            def set_run_metadata(self, *_args): pass
            def apply_event(self, **kwargs): self.events.append(kwargs)

        repositories = []
        day = dt.date(2026, 10, 26)
        candidate = {"ticker": "ABC", "status": "pending", "setup": {}, "reasons": []}
        event = {"date": dt.date(2026, 10, 29), "timing": "before_market"}
        with patch("src.webapp.services.martin_rs_replay.ReplayRepository", Repo), \
                patch("src.webapp.services.martin_rotation.require_completed_session"), \
                patch("src.market_data_access.load_many_ticker_windows", return_value={}), \
                patch("src.webapp.services.rs_leader_30m_pivot.candidates_from_saved_run", return_value=[candidate]), \
                patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": event, "XYZ": event}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=3), \
                patch("scripts.replay_martin_bar_retest_range.continuation_mark",
                    return_value={"mark_coverage": {"status": "complete"}}):
            execute_pivot_range(object(), sessions=[day], sources={day: {"source_run_id": 1, "hits": [],
                "source_fingerprint": "x", "replay_provenance": {}}},
                book_id="rs_leader_30m_pivot_replay_test", database_url="", intraday_dir=None,
                auto_freeze_missing=True, artifacts_dir=Path("."), earnings_policy="retrospective_holdings_v2")
        repo = repositories[0]
        self.assertEqual(repo.saved_candidates[0]["status"], "blocked_earnings")
        self.assertEqual(repo.saved_candidates[0]["earnings_risk"]["evidence_mode"],
            "earnings_holdings_retrospective_v2")
        self.assertEqual(repo.events[0]["event_type"], "pivot_entry_cancelled")
        self.assertEqual(repo.events[0]["payload"]["evidence_mode"],
            "earnings_holdings_retrospective_v2")

    def test_rs_pivot_schedules_bmo_trim_from_completed_bar(self):
        import pandas as pd
        from pathlib import Path
        from src.webapp.services.rs_leader_30m_pivot import execute_pivot_range

        class Repo:
            def __init__(self, _connection):
                self.position = {"id": 2, "ticker": "ABC", "opened_on": "2026-10-20",
                    "payload_json": {"entry_price": 100, "remaining_shares": 10, "initial_shares": 10,
                        "initial_r_per_share": 5, "stop": 90, "last_managed_date": "2026-10-27"}}
                repositories.append(self)
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return [self.position]
            def daily_bar_replay_provenance(self, *_args): return {}
            def save_session(self, **_kwargs): return 1
            def set_run_metadata(self, *_args): pass
            def set_position_payload(self, _id, payload): self.position = {**self.position, "payload_json": payload}
            def apply_event(self, **_kwargs): raise AssertionError("same-session close trim must not execute")

        day = dt.date(2026, 10, 28)
        repositories = []
        frame = pd.DataFrame([{"Open": 101, "High": 106, "Low": 100, "Close": 105, "Volume": 1000}],
            index=pd.DatetimeIndex([pd.Timestamp(day)]))
        mark = {"mark_coverage": {"status": "complete"}}
        with patch("src.webapp.services.martin_rs_replay.ReplayRepository", Repo), \
                patch("src.webapp.services.martin_rotation.require_completed_session"), \
                patch("src.market_data_access.load_many_ticker_windows", return_value={"ABC": frame}), \
                patch("src.webapp.services.rs_leader_30m_pivot.candidates_from_saved_run", return_value=[]), \
                patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": {
                    "date": dt.date(2026, 10, 30), "timing": "before_market"}}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event", return_value=2), \
                patch("src.webapp.services.earnings_risk.adr14_pct", return_value=12), \
                patch("src.webapp.services.martin_bar_retest_execution.manage_position", return_value=None), \
                patch("scripts.replay_martin_bar_retest_range.continuation_mark", return_value=mark):
            results = execute_pivot_range(object(), sessions=[day],
                sources={day: {"source_run_id": 1, "hits": [], "source_fingerprint": "x",
                    "replay_provenance": {}}}, book_id="rs_leader_30m_pivot_replay_test",
                database_url="", intraday_dir=None, auto_freeze_missing=True, artifacts_dir=Path("."),
                earnings_policy="retrospective_v1")
        self.assertEqual(results[0]["execution_actions"]["filled"], 0)
        self.assertEqual(results[0]["execution_actions"]["exited"], 0)
        self.assertTrue(repositories[0].position["payload_json"]["earnings_half_trim_scheduled"]["weak"])

    def test_rs_pivot_holdings_v2_exits_all_below_minimum_cushion(self):
        from pathlib import Path
        from src.webapp.services.rs_leader_30m_pivot import execute_pivot_range

        class Repo:
            def __init__(self, _connection):
                self.position = {"id": 4, "ticker": "ABC", "opened_on": "2026-10-20",
                    "status": "open", "payload_json": {"entry_price": 100, "remaining_shares": 10,
                        "initial_shares": 10, "initial_r_per_share": 5, "stop": 90}}
                self.events = []
                repositories.append(self)
            def _connect(self): return object()
            def pending_candidates(self, **_kwargs): return []
            def open_positions(self, **_kwargs): return [self.position] if self.position["status"] == "open" else []
            def daily_bar_replay_provenance(self, *_args): return {}
            def save_session(self, **_kwargs): return 1
            def set_run_metadata(self, *_args): pass
            def apply_event(self, **kwargs):
                self.events.append(kwargs)
                if kwargs.get("position_payload"):
                    self.position = {**self.position, "payload_json": kwargs["position_payload"],
                        "status": "closed" if kwargs.get("close_position") else "open"}

        repositories = []
        day_one, day_two = dt.date(2026, 10, 28), dt.date(2026, 10, 29)
        bar = {"open": 99.0, "high": 101.0, "low": 80.0, "close": 99.0, "volume": 1000}
        source = {"source_run_id": 1, "hits": [], "source_fingerprint": "x", "replay_provenance": {}}
        with patch("src.webapp.services.martin_rs_replay.ReplayRepository", Repo), \
                patch("src.webapp.services.martin_rotation.require_completed_session"), \
                patch("src.market_data_access.load_many_ticker_windows", return_value={}), \
                patch("src.webapp.services.rs_leader_30m_pivot.candidates_from_saved_run", return_value=[]), \
                patch("src.webapp.services.earnings_risk.load_earnings", return_value={"ABC": {
                    "date": dt.date(2026, 10, 30), "timing": "before_market"}}), \
                patch("src.webapp.services.earnings_risk.sessions_to_event",
                    side_effect=lambda date, _event: 2 if date.day == 28 else 1), \
                patch("src.webapp.services.earnings_risk.adr14_pct", return_value=None), \
                patch("src.webapp.services.martin_bar_retest_execution.bar_for", return_value=bar), \
                patch("src.webapp.services.martin_bar_retest_execution.manage_position", return_value=None), \
                patch("scripts.replay_martin_bar_retest_range.continuation_mark",
                    return_value={"mark_coverage": {"status": "complete"}}):
            execute_pivot_range(object(), sessions=[day_one, day_two], sources={day_one: source, day_two: source},
                book_id="rs_leader_30m_pivot_replay_test", database_url="", intraday_dir=None,
                auto_freeze_missing=True, artifacts_dir=Path("."),
                earnings_policy="retrospective_holdings_v2")
        repo = repositories[0]
        exit_event = next(item for item in repo.events if item["event_type"] == "earnings_exit")
        self.assertTrue(exit_event["close_position"])
        self.assertEqual(exit_event["position_payload"]["remaining_shares"], 0)
        self.assertEqual(exit_event["position_payload"]["exit_type"], "earnings_exit")
        self.assertEqual(exit_event["payload"]["shares"], 10)


if __name__ == "__main__":
    unittest.main()
