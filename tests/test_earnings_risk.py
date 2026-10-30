import datetime as dt
import json
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.webapp.services.earnings_risk import entry_decision, weak_cushion_trim


class EarningsRiskTests(unittest.TestCase):
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

    def test_weekend_report_date_counts_to_next_xnys_session(self):
        import pandas as pd
        class Calendar:
            def sessions_in_range(self, start, end):
                return pd.date_range(start, end, freq="B", tz="UTC")
        fake = SimpleNamespace(get_calendar=lambda *_args, **_kwargs: Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake}):
            from src.webapp.services.earnings_risk import sessions_to_event
            self.assertEqual(sessions_to_event(dt.date(2026, 10, 29), dt.date(2026, 10, 31)), 2)

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


if __name__ == "__main__":
    unittest.main()
