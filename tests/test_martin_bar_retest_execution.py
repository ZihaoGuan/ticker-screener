import datetime as dt
import unittest

import pandas as pd

from src.webapp.services.martin_bar_retest_execution import (
    MartinBarRetestExecutionService, bar_for, manage_position, schedule_ema9_exit, settle_order,
)
from src.webapp.services.martin_forward_test_service import order_outcome
from src.webapp.services.martin_rotation import account_budget, size_order


class Repo:
    def __init__(self, positions=None):
        self.updated = []
        self.events = []
        self.positions = positions or []
        self.pending = []

    def pending_candidates(self, **_kwargs): return self.pending
    def open_positions(self, **_kwargs): return self.positions
    def pending_orders(self, **_kwargs): return []
    def update_candidate(self, run_id, ticker, payload): self.updated.append((run_id, ticker, payload))
    def apply_event(self, **kwargs): self.events.append(kwargs); return True
    def account_state(self, _book): return account_budget(self.positions, [], 0)


class LifecycleRepo(Repo):
    def update_candidate(self, run_id, ticker, payload):
        super().update_candidate(run_id, ticker, payload)
        self.pending = [{"id": 7, "run_id": run_id, "ticker": ticker, "payload_json": payload}]

    def pending_candidates(self, **_kwargs): return self.pending
    def pending_orders(self, **_kwargs): return self.pending

    def fill_order(self, *, candidate_id, run_id, ticker, event_date, position_payload, event_payload, strategy_id):
        self.events.append({"event_type": "order_filled", "run_id": run_id, "ticker": ticker,
                            "event_date": event_date, "payload": event_payload, "strategy_id": strategy_id})
        self.pending = []
        self.positions = [{"id": 99, "ticker": ticker, "payload_json": position_payload}]
        return 99


class MartinBarRetestExecutionTests(unittest.TestCase):
    def test_gap_open_at_or_below_stop_cancels_order(self):
        result = order_outcome({"stop": 9, "limit_price": 10}, {"open": 8, "low": 7})
        self.assertEqual(result, ("cancelled", None, "opened_at_or_below_stop"))

    def test_entry_and_stop_same_bar_fills_then_stops_conservatively(self):
        result = settle_order({"stop": 9, "max_entry": 10, "shares": 10},
                              {"open": 9.5, "high": 10, "low": 8.5, "close": 9, "volume": 1},
                              dt.date(2026, 10, 20))
        self.assertEqual(result["status"], "filled")
        self.assertTrue(result["same_day_stop"])
        self.assertEqual(result["position"]["exit_type"], "stop")
        self.assertAlmostEqual(result["position"]["exit_price"], 8.991)

    def test_deferred_entry_day_trim_still_schedules_ema9_exit(self):
        days = pd.date_range("2026-10-14", periods=9, freq="B")
        frame = pd.DataFrame([{ "Open": 12, "High": 12, "Low": 11, "Close": 12, "Volume": 1000}
                              for _ in range(8)] + [{"Open": 10, "High": 14, "Low": 9.5,
                              "Close": 10, "Volume": 1000}], index=days)
        position = {"entry_price": 10, "stop": 9, "shares": 100, "remaining_shares": 100,
                    "initial_shares": 100, "initial_r_per_share": 1, "exit_next_open": False}
        outcome = manage_position(position, {"open": 10, "high": 14, "low": 9.5, "close": 10},
                                  days[-1].date(), frame, allow_trim=False)
        self.assertEqual(outcome[0], "ema9_exit_scheduled")
        self.assertTrue(outcome[1]["exit_next_open"])

    def test_trimmed_position_can_also_schedule_ema9_exit(self):
        days = pd.date_range("2026-10-14", periods=9, freq="B")
        frame = pd.DataFrame([{ "Open": 12, "High": 12, "Low": 11, "Close": 12, "Volume": 1000}
                              for _ in range(8)] + [{"Open": 10, "High": 14, "Low": 9.5,
                              "Close": 10, "Volume": 1000}], index=days)
        position = {"entry_price": 10, "stop": 9, "shares": 100, "remaining_shares": 100,
                    "initial_shares": 100, "initial_r_per_share": 1, "exit_next_open": False}
        trim = manage_position(position, {"open": 10, "high": 14, "low": 9.5, "close": 10}, days[-1].date(), frame)
        self.assertEqual(trim[0], "trim_3r")
        scheduled = schedule_ema9_exit(trim[1], {"open": 10, "high": 14, "low": 9.5, "close": 10},
                                       days[-1].date(), frame)
        self.assertTrue(scheduled[0]["exit_next_open"])

    def test_service_persists_ema9_schedule_after_successful_trim(self):
        days = pd.date_range("2026-10-14", periods=9, freq="B")
        frame = pd.DataFrame([{ "Open": 12, "High": 12, "Low": 11, "Close": 12, "Volume": 1000}
                              for _ in range(8)] + [{"Open": 10, "High": 14, "Low": 9.5,
                              "Close": 10, "Volume": 1000}], index=days)
        repo = Repo([{"id": 4, "ticker": "ABC", "payload_json": {"entry_price": 10, "stop": 9,
                    "shares": 100, "remaining_shares": 100, "initial_shares": 100,
                    "initial_r_per_share": 1, "exit_next_open": False}}])
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_trim_ema_test"
        service.run_session(day=days[-1].date(), run_id=26, candidates=[], source_on_time=False)
        event = next(item for item in repo.events if item["event_type"] == "ema9_exit_scheduled")
        self.assertTrue(event["position_payload"]["exit_next_open"])
        self.assertEqual(event["position_payload"]["holding_sessions"], 1)

    def test_capacity_and_name_risk_limit(self):
        positions = [{"ticker": "ABC", "payload_json": {"remaining_shares": 1500,
                     "entry_price": 10, "stop": 9}}]
        self.assertEqual(size_order({"ticker": "ABC", "limit_price": 10, "stop": 9},
                                    account_budget(positions, [], 0)), 500)

    def test_missing_session_bar_is_not_fabricated(self):
        frame = pd.DataFrame([{ "Open": 10, "High": 11, "Low": 9, "Close": 10, "Volume": 1}],
                             index=pd.to_datetime(["2026-10-19"]))
        self.assertIsNone(bar_for(frame, dt.date(2026, 10, 20)))

    def test_missing_pending_order_bar_is_blocked_and_logged(self):
        repo = Repo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {"max_entry": 10, "stop": 9}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        counts = service.run_session(day=dt.date(2026, 10, 20), run_id=10,
                                     candidates=[], source_on_time=False)
        self.assertEqual(counts["missing_bars"], 1)
        self.assertEqual(repo.events[0]["event_type"], "order_blocked_missing_bar")
        self.assertEqual(repo.events[0]["candidate_status"], "blocked_data")

    def test_only_on_time_source_schedules_confirmed_retest(self):
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0, "retrospective_source_scan": False}
        for source_on_time, expected in ((True, 1), (False, 0)):
            repo = Repo()
            service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
            service.book_id = "martin_range_bar_retest_execution_test"
            counts = service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                                         candidates=[candidate], source_on_time=source_on_time)
            self.assertEqual(counts["scheduled"], expected)
            self.assertEqual(len(repo.updated), expected)
            if repo.updated:
                self.assertEqual(repo.updated[0][2]["shares"], 500)

    def test_current_scan_cannot_make_a_retroactive_origin_eligible_for_new_buy(self):
        repo = Repo()
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        result = service.run_session(day=dt.date(2026, 10, 26), run_id=99,
            candidates=[{"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9,
                         "max_entry": 10, "original_source_retrospective": True}], source_on_time=True)
        self.assertEqual(result["scheduled"], 0)
        self.assertEqual(repo.updated, [])

    def test_retrospective_source_requires_opt_in_then_marks_hypothetical_fill(self):
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0, "retrospective_source_scan": True}
        repo = LifecycleRepo()
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_backdated_test"
        blocked = service.run_session(day=dt.date(2026, 10, 19), run_id=19,
                                      candidates=[candidate], source_on_time=False)
        self.assertEqual(blocked["scheduled"], 0)

        allowed = service.run_session(day=dt.date(2026, 10, 19), run_id=19,
                                      candidates=[candidate], source_on_time=False,
                                      simulate_backdated_buys=True)
        self.assertEqual(allowed["scheduled"], 1)
        order_payload = repo.updated[-1][2]
        self.assertTrue(order_payload["source_reconstructed_later"])
        self.assertEqual(order_payload["fill_basis"], "hypothetical_backdated_simulation")

        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime(["2026-10-20"]))
        service.bar_loader = lambda *_a, **_k: {"ABC": frame}
        filled = service.run_session(day=dt.date(2026, 10, 20), run_id=20, candidates=[], source_on_time=False)
        self.assertEqual(filled["filled"], 1)
        position = repo.positions[0]["payload_json"]
        self.assertTrue(position["backdated_simulation"])
        self.assertEqual(position["opened_on"], "2026-10-20")
        fill_event = next(event for event in repo.events if event["event_type"] == "order_filled")
        self.assertEqual(fill_event["payload"]["fill_basis"], "hypothetical_backdated_simulation")

    def test_oct23_confirmation_fills_on_oct26_and_persists_position_and_event(self):
        repo = LifecycleRepo()
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_lifecycle_test"
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0, "event_category": "gap_mover", "source_run_id": 7113}
        scheduled = service.run_session(day=dt.date(2026, 10, 23), run_id=23,
                                        candidates=[candidate], source_on_time=True)
        self.assertEqual(scheduled["scheduled"], 1)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime(["2026-10-26"]))
        service.bar_loader = lambda *_a, **_k: {"ABC": frame}
        settled = service.run_session(day=dt.date(2026, 10, 26), run_id=26,
                                      candidates=[], source_on_time=False)
        self.assertEqual(settled["filled"], 1)
        self.assertEqual(repo.positions[0]["payload_json"]["opened_on"], "2026-10-26")
        self.assertEqual(repo.positions[0]["payload_json"]["event_category"], "gap_mover")
        self.assertTrue(any(event["event_type"] == "order_filled" for event in repo.events))

    def test_eight_positions_block_scheduling(self):
        positions = [{"ticker": f"T{i}", "payload_json": {"entry_price": 10, "stop": 9,
                      "remaining_shares": 100}} for i in range(8)]
        repo = Repo(positions)
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                            candidates=[{"ticker": "ABC", "rank": 1, "status": "confirmed",
                                         "stop": 9.0, "max_entry": 10.0}], source_on_time=True)
        self.assertEqual(repo.updated, [])


if __name__ == "__main__":
    unittest.main()
