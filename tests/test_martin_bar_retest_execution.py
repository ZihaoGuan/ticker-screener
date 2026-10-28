import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.martin_bar_retest_execution import (
    MartinBarRetestExecutionService, bar_for, manage_position, schedule_ema9_exit, settle_order,
)
from src.webapp.services.martin_forward_test_service import order_outcome, position_exit
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

    def test_v2_trims_one_third_while_v1_keeps_ten_percent(self):
        day = dt.date(2026, 10, 20)
        position = {"entry_price": 10, "stop": 9, "shares": 100, "remaining_shares": 100,
            "initial_shares": 100, "initial_r_per_share": 1, "initial_r": 1, "exit_next_open": False}
        bar = {"open": 10, "high": 13, "low": 9.5, "close": 12}
        v1 = manage_position(position, bar, day)
        v2 = manage_position(position, bar, day, management_version="martin_bar_retest_management_v2")
        self.assertEqual(v1[2]["shares"], 10)
        self.assertEqual(v1[1]["remaining_shares"], 90)
        self.assertEqual(v2[2]["shares"], 33)
        self.assertEqual(v2[1]["remaining_shares"], 67)
        self.assertEqual(v2[2]["trim_fraction"], "1/3")
        self.assertEqual(v2[2]["management_version"], "martin_bar_retest_management_v2")

    def test_v3_moves_stop_to_breakeven_after_trim_and_stops_on_next_bar(self):
        day = dt.date(2026, 10, 20)
        position = {"entry_price": 10, "stop": 9, "original_stop": 9, "shares": 100,
            "remaining_shares": 100, "initial_shares": 100, "initial_r_per_share": 1,
            "initial_r": 1, "exit_next_open": False}
        trim = manage_position(position, {"open": 12, "high": 13, "low": 11.5, "close": 12}, day,
                               management_version="martin_bar_retest_management_v3")
        self.assertEqual(trim[0], "trim_3r")
        self.assertEqual(trim[1]["stop"], 10)
        self.assertEqual(trim[2]["stop_move_effective"], "next_session")
        self.assertIsNone(position_exit(position, {"open": 9.8, "low": 9.5, "high": 11}))
        self.assertEqual(position_exit(trim[1], {"open": 10.1, "low": 9.9, "high": 11}), ("stop", 9.99))

    def test_v3_gap_stop_uses_open_and_never_lowers_stop_above_entry(self):
        day = dt.date(2026, 10, 20)
        position = {"entry_price": 10, "stop": 11, "original_stop": 11, "shares": 100,
            "remaining_shares": 100, "initial_shares": 100, "initial_r_per_share": 1,
            "initial_r": 1, "exit_next_open": False}
        trim = manage_position(position, {"open": 12, "high": 13, "low": 11.5, "close": 12}, day,
                               management_version="martin_bar_retest_management_v3")
        self.assertEqual(trim[0], "trim_3r")
        self.assertEqual(trim[1]["stop"], 11)
        self.assertEqual(position_exit(trim[1], {"open": 10.5, "low": 10, "high": 12}), ("gap_stop", 10.4895))

    def test_v2_floor_rounding_skips_trim_below_three_shares(self):
        day = dt.date(2026, 10, 20)
        bar = {"open": 10, "high": 13, "low": 9.5, "close": 12}
        for shares in (1, 2):
            position = {"entry_price": 10, "stop": 9, "shares": shares, "remaining_shares": shares,
                "initial_shares": shares, "initial_r_per_share": 1, "initial_r": 1, "exit_next_open": False}
            outcome = manage_position(position, bar, day, management_version="martin_bar_retest_management_v2")
            self.assertEqual(outcome[0], "trim_3r_skipped")
            self.assertEqual(outcome[1]["remaining_shares"], shares)
            self.assertEqual(outcome[2]["shares"], 0)
            self.assertEqual(outcome[2]["trim_share_rounding"], "floor")

    def test_v2_small_position_records_trim_skip_only_once(self):
        day = dt.date(2026, 10, 20)
        position = {"entry_price": 10, "stop": 9, "shares": 2, "remaining_shares": 2,
            "initial_shares": 2, "initial_r_per_share": 1, "initial_r": 1, "exit_next_open": False}
        bar = {"open": 10, "high": 13, "low": 9.5, "close": 12}
        skipped = manage_position(position, bar, day, management_version="martin_bar_retest_management_v2")
        self.assertEqual(skipped[0], "trim_3r_skipped")
        next_day = manage_position(skipped[1], bar, day + dt.timedelta(days=1),
                                   management_version="martin_bar_retest_management_v2")
        self.assertIsNone(next_day)

    def test_v2_execution_persists_trim_fraction_and_management_version(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 10, "High": 13, "Low": 9.5, "Close": 12, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        repo = Repo([{"id": 4, "ticker": "ABC", "payload_json": {"entry_price": 10, "stop": 9,
            "shares": 100, "remaining_shares": 100, "initial_shares": 100,
            "initial_r_per_share": 1, "initial_r": 1, "exit_next_open": False}}])
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v2_trim"
        service.execution_version = "martin_bar_retest_execution_v2"
        service.run_session(day=day, run_id=22, candidates=[], source_on_time=False)
        event = next(item for item in repo.events if item["event_type"] == "trim_3r")
        self.assertEqual(event["payload"]["trim_fraction"], "1/3")
        self.assertEqual(event["payload"]["management_version"], "martin_bar_retest_management_v2")

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

    def test_v2_missing_frozen_bundle_uses_daily_fallback_and_marks_session(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        repo = LifecycleRepo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
            "max_entry": 10, "limit_price": 10, "stop": 9, "shares": 10}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v2_fallback"
        service.execution_version = "martin_bar_retest_execution_v2"
        counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False)
        self.assertEqual(counts["filled"], 1)
        self.assertEqual(counts["intraday_fallbacks"], 1)
        self.assertEqual(counts["intraday_fallback_reasons"], {"frozen_intraday_bundle_missing": 1})
        fill = next(event for event in repo.events if event["event_type"] == "order_filled")
        self.assertEqual(fill["payload"]["intraday_execution"]["evidence_status"], "frozen_intraday_bundle_missing")

    def test_auto_freeze_mode_blocks_daily_fallback_for_missing_bundle(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        repo = LifecycleRepo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
            "max_entry": 10, "limit_price": 10, "stop": 9, "shares": 10}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v2_strict"
        service.execution_version = "martin_bar_retest_execution_v4"
        counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False,
            require_frozen_intraday=True)
        self.assertEqual(counts["blocked_data"], 1)
        self.assertEqual(counts["filled"], 0)
        event = next(event for event in repo.events if event["event_type"] == "order_blocked_data")
        self.assertEqual(event["candidate_status"], "blocked_data")

    def test_v2_recorded_provider_failure_uses_daily_fallback(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        repo = LifecycleRepo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
            "max_entry": 10, "limit_price": 10, "stop": 9, "shares": 10}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v2_provider_failure"
        service.execution_version = "martin_bar_retest_execution_v2"
        counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False,
            frozen_intraday={"ABC": {"freeze_error": "provider_unavailable", "audit_fingerprint": "frozen-manifest",
                "exchange_lookup": {"attempted_symbols": ["NYSE:ABC", "NASDAQ:ABC"], "selected_symbol": None}}})
        fill = next(event for event in repo.events if event["event_type"] == "order_filled")
        self.assertEqual(counts["intraday_fallbacks"], 1)
        self.assertEqual(counts["intraday_fallback_reasons"], {"provider_unavailable": 1})
        self.assertEqual(counts["intraday_fallback_details"], [{
            "ticker": "ABC", "date": day.isoformat(), "requested_symbol": "NASDAQ:ABC",
            "resolved_symbol": None, "evidence_status": "provider_unavailable",
            "reason": "provider_unavailable",
        }])
        self.assertEqual(fill["payload"]["intraday_execution"]["evidence_status"], "provider_unavailable")
        self.assertEqual(fill["payload"]["intraday_execution"]["audit_fingerprint"], "frozen-manifest")
        self.assertEqual(fill["payload"]["intraday_execution"]["exchange_lookup"]["attempted_symbols"],
            ["NYSE:ABC", "NASDAQ:ABC"])

    def test_v2_frozen_fill_persists_hash_and_feed_provenance(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 12, "High": 13, "Low": 10.5, "Close": 12, "Volume": 200}],
                             index=pd.to_datetime([day.isoformat()]))
        candles = [{"time": 1, "open": 12, "high": 13, "low": 10.5, "close": 12, "volume": 100},
                   {"time": 2, "open": 12, "high": 13, "low": 10.5, "close": 12, "volume": 100}]
        data = {"query": {"symbol": "NASDAQ:ABC", "timeframe": "15", "session": "regular",
                "adjustment": "splits", "timezone": "America/New_York"},
            "metadata": {"full_name": "BATS:ABC", "subsession_id": "regular",
                "timezone": "America/New_York", "currency_code": "USD"}, "candles": candles}
        repo = LifecycleRepo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
            "max_entry": 11, "limit_price": 11, "stop": 10, "shares": 10}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v2_frozen"
        service.execution_version = "martin_bar_retest_execution_v2"
        counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False,
            frozen_intraday={"ABC": {"data": data, "requested_symbol": "NASDAQ:ABC",
                "exchange_lookup": {"attempted_symbols": ["NYSE:ABC", "NASDAQ:ABC"], "selected_symbol": "NASDAQ:ABC"},
                "expected_starts": [1, 2],
                "audit_fingerprint": "audit-hash", "candle_fingerprint": "candle-hash", "source_date": day.isoformat()}})
        self.assertEqual(counts["filled"], 1)
        position = repo.positions[0]["payload_json"]
        self.assertEqual(position["entry_price"], 11)
        self.assertEqual(position["intraday_execution"]["source_date"], day.isoformat())
        fill = next(event for event in repo.events if event["event_type"] == "order_filled")
        self.assertEqual(fill["payload"]["intraday_execution"]["candle_fingerprint"], "candle-hash")
        self.assertEqual(fill["payload"]["intraday_execution"]["evidence_status"], "alternate_feed_supported")
        self.assertTrue(fill["payload"]["intraday_execution"]["alternate_feed_supported"])
        self.assertEqual(fill["payload"]["intraday_execution"]["exchange_lookup"]["selected_symbol"], "NASDAQ:ABC")

    def test_v4_expired_intraday_order_does_not_daily_fill(self):
        self._assert_v4_intraday_terminal_is_authoritative("expired")

    def test_v4_cancelled_intraday_order_does_not_daily_fill(self):
        self._assert_v4_intraday_terminal_is_authoritative("cancelled")

    def _assert_v4_intraday_terminal_is_authoritative(self, status):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        repo = LifecycleRepo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
            "max_entry": 10, "limit_price": 10, "stop": 9, "shares": 10}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
        service.book_id = "martin_range_bar_retest_v4_terminal"
        service.execution_version = "martin_bar_retest_execution_v4"
        evidence = {"status": status, "reason": "intraday_terminal_test"}
        with patch("src.webapp.services.martin_intraday_execution.evaluate_frozen_order", return_value=evidence):
            counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False,
                frozen_intraday={"ABC": {"data": {}, "requested_symbol": "NASDAQ:ABC"}})
        self.assertEqual(counts["filled"], 0)
        self.assertEqual(counts[status], 1)
        self.assertFalse(any(event["event_type"] == "order_filled" for event in repo.events))
        terminal = next(event for event in repo.events if event["event_type"] == f"order_{status}")
        self.assertEqual(terminal["payload"]["intraday_execution"]["status"], status)
        self.assertEqual(counts["intraday_fallbacks"], 0)

    def test_v4_missing_or_invalid_frozen_evidence_uses_daily_fallback_and_counts_it(self):
        day = dt.date(2026, 10, 20)
        frame = pd.DataFrame([{"Open": 9.8, "High": 10.2, "Low": 9.5, "Close": 10.0, "Volume": 1000}],
                             index=pd.to_datetime([day.isoformat()]))
        for bundle in (None, {"data": {}, "requested_symbol": "NASDAQ:ABC"}):
            with self.subTest(bundle=bundle):
                repo = LifecycleRepo()
                repo.pending = [{"id": 9, "run_id": 8, "ticker": "ABC", "payload_json": {
                    "max_entry": 10, "limit_price": 10, "stop": 9, "shares": 10}}]
                service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
                service.book_id = "martin_range_bar_retest_v4_fallback"
                service.execution_version = "martin_bar_retest_execution_v4"
                invalid = {"status": "blocked_data", "reason": "invalid_fixture"}
                with patch("src.webapp.services.martin_intraday_execution.evaluate_frozen_order", return_value=invalid):
                    counts = service.run_session(day=day, run_id=10, candidates=[], source_on_time=False,
                        frozen_intraday={} if bundle is None else {"ABC": bundle})
                self.assertEqual(counts["filled"], 1)
                self.assertEqual(counts["intraday_fallbacks"], 1)
                fill = next(event for event in repo.events if event["event_type"] == "order_filled")
                self.assertEqual(fill["payload"]["intraday_execution"]["status"], "fallback_daily")

    def test_leveraged_pending_order_is_cancelled_before_bar_lookup(self):
        repo = Repo()
        repo.pending = [{"id": 9, "run_id": 8, "ticker": "MVLL", "payload_json": {"max_entry": 10, "stop": 9}}]
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        counts = service.run_session(day=dt.date(2026, 10, 20), run_id=10,
                                     candidates=[], source_on_time=False)
        self.assertEqual(counts["cancelled"], 1)
        self.assertEqual(repo.events[0]["event_type"], "order_cancelled")
        self.assertEqual(repo.events[0]["candidate_status"], "cancelled")
        self.assertEqual(repo.events[0]["payload"]["reason"], "leveraged_or_inverse_fund")

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

    def test_ten_positions_block_scheduling(self):
        positions = [{"ticker": f"T{i}", "payload_json": {"entry_price": 10, "stop": 9,
                      "remaining_shares": 100}} for i in range(10)]
        repo = Repo(positions)
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0}
        service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                            candidates=[candidate], source_on_time=True)
        self.assertEqual(repo.updated[0][2]["capacity_skip_reason"], "ten_positions_or_orders_limit")
        self.assertEqual(candidate["status"], "blocked_capacity")

    def test_tenth_position_can_be_scheduled(self):
        positions = [{"ticker": f"T{i}", "payload_json": {"entry_price": 10, "stop": 9,
                      "remaining_shares": 100}} for i in range(9)]
        repo = Repo(positions)
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0}
        service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                            candidates=[candidate], source_on_time=True)
        self.assertNotEqual(repo.updated[0][2].get("status"), "blocked_capacity")

    def test_capacity_skip_is_saved_with_explicit_reason(self):
        positions = [{"ticker": f"T{i}", "payload_json": {"entry_price": 10, "stop": 9,
                      "remaining_shares": 100}} for i in range(10)]
        repo = Repo(positions)
        service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
        service.book_id = "martin_range_bar_retest_execution_test"
        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0}
        service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                            candidates=[candidate], source_on_time=True)
        self.assertEqual(repo.updated[0][2]["status"], "blocked_capacity")
        self.assertEqual(repo.updated[0][2]["capacity_skip_reason"], "ten_positions_or_orders_limit")

    def test_sizing_capacity_skip_distinguishes_cash_and_risk_and_keeps_source_fields(self):
        class BudgetRepo(Repo):
            def __init__(self, budget):
                super().__init__()
                self.budget = budget
            def account_state(self, _book): return self.budget

        candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                     "max_entry": 10.0, "reasons": ["source_reason"], "source_run_id": 44,
                     "source_fingerprint": "source-fp"}
        cases = [({"available_cash": 0, "available_risk": 500}, "insufficient_available_cash"),
                 ({"available_cash": 1000, "available_risk": 0}, "insufficient_available_risk")]
        for budget, expected in cases:
            repo = BudgetRepo(budget)
            service = MartinBarRetestExecutionService(repo, bar_loader=lambda *_a, **_k: {})
            service.book_id = "martin_range_bar_retest_execution_test"
            candidate = {"ticker": "ABC", "rank": 1, "status": "confirmed", "stop": 9.0,
                         "max_entry": 10.0, "reasons": ["source_reason"], "source_run_id": 44,
                         "source_fingerprint": "source-fp"}
            service.run_session(day=dt.date(2026, 10, 19), run_id=42,
                                candidates=[candidate], source_on_time=True)
            saved = repo.updated[0][2]
            self.assertEqual(candidate["status"], "blocked_capacity")
            self.assertEqual(saved["capacity_skip_reasons"], [expected])
            self.assertIn("source_reason", saved["reasons"])
            self.assertEqual(saved["source_run_id"], 44)
            self.assertEqual(saved["source_fingerprint"], "source-fp")


if __name__ == "__main__":
    unittest.main()
