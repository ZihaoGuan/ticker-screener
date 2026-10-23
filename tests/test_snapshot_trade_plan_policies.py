from __future__ import annotations

import copy
import datetime as dt
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import pandas as pd

from src.trade_plan_policies import CURRENT_STOP_POLICY, STRUCTURAL_STOP_POLICY, structural_plan_payload
from src.webapp.services.martin_forward_test_service import classify_candidate, classify_rs_expanded_candidate
from src.webapp.services.watchlist_service import WatchlistService, _scanner_top_hits_source_fingerprint
from tests.test_ma_pullback_retest_screen import _support_reclaim_frame


class SnapshotTradePlanPoliciesTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.service = WatchlistService(artifacts_dir=Path(self.temp.name), database_url="postgres://example")
        self.day = dt.date(2026, 10, 22)
        self.frame = _support_reclaim_frame()
        self.frame.index = pd.bdate_range(end=self.day, periods=len(self.frame))
        self.row = {"ticker": "ABC", "daily_rs_rating": 95, "scanner_ids": ["rs"], "scanners": [],
                    "market_cap": 2e9, "close": 125, "adr14_pct": 4, "atr_to_sma50": 1,
                    "trade_readiness": "reclaim_trigger", "pullback_quality": {"state": "neutral"},
                    "adr_suitability": "risk_failure", "adr_reason": "legacy risk exceeds 5%",
                    "trade_plan": {"source_strategy_id": "ma_pullback_retest", "state": "active",
                        "signal_date": self.day.isoformat(), "trigger_price": 125, "stop_price": 110,
                        "stop_label": "Retest low / 1 ATR", "reference_risk_pct": 12,
                        "risk_valid": False, "risk_status": "over_limit", "max_entry_price": 110 / .95}}

    def attach(self, rows=None):
        rows = rows or {"ABC": copy.deepcopy(self.row)}
        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows", return_value={"ABC": self.frame}) as loader:
            self.service._attach_snapshot_trade_plans(rows, as_of_date=self.day, adr_frames={"ABC": self.frame.tail(14)})
        return rows, loader

    def test_dual_payload_preserves_legacy_and_recalculates_structural_context(self):
        rows, loader = self.attach()
        row = rows["ABC"]
        loader.assert_called_once_with(["ABC"], self.day, 1300, database_url="postgres://example")
        self.assertEqual({key: row[key] for key in self.row}, self.row)
        current, structural = row["trade_plans"][CURRENT_STOP_POLICY], row["trade_plans"][STRUCTURAL_STOP_POLICY]
        self.assertEqual(current["trade_plan"], self.row["trade_plan"])
        self.assertEqual(current["trade_readiness"], "reclaim_trigger")
        self.assertEqual(structural["availability"], "available")
        self.assertEqual(structural["trade_readiness"], "defined_risk")
        self.assertNotEqual(structural["adr_suitability"], "risk_failure")
        plan = structural["trade_plan"]
        self.assertEqual(plan["stop_price"], self.frame["Low"].iloc[-1])
        self.assertEqual(plan["stop_label"], "Daily reclaim candle low")
        self.assertAlmostEqual(plan["max_entry_price"], plan["stop_price"] / .95)
        self.assertAlmostEqual(structural["stop_to_adr"], plan["reference_risk_pct"] / 4)
        self.assertNotIn("legacy", structural["adr_reason"])
        structural["trade_plan"]["stop_price"] = 1
        self.assertEqual(current["trade_plan"]["stop_price"], 110)
        json.dumps(row, allow_nan=False)

    def test_readers_use_saved_structural_plan_not_new_frames_or_legacy_fields(self):
        rows, _ = self.attach()
        row = rows["ABC"]
        original = copy.deepcopy(row)
        item = classify_rs_expanded_candidate(row, 1, self.day, {})
        changed = self.frame.copy()
        changed["Low"] = .01
        self.assertEqual(item, classify_rs_expanded_candidate(row, 1, self.day, {"ABC": changed}))
        self.assertEqual(item["status"], "pending")
        self.assertEqual(item["stop"], row["trade_plans"][STRUCTURAL_STOP_POLICY]["trade_plan"]["stop_price"])
        self.assertEqual(classify_candidate(row, 1)["status"], "watch")
        self.assertEqual(row, original)

    def test_missing_old_stale_wrong_version_and_tampered_snapshots_block(self):
        rows, _ = self.attach()
        saved = rows["ABC"]["trade_plans"][STRUCTURAL_STOP_POLICY]
        variants = [(None, "structural_snapshot_plan_missing"),
                    ({**saved, "version": "future"}, "structural_snapshot_plan_missing"),
                    ({**saved, "as_of_date": "2026-10-21"}, "structural_confirmation_stale"),
                    ({**saved, "trade_plan": {**saved["trade_plan"], "signal_date": "2026-10-21"}}, "structural_confirmation_stale"),
                    ({**saved, "trade_plan": {**saved["trade_plan"], "stop_price": 1}}, "structural_snapshot_plan_invalid"),
                    ({**saved, "trade_plan": {**saved["trade_plan"], "stop_price": float("nan")}}, "plan_levels_missing"),
                    ({**saved, "evidence": {}}, "structural_evidence_missing"),
                    ({**saved, "evidence": {**saved["evidence"], "source_strategy_id": "ema21_pullback_buy"}}, "structural_evidence_missing"),
                    ({**saved, "evidence": {**saved["evidence"], "invalidation_date": "2026-10-21"}}, "structural_evidence_missing"),
                    ({**saved, "evidence": {**saved["evidence"], "invalidation_low": 1}}, "structural_evidence_missing"),
                    ({**saved, "evidence": {**saved["evidence"], "profile_id": "wrong"}}, "structural_evidence_missing")]
        for policy, code in variants:
            for rs in (85, 95):
                row = {**self.row, "daily_rs_rating": rs, "trade_plans": {STRUCTURAL_STOP_POLICY: policy}}
                with self.subTest(code=code, rs=rs):
                    item = classify_rs_expanded_candidate(row, 1, self.day, {})
                    self.assertEqual(item["status"], "blocked_data")
                    self.assertIn(code, item["reasons"])
                    self.assertNotIn("limit_price", item)

    def test_snapshot_loader_failure_is_explicit_and_does_not_change_legacy(self):
        rows = {"ABC": copy.deepcopy(self.row)}
        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows", side_effect=RuntimeError("offline")), self.assertLogs(level="WARNING"):
            self.service._attach_snapshot_trade_plans(rows, as_of_date=self.day, adr_frames={})
        saved = rows["ABC"]["trade_plans"][STRUCTURAL_STOP_POLICY]
        self.assertEqual(saved["availability"], "unavailable")
        self.assertEqual(saved["unavailable_reason"], "structural_evidence_missing")
        self.assertIsNone(saved["trade_plan"])
        self.assertEqual(rows["ABC"]["trade_plan"], self.row["trade_plan"])

    def test_ema_reuses_adr_frames_without_long_history_and_missing_data_is_explicit(self):
        row = {**self.row, "trade_plan": {**self.row["trade_plan"], "source_strategy_id": "ema21_pullback_buy"}}
        rows, loader = self.attach({"ABC": row})
        loader.assert_not_called()
        policy = rows["ABC"]["trade_plans"][STRUCTURAL_STOP_POLICY]
        self.assertEqual(policy["trade_plan"]["stop_price"], 110)
        self.assertEqual(policy["evidence"]["stop_basis"], "saved_support_test_low")
        missing = structural_plan_payload(row, self.day, self.frame.iloc[:100])
        self.assertEqual(missing["unavailable_reason"], "structural_evidence_missing")

    def test_both_policies_survive_history_persistence_and_reload_without_reenrichment(self):
        rows, _ = self.attach()
        board = {"target_trading_date": self.day.isoformat(), "cards": []}
        payload = {**board, "guru_board": {"rows": list(rows.values())}, "rows": []}
        history = self.service.screener_history_service
        with patch.object(history, "is_configured", return_value=True), patch.object(history, "persist_snapshot_run") as persist:
            self.service.persist_scanner_top_hits_snapshot(board_payload=board, precomputed_payload=payload)
        summary = json.loads(json.dumps(persist.call_args.kwargs["summary_payload"]))
        self.assertEqual(summary["snapshot_schema_version"], "top-hits-v10")
        detail = {"id": 10, "result_summary_json": summary, "hits": []}
        with patch.object(history, "is_configured", return_value=True), patch.object(history, "list_runs", return_value=[{"id": 10}]), \
             patch.object(history, "get_run", return_value=detail), \
             patch.object(self.service, "_attach_snapshot_trade_plans", side_effect=AssertionError("read must not rebuild")):
            restored = self.service._load_latest_persisted_scanner_top_hits_payload()
        self.assertEqual(restored["guru_board"], payload["guru_board"])
        selected = classify_rs_expanded_candidate(restored["guru_board"]["rows"][0], 1, self.day, {})
        self.assertEqual(selected["status"], "pending")

    def test_schema_change_triggers_scheduled_refresh_with_same_scanner_runs(self):
        board = {"target_trading_date": self.day.isoformat(), "cards": []}
        with patch("src.webapp.services.watchlist_service._SCANNER_TOP_HITS_SNAPSHOT_SCHEMA_VERSION", "top-hits-v9"):
            old_fingerprint, _ = _scanner_top_hits_source_fingerprint(board)
        old = {"target_trading_date": self.day.isoformat(), "source_fingerprint": old_fingerprint}
        with patch.object(self.service, "_load_latest_persisted_scanner_top_hits_payload", return_value=old):
            state = self.service.get_scanner_top_hits_snapshot_refresh_state(board_payload=board)
        self.assertTrue(state["has_current_snapshot"])
        self.assertTrue(state["source_changed"])


if __name__ == "__main__":
    unittest.main()
