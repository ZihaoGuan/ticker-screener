from __future__ import annotations

import copy
import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.webapp.services.martin_forward_test_service import (
    MartinForwardTestService, RS_EXPANDED_STRATEGY_ID, classify_candidate, classify_rs_expanded_candidate,
)
from src.webapp.repositories.martin_forward_test_repository import _performance_context, _rs_tier_metrics
from tests.test_martin_forward_test_service import _ForwardRepo


class RsExpandedTests(unittest.TestCase):
    def setUp(self):
        self.dates = pd.bdate_range(end="2026-10-22", periods=261)
        self.day = self.dates[-2].date()
        self.frame = pd.DataFrame({"Open": [70 + i * .2 for i in range(261)],
                                   "High": [71 + i * .2 for i in range(261)],
                                   "Low": [69 + i * .2 for i in range(261)],
                                   "Close": [70 + i * .2 for i in range(261)], "Volume": 1_000_000}, index=self.dates)
        self.spy = self.frame.copy()
        self.spy["Close"] = 400
        self.row = {"ticker": "ABC", "daily_rs_rating": 85, "scanner_ids": ["rs"],
                    "market_cap": 2e9, "close": 121.8, "adr14_pct": 4,
                    "trade_readiness": "defined_risk", "pullback_quality": {"state": "constructive"},
                    "trade_plan": {"trigger_price": 121.8, "stop_price": 116, "max_entry_price": 122,
                                   "state": "active", "source_strategy_id": "ma_pullback_retest", "signal_date": self.day.isoformat()}}

    def classify(self, row=None, frame=None, spy=None):
        return classify_rs_expanded_candidate(row or self.row, 1, self.day,
                    {"ABC": frame if frame is not None else self.frame, "SPY": spy if spy is not None else self.spy})

    def test_boundaries_and_baseline_unchanged(self):
        original = copy.deepcopy(self.row)
        for rating, tier, status in ((79.99, "research", "watch"), (80, "emerging", "pending"),
                                     (89.99, "emerging", "pending"), (90, "preferred", "pending"),
                                     (99, "preferred", "pending"), (None, "missing", "blocked_data"),
                                     (float("nan"), "invalid", "blocked_data"), (100, "invalid", "blocked_data"),
                                     (True, "invalid", "blocked_data")):
            with self.subTest(rating=rating):
                result = self.classify({**self.row, "daily_rs_rating": rating})
                self.assertEqual((result["rs_tier"], result["status"]), (tier, status))
        self.assertEqual(self.row, original)
        self.assertEqual(classify_candidate(self.row, 1)["status"], "blocked")
        baseline = classify_candidate({**self.row, "daily_rs_rating": 90}, 1)
        preferred = self.classify({**self.row, "daily_rs_rating": 90})
        self.assertEqual({k: preferred[k] for k in baseline}, baseline)

    def test_every_gate_and_future_bars(self):
        result = self.classify()
        self.assertTrue(all(result["rs_exception_checks"].values()))
        future = self.frame.copy()
        future.iloc[-1, future.columns.get_loc("Close")] = .001
        self.assertEqual(self.classify(frame=future), result)
        cases = [({**self.row, "trade_plan": {**self.row["trade_plan"], "state": "setup"}}, "rs_confirmation_required"),
                 ({**self.row, "trade_plan": {**self.row["trade_plan"], "signal_date": "2026-10-20"}}, "rs_confirmation_required"),
                 ({**self.row, "trade_plan": {**self.row["trade_plan"], "source_strategy_id": "unknown"}}, "rs_supported_plan_required"),
                 ({**self.row, "pullback_quality": {"state": "warning"}}, "rs_pullback_quality_required"),
                 ({**self.row, "pullback_quality": {}}, "rs_pullback_quality_missing"),
                 ({**self.row, "trade_plan": {**self.row["trade_plan"], "stop_price": 100}}, "risk_exceeds_5pct"),
                 ({**self.row, "trade_plan": {**self.row["trade_plan"], "stop_price": -1}}, "plan_levels_missing")]
        for row, reason in cases:
            with self.subTest(reason=reason):
                item = self.classify(row)
                self.assertNotEqual(item["status"], "pending")
                self.assertIn(reason, item["reasons"])
        for state in ("constructive", "neutral"):
            self.assertEqual(self.classify({**self.row, "pullback_quality": {"state": state}})["status"], "pending")
        wide = self.classify({**self.row, "trade_readiness": "reclaim_trigger", "trade_plan": {**self.row["trade_plan"], "stop_price": 100}})
        self.assertTrue(wide["rs_exception_checks"]["confirmed"])
        self.assertFalse(wide["rs_exception_checks"]["risk_within_5pct"])
        self.assertNotIn("awaiting_confirmation", wide["reasons"])
        self.assertEqual(self.classify({**self.row, "pullback_quality": {"state": "failed"}})["status"], "avoid")
        self.assertEqual(self.classify({**self.row, "pullback_quality": {"state": "unknown"}})["status"], "blocked_data")

    def test_stale_gaps_bad_volume_and_trend(self):
        for frame, spy in ((self.frame.iloc[:-2], self.spy), (self.frame, self.spy.iloc[:-2]),
                           (self.frame.drop(self.dates[-10]), self.spy), (self.frame, self.spy.drop(self.dates[-10]))):
            self.assertEqual(self.classify(frame=frame, spy=spy)["status"], "blocked_data")
        thin = self.frame.copy()
        thin["Volume"] = 10
        self.assertIn("rs_liquidity_below_minimum", self.classify(frame=thin)["reasons"])
        for value in (None, float("nan"), 0, -1):
            invalid = self.frame.copy()
            invalid.iloc[-2, invalid.columns.get_loc("Volume")] = value
            self.assertIn("rs_exception_data_missing", self.classify(frame=invalid)["reasons"])
        down = self.frame.copy()
        down["Close"] = list(reversed(down["Close"].tolist()))
        self.assertIn("rs_trend_not_qualified", self.classify(frame=down)["reasons"])
        fast_spy = self.spy.copy()
        fast_spy["Close"] = [200 + i * 10 for i in range(261)]
        self.assertIn("rs_relative_strength_not_improving", self.classify(spy=fast_spy)["reasons"])

    def test_new_book_isolated_two_session_fill_and_idempotent(self):
        repo, calls = _ForwardRepo(), []
        def loader(tickers, day, days, **kwargs):
            calls.append((tickers, days))
            return {ticker: self.spy if ticker == "SPY" else self.frame for ticker in tickers}
        service = MartinForwardTestService(repo, bar_loader=loader)
        def snapshot(day, source):
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source,
                    "source_fingerprint": str(source), "guru_board": {"rows": [self.row]}}
        service.run_session(as_of_date=self.day, snapshot=snapshot(self.day, 1), strategy_id=RS_EXPANDED_STRATEGY_ID)
        saved = repo.candidates[0]["payload_json"]
        self.assertEqual(saved["status"], "pending")
        self.assertLessEqual(saved["allocation_dollars"], .001 * saved["rs_exception_metrics"]["adv20_dollars"])
        self.assertTrue(saved["rs_exception_checks"]["order_notional_within_liquidity"])
        self.assertEqual(calls, [(["ABC"], 35), (["ABC", "SPY"], 260)])
        day2 = self.dates[-1].date()
        service.run_session(as_of_date=day2, snapshot=snapshot(day2, 2), strategy_id=RS_EXPANDED_STRATEGY_ID)
        service.run_session(as_of_date=day2, snapshot=snapshot(day2, 2), strategy_id=RS_EXPANDED_STRATEGY_ID)
        self.assertEqual(len(repo.positions), 1)
        self.assertEqual(repo.positions[0]["payload_json"]["rs_tier"], "emerging")
        self.assertEqual(len(repo.runs), 2)
        service.run_session(as_of_date=self.day, snapshot=snapshot(self.day, 1))
        self.assertEqual(len(repo.runs), 3)
        self.assertEqual(repo.candidates[-1]["status"], "blocked")

    def test_cohorts_closed_trade_drawdown_and_account_marks(self):
        positions = [{"status": "closed", "closed_on": day, "payload_json": {"rs_tier": tier, "realized_r": r,
                     "realized_pnl": r * 500, "holding_sessions": 3, "exit_type": exit_type}}
                     for tier, day, r, exit_type in (("emerging", "2026-10-01", 2, "ema9_exit"),
                        ("emerging", "2026-10-02", -1, "stop"), ("emerging", "2026-10-02", .5, "ema9_exit"))]
        result = _rs_tier_metrics(positions)["emerging"]
        self.assertEqual(result["closed_count"], 3)
        self.assertEqual(result["max_closed_drawdown_r"], -.5)
        self.assertAlmostEqual(result["stop_out_rate_pct"], 100 / 3)
        context = _performance_context(RS_EXPANDED_STRATEGY_ID, [{"session_date": "2026-10-02", "summary_json":
                    {"cumulative_r": 1.5, "account_equity": 100750, "r_drawdown": -.5}}], [], positions, [])
        self.assertEqual(context["history_source"], "stored_session_marks")
        self.assertEqual(context["metrics"]["account_equity"], 100750)
        self.assertIn("rs_tier_metrics", context)

    def test_qualification_loader_failure_keeps_preferred_baseline_available(self):
        repo = _ForwardRepo()
        def loader(tickers, day, days, **kwargs):
            if days == 260:
                raise RuntimeError("database offline")
            return {ticker: self.frame for ticker in tickers}
        rows = [self.row, {**self.row, "ticker": "PREF", "daily_rs_rating": 95}]
        snapshot = {"target_trading_date": self.day.isoformat(), "_snapshot_run_id": 1,
                    "source_fingerprint": "1", "guru_board": {"rows": rows}}
        with self.assertLogs(level="WARNING"):
            MartinForwardTestService(repo, bar_loader=loader).run_session(as_of_date=self.day, snapshot=snapshot, strategy_id=RS_EXPANDED_STRATEGY_ID)
        by_ticker = {row["ticker"]: row for row in repo.candidates}
        self.assertEqual(by_ticker["PREF"]["status"], "pending")
        self.assertEqual(by_ticker["ABC"]["status"], "blocked_data")
        self.assertEqual(by_ticker["PREF"]["rank"], 1)
        self.assertEqual(by_ticker["ABC"]["payload_json"]["source_snapshot_rank"], 1)

    def test_actual_order_notional_is_checked_without_resizing(self):
        repo = _ForwardRepo()
        candidate = self.classify()
        candidate["rs_exception_metrics"]["adv20_dollars"] = 1_000_000
        row = {**self.row, "_rs_candidate": candidate}
        service = MartinForwardTestService(repo, bar_loader=lambda *args, **kwargs: {})
        snapshot = {"target_trading_date": self.day.isoformat(), "_snapshot_run_id": 1, "source_fingerprint": "1"}
        run = service._save_snapshot(self.day, snapshot, [row], RS_EXPANDED_STRATEGY_ID)
        service._reserve_new_candidates([row], self.day, run, {}, {}, RS_EXPANDED_STRATEGY_ID)
        saved = repo.candidates[0]["payload_json"]
        self.assertEqual(saved["status"], "watch")
        self.assertFalse(saved["selected_for_book"])
        self.assertEqual(saved["reasons"], ["rs_order_notional_exceeds_liquidity"])
        self.assertFalse(saved["rs_exception_checks"]["order_notional_within_liquidity"])

    def test_waiting_capacity_does_not_claim_order_liquidity_passed(self):
        repo = _ForwardRepo()
        repo.positions = [{"id": index + 1, "strategy_id": RS_EXPANDED_STRATEGY_ID, "ticker": f"OPEN{index}",
                           "status": "open", "opened_on": self.day,
                           "payload_json": {"entry_price": 100, "remaining_shares": 200}} for index in range(5)]
        candidate = self.classify()
        row = {**self.row, "_rs_candidate": candidate}
        service = MartinForwardTestService(repo, bar_loader=lambda *args, **kwargs: {})
        snapshot = {"target_trading_date": self.day.isoformat(), "_snapshot_run_id": 1, "source_fingerprint": "1"}
        run = service._save_snapshot(self.day, snapshot, [row], RS_EXPANDED_STRATEGY_ID)
        service._reserve_new_candidates([row], self.day, run, {}, {}, RS_EXPANDED_STRATEGY_ID)
        saved = repo.candidates[0]["payload_json"]
        self.assertEqual(saved["status"], "waiting_capacity")
        self.assertNotIn("order_notional_within_liquidity", saved["rs_exception_checks"])
        self.assertNotIn("actual_order_notional", saved["rs_exception_metrics"])

    def test_daily_runner_invokes_new_partition(self):
        from scripts import run_martin_forward_test as runner
        with patch.object(runner, "load_webapp_config", return_value=SimpleNamespace(artifacts_dir="/tmp", database_url="")), \
             patch.object(runner, "WatchlistService") as watchlist, \
             patch.object(runner, "MartinForwardTestService") as service, \
             patch("sys.argv", ["run_martin_forward_test.py"]), patch("builtins.print"):
            watchlist.return_value._load_latest_persisted_scanner_top_hits_payload.return_value = {"target_trading_date": self.day.isoformat()}
            service.return_value.run_session.return_value = {}
            self.assertEqual(runner.main(), 0)
        calls = service.return_value.run_session.call_args_list
        self.assertEqual(len(calls), 4)
        self.assertEqual(calls[-1].kwargs["strategy_id"], RS_EXPANDED_STRATEGY_ID)


if __name__ == "__main__":
    unittest.main()
