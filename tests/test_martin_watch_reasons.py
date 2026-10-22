import math
import unittest

from src.webapp.services.martin_forward_test_service import classify_candidate
from src.webapp.services.watchlist_service import _build_trade_plan, _trade_readiness


class MartinWatchReasonsTests(unittest.TestCase):
    def setUp(self):
        self.row = {"ticker": "TEST", "market_cap": 2e9, "day_close": 50, "daily_rs_rating": 95,
                    "scanner_ids": ["rs"], "adr14_pct": 4, "trade_readiness": "pullback_zone",
                    "trade_plan": {"state": "ready", "trigger_price": 50, "stop_price": 48,
                                   "max_entry_price": 48 / .95, "reference_risk_pct": 4,
                                   "source_strategy_id": "ma_pullback_retest", "signal_date": "2026-10-21"}}

    def classify(self, **plan_updates):
        return classify_candidate({**self.row, "trade_plan": {**self.row["trade_plan"], **plan_updates}}, 1)

    def test_ready_levels_explain_confirmation_and_keep_watch(self):
        result = self.classify()
        self.assertEqual(result["status"], "watch")
        self.assertEqual(result["reasons"], ["awaiting_confirmation"])
        self.assertEqual(result["plan_state"], "ready")
        self.assertEqual(result["reference_risk_pct"], 4)
        self.assertEqual(result["source_strategy_id"], "ma_pullback_retest")

    def test_ready_over_limit_can_explain_both_obstacles(self):
        result = self.classify(stop_price=47, reference_risk_pct=6)
        self.assertEqual(result["status"], "watch")
        self.assertCountEqual(result["reasons"], ["awaiting_confirmation", "risk_exceeds_5pct"])

    def test_active_over_limit_is_not_missing_levels(self):
        result = self.classify(state="active", stop_price=47, reference_risk_pct=6)
        self.assertEqual(result["reasons"], ["risk_exceeds_5pct"])
        self.assertEqual(result["status"], "watch")

    def test_risk_reason_uses_unrounded_levels_at_the_cap_boundary(self):
        result = self.classify(state="active", stop_price=47.499, reference_risk_pct=5.0)
        self.assertEqual(result["reasons"], ["risk_exceeds_5pct"])

    def test_no_supported_plan_is_distinct_from_missing_plan_levels(self):
        result = classify_candidate({**self.row, "trade_readiness": "leader", "trade_plan": None}, 1)
        self.assertEqual(result["reasons"], ["no_supported_plan"])
        result = self.classify(state="active", trigger_price=None, stop_price=None)
        self.assertEqual(result["reasons"], ["plan_levels_missing"])

    def test_nonfinite_nonpositive_or_inverted_levels_remain_watch(self):
        for stop in (math.nan, math.inf, -1, 0, 50, 51):
            with self.subTest(stop=stop):
                result = self.classify(state="active", stop_price=stop)
                self.assertEqual(result["status"], "watch")
                self.assertEqual(result["reasons"], ["plan_levels_missing"])

    def test_pending_eligibility_and_limit_are_unchanged(self):
        row = {**self.row, "trade_readiness": "defined_risk"}
        result = classify_candidate(row, 1)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["reasons"], [])
        self.assertAlmostEqual(result["limit_price"], 48 / .95)
        row["adr14_pct"] = None
        self.assertEqual(classify_candidate(row, 1)["reasons"], ["missing_adr_or_stop"])


class CompletePlanSourceTests(unittest.TestCase):
    def setUp(self):
        self.legacy = {"_source_strategy_id": "ema21_pullback_buy", "_source_date": "2026-10-21"}
        self.retest = {"_source_strategy_id": "ma_pullback_retest", "_source_date": "2026-10-21",
                       "signal_state": "ready", "trigger_price": 14.886, "stop_price": 14.301004253273385}

    def test_avpt_falls_back_to_complete_same_session_without_confirmation(self):
        plan = _build_trade_plan([self.legacy, self.retest])
        self.assertEqual(plan["source_strategy_id"], "ma_pullback_retest")
        self.assertEqual(plan["state"], "ready")
        self.assertEqual(plan["stop_price"], self.retest["stop_price"])
        self.assertEqual(_trade_readiness({"trade_plan": plan}), "pullback_zone")
        candidate = classify_candidate({"ticker": "AVPT", "market_cap": 3.03e9, "day_close": 15.72,
            "daily_rs_rating": 91.255587, "scanner_ids": ["rs"], "adr14_pct": 4.1,
            "trade_plan": plan, "trade_readiness": _trade_readiness({"trade_plan": plan})}, 1)
        self.assertEqual(candidate["status"], "watch")
        self.assertEqual(candidate["reasons"], ["awaiting_confirmation"])

    def test_existing_complete_active_priority_is_preserved_even_above_five_percent(self):
        plan = _build_trade_plan([{**self.legacy, "trigger_price": 20, "stop_price": 18}, self.retest])
        self.assertEqual(plan["source_strategy_id"], "ema21_pullback_buy")
        self.assertEqual(plan["risk_status"], "over_limit")

    def test_incomplete_or_invalid_active_source_falls_back_without_mixing(self):
        for trigger, stop in ((None, 10), (math.nan, 10), (math.inf, 10), (20, 0), (20, -1), (20, 20), (20, 21)):
            with self.subTest(trigger=trigger, stop=stop):
                plan = _build_trade_plan([{**self.legacy, "trigger_price": trigger, "stop_price": stop}, self.retest])
                self.assertEqual(plan["source_strategy_id"], "ma_pullback_retest")
                self.assertEqual(plan["trigger_price"], self.retest["trigger_price"])
                self.assertEqual(plan["stop_price"], self.retest["stop_price"])

    def test_older_or_undated_alternative_cannot_revive_an_old_plan(self):
        for day in ("2026-10-20", "2026-10-22", "", "invalid-date"):
            with self.subTest(day=day):
                plan = _build_trade_plan([self.legacy, {**self.retest, "_source_date": day}])
                self.assertEqual(plan["source_strategy_id"], "ema21_pullback_buy")
                self.assertIsNone(plan["trigger_price"])
        plan = _build_trade_plan([{**self.legacy, "_source_date": ""}, {**self.retest, "_source_date": ""}])
        self.assertEqual(plan["source_strategy_id"], "ema21_pullback_buy")

    def test_all_incomplete_keeps_preferred_source_for_diagnosis(self):
        plan = _build_trade_plan([self.legacy, {**self.retest, "trigger_price": None}])
        self.assertEqual(plan["source_strategy_id"], "ema21_pullback_buy")
        self.assertEqual(plan["risk_status"], "invalid_levels")
