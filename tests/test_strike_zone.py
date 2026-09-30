from __future__ import annotations

import datetime as dt
import unittest

from src.webapp.services.strike_zone import build_strike_zone


class StrikeZoneTests(unittest.TestCase):
    def test_active_requires_fresh_trigger_and_confirmation(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.2,
                "earnings_days": 14,
                "daily_rs_rating": 95,
                "stage_analysis": {"alias": "2A"},
                "signal_state": "active",
                "active_profiles": ["D EMA21"],
                "scanners": [
                    {"id": "ma_pullback_retest", "sort_date": "2026-09-25"},
                    {"id": "trend_template", "sort_date": "2026-09-25"},
                ],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        self.assertEqual(result["state"], "active")
        self.assertEqual(result["score"], 65)
        self.assertIn("reclaim", str(result["primary_signal"]))
        groups = {item["id"]: item for item in result["score_breakdown"]["groups"]}
        self.assertEqual(groups["trigger"]["awarded_points"], 40)
        self.assertEqual(groups["confirmation"]["awarded_points"], 25)

    def test_ready_uses_tight_setup_without_an_entry_trigger(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "daily_rs_rating": 92,
                "stage_analysis": {"alias": "2B"},
                "rmv": {"rank": 1, "as_of_date": "2026-09-25"},
                "scanners": [{"id": "three_weeks_tight", "sort_date": "2026-09-25"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        self.assertEqual(result["state"], "ready")
        self.assertGreaterEqual(int(result["score"]), 45)
        self.assertEqual(result["primary_signal"], "three weeks tight")

    def test_hard_risks_override_other_evidence(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 5.1,
                "earnings_days": 2,
                "stage_analysis": {"alias": "4A"},
                "scanners": [{"id": "darvas_box_breakout", "sort_date": "2026-09-25"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        self.assertEqual(result["state"], "avoid")
        self.assertEqual(result["score"], 0)
        self.assertGreaterEqual(len(result["warnings"]), 3)
        self.assertTrue(result["score_breakdown"]["blocked"])

    def test_stale_breakout_does_not_remain_active(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "daily_rs_rating": 95,
                "stage_analysis": {"alias": "2A"},
                "scanners": [{"id": "darvas_box_breakout", "sort_date": "2026-09-10"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        self.assertEqual(result["state"], "context")
        self.assertIsNone(result["primary_signal"])

    def test_rs_phase_lifecycle_confirms_but_does_not_create_an_active_entry(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "rs_phase_state": "quick_reclaim",
                "daily_rs_new_high_before_price": True,
                "scanners": [],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        self.assertEqual(result["state"], "context")
        self.assertEqual(result["score"], 10)
        self.assertIn("RS Phase quick reclaim leads price", [item["label"] for item in result["supporting_signals"]])

    def test_wyckoff_buy_signal_appears_in_hover_score_breakdown(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "daily_rs_rating": 95,
                "stage_analysis": {"alias": "2A"},
                "scanners": [{"id": "wyckoff_buy_signal", "sort_date": "2026-09-25"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        trigger_group = next(item for item in result["score_breakdown"]["groups"] if item["id"] == "trigger")
        self.assertEqual(trigger_group["awarded_points"], 35)
        self.assertEqual(trigger_group["signals"][0]["label"], "Wyckoff buy signal")
        self.assertEqual(trigger_group["signals"][0]["points"], 35)

    def test_vcs_critical_tightness_adds_fresh_setup_points(self) -> None:
        result = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "scanners": [{"id": "vcs_critical_tightness", "sort_date": "2026-09-25"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        setup_group = next(item for item in result["score_breakdown"]["groups"] if item["id"] == "setup")
        self.assertEqual(setup_group["awarded_points"], 20)
        self.assertEqual(setup_group["signals"][0]["label"], "VCS Critical Tight")
        self.assertEqual(result["primary_signal"], "VCS Critical Tight")

    def test_macd_golden_cross_is_a_fresh_five_point_confirmation(self) -> None:
        fresh = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "scanners": [{"id": "macd_golden_cross", "sort_date": "2026-09-25"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )
        stale = build_strike_zone(
            {
                "atr_to_sma50": 1.0,
                "earnings_days": 14,
                "scanners": [{"id": "macd_golden_cross", "sort_date": "2026-09-10"}],
            },
            as_of_date=dt.date(2026, 9, 27),
        )

        fresh_group = next(item for item in fresh["score_breakdown"]["groups"] if item["id"] == "confirmation")
        stale_group = next(item for item in stale["score_breakdown"]["groups"] if item["id"] == "confirmation")
        self.assertEqual(fresh_group["awarded_points"], 5)
        self.assertEqual(fresh_group["signals"][0]["label"], "MACD Golden Cross")
        self.assertTrue(fresh_group["signals"][0]["fresh"])
        self.assertEqual(stale_group["awarded_points"], 0)
        self.assertFalse(stale_group["signals"][0]["fresh"])


if __name__ == "__main__":
    unittest.main()
