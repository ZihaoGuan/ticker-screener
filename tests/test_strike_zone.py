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


if __name__ == "__main__":
    unittest.main()
