"""Independent review checks at the book's pricing and persistence boundaries."""
import json
import unittest
from datetime import date

from src.martin_daily_gap_retest import rank_confirmations, resolve_gap_next_open, size_gap_order
from src.webapp.services.martin_gap_retest_service import _published_before_session, manage_position


class GapRetestPolicyBoundaryTests(unittest.TestCase):
    def test_allocated_order_is_json_serializable_for_real_repository(self):
        order = size_gap_order({"stop": 97, "max_entry": 100, "adv20_dollars": 30_000_000},
                               available_cash=100_000, available_risk=2_500, slots_available=8)
        saved = json.loads(json.dumps(order))
        self.assertEqual(saved["shares"], 166)
        self.assertEqual(saved["reservation_cash"], 16_600)
        self.assertEqual(saved["planned_stop_risk"], 498)

    def test_zero_relative_strength_ranks_above_negative(self):
        rows = [{"ticker": "FLAT", "relative_return20_pct": 0, "adr14_pct": 5, "adv20_dollars": 30_000_000},
                {"ticker": "LAG", "relative_return20_pct": -1, "adr14_pct": 5, "adv20_dollars": 30_000_000}]
        self.assertEqual([r["ticker"] for r in rank_confirmations(rows)], ["FLAT", "LAG"])

    def test_published_gap_run_must_be_after_close_and_before_next_open(self):
        friday, monday = date(2025, 1, 10), date(2025, 1, 13)
        self.assertFalse(_published_before_session({"published_at": "2025-01-10T20:00:00Z"}, friday, monday))
        self.assertTrue(_published_before_session({"published_at": "2025-01-11T12:00:00Z"}, friday, monday))
        self.assertFalse(_published_before_session({"published_at": "2025-01-13T14:30:00Z"}, friday, monday))
        self.assertFalse(_published_before_session({"published_at": "2025-01-10T22:00:00"}, friday, monday))

    def test_stop_wins_when_daily_bar_also_crosses_three_r(self):
        position = {"entry_price": 100, "stop": 99, "initial_r_per_share": 1,
                    "remaining_shares": 100, "initial_shares": 100, "opened_on": "2025-01-09"}
        actions = manage_position(position, {"open": 100, "high": 105, "low": 98, "close": 104},
                                  day=date(2025, 1, 10), ema9=100)
        self.assertEqual(actions[0]["type"], "stop")
        self.assertAlmostEqual(actions[0]["price"], 98.901)
        self.assertFalse(any(a["type"] == "trim_3r" for a in actions))

    def test_trim_does_not_lose_same_day_ema_exit_signal(self):
        position = {"entry_price": 100, "stop": 99, "initial_r_per_share": 1,
                    "remaining_shares": 100, "initial_shares": 100, "opened_on": "2025-01-09"}
        actions = manage_position(position, {"open": 103, "high": 104, "low": 102, "close": 102.8},
                                  day=date(2025, 1, 10), ema9=103.5)
        self.assertEqual(actions[0]["type"], "trim_3r")
        self.assertEqual(actions[0]["shares"], 10)
        self.assertTrue(actions[0].get("schedule_exit"))

    def test_next_open_order_never_uses_later_day_low(self):
        result = resolve_gap_next_open({"stop": 97, "max_entry": 100, "shares": 100},
                                       {"open": 101, "low": 99, "high": 102, "close": 100})
        self.assertEqual(result["status"], "expired")


if __name__ == "__main__":
    unittest.main()
