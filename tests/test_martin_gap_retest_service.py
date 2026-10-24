from __future__ import annotations

import unittest
from datetime import date

from src.webapp.services.martin_gap_retest_service import active_gap_events, manage_position


class GapRetestServiceTests(unittest.TestCase):
    def test_admission_uses_only_active_gap_signal(self):
        source = {"id": 42, "target_trading_date": "2026-10-23", "source_fingerprint": "f",
                  "hits": [
                      {"ticker": "GAP", "hit_payload_json": {"ticker": "GAP", "power_gap_volume_context": {
                          "active_signals": ["gap"], "rules_version": "power_gap_volume_watch_v1", "gap_event": {"date": "2026-10-22", "low": 10}}}},
                      {"ticker": "VOL", "hit_payload_json": {"ticker": "VOL", "power_gap_volume_context": {
                          "active_signals": ["hve"]}}},
                  ]}
        events = active_gap_events(source, date(2026, 10, 23))
        self.assertEqual(["GAP"], [event["ticker"] for event in events])
        self.assertEqual(42, events[0]["source_run_id"])

    def test_gap_stop_uses_open_and_adverse_slippage(self):
        position = {"entry_price": 99, "stop": 97, "remaining_shares": 100, "initial_shares": 100}
        action = manage_position(position, {"open": 96, "high": 100, "low": 95, "close": 99}, day=date(2025, 1, 2))[0]
        self.assertEqual("gap_stop", action["type"])
        self.assertEqual(95.904, action["price"])

    def test_entry_day_trim_is_suppressed(self):
        position = {"entry_price": 100, "stop": 97, "initial_r_per_share": 3, "initial_shares": 100,
                    "remaining_shares": 100, "opened_on": "2025-01-02", "trimmed": False}
        actions = manage_position(position, {"open": 110, "high": 111, "low": 99, "close": 110}, day=date(2025, 1, 2))
        self.assertEqual("mark", actions[0]["type"])


if __name__ == "__main__":
    unittest.main()
