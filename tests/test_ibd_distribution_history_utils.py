from __future__ import annotations

import unittest

from src.ibd_distribution_day_monitor.history_utils import prepare_effective_history


class IbdDistributionHistoryUtilsTests(unittest.TestCase):
    def test_prepare_effective_history_snaps_to_latest_loaded_session_before_as_of(self) -> None:
        history = [
            {"date": "2026-06-25", "close": 1, "volume": 1},
            {"date": "2026-06-24", "close": 1, "volume": 1},
            {"date": "2026-06-23", "close": 1, "volume": 1},
        ]

        effective, audit = prepare_effective_history(
            history,
            as_of="2026-06-26",
            required_min_sessions=2,
        )

        self.assertEqual(effective[0]["date"], "2026-06-25")
        self.assertEqual(audit["as_of_resolved"], "2026-06-25")
        self.assertIn("as_of_snapped_to_latest_available_session", audit["audit_flags"])
