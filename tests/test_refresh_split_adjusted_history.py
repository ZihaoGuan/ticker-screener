from __future__ import annotations

import datetime as dt
import unittest

from scripts.refresh_split_adjusted_history import _calendar_start, _merge_events, _parse_calendar_rows


class RefreshSplitAdjustedHistoryTests(unittest.TestCase):
    def test_calendar_start_supports_historical_backfill(self) -> None:
        self.assertEqual(_calendar_start(dt.date(2026, 9, 19), 90).isoformat(), "2026-06-21")
        with self.assertRaises(ValueError):
            _calendar_start(dt.date(2026, 9, 19), -1)

    def test_calendar_announcements_are_normalized_and_ratio_corrections_requeue(self) -> None:
        announcements = _parse_calendar_rows(
            {
                "data": {
                    "rows": [
                        {
                            "symbol": "abcd",
                            "name": "Example Corp",
                            "ratio": "1 : 10",
                            "executionDate": "09/22/2026",
                        }
                    ]
                }
            }
        )
        state = {
            "events": [
                {
                    "ticker": "ABCD",
                    "execution_date": "2026-09-22",
                    "ratio": "1 : 5",
                    "status": "completed",
                    "processed_at": "2026-09-21T00:00:00+00:00",
                }
            ]
        }

        events = _merge_events(state, announcements, "2026-09-19T00:00:00+00:00")

        self.assertEqual(events[0]["ticker"], "ABCD")
        self.assertEqual(events[0]["execution_date"], "2026-09-22")
        self.assertEqual(events[0]["ratio"], "1 : 10")
        self.assertEqual(events[0]["status"], "pending")
        self.assertNotIn("processed_at", events[0])


if __name__ == "__main__":
    unittest.main()
