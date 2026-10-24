from __future__ import annotations

import datetime as dt
import importlib.util
import unittest

from src.webapp.services.martin_rotation import session_dates


@unittest.skipUnless(importlib.util.find_spec("exchange_calendars"), "exchange_calendars is not installed")
class RotationCalendarTests(unittest.TestCase):
    def test_session_history_handles_lookback_start_on_weekends(self):
        import exchange_calendars as xcals

        calendar = xcals.get_calendar("XNYS", start="2025-01-01", end="2026-11-01")
        for day in (dt.date(2026, 10, value) for value in range(19, 24)):
            with self.subTest(day=day):
                expected = [label.date() for label in calendar.sessions_in_range("2026-08-01", day.isoformat())][-35:]
                self.assertEqual(session_dates(day), expected)


if __name__ == "__main__":
    unittest.main()
