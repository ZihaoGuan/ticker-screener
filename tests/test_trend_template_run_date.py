from __future__ import annotations

import datetime as dt
import unittest

from scripts.run_trend_template_screen import _latest_completed_xnys_session, _resolve_as_of_date


class TrendTemplateRunDateTests(unittest.TestCase):
    def test_utc_midnight_uses_prior_us_session_after_close(self) -> None:
        now = dt.datetime(2026, 11, 12, 0, 3, tzinfo=dt.timezone.utc)
        self.assertEqual(_latest_completed_xnys_session(now), dt.date(2026, 11, 11))

    def test_before_market_close_uses_previous_completed_session(self) -> None:
        now = dt.datetime(2026, 11, 11, 20, 59, tzinfo=dt.timezone.utc)
        self.assertEqual(_latest_completed_xnys_session(now), dt.date(2026, 11, 10))

    def test_weekend_and_holiday_skip_to_latest_completed_session(self) -> None:
        # Saturday after Thanksgiving; Thu 26 Nov 2026 is a market holiday.
        now = dt.datetime(2026, 11, 28, 18, 0, tzinfo=dt.timezone.utc)
        self.assertEqual(_latest_completed_xnys_session(now), dt.date(2026, 11, 27))

    def test_explicit_historical_date_bypasses_clock_resolution(self) -> None:
        explicit = dt.date(2024, 1, 2)
        self.assertEqual(
            _resolve_as_of_date("2024-01-02", dt.datetime(2026, 11, 12, tzinfo=dt.timezone.utc)),
            explicit,
        )


if __name__ == "__main__":
    unittest.main()
