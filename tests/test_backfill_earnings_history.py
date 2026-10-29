from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.backfill_earnings_history import check_apply_allowed, fetch_with_retries, read_seed


class EarningsHistoryBackfillTests(unittest.TestCase):
    def test_seed_filters_universe_and_uses_date_only_without_timing_claim(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.csv"
            path.write_text(
                "Ticker,Date,Earnings Date,EPS Estimate,Reported EPS\n"
                "ABC,2016-03-01,2016-03-01 16:30:00-05:00,1.0,1.2\n"
                "ABC,2026-01-27,2026-01-27 16:30:00-05:00,0.5,NaN\n"
                "XYZ,2018-04-02,2018-04-02 08:00:00-04:00,0.4,0.5\n"
                "ABC,2014-04-02,2014-04-02 16:30:00-04:00,0.2,0.2\n",
                encoding="utf-8")
            rows = read_seed(path, {"ABC"})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ticker"], "ABC")
        self.assertEqual(rows[0]["earnings_date"], "2016-03-01")
        self.assertEqual(rows[0]["session_timing"], "after_market")
        self.assertEqual(rows[0]["eps_actual"], 1.2)
        self.assertEqual(rows[0]["eps_estimate"], 1.0)
        self.assertEqual(rows[0]["source"], "retrospective_backfill:vivek_v_rao_earnings_dates_csv")

    def test_yahoo_429_uses_three_total_attempts_then_stops(self):
        calls, sleeps = [], []
        def rate_limit(_ticker):
            calls.append(1)
            raise RuntimeError("HTTP 429")
        rows, error, stop = fetch_with_retries("ABC", rate_limit, sleeps.append)
        self.assertEqual(rows, [])
        self.assertEqual(len(calls), 3)
        self.assertEqual(sleeps, [1, 2])
        self.assertTrue(stop)
        self.assertIn("3 total attempts", error)

    def test_incomplete_proposal_requires_explicit_override(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            check_apply_allowed({"complete": False}, allow_incomplete=False)
        check_apply_allowed({"complete": False}, allow_incomplete=True)


if __name__ == "__main__":
    unittest.main()
