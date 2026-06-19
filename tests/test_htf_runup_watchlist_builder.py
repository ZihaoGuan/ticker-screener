from __future__ import annotations

import unittest

from src.htf_runup_screen import HtfRunupHit
from src.htf_runup_watchlist_builder import build_htf_runup_watchlist


class HtfRunupWatchlistBuilderTests(unittest.TestCase):
    def test_build_watchlist_includes_new_htf_setup_fields(self) -> None:
        hit = HtfRunupHit(
            ticker="NVDA",
            sector="Technology",
            exchange="NASDAQ",
            benchmark_ticker="SPY",
            current_price=125.0,
            ema_21=118.0,
            price_above_ema21=True,
            runup_window_days=40,
            runup_pct=110.0,
            pullback_from_high_pct=6.5,
            runup_low=60.0,
            runup_high=133.0,
            runup_low_date="2026-05-01",
            runup_high_date="2026-06-10",
            has_htf_setup=True,
            htf_setup_pivot_price=126.8,
            htf_setup_distance_to_pivot_pct=0.014,
            htf_setup_flag_days=11,
            htf_setup_pole_gain_ratio=2.05,
            reasons=["110.0% runup in 40 sessions", "current HTF setup detected 1.4% below pivot"],
        )

        payload = build_htf_runup_watchlist([hit])[0]

        self.assertTrue(payload["has_htf_setup"])
        self.assertEqual(payload["htf_setup_pivot_price"], 126.8)
        self.assertEqual(payload["htf_setup_gap_pct"], 1.4)
        self.assertIn("Current HTF setup detected", payload["summary"])


if __name__ == "__main__":
    unittest.main()
