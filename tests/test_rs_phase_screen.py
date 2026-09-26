from __future__ import annotations

import unittest

import pandas as pd

from src.rs_phase_screen import classify_rs_phase_lifecycle, compute_rs_phase_context, find_recent_rs_phase_hit
from src.universe import UniverseTicker


def _frame(closes: list[float], *, high_spike: float | None = None) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=len(closes), freq="B")
    highs = [value * 1.01 for value in closes]
    if high_spike is not None and len(highs) > 10:
        highs[-10] = high_spike
    return pd.DataFrame(
        {
            "Open": closes,
            "High": highs,
            "Low": [value * 0.99 for value in closes],
            "Close": closes,
            "Volume": [1_000_000] * len(closes),
        },
        index=dates,
    )


class RsPhaseScreenTests(unittest.TestCase):
    def test_lifecycle_marks_fresh_phase_and_mature_phase(self) -> None:
        fresh = classify_rs_phase_lifecycle(pd.Series([False, True, True, True]))
        mature = classify_rs_phase_lifecycle(pd.Series([False, *([True] * 21)]))

        self.assertEqual(fresh["rs_phase_state"], "new")
        self.assertEqual(fresh["rs_phase_badge_label"], "RS New · 3D")
        self.assertEqual(mature["rs_phase_state"], "mature")
        self.assertEqual(mature["rs_phase_badge_label"], "RS Mature · 21D")

    def test_lifecycle_distinguishes_quick_reclaim_from_confirmed_loss(self) -> None:
        quick_reclaim = classify_rs_phase_lifecycle(pd.Series([False, True, True, True, False, False, True]))
        one_day_dip = classify_rs_phase_lifecycle(pd.Series([False, True, True, True, False]))
        confirmed_loss = classify_rs_phase_lifecycle(pd.Series([False, True, True, True, False, False]))

        self.assertEqual(quick_reclaim["rs_phase_state"], "quick_reclaim")
        self.assertTrue(quick_reclaim["rs_phase_quick_reclaim"])
        self.assertEqual(quick_reclaim["rs_phase_below_days_before_reclaim"], 2)
        self.assertEqual(one_day_dip["rs_phase_state"], "inactive")
        self.assertFalse(one_day_dip["rs_phase_loss_confirmed"])
        self.assertEqual(confirmed_loss["rs_phase_state"], "lost")
        self.assertTrue(confirmed_loss["rs_phase_loss_confirmed"])

    def test_context_marks_active_rs_phase_and_before_price_high(self) -> None:
        stock_closes = [100.0 + index * 0.5 for index in range(80)]
        stock_closes[-11] = 150.0
        benchmark_closes = [100.0] * 70 + [70.0] * 10
        benchmark = _frame(benchmark_closes)
        stock = _frame(stock_closes)

        context = compute_rs_phase_context(stock, benchmark)

        self.assertIsNotNone(context)
        assert context is not None
        self.assertTrue(context["rs_phase_active"])
        self.assertGreaterEqual(context["rs_phase_active_days"], 3)
        self.assertTrue(context["daily_rs_new_high"])
        self.assertTrue(context["daily_rs_new_high_before_price"])

    def test_hit_requires_active_days_threshold(self) -> None:
        benchmark = _frame([100.0] * 80)
        stock = _frame([100.0] * 70 + [90.0, 91.0, 92.0, 93.0, 94.0, 110.0, 112.0, 114.0, 116.0, 118.0])
        ticker = UniverseTicker(symbol="TEST", sector="Software", industry="Apps", exchange="NYSE")

        hit = find_recent_rs_phase_hit(stock, benchmark, ticker=ticker, benchmark_ticker="SPY", min_active_days=3)

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "TEST")
        self.assertGreaterEqual(hit.rs_phase_active_days, 3)
        self.assertIn("RS line above 21 EMA", hit.reasons[0])

    def test_falling_rs_line_does_not_pass(self) -> None:
        benchmark = _frame([100.0] * 80)
        stock = _frame([140.0 - index * 0.5 for index in range(80)])
        ticker = UniverseTicker(symbol="WEAK")

        hit = find_recent_rs_phase_hit(stock, benchmark, ticker=ticker, benchmark_ticker="SPY", min_active_days=3)

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
