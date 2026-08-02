from __future__ import annotations

import unittest

import pandas as pd

from src.rs_phase_screen import compute_rs_phase_context, find_recent_rs_phase_hit
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
    def test_context_marks_active_rs_phase_and_before_price_high(self) -> None:
        benchmark = _frame([100.0] * 80)
        stock = _frame([100.0 + index * 0.5 for index in range(80)], high_spike=160.0)

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
