from __future__ import annotations

import datetime as dt
import unittest

from src.earnings_trade_analyzer_runtime import analyze_stock as analyze_earnings_stock
from src.earnings_trade_analyzer_runtime import apply_entry_filter
from src.pead_screener_runtime import analyze_stock as analyze_pead_stock


def _build_daily_prices() -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    base_price = 100.0
    for day in range(70):
        date_str = (dt.date(2026, 6, 30) - dt.timedelta(days=day)).isoformat()
        close_price = base_price - (day * 0.5)
        row = {
            "date": date_str,
            "open": close_price - 0.4,
            "high": close_price + 1.0,
            "low": close_price - 1.0,
            "close": close_price,
            "volume": 1_500_000 + (day * 10_000),
        }
        rows.append(row)

    rows[0].update({"date": "2026-06-30", "open": 119.0, "high": 121.0, "low": 118.5, "close": 120.0, "volume": 3_000_000})
    rows[1].update({"date": "2026-06-29", "open": 111.0, "high": 112.0, "low": 109.0, "close": 110.0, "volume": 2_800_000})
    rows[2].update({"date": "2026-06-26", "open": 107.0, "high": 108.0, "low": 104.0, "close": 105.0, "volume": 2_400_000})
    rows[3].update({"date": "2026-06-25", "open": 96.0, "high": 98.0, "low": 95.0, "close": 97.0, "volume": 2_200_000})
    rows[4].update({"date": "2026-06-24", "open": 95.5, "high": 97.0, "low": 94.0, "close": 96.0, "volume": 2_100_000})
    rows[5].update({"date": "2026-06-23", "open": 94.0, "high": 95.0, "low": 92.0, "close": 93.0, "volume": 2_000_000})
    rows[6].update({"date": "2026-06-22", "open": 95.0, "high": 96.0, "low": 93.0, "close": 94.0, "volume": 1_900_000})
    return rows


class PostEarningsRuntimeTests(unittest.TestCase):
    def test_earnings_trade_runtime_scores_stock(self) -> None:
        analysis = analyze_earnings_stock(_build_daily_prices(), "2026-06-26", "amc")
        self.assertGreater(analysis["composite"]["composite_score"], 0)
        self.assertAlmostEqual(analysis["gap"]["gap_pct"], 5.71, places=2)
        self.assertIn(analysis["composite"]["grade"], {"B", "C"})

    def test_entry_filter_keeps_stronger_setup(self) -> None:
        kept = apply_entry_filter(
            [
                {"symbol": "AAA", "current_price": 35, "gap_pct": 5, "composite_score": 75},
                {"symbol": "BBB", "current_price": 20, "gap_pct": 5, "composite_score": 75},
            ]
        )
        self.assertEqual([item["symbol"] for item in kept], ["AAA"])

    def test_pead_runtime_detects_breakout(self) -> None:
        analysis = analyze_pead_stock(
            symbol="TEST",
            daily_prices=_build_daily_prices(),
            earnings_date="2026-06-26",
            earnings_timing="amc",
            gap_pct=5.71,
            current_price=120.0,
            watch_weeks=5,
        )
        assert analysis is not None
        self.assertIn(analysis["stage"], {"MONITORING", "SIGNAL_READY", "BREAKOUT"})
        self.assertGreaterEqual(analysis["composite_score"], 0)


if __name__ == "__main__":
    unittest.main()
