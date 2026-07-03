from __future__ import annotations

import unittest

from src.gap_fill_screen import _scan_open_overhead_gap


def _price_bar(date: str, *, open: float, high: float, low: float, close: float) -> dict[str, object]:
    return {
        "formatted_date": date,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
    }


class GapFillScreenTests(unittest.TestCase):
    def test_scan_uses_remaining_unfilled_zone_after_partial_reclaim(self) -> None:
        price_data = [
            _price_bar("2025-10-29", open=105.4, high=106.04, low=103.35, close=104.55),
            _price_bar("2025-10-30", open=81.49, high=82.20, low=75.75, close=77.25),
            _price_bar("2025-10-31", open=77.34, high=80.97, low=76.88, close=78.96),
            _price_bar("2025-11-03", open=78.85, high=84.03, low=76.88, close=83.82),
            _price_bar("2026-05-19", open=89.98, high=94.89, low=89.21, close=90.02),
            _price_bar("2026-07-02", open=87.18, high=90.50, low=86.84, close=89.94),
        ]

        gap = _scan_open_overhead_gap(
            price_data,
            lookback_days=180,
            min_gap_pct=0.03,
            current_price=89.94,
        )

        self.assertIsNotNone(gap)
        assert gap is not None
        self.assertEqual(gap["gap_date"], "2025-10-30")
        self.assertAlmostEqual(float(gap["gap_bottom"]), 94.89, places=2)
        self.assertAlmostEqual(float(gap["gap_top"]), 103.35, places=2)
        self.assertAlmostEqual(float(gap["distance_to_gap_bottom_pct"]), 5.5037, places=3)
        self.assertFalse(bool(gap["gap_reclaimed"]))

    def test_scan_rejects_gap_once_fully_filled(self) -> None:
        price_data = [
            _price_bar("2025-10-29", open=105.4, high=106.04, low=103.35, close=104.55),
            _price_bar("2025-10-30", open=81.49, high=82.20, low=75.75, close=77.25),
            _price_bar("2025-11-10", open=95.0, high=103.40, low=94.5, close=102.0),
            _price_bar("2026-07-02", open=101.0, high=102.0, low=99.0, close=100.0),
        ]

        gap = _scan_open_overhead_gap(
            price_data,
            lookback_days=180,
            min_gap_pct=0.03,
            current_price=100.0,
        )

        self.assertIsNone(gap)


if __name__ == "__main__":
    unittest.main()
