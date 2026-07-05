from __future__ import annotations

import unittest

import pandas as pd

from src.config import AppConfig
from src.earnings_gap_screen import _build_price_frame, find_recent_gap_signal


class _FakeFinancials:
    def __init__(self, price_rows: list[dict[str, object]]) -> None:
        self._price_rows = price_rows

    def _get_clean_price_data(self) -> list[dict[str, object]]:
        return self._price_rows


def _price_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx, date_value in enumerate(pd.date_range(start="2026-04-01", periods=60, freq="B")):
        close_value = 50.0 + (idx * 0.25)
        rows.append(
            {
                "formatted_date": date_value.date().isoformat(),
                "open": round(close_value - 0.3, 4),
                "high": round(close_value + 0.6, 4),
                "low": round(close_value - 0.6, 4),
                "close": round(close_value, 4),
                "volume": 1_000_000.0,
            }
        )
    return rows


class EarningsGapScreenTests(unittest.TestCase):
    def test_monster_gap_rejects_large_close_gain_without_true_gap(self) -> None:
        rows = _price_rows()
        rows[-2].update({"open": 66.0, "high": 68.0, "low": 65.5, "close": 67.01, "volume": 1_100_000.0})
        rows[-1].update({"open": 67.89, "high": 88.58, "low": 67.5, "close": 85.8, "volume": 7_006_800.0})

        frame = _build_price_frame(_FakeFinancials(rows))
        snapshot = find_recent_gap_signal(frame, config=AppConfig(), profile="monster-gap", lookback_days=15)

        self.assertIsNone(snapshot)

    def test_monster_gap_uses_true_gap_pct_against_prior_high(self) -> None:
        rows = _price_rows()
        rows[-2].update({"open": 58.0, "high": 60.0, "low": 57.5, "close": 59.0, "volume": 1_100_000.0})
        rows[-1].update({"open": 76.0, "high": 84.0, "low": 75.0, "close": 82.0, "volume": 6_000_000.0})

        frame = _build_price_frame(_FakeFinancials(rows))
        snapshot = find_recent_gap_signal(frame, config=AppConfig(), profile="monster-gap", lookback_days=15)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.gap_pct, 25.0, places=2)
        self.assertAlmostEqual(snapshot.close_gap_pct, ((82.0 / 59.0) - 1.0) * 100.0, places=2)
        self.assertIn("low above prior high 60.00", snapshot.reasons)


if __name__ == "__main__":
    unittest.main()
