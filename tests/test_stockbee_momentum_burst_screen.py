from __future__ import annotations

import pandas as pd
import unittest

from src.stockbee_momentum_burst_screen import (
    evaluate_stockbee_momentum_burst_frame,
    find_recent_stockbee_momentum_burst_hit,
)
from src.universe import UniverseTicker


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame.set_index("Date")


def _good_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = [
        {"Date": "2026-06-20", "Open": 100.0, "High": 105.0, "Low": 101.0, "Close": 104.5, "Volume": 600_000},
        {"Date": "2026-06-19", "Open": 99.8, "High": 100.8, "Low": 99.1, "Close": 100.0, "Volume": 120_000},
        {"Date": "2026-06-18", "Open": 100.1, "High": 100.9, "Low": 99.3, "Close": 99.8, "Volume": 130_000},
        {"Date": "2026-06-17", "Open": 99.5, "High": 100.4, "Low": 98.8, "Close": 100.1, "Volume": 125_000},
        {"Date": "2026-06-16", "Open": 99.6, "High": 100.3, "Low": 98.7, "Close": 99.7, "Volume": 115_000},
        {"Date": "2026-06-15", "Open": 100.0, "High": 100.6, "Low": 99.2, "Close": 99.9, "Volume": 118_000},
    ]
    for index in range(25):
        rows.append(
            {
                "Date": f"2026-05-{31 - index:02d}",
                "Open": 98.5,
                "High": 101.0,
                "Low": 97.8,
                "Close": 99.0 + (index % 3) * 0.2,
                "Volume": 180_000,
            }
        )
    return _frame(rows)


class StockbeeMomentumBurstScreenTests(unittest.TestCase):
    def test_good_4pct_breakout_returns_hit(self) -> None:
        hit = find_recent_stockbee_momentum_burst_hit(
            _good_frame(),
            ticker=UniverseTicker(symbol="TEST", sector="Technology", industry="Software"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertIn(hit.state, {"ACTIONABLE_DAY1", "MANUAL_REVIEW", "WATCH_ONLY"})
        self.assertIn("4pct_breakout", hit.trigger_tags)
        self.assertIn("range_expansion", hit.trigger_tags)
        self.assertLess(hit.risk_pct_to_stop, 4.0)
        self.assertEqual(hit.reject_reasons, [])

    def test_no_trigger_is_rejected(self) -> None:
        frame = _good_frame().copy()
        latest = pd.Timestamp("2026-06-20")
        frame.loc[latest, "Open"] = 100.1
        frame.loc[latest, "High"] = 100.5
        frame.loc[latest, "Low"] = 99.7
        frame.loc[latest, "Close"] = 100.2
        frame.loc[latest, "Volume"] = 180_000

        result = evaluate_stockbee_momentum_burst_frame(frame, ticker=UniverseTicker(symbol="FLAT"))

        self.assertEqual(result["state"], "REJECTED")
        self.assertIn("no_momentum_burst_trigger", result["reject_reasons"])

    def test_risk_too_wide_is_hard_rejected(self) -> None:
        frame = _good_frame().copy()
        frame.loc[pd.Timestamp("2026-06-20"), "Low"] = 80.0

        result = evaluate_stockbee_momentum_burst_frame(frame, ticker=UniverseTicker(symbol="WIDE"))

        self.assertEqual(result["state"], "REJECTED")
        self.assertIn("risk_too_wide", result["reject_reasons"])


if __name__ == "__main__":
    unittest.main()
