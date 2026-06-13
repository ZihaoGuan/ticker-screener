from __future__ import annotations

import unittest

import pandas as pd

from src.sepa_vcp_screen import build_sepa_dashboard_snapshot, find_recent_sepa_vcp_hit
from src.universe import UniverseTicker


def _sepa_vcp_stock_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-02", periods=320, freq="B")
    close_values: list[float] = []
    for idx in range(len(index)):
        if idx < 315:
            close_values.append(80.0 + (idx * 0.35))
        else:
            close_values.extend([189.2, 189.8, 190.1, 189.9, 190.3])
            break
    open_values = [value - 0.35 for value in close_values]
    high_values = [value + 0.85 for value in close_values]
    low_values = [value - 0.95 for value in close_values]
    volume_values = [1_200_000.0 for _ in close_values]
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _sepa_vcp_benchmark_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-02", periods=320, freq="B")
    close_values = [100.0 + (idx * 0.05) for idx in range(len(index))]
    return pd.DataFrame(
        {
            "Open": [value - 0.1 for value in close_values],
            "High": [value + 0.4 for value in close_values],
            "Low": [value - 0.4 for value in close_values],
            "Close": close_values,
            "Volume": [2_000_000.0 for _ in close_values],
        },
        index=index,
    )


class SepaVcpScreenTests(unittest.TestCase):
    def test_build_sepa_dashboard_snapshot_returns_latest_statuses(self) -> None:
        snapshot = build_sepa_dashboard_snapshot(
            _sepa_vcp_stock_frame(),
            _sepa_vcp_benchmark_frame(),
            benchmark_ticker="SPY",
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.tpr_status, "PASSED")
        self.assertEqual(snapshot.buy_risk_status, "Low Risk")
        self.assertEqual(snapshot.pressure_status, "Buying")
        self.assertGreater(snapshot.rpr_score, 80.0)
        self.assertTrue(snapshot.vcp_trigger)
        self.assertTrue(snapshot.recent_vcp_signal)
        self.assertEqual(snapshot.recent_vcp_signal_date, "2025-03-24")

    def test_find_recent_sepa_vcp_hit_returns_recent_signal(self) -> None:
        hit = find_recent_sepa_vcp_hit(
            _sepa_vcp_stock_frame(),
            _sepa_vcp_benchmark_frame(),
            ticker=UniverseTicker(symbol="AAPL"),
            benchmark_ticker="SPY",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "recent_vcp_squeeze")
        self.assertEqual(hit.tpr_status, "PASSED")
        self.assertEqual(hit.buy_risk_status, "Low Risk")
        self.assertGreater(hit.rpr_score, 80.0)
        self.assertLess(hit.vcp_range_pct, 2.5)


if __name__ == "__main__":
    unittest.main()
