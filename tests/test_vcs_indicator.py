from __future__ import annotations

import unittest

import pandas as pd

from src.vcs_indicator import latest_vcs_snapshot


def _vcs_frame(*, compressed: bool, variant: int = 0) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=90, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 70:
            center = 100.0 + (idx * (0.35 + (0.03 * variant)))
            bar_range = 5.0 + (variant * 0.4) - min(idx * 0.02, 1.0)
            volume = 2_000_000.0 + (variant * 100_000.0) - (idx * 2_000.0)
        else:
            if compressed:
                center = 124.0 + ((idx - 70) * 0.02)
                bar_range = 0.35
                volume = 450_000.0
            else:
                center = 124.0 + ((idx - 70) * (0.25 + (0.05 * variant)))
                bar_range = 1.6 + (variant * 1.2) + (((idx - 70) % 3) * 0.7)
                volume = 1_200_000.0 + (variant * 300_000.0)
        open_value = center - (bar_range * 0.35)
        close_value = center + (bar_range * (0.25 if idx % 2 == 0 else -0.15))
        high_value = max(open_value, close_value) + (bar_range * 0.3)
        low_value = min(open_value, close_value) - (bar_range * 0.35)
        open_values.append(open_value)
        close_values.append(close_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(max(volume, 100_000.0))

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


class VcsIndicatorTests(unittest.TestCase):
    def test_latest_vcs_snapshot_returns_critical_stage_for_extreme_compression(self) -> None:
        snapshot = latest_vcs_snapshot(_vcs_frame(compressed=True))

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertGreaterEqual(snapshot.score, 80.0)
        self.assertEqual(snapshot.stage, "critical")
        self.assertEqual(snapshot.color_zone, "green")

    def test_latest_vcs_snapshot_returns_setup_or_base_for_looser_frame(self) -> None:
        snapshot = latest_vcs_snapshot(_vcs_frame(compressed=False, variant=1))

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertLess(snapshot.score, 80.0)
        self.assertEqual(snapshot.stage, "setup")


if __name__ == "__main__":
    unittest.main()
