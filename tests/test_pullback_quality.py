from __future__ import annotations

import unittest

import pandas as pd

from src.pullback_quality import evaluate_pullback_quality


def _quality_frame(*, aggressive: bool = False, failed: bool = False) -> pd.DataFrame:
    index = pd.bdate_range("2026-01-02", periods=32)
    baseline_close = [100.0 + (0.2 * index_value) for index_value in range(24)]
    if aggressive:
        pullback_close = [105.0, 104.0, 103.0, 102.0, 101.0, 100.5, 100.2, 98.0 if failed else 100.0]
        open_ = [value + 1.0 for value in pullback_close]
        high = [value + 1.2 for value in pullback_close]
        low = [value - 0.25 for value in pullback_close]
        volume = [2_000_000] * len(pullback_close)
    else:
        pullback_close = [104.95, 104.8, 104.65, 104.5, 104.4, 104.3, 104.2, 104.15]
        open_ = [value + 0.12 for value in pullback_close]
        high = [value + 0.22 for value in pullback_close]
        low = [value - 0.20 for value in pullback_close]
        volume = [650_000, 625_000, 600_000, 575_000, 550_000, 525_000, 500_000, 475_000]
    baseline_open = [value - 0.10 for value in baseline_close]
    return pd.DataFrame(
        {
            "Open": baseline_open + open_,
            "High": [value + 0.30 for value in baseline_close] + high,
            "Low": [value - 0.30 for value in baseline_close] + low,
            "Close": baseline_close + pullback_close,
            "Volume": [1_000_000] * len(baseline_close) + volume,
        },
        index=index,
    )


class PullbackQualityTests(unittest.TestCase):
    def test_controlled_low_volume_drift_is_constructive(self) -> None:
        quality = evaluate_pullback_quality(_quality_frame(), support_price=104.0, atr14=1.0)

        self.assertIsNotNone(quality)
        assert quality is not None
        self.assertEqual(quality.state, "constructive")
        self.assertGreaterEqual(quality.score, 70)
        self.assertLess(quality.down_volume_ratio or 1.0, 0.8)
        self.assertEqual(quality.aggressive_downside_days, 0)

    def test_aggressive_high_volume_selling_is_warning(self) -> None:
        quality = evaluate_pullback_quality(_quality_frame(aggressive=True), support_price=99.5, atr14=1.0)

        self.assertIsNotNone(quality)
        assert quality is not None
        self.assertEqual(quality.state, "warning")
        self.assertGreaterEqual(quality.aggressive_downside_days, 2)
        self.assertGreater(quality.down_volume_ratio or 0.0, 1.5)

    def test_decisive_support_break_with_volume_is_failed(self) -> None:
        quality = evaluate_pullback_quality(_quality_frame(aggressive=True, failed=True), support_price=100.0, atr14=1.0)

        self.assertIsNotNone(quality)
        assert quality is not None
        self.assertEqual(quality.state, "failed")
        self.assertLess(quality.support_distance_atr, -0.75)


if __name__ == "__main__":
    unittest.main()
