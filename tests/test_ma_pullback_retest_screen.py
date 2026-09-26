from __future__ import annotations

import unittest

import pandas as pd

from src.ma_pullback_retest_screen import find_ma_pullback_retest_hit
from src.universe import UniverseTicker


def _support_reclaim_frame() -> pd.DataFrame:
    index = pd.bdate_range("2024-01-01", periods=250)
    baseline = [100.0 + (0.1 * idx) for idx in range(247)]
    close = baseline + [124.3, 124.5, 125.0]
    return pd.DataFrame(
        {
            "Open": [value - 0.3 for value in baseline] + [124.0, 124.1, 124.5],
            "High": [value + 0.4 for value in baseline] + [124.7, 124.8, 125.4],
            "Low": [value - 0.4 for value in baseline] + [123.8, 123.9, 124.0],
            "Close": close,
            "Volume": [1_000_000] * len(index),
        },
        index=index,
    )


class MaPullbackRetestScreenTests(unittest.TestCase):
    def test_finds_active_daily_support_reclaim_and_preserves_all_matches(self) -> None:
        hit = find_ma_pullback_retest_hit(_support_reclaim_frame(), ticker=UniverseTicker(symbol="TEST"))

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_state, "active")
        self.assertIn("D EMA8", hit.active_profiles)
        self.assertGreater(hit.stop_price, 0)

    def test_requires_enough_history_for_long_term_supports(self) -> None:
        hit = find_ma_pullback_retest_hit(_support_reclaim_frame().tail(200), ticker=UniverseTicker(symbol="TEST"))

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
