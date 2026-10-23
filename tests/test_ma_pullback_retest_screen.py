from __future__ import annotations

import unittest
from unittest.mock import patch

import pandas as pd

from src.ma_pullback_retest_screen import find_ma_pullback_retest_hit, structural_retest_plan, _profile_signal, _atr14, PULLBACK_PROFILES, _moving_average
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
        self.assertIn(hit.pullback_quality["state"], {"constructive", "neutral", "warning", "failed"})
        self.assertIn("score", hit.pullback_quality)

    def test_requires_enough_history_for_long_term_supports(self) -> None:
        hit = find_ma_pullback_retest_hit(_support_reclaim_frame().tail(200), ticker=UniverseTicker(symbol="TEST"))

        self.assertIsNone(hit)

    def test_structural_selection_prefers_active_profile_over_nearer_ready(self):
        frame = _support_reclaim_frame()
        def signal(bars, profile, atr, **kwargs):
            return ("ready", 124.4, .1) if profile.id == "daily_ema8" else ("active", 124.2, .8) if profile.id == "daily_ema21" else None
        with patch("src.ma_pullback_retest_screen._profile_signal", side_effect=signal):
            plan = structural_retest_plan(frame, as_of_date=frame.index[-1].date())
        self.assertEqual(plan["profile_id"], "daily_ema21")
        self.assertEqual(plan["state"], "active")
        self.assertEqual(plan["trigger_price"], 124.2)
        self.assertEqual(plan["stop_price"], 124)

    def test_structural_weekly_reference_excludes_developing_week(self):
        frame = _support_reclaim_frame()
        frame.index = pd.bdate_range(end="2026-10-22", periods=len(frame))
        with patch("src.ma_pullback_retest_screen._moving_average", wraps=_moving_average) as average:
            _profile_signal(frame, PULLBACK_PROFILES[-2], _atr14(frame), completed_weekly=True)
        self.assertEqual(average.call_args.args[0].index[-1].date().isoformat(), "2026-10-16")

    def test_structural_rejects_invalid_prices_and_preserves_legacy_policy(self):
        frame = _support_reclaim_frame()
        day = frame.index[-1].date()
        legacy = find_ma_pullback_retest_hit(frame, ticker=UniverseTicker(symbol="TEST"))
        structural = structural_retest_plan(frame, as_of_date=day)
        self.assertNotEqual(legacy.stop_price, structural["stop_price"])
        self.assertEqual(find_ma_pullback_retest_hit(frame, ticker=UniverseTicker(symbol="TEST")), legacy)
        for value in (float("inf"), 0, -1, 126):
            broken = frame.copy()
            broken.iloc[-1, broken.columns.get_loc("Low")] = value
            self.assertIsNone(structural_retest_plan(broken, as_of_date=day))


if __name__ == "__main__":
    unittest.main()
