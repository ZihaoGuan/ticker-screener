from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from src.config import AppConfig, load_app_config
from src.rs_new_high import RS_NEW_HIGH_LOOKBACK, compute_rs_new_high_flags
from src.rs_screen import _compute_rs_new_high_flags
from src.webapp.services.watchlist_service import (
    WatchlistService,
    _build_chart_preview_payload,
    _resolve_chart_request,
)
from src.webapp.services.watchlist_service import _compute_rs_new_high_flags as watchlist_rs_new_high_flags
from vendor.cookstock.src.cookStock import cookFinancials
from vendor.trade_master_signals.render_watchlist_candles import (
    compute_rs_analysis,
    compute_rs_new_high_flags as renderer_rs_new_high_flags,
)


class CanonicalRsNewHighTests(unittest.TestCase):
    def test_python_entry_points_share_the_canonical_helper_and_lookback(self) -> None:
        self.assertEqual(RS_NEW_HIGH_LOOKBACK, 250)
        self.assertEqual(AppConfig().rs_new_high_daily_lookback_days, 250)
        self.assertEqual(load_app_config().rs_new_high_daily_lookback_days, 250)
        self.assertIs(_compute_rs_new_high_flags, compute_rs_new_high_flags)
        self.assertIs(watchlist_rs_new_high_flags, compute_rs_new_high_flags)
        self.assertIs(renderer_rs_new_high_flags, compute_rs_new_high_flags)

    def test_250_bar_window_rejects_a_high_that_only_clears_the_old_50_bar_window(self) -> None:
        index = pd.date_range("2025-01-01", periods=251, freq="B")
        rs_line = pd.Series([1.0] * 251, index=index)
        rs_line.iloc[100] = 2.0
        rs_line.iloc[-1] = 1.5
        highs = pd.Series([10.0] * 251, index=index)
        highs.iloc[-1] = 11.0

        canonical, before_price = compute_rs_new_high_flags(rs_line, highs)
        old_50_bar, _ = compute_rs_new_high_flags(rs_line, highs, lookback=50)

        self.assertFalse(bool(canonical.iloc[-1]))
        self.assertFalse(bool(before_price.iloc[-1]))
        self.assertTrue(bool(old_50_bar.iloc[-1]))

    def test_intraday_high_match_disqualifies_before_price_even_when_close_is_below(self) -> None:
        index = pd.date_range("2026-01-01", periods=3, freq="B")
        rs_line = pd.Series([1.0, 1.1, 1.2], index=index)
        highs = pd.Series([10.0, 12.0, 12.0], index=index)
        closes = pd.Series([10.0, 11.5, 11.4], index=index)

        rs_high, before_price = compute_rs_new_high_flags(rs_line, highs, lookback=250)
        _, close_based_before_price = compute_rs_new_high_flags(rs_line, closes, lookback=250)
        cookstock_rs_high, cookstock_before_price = cookFinancials._compute_rs_new_high_flags(
            None, rs_line, highs, 250
        )

        self.assertTrue(bool(rs_high.iloc[-1]))
        self.assertFalse(bool(before_price.iloc[-1]))
        self.assertTrue(bool(close_based_before_price.iloc[-1]))
        pd.testing.assert_series_equal(cookstock_rs_high, rs_high)
        pd.testing.assert_series_equal(cookstock_before_price, before_price)

    def test_vendor_renderer_uses_intraday_high_and_250_bar_default(self) -> None:
        index = pd.date_range("2026-01-01", periods=3, freq="B")
        history = pd.DataFrame(
            {"Close": [10.0, 11.5, 11.4], "High": [10.0, 12.0, 12.0]},
            index=index,
        )
        benchmark = pd.DataFrame(
            {
                "Open": [10.0, 12.0, 8.0],
                "High": [10.0, 12.0, 8.0],
                "Low": [10.0, 12.0, 8.0],
                "Close": [10.0, 12.0, 8.0],
                "Volume": [1000, 1000, 1000],
            },
            index=index,
        )

        analysis = compute_rs_analysis(history, benchmark)

        self.assertTrue(bool(analysis.daily_new_high.iloc[-1]))
        self.assertFalse(bool(analysis.daily_new_high_before_price.iloc[-1]))

    def test_both_watchlist_chart_backends_confirm_price_with_intraday_high(self) -> None:
        index = pd.date_range("2026-01-01", periods=3, freq="B")
        stock = pd.DataFrame(
            {
                "Open": [10.0, 11.0, 11.0],
                "High": [10.0, 12.0, 12.0],
                "Low": [9.0, 10.5, 10.5],
                "Close": [10.0, 11.5, 11.4],
                "Volume": [1000, 1000, 1000],
            },
            index=index,
        )
        benchmark = pd.DataFrame(
            {
                "Open": [10.0, 12.0, 8.0],
                "High": [10.0, 12.0, 8.0],
                "Low": [10.0, 12.0, 8.0],
                "Close": [10.0, 12.0, 8.0],
                "Volume": [1000, 1000, 1000],
            },
            index=index,
        )
        as_of = index[-1].date()
        preview = _build_chart_preview_payload(
            ticker="TEST",
            period="18mo",
            requested_as_of_date=as_of,
            benchmark_ticker="SPY",
            chart_request=_resolve_chart_request(period="18mo", as_of_date=as_of),
            frame=stock,
            benchmark_frame=benchmark,
            data_source="test",
        )
        service = WatchlistService(artifacts_dir=Path("/private/tmp"))
        service._load_chart_frames = lambda **_: (stock, benchmark, "test")
        service._load_latest_weinstein_stage_map = lambda *_: {}
        service._load_latest_position_action = lambda *_: None
        service._load_relative_trend_strength = lambda *_: None

        from unittest.mock import patch

        with patch("src.webapp.services.watchlist_service.load_ticker_metadata_map", return_value={"TEST": {}}):
            full_chart = service.get_chart_overlays_payload("TEST", as_of_date=as_of)

        self.assertEqual(preview["rs_markers"][-1]["kind"], "daily_new_high")
        self.assertEqual(full_chart["rs_markers"][-1]["kind"], "daily_new_high")


if __name__ == "__main__":
    unittest.main()
