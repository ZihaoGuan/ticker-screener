from __future__ import annotations

import unittest
import tempfile
import datetime as dt
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.darvas_box_screen import (
    DARVAS_DATABASE_HISTORY_SESSIONS,
    DARVAS_FALLBACK_HISTORY_DAYS,
    DARVAS_REJECTION_REASONS,
    evaluate_darvas_box_breakout,
    find_darvas_box_breakout_hit,
    run_darvas_box_breakout_screen,
)
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from scripts.replay_darvas_box_breakout import aggregate_replay_results
from src.webapp.services.run_service import RunService
from src.webapp.services.watchlist_service import WatchlistService


def _darvas_breakout_frame(*, breakout_volume: float = 1_600_000.0) -> pd.DataFrame:
    dates = pd.bdate_range("2025-08-01", periods=280)
    advance = np.linspace(50.0, 100.0, 259)
    box_close = np.array([98.0, 98.8, 99.2, 98.6, 99.0] * 4)
    close = np.concatenate([advance, box_close, [101.0]])
    high = close * 1.01
    low = close * 0.99
    high[259:279] = np.array([100.0, 100.0, 100.0, 99.8, 100.0] * 4)
    low[259:279] = 94.0
    high[-1] = 102.0
    low[-1] = 100.0
    volume = np.full(len(close), 1_000_000.0)
    volume[-1] = breakout_volume
    return pd.DataFrame(
        {"Open": close * 0.995, "High": high, "Low": low, "Close": close, "Volume": volume},
        index=dates,
    )


class DarvasBoxScreenTest(unittest.TestCase):
    def test_evaluator_reports_ordered_primary_rejections_and_preserves_pass(self) -> None:
        base = _darvas_breakout_frame()
        ticker = UniverseTicker(symbol="TEST")
        cases = {}
        cases["insufficient_history"] = base.tail(100)
        frame = base.copy(); frame.iloc[259:279, frame.columns.get_loc("Low")] = 80.0
        cases["width_too_large"] = frame
        frame = base.copy(); frame.iloc[259:279, frame.columns.get_loc("High")] = 98.5
        frame.iloc[259, frame.columns.get_loc("High")] = 100.0
        cases["insufficient_resistance_confirmation"] = frame
        frame = base.copy(); frame.iloc[-1, frame.columns.get_loc("Close")] = 99.0
        cases["breakout_not_reached"] = frame
        frame = base.copy(); frame.iloc[-1, frame.columns.get_loc("Close")] = 100.3
        cases["breakout_buffer_not_reached"] = frame
        frame = base.copy(); frame.iloc[100, frame.columns.get_loc("High")] = 150.0
        cases["not_near_high"] = frame
        cases["absolute_volume_too_low"] = _darvas_breakout_frame(breakout_volume=99_999.0)
        cases["relative_volume_too_low"] = _darvas_breakout_frame(breakout_volume=1_400_000.0)
        frame = base.copy(); frame.iloc[259:279, frame.columns.get_loc("High")] = 0.0
        cases["no_qualifying_box"] = frame
        self.assertEqual(set(cases), set(DARVAS_REJECTION_REASONS))
        for expected, frame in cases.items():
            with self.subTest(reason=expected):
                result = evaluate_darvas_box_breakout(frame, ticker=ticker)
                self.assertIsNone(result.hit)
                self.assertEqual(result.rejection_reason, expected)
        passing = evaluate_darvas_box_breakout(base, ticker=ticker)
        self.assertIsNotNone(passing.hit)
        self.assertIsNone(passing.rejection_reason)

    def test_result_serialization_reconciles_pass_reject_and_data_failure(self) -> None:
        frame = _darvas_breakout_frame()
        tickers = [UniverseTicker(symbol="PASS"), UniverseTicker(symbol="REJECT"), UniverseTicker(symbol="FAIL")]
        bad = frame.copy(); bad.iloc[-1, bad.columns.get_loc("Close")] = 99.0
        with patch("src.darvas_box_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.darvas_box_screen.load_many_ticker_windows",
            return_value={"PASS": frame, "REJECT": bad, "FAIL": frame},
        ), patch("src.darvas_box_screen.db_frame_has_recent_coverage", side_effect=[True, True, True]):
            # Force one evaluation exception, classified as a data/evaluation failure.
            with patch("src.darvas_box_screen.evaluate_darvas_box_breakout", side_effect=[
                evaluate_darvas_box_breakout(frame, ticker=tickers[0]),
                evaluate_darvas_box_breakout(bad, ticker=tickers[1]), RuntimeError("broken"),
            ]):
                result = run_darvas_box_breakout_screen(AppConfig(), tickers, as_of_date=frame.index[-1].date())
        payload = result.to_dict()
        self.assertEqual(
            payload["accounting"],
            {"total_tickers": 3, "passed_tickers": 1, "strategy_rejected": 1, "data_failures": 1},
        )
        self.assertEqual(payload["rejection_counts"]["breakout_not_reached"], 1)
        self.assertEqual(sum(payload["rejection_counts"].values()), 1)
        self.assertEqual(len(payload["failed_tickers"]), 1)

    def test_replay_aggregation_sums_date_level_telemetry(self) -> None:
        aggregate = aggregate_replay_results([
            {"rejection_counts": {"not_near_high": 2}, "accounting": {"total_tickers": 3, "passed_tickers": 1, "strategy_rejected": 2, "data_failures": 0}},
            {"rejection_counts": {"not_near_high": 1, "width_too_large": 1}, "accounting": {"total_tickers": 3, "passed_tickers": 0, "strategy_rejected": 2, "data_failures": 1}},
        ])
        self.assertEqual(aggregate["date_count"], 2)
        self.assertEqual(aggregate["rejection_counts"], {"not_near_high": 3, "width_too_large": 1})
        self.assertEqual(aggregate["total_tickers"], 6)
        self.assertEqual(aggregate["data_failures"], 1)

    def test_identifies_a_volume_backed_close_above_a_confirmed_box_near_52_week_high(self) -> None:
        hit = find_darvas_box_breakout_hit(_darvas_breakout_frame(), ticker=UniverseTicker(symbol="TEST"))

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "TEST")
        self.assertEqual(hit.box_days, 20)
        self.assertAlmostEqual(hit.box_high, 100.0)
        self.assertGreater(hit.current_price, hit.breakout_price)
        self.assertGreaterEqual(hit.volume_ratio_50, 1.5)
        self.assertLessEqual(hit.distance_from_52w_high_pct, 10.0)

    def test_rejects_a_breakout_without_a_volume_spike(self) -> None:
        hit = find_darvas_box_breakout_hit(
            _darvas_breakout_frame(breakout_volume=1_400_000.0),
            ticker=UniverseTicker(symbol="TEST"),
        )

        self.assertIsNone(hit)

    def test_is_available_to_ad_hoc_and_manual_screener_runs(self) -> None:
        self.assertIn("darvas_box_breakout", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            action_ids = {item["id"] for item in RunService(project_root=Path(directory)).list_actions()}
        self.assertIn("darvas_box_breakout", action_ids)

    def test_run_uses_recent_database_bars_before_internet_fallback(self) -> None:
        frame = _darvas_breakout_frame()
        ticker = UniverseTicker(symbol="TEST")
        with patch("src.darvas_box_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.darvas_box_screen.load_many_ticker_windows", return_value={"TEST": frame}
        ) as load_frames, patch("src.darvas_box_screen.load_configured_cookstock") as load_cookstock:
            result = run_darvas_box_breakout_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")
        load_frames.assert_called_once_with(
            ["TEST"], frame.index[-1].date(), DARVAS_DATABASE_HISTORY_SESSIONS, database_url="postgres://example"
        )
        load_cookstock.assert_not_called()

    def test_recent_but_short_database_history_falls_back_and_captures_failure(self) -> None:
        frame = _darvas_breakout_frame().tail(251)
        ticker = UniverseTicker(symbol="TEST")
        cookstock = MagicMock()
        cookstock.cookFinancials.side_effect = RuntimeError("fallback unavailable")
        with patch("src.darvas_box_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.darvas_box_screen.load_many_ticker_windows", return_value={"TEST": frame}
        ) as load_frames, patch("src.darvas_box_screen.load_configured_cookstock", return_value=cookstock):
            result = run_darvas_box_breakout_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        load_frames.assert_called_once_with(
            ["TEST"], frame.index[-1].date(), DARVAS_DATABASE_HISTORY_SESSIONS, database_url="postgres://example"
        )
        cookstock.cookFinancials.assert_called_once_with(
            "TEST", benchmarkTicker=AppConfig().benchmark_ticker, historyLookbackDays=DARVAS_FALLBACK_HISTORY_DAYS
        )
        self.assertEqual(result.failed_tickers, [{"ticker": "TEST", "error": "fallback unavailable"}])

    def test_recent_database_frames_without_usable_ohlcv_fall_back(self) -> None:
        complete = _darvas_breakout_frame()
        malformed_value = complete.copy()
        malformed_value["Close"] = "bad"
        for frame in (complete.drop(columns=["Close"]), malformed_value):
            with self.subTest(columns=list(frame.columns)):
                ticker = UniverseTicker(symbol="TEST")
                cookstock = MagicMock()
                cookstock.cookFinancials.side_effect = RuntimeError("fallback unavailable")
                with patch("src.darvas_box_screen.resolve_database_url", return_value="postgres://example"), patch(
                    "src.darvas_box_screen.load_many_ticker_windows", return_value={"TEST": frame}
                ), patch("src.darvas_box_screen.load_configured_cookstock", return_value=cookstock):
                    result = run_darvas_box_breakout_screen(
                        AppConfig(), [ticker], as_of_date=complete.index[-1].date()
                    )

                cookstock.cookFinancials.assert_called_once()
                self.assertEqual(result.failed_tickers, [{"ticker": "TEST", "error": "fallback unavailable"}])

    def test_scanner_board_has_a_darvas_card_for_persisted_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            watchlists = Path(directory) / "watchlists"
            watchlists.mkdir()
            path = watchlists / "darvas_box_breakout_2026-06-12.json"
            path.write_text(json.dumps([{"ticker": "NVDA"}, {"ticker": "CRWD"}]), encoding="utf-8")
            timestamp = dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp()
            import os

            os.utime(path, (timestamp, timestamp))
            payload = WatchlistService(artifacts_dir=Path(directory)).get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["darvas_box_breakout"]["available"])
        self.assertEqual(cards["darvas_box_breakout"]["preview_tickers"], ["NVDA", "CRWD"])


if __name__ == "__main__":
    unittest.main()
