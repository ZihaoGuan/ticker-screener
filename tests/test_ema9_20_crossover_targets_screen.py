from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.artifact_paths import build_screener_artifact_paths
from src.ema9_20_crossover_targets_screen import find_recent_ema9_20_crossover_target_hit, run_ema9_20_crossover_targets_screen
from src.ema9_20_crossover_targets_watchlist_builder import build_ema9_20_crossover_targets_watchlist
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService


def _frame(*, outcome: str = "active") -> pd.DataFrame:
    prices = [100.0] * 500 + [150.0] * 20 + [100.0] * 110 + [90.0] * 19 + [130.0]
    if outcome == "bull":
        prices.extend([140.0] * 10)
    elif outcome == "bear":
        prices.extend([80.0] * 10)
    index = pd.date_range("2024-01-02", periods=len(prices), freq="B")
    return pd.DataFrame(
        {
            "Open": [price - 0.2 for price in prices],
            "High": [price + 0.5 for price in prices],
            "Low": [price - 0.5 for price in prices],
            "Close": prices,
        },
        index=index,
    )


class Ema9_20CrossoverTargetsScreenTests(unittest.TestCase):
    def test_fresh_confirmed_buy_signal_creates_fixed_targets_without_same_bar_hits(self) -> None:
        frame = _frame()
        hit = find_recent_ema9_20_crossover_target_hit(
            frame,
            ticker=UniverseTicker(symbol="TEST"),
            as_of_date=frame.index[-1].date(),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertTrue(hit.is_fresh_signal)
        self.assertGreater(hit.bull_target_2, hit.bull_target_1)
        self.assertLess(hit.bear_target_2 or 0.0, hit.bear_target_1 or 0.0)
        self.assertIsNone(hit.bull_target_1_hit_date)
        self.assertIsNone(hit.bear_target_1_hit_date)
        self.assertEqual(hit.target_status, "active")
        entry = build_ema9_20_crossover_targets_watchlist([hit])[0]
        self.assertEqual(entry["bull_target_1"], round(hit.bull_target_1, 4))
        self.assertEqual(entry["target_status"], "active")

    def test_records_first_bull_and_bear_target_hits_after_signal_bar(self) -> None:
        bull_frame = _frame(outcome="bull")
        bull = find_recent_ema9_20_crossover_target_hit(
            bull_frame,
            ticker=UniverseTicker(symbol="BULL"),
            as_of_date=bull_frame.index[-1].date(),
        )
        bear_frame = _frame(outcome="bear")
        bear = find_recent_ema9_20_crossover_target_hit(
            bear_frame,
            ticker=UniverseTicker(symbol="BEAR"),
            as_of_date=bear_frame.index[-1].date(),
        )

        self.assertIsNotNone(bull)
        self.assertIsNotNone(bear)
        assert bull is not None and bear is not None
        self.assertFalse(bull.is_fresh_signal)
        self.assertEqual(bull.target_status, "bull_target_2_hit")
        self.assertEqual(bull.bull_target_1_bars_to_hit, 1)
        self.assertEqual(bull.bull_target_2_bars_to_hit, 1)
        self.assertEqual(bear.target_status, "bear_target_2_hit")
        self.assertEqual(bear.bear_target_1_bars_to_hit, 1)
        self.assertEqual(bear.bear_target_2_bars_to_hit, 1)

    def test_database_first_runner_and_catalog_registration(self) -> None:
        frame = _frame()
        ticker = UniverseTicker(symbol="TEST")
        with patch("src.ema9_20_crossover_targets_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.ema9_20_crossover_targets_screen.load_many_ticker_windows", return_value={"TEST": frame}
        ), patch("src.ema9_20_crossover_targets_screen.load_configured_cookstock") as load_cookstock:
            result = run_ema9_20_crossover_targets_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.fresh_buy_signals, 1)
        load_cookstock.assert_not_called()
        self.assertIn("ema9_20_crossover_targets", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            action_ids = {item["id"] for item in RunService(project_root=Path(directory)).list_actions()}
        self.assertIn("ema9_20_crossover_targets", action_ids)
        paths = build_screener_artifact_paths(Path("/tmp/artifacts"), strategy_id="ema9_20_crossover_targets", date_label="2026-06-29")
        self.assertIn("ema9_20_crossover_targets", str(paths.watchlist_path))


if __name__ == "__main__":
    unittest.main()
