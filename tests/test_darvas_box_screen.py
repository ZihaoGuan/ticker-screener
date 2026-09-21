from __future__ import annotations

import unittest
import tempfile
import datetime as dt
import json
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.darvas_box_screen import find_darvas_box_breakout_hit, run_darvas_box_breakout_screen
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
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
        ), patch("src.darvas_box_screen.load_configured_cookstock") as load_cookstock:
            result = run_darvas_box_breakout_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")
        load_cookstock.assert_not_called()

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
