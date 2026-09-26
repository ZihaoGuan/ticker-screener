from __future__ import annotations

import unittest
import tempfile
import datetime as dt
import json
from pathlib import Path

import numpy as np
import pandas as pd
from unittest.mock import patch

from src.config import AppConfig
from src.minervini_vcp_detector_screen import evaluate_minervini_vcp_detector, run_minervini_vcp_detector_screen
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.watchlist_service import WatchlistService


def _qualified_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-08-01", periods=540)
    close = np.concatenate(
        (
            np.linspace(50.0, 80.0, 440),
            np.linspace(80.2, 95.0, 80),
            np.linspace(96.0, 115.0, 20),
        )
    )
    high = close * 1.01
    high[-100:] = 120.0
    low = close * 0.99
    volume = np.concatenate((np.full(510, 2_000_000.0), np.linspace(1_500_000.0, 1_000_000.0, 30)))
    return pd.DataFrame({"High": high, "Low": low, "Close": close, "Volume": volume}, index=dates)


class MinerviniVcpDetectorTests(unittest.TestCase):
    def test_matches_trend_template_near_pivot_with_rising_lows_and_contracting_volume(self) -> None:
        snapshot = evaluate_minervini_vcp_detector(_qualified_frame(), market_cap=3_000_000_000.0)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertEqual(snapshot.volume_contracting_comparisons, 6)
        self.assertTrue(snapshot.criteria["daily_100d_resistance_stable_10d"])
        self.assertTrue(snapshot.criteria["higher_lows_10_20_30d"])

    def test_run_requires_persisted_market_cap_and_uses_qualified_database_frame(self) -> None:
        ticker = UniverseTicker(symbol="TEST", sector="Technology", industry="Software", exchange="NASDAQ")
        frame = _qualified_frame()
        with (
            patch("src.minervini_vcp_detector_screen.resolve_database_url", return_value="postgres://example"),
            patch("src.minervini_vcp_detector_screen.load_many_ticker_windows", return_value={"TEST": frame}),
            patch("src.minervini_vcp_detector_screen.load_ticker_metadata_map", return_value={"TEST": {"sector": "Technology"}}),
            patch(
                "src.minervini_vcp_detector_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
                return_value={"TEST": {"market_cap": 3_000_000_000.0}},
            ),
        ):
            result = run_minervini_vcp_detector_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")

    def test_is_available_for_ad_hoc_evaluation_and_scanner_runs(self) -> None:
        self.assertIn("minervini_vcp_detector", build_screener_catalog(AppConfig()))
        with tempfile.TemporaryDirectory() as directory:
            actions = {item["id"]: item for item in RunService(project_root=Path(directory)).list_actions()}
        self.assertIn("minervini_vcp_detector", actions)
        self.assertIn("scripts/run_minervini_vcp_detector_screen.py", actions["minervini_vcp_detector"]["command"])

    def test_scanner_board_exposes_the_minervini_vcp_detector_card(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifacts_dir = Path(directory)
            watchlists_dir = artifacts_dir / "watchlists"
            watchlists_dir.mkdir()
            watchlist_path = watchlists_dir / "minervini_vcp_detector_2026-06-12.json"
            watchlist_path.write_text(json.dumps([{"ticker": "NVDA"}]), encoding="utf-8")
            timestamp = dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp()
            watchlist_path.touch()
            import os

            os.utime(watchlist_path, (timestamp, timestamp))
            service = WatchlistService(artifacts_dir=artifacts_dir)
            with patch.object(service, "_get_scanner_board_from_database", return_value=None), patch(
                "src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()
            ):
                payload = service.get_scanner_board(now=dt.datetime(2026, 6, 13, 1, tzinfo=dt.timezone.utc))

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["minervini_vcp_detector"]["available"])
        self.assertEqual(cards["minervini_vcp_detector"]["preview_tickers"], ["NVDA"])


if __name__ == "__main__":
    unittest.main()
