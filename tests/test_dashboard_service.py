from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.dashboard_service import DashboardService


class DashboardServiceTests(unittest.TestCase):
    def test_get_dashboard_context_includes_spy_market_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-05", periods=90, freq="B")
            close_values = [100.0 + (idx * 0.8) for idx in range(len(index) - 8)]
            close_values.extend([176.0, 181.0, 187.0, 194.0, 201.0, 208.0, 214.0, 210.0])
            frame = pd.DataFrame(
                {
                    "Open": [value - 1.0 for value in close_values],
                    "High": [value + 2.0 for value in close_values],
                    "Low": [value - 2.0 for value in close_values],
                    "Close": close_values,
                    "Volume": [1_500_000 for _ in close_values],
                },
                index=index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=frame.copy()), patch(
                "src.webapp.services.dashboard_service.load_app_config"
            ) as mock_config:
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        regime = payload["market_health"]["regime"]
        self.assertEqual(regime["ticker"], "SPY")
        self.assertEqual(regime["data_source"], "database")
        self.assertIsNotNone(regime["latest"])
        regime_latest = regime["latest"]
        assert regime_latest is not None
        self.assertTrue(regime_latest["weekly_uptrend"])
        self.assertIn(regime_latest["regime"], {"healthy_uptrend", "healthy_pullback"})

        rsi_divergence = payload["market_health"]["rsi_divergence"]
        self.assertEqual(rsi_divergence["ticker"], "SPY")
        self.assertEqual(rsi_divergence["data_source"], "database")

        spy_extension = payload["market_health"]["spy_extension"]
        self.assertEqual(spy_extension["ticker"], "SPY")
        self.assertEqual(spy_extension["label"], "10W SMA")
        self.assertEqual(spy_extension["data_source"], "database")
        self.assertIsNotNone(spy_extension["latest"])
        latest = spy_extension["latest"]
        assert latest is not None
        self.assertIn(latest["state"], {"warning", "extreme"})
        self.assertGreater(latest["extension_pct"], 11.0)

    def test_get_dashboard_context_handles_missing_spy_market_health(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=None), patch(
                "src.webapp.services.dashboard_service._download_history_frame",
                return_value=None,
            ), patch("src.webapp.services.dashboard_service.load_app_config") as mock_config:
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        spy_extension = payload["market_health"]["spy_extension"]
        regime = payload["market_health"]["regime"]
        rsi_divergence = payload["market_health"]["rsi_divergence"]
        self.assertEqual(regime["data_source"], "unavailable")
        self.assertIsNone(regime["latest"])
        self.assertEqual(rsi_divergence["data_source"], "unavailable")
        self.assertIsNone(rsi_divergence["latest"])
        self.assertEqual(spy_extension["data_source"], "unavailable")
        self.assertIsNone(spy_extension["latest"])

    def test_get_dashboard_context_flags_healthy_pullback_when_weekly_uptrend_but_daily_below_21ema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-05", periods=120, freq="B")
            close_values = [100.0 + (idx * 0.5) for idx in range(115)]
            close_values.extend([150.0, 148.0, 146.0, 144.0, 142.0])
            frame = pd.DataFrame(
                {
                    "Open": [value - 1.0 for value in close_values],
                    "High": [value + 2.0 for value in close_values],
                    "Low": [value - 2.0 for value in close_values],
                    "Close": close_values,
                    "Volume": [1_500_000 for _ in close_values],
                },
                index=index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=frame.copy()), patch(
                "src.webapp.services.dashboard_service.load_app_config"
            ) as mock_config:
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        latest = payload["market_health"]["regime"]["latest"]
        assert latest is not None
        self.assertTrue(latest["weekly_uptrend"])
        self.assertTrue(latest["daily_downtrend"])
        self.assertEqual(latest["regime"], "healthy_pullback")


if __name__ == "__main__":
    unittest.main()
