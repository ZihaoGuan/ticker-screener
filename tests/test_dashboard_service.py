from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.dashboard_service import DashboardService


def _mock_options_positioning_summary() -> dict[str, object]:
    return {
        "ticker": "SPY",
        "api_as_of": "2026-06-22T20:00:00Z",
        "spot": 600.0,
        "net_gex": 123456789.0,
        "gex_regime": "positive",
        "gex_label": "Positive Gamma",
        "gamma_flip": 592.0,
        "distance_to_flip_pct": 1.35,
        "call_wall": 605.0,
        "put_wall": 590.0,
        "atm_pin_strike": 600.0,
        "put_call_oi_ratio": 0.82,
        "summary": "Dealers likely dampen moves; spot above gamma flip 592.00; put wall 590.00, call wall 605.00.",
        "methodology": "FlashAlpha GEX API snapshot persisted at close; dashboard reads stored DB summary only.",
    }


def _mock_sparse_options_positioning_summary() -> dict[str, object]:
    return {
        "date_label": "2026-06-21",
        "as_of_date": "2026-06-21",
        "gex_regime": "positive",
    }


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
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        regime = payload["market_health"]["regime"]
        self.assertEqual(regime["ticker"], "SPY")
        self.assertEqual(regime["data_source"], "database")
        self.assertIsNotNone(regime["latest"])
        regime_latest = regime["latest"]
        assert regime_latest is not None
        self.assertTrue(regime_latest["weekly_uptrend"])
        self.assertIn(regime_latest["regime"], {"perfect_convergence_bull", "healthy_chaos"})
        self.assertIn(regime_latest["summary"], {"Trend and short-term action aligned", "Weekly uptrend, daily reset"})

        rsi_divergence = payload["market_health"]["rsi_divergence"]
        self.assertEqual(rsi_divergence["ticker"], "SPY")
        self.assertEqual(rsi_divergence["data_source"], "database")
        if rsi_divergence["latest"] is not None:
            self.assertIn(rsi_divergence["latest"]["state"], {"fresh_top_warning", "active_top_warning", "lifted", "invalidated"})

        bearish_td9 = payload["market_health"]["bearish_td9"]
        self.assertEqual(bearish_td9["ticker"], "SPY")
        self.assertEqual(bearish_td9["data_source"], "database")

        spy_extension = payload["market_health"]["spy_extension"]
        options_positioning = payload["market_health"]["options_positioning"]
        self.assertEqual(spy_extension["ticker"], "SPY")
        self.assertEqual(spy_extension["label"], "10W SMA")
        self.assertEqual(spy_extension["data_source"], "database")
        self.assertIsNotNone(spy_extension["latest"])
        self.assertEqual(options_positioning["ticker"], "SPY")
        self.assertEqual(options_positioning["data_source"], "database")
        self.assertIsNotNone(options_positioning["latest"])
        latest = spy_extension["latest"]
        assert latest is not None
        self.assertIn(latest["state"], {"warning", "extreme"})
        self.assertGreater(latest["extension_pct"], 11.0)

    def test_get_dashboard_context_prefers_most_complete_options_positioning_summary(self) -> None:
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
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[
                    {"result_summary_json": _mock_sparse_options_positioning_summary()},
                    {"result_summary_json": _mock_options_positioning_summary()},
                ],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        options_positioning = payload["market_health"]["options_positioning"]
        latest = options_positioning["latest"]
        self.assertIsNotNone(latest)
        assert latest is not None
        self.assertEqual(latest["spot"], 600.0)
        self.assertEqual(latest["gex_label"], "Positive Gamma")
        self.assertEqual(latest["summary"], _mock_options_positioning_summary()["summary"])

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
        bearish_td9 = payload["market_health"]["bearish_td9"]
        options_positioning = payload["market_health"]["options_positioning"]
        self.assertEqual(regime["data_source"], "unavailable")
        self.assertIsNone(regime["latest"])
        self.assertEqual(rsi_divergence["data_source"], "unavailable")
        self.assertIsNone(rsi_divergence["latest"])
        self.assertEqual(bearish_td9["data_source"], "unavailable")
        self.assertIsNone(bearish_td9["latest"])
        self.assertEqual(options_positioning["data_source"], "unavailable")
        self.assertIsNone(options_positioning["latest"])
        self.assertEqual(spy_extension["data_source"], "unavailable")
        self.assertIsNone(spy_extension["latest"])

    def test_get_dashboard_context_falls_back_to_internet_when_db_frame_cannot_build_usable_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            db_index = pd.date_range(start="2026-01-06", periods=3, freq="B")
            db_frame = pd.DataFrame(
                {
                    "Open": [float("nan") for _ in range(len(db_index))],
                    "High": [float("nan") for _ in range(len(db_index))],
                    "Low": [float("nan") for _ in range(len(db_index))],
                    "Close": [float("nan") for _ in range(len(db_index))],
                    "Volume": [float("nan") for _ in range(len(db_index))],
                },
                index=db_index,
            )
            fresh_index = pd.date_range(end="2026-06-12", periods=140, freq="B")
            fresh_closes = [100.0 + (idx * 0.6) for idx in range(len(fresh_index))]
            fresh_frame = pd.DataFrame(
                {
                    "Open": [value - 1.0 for value in fresh_closes],
                    "High": [value + 2.0 for value in fresh_closes],
                    "Low": [value - 2.0 for value in fresh_closes],
                    "Close": fresh_closes,
                    "Volume": [1_500_000 for _ in fresh_closes],
                },
                index=fresh_index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=db_frame.copy()), patch(
                "src.webapp.services.dashboard_service._download_history_frame",
                return_value=fresh_frame.copy(),
            ), patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                with patch("src.webapp.services.dashboard_service.load_app_config") as mock_config:
                    mock_config.return_value.benchmark_ticker = "SPY"
                    payload = service.get_dashboard_context()

        self.assertEqual(payload["market_health"]["regime"]["data_source"], "internet")
        self.assertIsNotNone(payload["market_health"]["regime"]["latest"])
        self.assertIsNotNone(payload["market_health"]["spy_extension"]["latest"])

    def test_get_dashboard_context_keeps_latest_db_trading_day_when_internet_fallback_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-05", periods=90, freq="B")
            close_values = [100.0 + (idx * 0.8) for idx in range(len(index) - 8)]
            close_values.extend([176.0, 181.0, 187.0, 194.0, 201.0, 208.0, 214.0, 210.0])
            db_frame = pd.DataFrame(
                {
                    "Open": [value - 1.0 for value in close_values],
                    "High": [value + 2.0 for value in close_values],
                    "Low": [value - 2.0 for value in close_values],
                    "Close": close_values,
                    "Volume": [1_500_000 for _ in close_values],
                },
                index=index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=db_frame.copy()), patch(
                "src.webapp.services.dashboard_service._download_history_frame",
                return_value=None,
            ), patch("src.webapp.services.dashboard_service.load_app_config") as mock_config:
                with patch(
                    "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                    return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
                ):
                    mock_config.return_value.benchmark_ticker = "SPY"
                    payload = service.get_dashboard_context()

        self.assertEqual(payload["market_health"]["regime"]["data_source"], "database")
        self.assertIsNotNone(payload["market_health"]["regime"]["latest"])
        self.assertIsNotNone(payload["market_health"]["spy_extension"]["latest"])

    def test_get_dashboard_context_flags_healthy_chaos_when_weekly_uptrend_but_daily_below_21ema(self) -> None:
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
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        latest = payload["market_health"]["regime"]["latest"]
        assert latest is not None
        self.assertTrue(latest["weekly_uptrend"])
        self.assertTrue(latest["daily_downtrend"])
        self.assertEqual(latest["regime"], "healthy_chaos")
        self.assertEqual(latest["regime_label"], "Healthy Chaos")

    def test_get_dashboard_context_flags_bear_market_rally_when_weekly_below_but_daily_above_21ema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-05", periods=140, freq="B")
            close_values = [220.0 - (idx * 0.7) for idx in range(130)]
            close_values.extend([129.0, 131.0, 134.0, 137.0, 141.0, 145.0, 148.0, 151.0, 154.0, 157.0])
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
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        latest = payload["market_health"]["regime"]["latest"]
        assert latest is not None
        self.assertFalse(latest["weekly_uptrend"])
        self.assertFalse(latest["daily_downtrend"])
        self.assertEqual(latest["regime"], "bear_market_rally")
        self.assertEqual(latest["regime_label"], "Bear Market Rally")

    def test_get_dashboard_context_includes_bearish_td9_when_latest_bar_completes_setup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-02", periods=20, freq="B")
            close_values = [
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
                99.0,
                101.0,
                102.0,
                103.0,
                104.0,
                105.0,
                106.0,
                107.0,
                108.0,
                109.0,
            ]
            frame = pd.DataFrame(
                {
                    "Open": [value - 0.2 for value in close_values],
                    "High": [value + 0.8 for value in close_values],
                    "Low": [value - 0.8 for value in close_values],
                    "Close": close_values,
                    "Volume": [1_000_000.0 for _ in close_values],
                },
                index=index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=frame.copy()), patch(
                "src.webapp.services.dashboard_service.load_app_config"
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        latest = payload["market_health"]["bearish_td9"]["latest"]
        assert latest is not None
        self.assertEqual(latest["label"], "Bearish TD9")
        self.assertEqual(latest["signal_date"], "2026-01-29")
        self.assertEqual(latest["setup_count"], 9)

    def test_get_dashboard_context_includes_new_rsi_top_warning_signal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = DashboardService(database_url="", artifacts_dir=Path(temp_dir))
            index = pd.date_range(start="2026-01-02", periods=80, freq="B")
            close_values = [
                100, 102, 104, 106, 108, 110, 112, 114, 116, 118,
                120, 122, 124, 126, 128, 130, 132, 134, 136, 138,
                140, 138, 136, 134, 132, 130, 128, 126, 124, 122,
                124, 126, 128, 130, 132, 134, 136, 138, 140, 142,
                144, 146, 148, 150, 152, 154, 156, 158, 160, 162,
                161, 160, 159, 158, 157, 156, 155, 154, 153, 152,
                153, 154, 155, 156, 157, 158, 159, 160, 161, 162,
                163, 164, 165, 166, 167, 168, 167, 166, 165, 164,
            ]
            frame = pd.DataFrame(
                {
                    "Open": [value - 1.0 for value in close_values],
                    "High": [value + 2.0 for value in close_values],
                    "Low": [value - 2.0 for value in close_values],
                    "Close": close_values,
                    "Volume": [1_000_000 for _ in close_values],
                },
                index=index,
            )

            with patch("src.webapp.services.dashboard_service.load_daily_bars_frame_from_db", return_value=frame.copy()), patch(
                "src.webapp.services.dashboard_service.load_app_config"
            ) as mock_config, patch(
                "src.webapp.repositories.dashboard_repository.HistoryRepository.list_screen_runs",
                return_value=[{"result_summary_json": _mock_options_positioning_summary()}],
            ):
                mock_config.return_value.benchmark_ticker = "SPY"
                payload = service.get_dashboard_context()

        latest = payload["market_health"]["rsi_divergence"]["latest"]
        assert latest is not None
        self.assertIn(latest["state"], {"fresh_top_warning", "active_top_warning"})
        self.assertIn(latest["label"], {"Fresh Top Warning", "Active Top Warning"})


if __name__ == "__main__":
    unittest.main()
