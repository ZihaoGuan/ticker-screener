from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.watchlist_service import WatchlistService, _clear_chart_payload_cache


class WatchlistServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        _clear_chart_payload_cache()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.addCleanup(_clear_chart_payload_cache)
        artifacts_dir = Path(self.temp_dir.name)
        watchlists_dir = artifacts_dir / "watchlists"
        watchlists_dir.mkdir(parents=True, exist_ok=True)
        (watchlists_dir / "weekly_htf_pullback_2026-05-31.json").write_text(
            '[{"ticker":"NVDA","company_name":"NVIDIA"}]',
            encoding="utf-8",
        )
        self.service = WatchlistService(artifacts_dir=artifacts_dir)

    def _write_watchlist(self, stem: str, *, tickers: list[str], modified_at: dt.datetime) -> None:
        path = Path(self.temp_dir.name) / "watchlists" / f"{stem}.json"
        payload = [{"ticker": ticker} for ticker in tickers]
        path.write_text(str(payload).replace("'", '"'), encoding="utf-8")
        timestamp = modified_at.timestamp()
        path.touch()
        import os

        os.utime(path, (timestamp, timestamp))

    def _long_price_frame(self) -> pd.DataFrame:
        index = pd.date_range(start="2024-01-02", periods=320, freq="B")
        close_values: list[float] = []
        for idx in range(len(index)):
            if idx < 315:
                close_values.append(80.0 + (idx * 0.35))
            else:
                close_values.extend([189.2, 189.8, 190.1, 189.9, 190.3])
                break
        return pd.DataFrame(
            {
                "Open": [value - 0.35 for value in close_values],
                "High": [value + 0.85 for value in close_values],
                "Low": [value - 0.95 for value in close_values],
                "Close": close_values,
                "Volume": [1_200_000.0 for _ in close_values],
            },
            index=index,
        )

    def test_get_watchlist_detail_fails_open_when_universe_load_errors(self) -> None:
        with patch("src.webapp.services.watchlist_service.load_universe", side_effect=RuntimeError("nasdaq offline")), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ):
            payload = self.service.get_watchlist_detail("weekly_htf_pullback_2026-05-31")

        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["ticker"], "NVDA")

    def test_get_scanner_board_uses_previous_trading_day_before_new_york_cutoff(self) -> None:
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-11",
            tickers=["APP", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 0, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-12",
            tickers=["PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "fearzone_2026-06-11",
            tickers=["TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 1, 0, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "td9_bullish_2026-06-11",
            tickers=["SHOP"],
            modified_at=dt.datetime(2026, 6, 12, 1, 5, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_2026-06-06",
            tickers=["MSFT", "META"],
            modified_at=dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 12, 20, 30, tzinfo=dt.timezone.utc)
            )

        self.assertEqual(payload["target_trading_date"], "2026-06-11")
        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["sean_gap_up"]["stem"], "sean_peg_earnings_gap_2026-06-11")
        self.assertEqual(cards["sean_gap_up"]["entry_count"], 2)
        self.assertEqual(cards["fearzone"]["stem"], "fearzone_2026-06-11")
        self.assertEqual(cards["td9_bullish"]["stem"], "td9_bullish_2026-06-11")
        self.assertEqual(cards["weekly_rs"]["stem"], "weekly_rs_new_high_2026-06-06")

    def test_get_scanner_board_uses_same_day_after_new_york_cutoff(self) -> None:
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-12",
            tickers=["PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "fearzone_2026-06-12",
            tickers=["TSLA", "HOOD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "td9_bullish_2026-06-12",
            tickers=["SHOP"],
            modified_at=dt.datetime(2026, 6, 12, 23, 45, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_2026-06-06",
            tickers=["MSFT"],
            modified_at=dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        self.assertEqual(payload["target_trading_date"], "2026-06-12")
        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["sean_gap_up"]["stem"], "sean_peg_earnings_gap_2026-06-12")
        self.assertEqual(cards["fearzone"]["stem"], "fearzone_2026-06-12")
        self.assertEqual(cards["td9_bullish"]["stem"], "td9_bullish_2026-06-12")
        self.assertEqual(cards["fearzone"]["preview_tickers"], ["TSLA", "HOOD"])

    def test_get_chart_payload_snaps_to_latest_available_trading_day(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [100.0, 102.0],
                "High": [103.0, 104.0],
                "Low": [99.0, 101.0],
                "Close": [102.0, 103.0],
                "Volume": [1_000_000, 1_200_000],
            },
            index=pd.to_datetime(["2026-05-28", "2026-05-29"]),
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"NVDA", "SPY"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 30))

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["requested_as_of_date"], "2026-05-30")
        self.assertEqual(payload["resolved_as_of_date"], "2026-05-29")
        self.assertEqual(payload["latest_available_date"], "2026-05-29")
        self.assertEqual(payload["candles"][-1]["time"], "2026-05-29")
        self.assertEqual(payload["data_source"], "internet")
        self.assertIn("market_extension", payload)
        self.assertIn("vcs", payload)

    def test_get_watchlist_detail_filters_excluded_tickers(self) -> None:
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "fearzone_2026-06-13.json").write_text(
            '[{"ticker":"NVDA"},{"ticker":"TSLA"}]',
            encoding="utf-8",
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ):
            payload = self.service.get_watchlist_detail("fearzone_2026-06-13")

        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["ticker"], "NVDA")

    def test_get_scanner_board_filters_excluded_tickers_from_counts_and_previews(self) -> None:
        self._write_watchlist(
            "sepa_vcp_2026-06-12",
            tickers=["NVDA", "TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["sepa_vcp"]["entry_count"], 1)
        self.assertEqual(cards["sepa_vcp"]["preview_tickers"], ["NVDA"])

    def test_get_scanner_board_includes_trend_template_card(self) -> None:
        self._write_watchlist(
            "trend_template_2026-06-12",
            tickers=["NVDA", "CRWD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["trend_template"]["available"])
        self.assertEqual(cards["trend_template"]["entry_count"], 2)
        self.assertEqual(cards["trend_template"]["preview_tickers"], ["NVDA", "CRWD"])

    def test_get_scanner_board_includes_sean_breakout_card(self) -> None:
        self._write_watchlist(
            "sean_breakout_2026-06-12",
            tickers=["APP", "CRDO"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["sean_breakout"]["available"])
        self.assertEqual(cards["sean_breakout"]["entry_count"], 2)
        self.assertEqual(cards["sean_breakout"]["preview_tickers"], ["APP", "CRDO"])

    def test_get_scanner_board_marks_card_unavailable_when_all_results_excluded(self) -> None:
        self._write_watchlist(
            "sepa_vcp_2026-06-12",
            tickers=["TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertFalse(cards["sepa_vcp"]["available"])
        self.assertEqual(cards["sepa_vcp"]["entry_count"], 0)

    def test_get_chart_payload_coerces_decimal_db_values(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [Decimal("90.0"), Decimal("100.0"), Decimal("102.0")],
                "High": [Decimal("91.0"), Decimal("103.0"), Decimal("104.0")],
                "Low": [Decimal("89.0"), Decimal("99.0"), Decimal("101.0")],
                "Close": [Decimal("90.5"), Decimal("102.0"), Decimal("103.0")],
                "Adj Close": [Decimal("90.5"), Decimal("102.0"), Decimal("103.0")],
                "Volume": [900000, 1000000, 1200000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"NVDA": frame.copy(), "SPY": frame.copy()}):
            payload = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(payload["data_source"], "database")
        self.assertEqual(payload["candles"][-1]["close"], 103.0)
        self.assertTrue(len(payload["ipo_vwap"]) > 0)
        self.assertEqual(payload["market_extension"]["config"]["label"], "10W SMA")

    def test_get_chart_payload_falls_back_to_internet_for_missing_benchmark(self) -> None:
        ticker_frame = pd.DataFrame(
            {
                "Open": [90.0, 100.0, 102.0],
                "High": [91.0, 103.0, 104.0],
                "Low": [89.0, 99.0, 101.0],
                "Close": [90.5, 102.0, 103.0],
                "Adj Close": [90.5, 102.0, 103.0],
                "Volume": [900_000, 1_000_000, 1_200_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        benchmark_frame = pd.DataFrame(
            {
                "Open": [490.0, 500.0, 505.0],
                "High": [491.0, 506.0, 507.0],
                "Low": [489.0, 498.0, 503.0],
                "Close": [490.5, 504.0, 506.0],
                "Adj Close": [490.5, 504.0, 506.0],
                "Volume": [1_900_000, 2_000_000, 2_100_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"NVDA": ticker_frame.copy()}), patch(
            "src.webapp.services.watchlist_service._download_history_frame",
            return_value=benchmark_frame.copy(),
        ):
            payload = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(payload["data_source"], "database+ticker/internet+benchmark")
        self.assertTrue(len(payload["rs_line"]) > 0)

    def test_get_chart_payload_falls_back_to_internet_for_shallow_db_ticker_history(self) -> None:
        shallow_ticker_frame = pd.DataFrame(
            {
                "Open": [20.0, 21.0],
                "High": [21.0, 22.0],
                "Low": [19.0, 20.0],
                "Close": [20.5, 21.5],
                "Adj Close": [20.5, 21.5],
                "Volume": [1_000_000, 1_100_000],
            },
            index=pd.to_datetime(["2026-06-04", "2026-06-05"]),
        )
        full_frame = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0],
                "High": [101.0, 102.0, 103.0],
                "Low": [99.0, 100.0, 101.0],
                "Close": [100.5, 101.5, 102.5],
                "Adj Close": [100.5, 101.5, 102.5],
                "Volume": [2_000_000, 2_100_000, 2_200_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-06-04", "2026-06-05"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"FCEL", "SPY"}:
                return full_frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"FCEL": shallow_ticker_frame.copy()}), patch(
            "src.webapp.services.watchlist_service.yf.download",
            side_effect=fake_download,
        ):
            payload = service.get_chart_payload("FCEL", as_of_date=dt.date(2026, 6, 5))

        self.assertEqual(payload["data_source"], "internet")
        self.assertEqual(payload["candles"][0]["time"], "2026-06-04")
        self.assertEqual(payload["candles"][-1]["time"], "2026-06-05")

    def test_get_chart_payload_includes_market_extension_overlay(self) -> None:
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

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"SPY", "QQQ"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("SPY", as_of_date=dt.date(2026, 5, 8))

        self.assertGreater(len(payload["market_extension"]["line"]), 0)
        self.assertIsNotNone(payload["market_extension"]["latest"])
        latest = payload["market_extension"]["latest"]
        assert latest is not None
        self.assertIn(latest["state"], {"warning", "extreme"})
        self.assertGreater(latest["extension_pct"], 11.0)

    def test_get_chart_payload_includes_sepa_dashboard_snapshot(self) -> None:
        ticker_frame = self._long_price_frame()
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100.0 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "High": [100.4 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Low": [99.6 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Close": [100.0 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Volume": [2_000_000.0 for _ in range(len(ticker_frame.index))],
            },
            index=ticker_frame.index,
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers == "NVDA":
                return ticker_frame.copy()
            if tickers == "SPY":
                return benchmark_frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("NVDA", as_of_date=dt.date(2025, 3, 24))

        self.assertIn("sepa_dashboard", payload)
        self.assertIsNotNone(payload["sepa_dashboard"])
        dashboard = payload["sepa_dashboard"]
        assert dashboard is not None
        self.assertEqual(dashboard["tpr_status"], "PASSED")
        self.assertEqual(dashboard["buy_risk_status"], "Low Risk")
        self.assertEqual(dashboard["pressure_status"], "Buying")
        self.assertEqual(dashboard["recent_vcp_signal_date"], "2025-03-24")

    def test_get_chart_payload_uses_backend_cache_for_repeat_requests(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ) as load_patch:
            first = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))
            second = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        self.assertEqual(load_patch.call_count, 1)
        self.assertEqual(first["candles"], second["candles"])
        self.assertEqual(first["data_source"], "database")

    def test_get_chart_payload_does_not_cache_empty_payloads(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={},
        ) as load_patch:
            first = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))
            second = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(load_patch.call_count, 2)
        self.assertEqual(first["candles"], [])
        self.assertEqual(second["candles"], [])

    def test_get_chart_payload_skips_setup_markers_by_default(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_ftd_sweep_markers",
        ) as markers_patch:
            payload = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        markers_patch.assert_not_called()
        self.assertEqual(payload["setup_markers"], [])

    def test_get_chart_payload_includes_setup_markers_when_requested(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_ftd_sweep_markers",
            return_value=[{"time": "2025-03-24", "kind": "ftd_sweep_breakout", "label": "FTD Sweep"}],
        ) as markers_patch:
            payload = service.get_chart_payload(
                "NVDA",
                period="6mo",
                as_of_date=dt.date(2025, 3, 24),
                include_setup_markers=True,
            )

        markers_patch.assert_called_once()
        self.assertEqual(payload["setup_markers"][0]["kind"], "ftd_sweep_breakout")

    def test_get_chart_fundamentals_payload_includes_rating_bundle(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        with patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, None, None, {"earnings": {}, "holders": {}, "statistics": {}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_ticker_rating_bundle",
            return_value={
                "fundamentals_snapshot": {"as_of_date": "2026-06-13", "sector": "Technology"},
                "rating_snapshot": {"overall_rating": 88.5, "rating_status": "ok"},
                "rating_diagnostics": {"missing_metric_names": [], "insufficient_baseline_metrics": []},
            },
        ):
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["fundamentals_snapshot"]["sector"], "Technology")
        self.assertEqual(payload["rating_snapshot"]["overall_rating"], 88.5)
        self.assertEqual(payload["rating_diagnostics"]["missing_metric_names"], [])

    def test_get_chart_fundamentals_payload_uses_db_cache_when_complete(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        cached_entry = {
            "ticker": "NVDA",
            "as_of_date": "2026-06-15",
            "earnings_eps_history": [{"date": "2026-05-28", "eps_estimate": 0.9, "reported_eps": 1.1, "surprise_pct": 22.2}],
            "holders_float_held_by_institutions_pct": 79.25,
            "revenue_yoy_pct": 85.2,
            "earnings_yoy_pct": 210.6,
            "implied_move": {"strike": 100.0, "straddle_mid": 8.5, "dollar_move": 8.5, "percent_move": 7.8},
            "source_summary": {
                "diagnostics": {
                    "earnings": {"status": "ok", "attempts": [{"cache": True}]},
                    "holders": {"status": "ok", "attempts": [{"cache": True}]},
                    "statistics": {"status": "ok", "attempts": [{"cache": True}]},
                    "options": {"status": "ok", "attempts": [{"cache": True}]},
                }
            },
        }
        with patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_chart_fundamentals_cache_entry",
            return_value=cached_entry,
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
        ) as scrape_patch, patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
        ) as implied_patch:
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["holders_float_held_by_institutions_pct"], 79.25)
        self.assertEqual(payload["implied_move"]["percent_move"], 7.8)
        self.assertEqual(payload["diagnostics"]["earnings"]["status"], "ok")
        scrape_patch.assert_not_called()
        implied_patch.assert_not_called()

    def test_get_chart_fundamentals_payload_persists_merged_cache_on_scrape(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        cached_entry = {
            "ticker": "NVDA",
            "as_of_date": "2026-06-08",
            "earnings_eps_history": [{"date": "2026-02-28", "eps_estimate": 0.7, "reported_eps": 0.8, "surprise_pct": 14.0}],
            "holders_float_held_by_institutions_pct": 71.5,
            "revenue_yoy_pct": None,
            "earnings_yoy_pct": 55.0,
            "implied_move": {"strike": 95.0, "straddle_mid": 6.0, "dollar_move": 6.0, "percent_move": 5.0},
            "source_summary": {},
        }
        with patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_chart_fundamentals_cache_entry",
            return_value=cached_entry,
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, 88.1, None, {"earnings": {"status": "error", "attempts": []}, "holders": {"status": "error", "attempts": []}, "statistics": {"status": "ok", "attempts": []}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {"status": "error", "attempts": []}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.upsert_chart_fundamentals_cache_entry",
        ) as upsert_patch:
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["revenue_yoy_pct"], 88.1)
        self.assertEqual(payload["holders_float_held_by_institutions_pct"], 71.5)
        self.assertEqual(payload["earnings_eps_history"][0]["date"], "2026-02-28")
        self.assertEqual(payload["implied_move"]["percent_move"], 5.0)
        upsert_patch.assert_called_once()

    def test_get_chart_insider_payload_filters_recent_rows(self) -> None:
        insider_dir = Path(self.temp_dir.name) / "raw" / "insider"
        insider_dir.mkdir(parents=True, exist_ok=True)
        (insider_dir / "insider_trades_latest.json").write_text(
            """
            {
              "generated_at": "2026-06-12T00:00:00+00:00",
              "caches": {
                "NVDA|2026-05-31|14": {
                  "ticker": "NVDA",
                  "requested_tickers": ["NVDA"],
                  "as_of_date": "2026-05-31",
                  "lookback_days": 14,
                  "refreshed_at": "2026-06-12T00:00:00+00:00",
                  "entries": [
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-31",
                      "transaction_date": "2026-05-30",
                      "owner_name": "Jane Insider",
                      "position": "Officer, CEO",
                      "type": "BUY",
                      "shares": 1000,
                      "price": 10.25,
                      "gross_amount": 10250.0,
                      "net_amount": 10250.0,
                      "shares_owned_after": 15000,
                      "is_10b5_1": false,
                      "source_url": "https://www.sec.gov/Archives/example-buy.xml"
                    },
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-26",
                      "transaction_date": "2026-05-25",
                      "owner_name": "Jane Insider",
                      "position": "Officer, CEO",
                      "type": "SELL",
                      "shares": 500,
                      "price": 12.0,
                      "gross_amount": 6000.0,
                      "net_amount": -6000.0,
                      "shares_owned_after": 14500,
                      "is_10b5_1": true,
                      "source_url": "https://www.sec.gov/Archives/example-sell.xml"
                    },
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-01",
                      "transaction_date": "2026-05-01",
                      "owner_name": "Old Insider",
                      "position": "Director",
                      "type": "BUY",
                      "shares": 50,
                      "price": 1.0,
                      "gross_amount": 50.0,
                      "net_amount": 50.0,
                      "shares_owned_after": 15050,
                      "is_10b5_1": false,
                      "source_url": "https://www.sec.gov/Archives/example-old.xml"
                    }
                  ]
                }
              }
            }
            """,
            encoding="utf-8",
        )

        payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["resolved_as_of_date"], "2026-05-31")
        self.assertEqual(payload["window_start_date"], "2026-05-17")
        self.assertEqual(payload["summary"]["total_count"], 2)
        self.assertEqual(payload["summary"]["buy_count"], 1)
        self.assertEqual(payload["summary"]["sell_count"], 1)
        self.assertEqual(payload["summary"]["net_amount"], 4250.0)
        self.assertEqual(payload["cache_status"], "hit")
        self.assertEqual(payload["fetch_status"], "skipped")
        self.assertEqual(payload["entries"][0]["owner_name"], "Jane Insider")
        self.assertEqual(payload["entries"][1]["is_10b5_1"], True)

    def test_get_chart_insider_payload_fetches_on_cache_miss(self) -> None:
        fetched_payload = {
            "generated_at": "2026-06-02T12:00:00+00:00",
            "source": "sec_form4_submissions",
            "requested_tickers": ["NVDA"],
            "lookback_days": 14,
            "as_of_date": "2026-05-31",
            "entries": [
                {
                    "ticker": "NVDA",
                    "filing_date": "2026-05-31",
                    "transaction_date": "2026-05-30",
                    "owner_name": "Fresh Insider",
                    "position": "Director",
                    "type": "BUY",
                    "shares": 200,
                    "price": 20.0,
                    "gross_amount": 4000.0,
                    "net_amount": 4000.0,
                    "shares_owned_after": 1000,
                    "is_10b5_1": False,
                    "source_url": "https://www.sec.gov/Archives/fresh.xml",
                }
            ],
        }

        with patch("src.webapp.services.watchlist_service.fetch_insider_trades_window", return_value=fetched_payload):
            payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["cache_status"], "miss")
        self.assertEqual(payload["fetch_status"], "fetched")
        self.assertEqual(payload["entries"][0]["owner_name"], "Fresh Insider")
        saved = self.service.insider_repository.load_cache_window(ticker="NVDA", as_of_date="2026-05-31", lookback_days=14)
        self.assertIsNotNone(saved)
        self.assertEqual(saved["entries"][0]["owner_name"], "Fresh Insider")

    def test_get_chart_insider_payload_returns_stale_cache_when_refresh_fails(self) -> None:
        insider_dir = Path(self.temp_dir.name) / "raw" / "insider"
        insider_dir.mkdir(parents=True, exist_ok=True)
        (insider_dir / "insider_trades_latest.json").write_text(
            """
            {
              "generated_at": "2026-05-01T00:00:00+00:00",
              "caches": {
                "NVDA|2026-05-31|14": {
                  "ticker": "NVDA",
                  "requested_tickers": ["NVDA"],
                  "as_of_date": "2026-05-31",
                  "lookback_days": 14,
                  "refreshed_at": "2026-05-01T00:00:00+00:00",
                  "entries": [
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-31",
                      "transaction_date": "2026-05-30",
                      "owner_name": "Stale Insider",
                      "position": "Officer",
                      "type": "SELL",
                      "shares": 100,
                      "price": 15.0,
                      "gross_amount": 1500.0,
                      "net_amount": -1500.0,
                      "shares_owned_after": 900,
                      "is_10b5_1": true,
                      "source_url": "https://www.sec.gov/Archives/stale.xml"
                    }
                  ]
                }
              }
            }
            """,
            encoding="utf-8",
        )

        with patch("src.webapp.services.watchlist_service.fetch_insider_trades_window", side_effect=RuntimeError("sec down")):
            payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["cache_status"], "stale")
        self.assertEqual(payload["fetch_status"], "failed")
        self.assertIn("sec down", payload["notice"])
        self.assertEqual(payload["entries"][0]["owner_name"], "Stale Insider")


if __name__ == "__main__":
    unittest.main()
