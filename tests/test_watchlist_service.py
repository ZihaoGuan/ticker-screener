from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.watchlist_service import WatchlistService


class WatchlistServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        artifacts_dir = Path(self.temp_dir.name)
        watchlists_dir = artifacts_dir / "watchlists"
        watchlists_dir.mkdir(parents=True, exist_ok=True)
        (watchlists_dir / "weekly_htf_pullback_2026-05-31.json").write_text(
            '[{"ticker":"NVDA","company_name":"NVIDIA"}]',
            encoding="utf-8",
        )
        self.service = WatchlistService(artifacts_dir=artifacts_dir)

    def test_get_watchlist_detail_fails_open_when_universe_load_errors(self) -> None:
        with patch("src.webapp.services.watchlist_service.load_universe", side_effect=RuntimeError("nasdaq offline")), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ):
            payload = self.service.get_watchlist_detail("weekly_htf_pullback_2026-05-31")

        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["ticker"], "NVDA")

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
