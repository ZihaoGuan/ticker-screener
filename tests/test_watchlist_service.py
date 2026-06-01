from __future__ import annotations

import datetime as dt
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


if __name__ == "__main__":
    unittest.main()
