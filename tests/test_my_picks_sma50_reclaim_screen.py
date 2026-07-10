from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.my_picks_sma50_reclaim_screen import (
    find_my_picks_sma50_reclaim_hit,
    run_my_picks_sma50_reclaim_screen,
)
from scripts.run_my_picks_sma50_reclaim_screen import _notify_discord_hits


def _build_reclaim_frame() -> pd.DataFrame:
    index = pd.bdate_range("2026-03-02", periods=70)
    closes = [100.0 + (i * 0.5) for i in range(69)] + [129.5]
    opens = [value - 0.8 for value in closes[:-1]] + [124.0]
    highs = [value + 1.2 for value in closes[:-1]] + [130.8]
    lows = [value - 1.0 for value in closes[:-1]]
    frame = pd.DataFrame(
        {
            "Open": opens,
            "High": highs,
            "Low": lows + [117.5],
            "Close": closes,
            "Volume": [1_000_000 + (i * 1_000) for i in range(len(index))],
        },
        index=index,
    )
    return frame


def _build_non_reclaim_frame() -> pd.DataFrame:
    frame = _build_reclaim_frame()
    frame.loc[frame.index[-1], "Low"] = 126.0
    return frame


class MyPicksSma50ReclaimScreenTests(unittest.TestCase):
    def test_find_hit_when_latest_bar_reclaims_sma50_and_emas_are_above(self) -> None:
        hit = find_my_picks_sma50_reclaim_hit(_build_reclaim_frame(), ticker="NVDA", notes="Leader")

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "NVDA")
        self.assertEqual(hit.notes, "Leader")
        self.assertGreater(hit.current_price, hit.sma50)
        self.assertGreater(hit.ema9, hit.sma50)
        self.assertGreater(hit.ema21, hit.sma50)
        self.assertLessEqual(hit.session_low, hit.sma50)

    def test_find_hit_returns_none_when_latest_bar_does_not_touch_sma50(self) -> None:
        hit = find_my_picks_sma50_reclaim_hit(_build_non_reclaim_frame(), ticker="NVDA")

        self.assertIsNone(hit)

    def test_run_screen_scans_current_my_picks_list(self) -> None:
        picks = [{"ticker": "NVDA", "notes": "Core leader"}, {"ticker": "AAPL", "notes": "No signal"}]

        def fake_load_frame(
            ticker: str,
            *,
            start_date: dt.date,
            end_date: dt.date,
            market_data_source: str,
            database_url: str,
        ) -> pd.DataFrame:
            _ = (start_date, end_date, market_data_source, database_url)
            return _build_reclaim_frame() if ticker == "NVDA" else _build_non_reclaim_frame()

        with patch("src.my_picks_sma50_reclaim_screen.MyPicksRepository.list_picks", return_value=picks), patch(
            "src.my_picks_sma50_reclaim_screen._load_price_frame",
            side_effect=fake_load_frame,
        ):
            result = run_my_picks_sma50_reclaim_screen(
                as_of_date=dt.date(2026, 7, 9),
                market_data_source="database-first",
                database_url="postgres://example",
            )

        self.assertEqual(result.total_tickers, 2)
        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertEqual(result.hits[0].notes, "Core leader")

    def test_notify_discord_hits_reports_failed_status_when_send_fails(self) -> None:
        payload = {
            "hits": [
                {
                    "ticker": "NVDA",
                    "current_price": 129.5,
                    "sma50": 120.0,
                    "ema9": 126.0,
                    "ema21": 123.5,
                }
            ]
        }

        with patch("scripts.run_my_picks_sma50_reclaim_screen.DiscordNotificationService.get_settings", return_value={"webhook_url": "https://discord.example/webhook", "effective_app_base_url": "https://ticker.example.com"}), patch(
            "scripts.run_my_picks_sma50_reclaim_screen.DiscordNotificationService.send_message",
            return_value=False,
        ):
            sent, status = _notify_discord_hits(
                as_of_date=dt.date(2026, 7, 9),
                result_payload=payload,
                watchlist_file="/tmp/my_picks_sma50_reclaim_2026-07-09.json",
            )

        self.assertFalse(sent)
        self.assertEqual(status, "failed: attempted 1 hit(s)")

    def test_notify_discord_hits_reports_skip_when_webhook_missing(self) -> None:
        with patch("scripts.run_my_picks_sma50_reclaim_screen.DiscordNotificationService.get_settings", return_value={"webhook_url": "", "effective_app_base_url": ""}):
            sent, status = _notify_discord_hits(
                as_of_date=dt.date(2026, 7, 9),
                result_payload={"hits": [{"ticker": "NVDA"}]},
                watchlist_file="/tmp/my_picks_sma50_reclaim_2026-07-09.json",
            )

        self.assertFalse(sent)
        self.assertEqual(status, "skipped: discord webhook not configured")


if __name__ == "__main__":
    unittest.main()
