from __future__ import annotations

import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.webapp.services.my_picks_service import MyPicksService


class _FakeMyPicksRepository:
    def __init__(self) -> None:
        self.rows: dict[int, dict[str, object]] = {}
        self.next_id = 1

    @property
    def database_url(self) -> str:
        return "postgres://example"

    def is_configured(self) -> bool:
        return True

    def list_picks(self):
        return sorted(self.rows.values(), key=lambda item: (str(item["created_at"]), int(item["id"])), reverse=True)

    def create_pick(self, **kwargs: object):
        payload = {
            "id": self.next_id,
            "created_at": dt.datetime(2026, 6, 23, 12, self.next_id, tzinfo=dt.timezone.utc),
            **kwargs,
        }
        self.rows[self.next_id] = payload
        self.next_id += 1
        return dict(payload)

    def delete_pick(self, pick_id: int):
        if pick_id not in self.rows:
            return False
        self.rows.pop(pick_id)
        return True

    def list_recent_signal_summary(self, tickers: list[str], *, lookback_days: int = 45):
        _ = lookback_days
        return {
            ticker: {
                "signal_count": 2,
                "latest_signal_date": "2026-06-20",
                "recent_signals": [
                    {"strategy_id": "weekly_rs", "signal_date": "2026-06-20"},
                    {"strategy_id": "htf_pullback", "signal_date": "2026-06-19"},
                ],
            }
            for ticker in tickers
        }


class _FakeRatingsRepository:
    def ensure_ticker_metadata_stub(self, ticker: str, source: str = "my-picks", sector: str | None = None, industry: str | None = None):
        _ = (ticker, source, sector, industry)

    def load_latest_rating_snapshots_for_tickers(self, tickers: list[str]):
        return {
            ticker: {
                "as_of_date": "2026-06-23",
                "sector": "Technology",
                "industry": "Software",
                "perf_year_pct": 44.2,
                "perf_ytd_pct": 18.4,
                "overall_rating": 8.7,
                "current_rank": 12,
                "rating_status": "ok",
            }
            for ticker in tickers
        }

    def load_latest_technical_rating_snapshots_for_tickers(self, tickers: list[str], *, as_of_date=None, allow_older_as_of_date=False):
        _ = (as_of_date, allow_older_as_of_date)
        return {
            ticker: {
                "as_of_date": "2026-06-23",
                "sector": "Technology",
                "industry": "Software",
                "overall_rating": 8.2,
                "leadership_score": 91.0,
                "rating_band": "A",
                "technical_status": "ok",
            }
            for ticker in tickers
        }

    def load_latest_technical_indicator_ratings_for_tickers(self, tickers: list[str], *, as_of_date=None):
        _ = as_of_date
        return {
            ticker: {
                "1d": {"rating_label": "Strong"},
                "1w": {"rating_label": "Buy"},
            }
            for ticker in tickers
        }


class _FakeWatchlistRepository:
    def load_latest_stored_canslim_score_map(self, tickers: list[str]):
        return {
            ticker: {
                "canslim_score": 11,
                "canslim_max_score": 14,
                "canslim_rank": 3,
            }
            for ticker in tickers
        }

    def load_latest_stored_vcp_score_map(self, tickers: list[str]):
        return {
            ticker: {
                "vcp_score": 84.5,
                "vcp_rating": "Strong VCP",
            }
            for ticker in tickers
        }


def _build_price_frame_map(*tickers: str):
    import pandas as pd

    index = pd.bdate_range(end="2026-06-24", periods=60)
    close = [100.0] * 58 + [100.0, 110.0]
    open_values = [99.5] * 58 + [99.5, 101.0]
    high = [100.5] * 58 + [100.5, 111.0]
    low = [99.0] * 58 + [99.5, 100.0]
    frame = pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": [1_000_000.0] * 60,
        },
        index=index,
    )
    return {ticker: frame.copy() for ticker in tickers}


def _build_trendline_snapshot_map(*tickers: str):
    return {
        ticker: {
            "trade_date": "2026-06-24",
            "close": 110.0,
            "daily_ema9": 105.0,
            "daily_ema21": 102.0,
            "daily_sma50": 104.0,
        }
        for ticker in tickers
    }


class MyPicksServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakeMyPicksRepository()
        self.service = MyPicksService(repository=self.repo)
        self.service.ratings_repository = _FakeRatingsRepository()
        self.service.watchlist_repository = _FakeWatchlistRepository()

    def test_create_pick_enriches_rating_and_signal_context(self) -> None:
        with patch("src.webapp.services.my_picks_service.load_many_ticker_windows_for_range", return_value=_build_price_frame_map("MSFT")), patch(
            "src.webapp.services.my_picks_service.load_latest_trendline_snapshot_map",
            return_value=_build_trendline_snapshot_map("MSFT"),
        ), patch(
            "src.webapp.services.my_picks_service.evaluate_trend_template",
            return_value=SimpleNamespace(matched=True, criteria_passed=8, criteria_total=8),
        ):
            row = self.service.create_pick(ticker="msft", notes="leader", actor_user_id=7)

        self.assertEqual(row["ticker"], "MSFT")
        self.assertEqual(row["sector"], "Technology")
        self.assertEqual(row["fundamental_rating"], 8.7)
        self.assertEqual(row["leadership_score"], 91.0)
        self.assertTrue(row["trend_template_match"])
        self.assertEqual(row["trend_template_criteria_passed"], 8)
        self.assertEqual(row["trend_template_criteria_total"], 8)
        self.assertEqual(row["trend_template_label"], "8/8")
        self.assertEqual(row["canslim_score"], 11)
        self.assertEqual(row["canslim_max_score"], 14)
        self.assertEqual(row["vcp_score"], 84.5)
        self.assertEqual(row["vcp_rating"], "Strong VCP")
        self.assertEqual(row["recent_signal_count"], 2)
        self.assertEqual(row["technical_indicator_ratings"]["1d"]["rating_label"], "Strong")
        self.assertEqual(row["daily_sma50"], 104.0)
        self.assertTrue(row["price_above_sma50"])
        self.assertAlmostEqual(row["change_1d_pct"], 10.0)
        self.assertAlmostEqual(row["change_since_added_pct"], 10.0)
        self.assertAlmostEqual(row["change_from_52wk_low_pct"], ((110.0 / 99.0) - 1.0) * 100.0)
        self.assertEqual(row["bollinger_band_status"], "above_upper_band")
        self.assertTrue(row["ema9_tested_since_added"])
        self.assertTrue(row["ema21_tested_since_added"])
        self.assertTrue(row["sma50_tested_since_added"])

    def test_create_pick_attaches_latest_position_action(self) -> None:
        with patch("src.webapp.services.my_picks_service.load_many_ticker_windows_for_range", return_value=_build_price_frame_map("MSFT")), patch(
            "src.webapp.services.my_picks_service.load_latest_trendline_snapshot_map",
            return_value=_build_trendline_snapshot_map("MSFT"),
        ), patch(
            "src.webapp.services.my_picks_service.evaluate_trend_template",
            return_value=SimpleNamespace(matched=True, criteria_passed=8, criteria_total=8),
        ), patch.object(
            self.service.position_decision_repository,
            "load_latest_decision_map",
            return_value={
                "MSFT": {
                    "ticker": "MSFT",
                    "as_of_date": dt.date(2026, 6, 24),
                    "action": "add_position",
                    "action_score": 81.2,
                    "trend_state": "healthy",
                    "extension_state": "normal",
                    "danger_signal_count": 0,
                    "reason_summary": "Trend intact.",
                    "evidence_json": {"danger_flags": []},
                }
            },
        ):
            row = self.service.create_pick(ticker="msft", notes="leader", actor_user_id=7)

        self.assertEqual(row["position_action"]["action"], "add_position")
        self.assertEqual(row["position_action"]["action_score"], 81.2)

    def test_get_context_sorts_newest_first(self) -> None:
        self.service.create_pick(ticker="AAPL")
        self.service.create_pick(ticker="NVDA")

        with patch("src.webapp.services.my_picks_service.load_many_ticker_windows_for_range", return_value=_build_price_frame_map("AAPL", "NVDA")), patch(
            "src.webapp.services.my_picks_service.load_latest_trendline_snapshot_map",
            return_value=_build_trendline_snapshot_map("AAPL", "NVDA"),
        ), patch(
            "src.webapp.services.my_picks_service.evaluate_trend_template",
            return_value=SimpleNamespace(matched=False, criteria_passed=6, criteria_total=8),
        ):
            payload = self.service.get_context()

        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["rows"][0]["ticker"], "NVDA")
        self.assertEqual(payload["rows"][1]["ticker"], "AAPL")
        self.assertEqual(payload["available_added_dates"], ["2026-06-23"])
        self.assertEqual(payload["rows"][0]["canslim_score"], 11)
        self.assertEqual(payload["rows"][0]["vcp_score"], 84.5)
        self.assertFalse(payload["rows"][0]["trend_template_match"])
        self.assertEqual(payload["rows"][0]["trend_template_label"], "6/8")
        self.assertAlmostEqual(payload["rows"][0]["change_1d_pct"], 10.0)
        self.assertAlmostEqual(payload["rows"][0]["change_since_added_pct"], 10.0)
        self.assertEqual(payload["rows"][0]["bollinger_band_status"], "above_upper_band")
        self.assertEqual(payload["rows"][0]["daily_sma50"], 104.0)
        self.assertTrue(payload["rows"][0]["price_above_sma50"])
        self.assertTrue(payload["rows"][0]["ema9_tested_since_added"])
        self.assertTrue(payload["rows"][0]["ema21_tested_since_added"])
        self.assertTrue(payload["rows"][0]["sma50_tested_since_added"])

    def test_delete_pick_requires_existing_id(self) -> None:
        row = self.service.create_pick(ticker="PLTR")
        self.service.delete_pick(row["id"])

        with self.assertRaisesRegex(ValueError, "Pick not found"):
            self.service.delete_pick(row["id"])


if __name__ == "__main__":
    unittest.main()
