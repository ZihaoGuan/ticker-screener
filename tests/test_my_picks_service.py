from __future__ import annotations

import datetime as dt
import unittest
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


def _build_price_frame_map(*tickers: str):
    import pandas as pd

    frame = pd.DataFrame(
        {
            "Close": [100.0, 110.0],
        },
        index=pd.to_datetime(["2026-06-23", "2026-06-24"]),
    )
    return {ticker: frame.copy() for ticker in tickers}


class MyPicksServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakeMyPicksRepository()
        self.service = MyPicksService(repository=self.repo)
        self.service.ratings_repository = _FakeRatingsRepository()

    def test_create_pick_enriches_rating_and_signal_context(self) -> None:
        with patch("src.webapp.services.my_picks_service.load_many_ticker_windows_for_range", return_value=_build_price_frame_map("MSFT")):
            row = self.service.create_pick(ticker="msft", notes="leader", actor_user_id=7)

        self.assertEqual(row["ticker"], "MSFT")
        self.assertEqual(row["sector"], "Technology")
        self.assertEqual(row["fundamental_rating"], 8.7)
        self.assertEqual(row["technical_rating"], 8.2)
        self.assertEqual(row["leadership_score"], 91.0)
        self.assertEqual(row["recent_signal_count"], 2)
        self.assertEqual(row["technical_indicator_ratings"]["1d"]["rating_label"], "Strong")
        self.assertAlmostEqual(row["change_1d_pct"], 10.0)
        self.assertAlmostEqual(row["change_since_added_pct"], 10.0)

    def test_get_context_sorts_newest_first(self) -> None:
        self.service.create_pick(ticker="AAPL")
        self.service.create_pick(ticker="NVDA")

        with patch("src.webapp.services.my_picks_service.load_many_ticker_windows_for_range", return_value=_build_price_frame_map("AAPL", "NVDA")):
            payload = self.service.get_context()

        self.assertEqual(payload["total_count"], 2)
        self.assertEqual(payload["rows"][0]["ticker"], "NVDA")
        self.assertEqual(payload["rows"][1]["ticker"], "AAPL")
        self.assertEqual(payload["available_added_dates"], ["2026-06-23"])
        self.assertAlmostEqual(payload["rows"][0]["change_1d_pct"], 10.0)
        self.assertAlmostEqual(payload["rows"][0]["change_since_added_pct"], 10.0)

    def test_delete_pick_requires_existing_id(self) -> None:
        row = self.service.create_pick(ticker="PLTR")
        self.service.delete_pick(row["id"])

        with self.assertRaisesRegex(ValueError, "Pick not found"):
            self.service.delete_pick(row["id"])


if __name__ == "__main__":
    unittest.main()
