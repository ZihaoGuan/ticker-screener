import datetime as dt
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.webapp.services import sector_leaderboard_service as module
from src.webapp.services.sector_leaderboard_service import SectorEtf, SectorHolding, SectorLeaderboardService


def _frame(closes: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "Open": [value - 0.5 for value in closes],
            "High": [value + 1.0 for value in closes],
            "Low": [value - 1.0 for value in closes],
            "Close": closes,
            "Adj Close": closes,
            "Volume": [1_000_000 + index for index, _ in enumerate(closes)],
        },
        index=dates,
    )


class SectorLeaderboardServiceTest(unittest.TestCase):
    def test_ranks_rows_and_attaches_holding_direction(self):
        etfs = (
            SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("ONE", 10.0),)),
            SectorEtf("BBB", "Beta", "Test", "https://example.test/bbb", (SectorHolding("TWO", 5.0),)),
        )
        frames = {
            "AAA": _frame([100 + index for index in range(270)]),
            "BBB": _frame([100 + index * 0.5 for index in range(269)] + [80]),
            "ONE": _frame([10, 11, 12]),
            "TWO": _frame([10, 9, 8]),
        }

        def fake_load_many_ticker_windows(tickers, as_of_date, trading_days_needed, *, database_url=None):
            return {ticker: frames[ticker] for ticker in tickers if ticker in frames}

        with patch.object(module, "load_many_ticker_windows", fake_load_many_ticker_windows), patch.object(
            SectorLeaderboardService,
            "_load_technical_rating_map",
            return_value={"ONE": {"daily_rs_rating": 91.0}, "TWO": {"daily_rs_rating": 42.0}},
        ):
            payload = SectorLeaderboardService(database_url="postgres://example", etfs=etfs).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(payload["rows"][0]["ticker"], "AAA")
        self.assertEqual(payload["rows"][0]["price"], 369.0)
        self.assertEqual(payload["rows"][0]["day_change_pct"], 0.27)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["day_change_pct"], 9.09)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["daily_rs_rating"], 91.0)
        self.assertEqual(payload["rows"][1]["ticker"], "BBB")
        self.assertEqual(payload["rows"][1]["top_holdings"][0]["day_change_pct"], -11.11)

    def test_keeps_empty_rows_when_database_has_no_bars(self):
        etfs = (SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("ONE", 10.0),)),)

        with patch.object(module, "load_many_ticker_windows", lambda *args, **kwargs: {}), patch.object(
            SectorLeaderboardService,
            "_load_technical_rating_map",
            return_value={},
        ):
            payload = SectorLeaderboardService(etfs=etfs).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(
            payload["rows"],
            [
                {
                    "ticker": "AAA",
                    "description": "Alpha",
                    "provider": "Test",
                    "source_url": "https://example.test/aaa",
                    "price": None,
                    "day_change_pct": None,
                    "week_change_pct": None,
                    "month_change_pct": None,
                    "year_change_pct": None,
                    "atr_pct": None,
                    "volume": None,
                    "latest_date": None,
                    "top_holdings": [{"ticker": "ONE", "name": "", "weight": 10.0, "shares_held": None, "day_change_pct": None, "daily_rs_rating": None}],
                }
            ],
        )

    def test_uses_cached_ssga_holdings_when_available(self):
        etfs = (SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("OLD", 10.0),)),)

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "sector_etf_holdings"
            cache_dir.mkdir()
            (cache_dir / "latest.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-07-30T12:00:00+00:00",
                        "results": {
                            "AAA": {
                                "holdings": [
                                    {
                                        "ticker": "NEW",
                                        "name": "New Holding",
                                        "weight": 17.5,
                                        "shares_held": 123.0,
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(module, "load_many_ticker_windows", lambda *args, **kwargs: {}), patch.object(
                SectorLeaderboardService,
                "_load_technical_rating_map",
                return_value={"NEW": {"daily_rs_rating": 88.0}},
            ):
                payload = SectorLeaderboardService(etfs=etfs, artifacts_dir=Path(tmpdir)).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(payload["source"]["holdings_cache_generated_at"], "2026-07-30T12:00:00+00:00")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["ticker"], "NEW")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["name"], "New Holding")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["shares_held"], 123.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["daily_rs_rating"], 88.0)


if __name__ == "__main__":
    unittest.main()
