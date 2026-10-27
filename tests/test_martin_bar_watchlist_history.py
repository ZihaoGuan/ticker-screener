from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from src.webapp.services.martin_bar_watchlist_history import build_follow_through_page


class FakeHistory:
    def __init__(self, run, hits, bars):
        self.run, self.hits, self.bars = run, hits, bars

    def get_screen_run(self, run_id, *, include_hits):
        if run_id != self.run["id"]:
            return None
        return dict(self.run)

    def list_passed_screen_run_hits(self, run_id, *, limit, offset):
        passed = [hit for hit in self.hits if hit.get("passed")]
        return passed[offset:offset + limit]

    def list_daily_bars_after(self, tickers, after_date, *, limit_per_ticker):
        return {ticker: self.bars.get(ticker, [])[:limit_per_ticker] for ticker in tickers}


class MartinBarWatchlistHistoryTests(unittest.TestCase):
    def test_returns_use_next_session_open_and_market_session_horizons(self):
        run_date = dt.date(2026, 1, 1)
        bars = {}
        for ticker in ("ABC", "SPY", "QQQ"):
            bars[ticker] = [
                {"trade_date": run_date + dt.timedelta(days=i + 1), "open": 100, "close": 100 + i, "low": 95 - i,
                 "source": "tradingview", "split_factor": 1}
                for i in range(63)
            ]
        repo = FakeHistory(
            {"id": 9, "strategy_id": "martin_bar_watchlist_v1", "run_date": run_date,
             "created_at": dt.datetime(2026, 1, 1, 23), "hit_count": 1},
            [{"ticker": "ABC", "passed": True, "rank": 1, "signal_date": run_date,
              "reasons_json": ["leader_30_session"]}], bars,
        )
        with patch("src.webapp.services.martin_bar_watchlist_history._completed_xnys_sessions", return_value=[run_date + dt.timedelta(days=i + 1) for i in range(63)]):
            result = build_follow_through_page(repo, 9)
        row = result["rows"][0]
        self.assertAlmostEqual(row["horizons"]["5"]["return_pct"], 4)
        self.assertAlmostEqual(row["horizons"]["21"]["return_pct"], 20)
        self.assertAlmostEqual(row["horizons"]["63"]["return_pct"], 62)
        self.assertAlmostEqual(row["horizons"]["5"]["downside_pct"], -9)
        self.assertAlmostEqual(row["benchmarks"]["SPY"]["21"], 20)
        self.assertEqual(row["reasons"], ["leader_30_session"])
        self.assertFalse(row["retrospective_publication"])

    def test_incomplete_horizon_and_adjustment_basis_are_explicit(self):
        signal = dt.date(2026, 1, 1)
        rows = [{"trade_date": signal + dt.timedelta(days=i), "open": 10, "close": 11, "low": 9,
                 "source": "vendor-a" if i < 3 else "vendor-b", "split_factor": 1} for i in range(1, 22)]
        rows[2]["split_factor"] = 2
        repo = FakeHistory(
            {"id": 10, "strategy_id": "martin_bar_watchlist_v1", "run_date": signal,
             "created_at": dt.datetime(2026, 1, 3, 0), "hit_count": 1},
            [{"ticker": "ABC", "passed": True, "rank": 1, "reasons_json": []}],
            {"ABC": rows, "SPY": [dict(row, source="tradingview") for row in rows],
             "QQQ": [dict(row, source="tradingview") for row in rows]},
        )
        with patch("src.webapp.services.martin_bar_watchlist_history._completed_xnys_sessions", return_value=[signal + dt.timedelta(days=i) for i in range(1, 64)]):
            result = build_follow_through_page(repo, 10)
        horizon = result["rows"][0]["horizons"]
        self.assertEqual(horizon["5"]["status"], "adjustment_basis_issue")
        self.assertEqual(horizon["21"]["status"], "adjustment_basis_issue")
        self.assertEqual(horizon["63"]["status"], "missing_bars")
        self.assertTrue(result["rows"][0]["retrospective_publication"])

    def test_missing_market_session_bar_is_not_counted_as_a_shorter_horizon(self):
        signal = dt.date(2026, 1, 1)
        sessions = [signal + dt.timedelta(days=i) for i in range(1, 64)]
        bars = {ticker: [{"trade_date": day, "open": 100, "close": 100 + index + 1, "low": 99,
                          "source": "tradingview", "split_factor": 1} for index, day in enumerate(sessions)]
                for ticker in ("ABC", "SPY", "QQQ")}
        bars["SPY"] = [bar for bar in bars["SPY"] if bar["trade_date"] != sessions[2]]
        repo = FakeHistory(
            {"id": 11, "strategy_id": "martin_bar_watchlist_v1", "run_date": signal,
             "created_at": dt.datetime(2026, 1, 1, 23), "hit_count": 1},
            [{"ticker": "ABC", "passed": True, "rank": 1, "reasons_json": []}], bars,
        )
        with patch("src.webapp.services.martin_bar_watchlist_history._completed_xnys_sessions", return_value=sessions):
            result = build_follow_through_page(repo, 11)
        horizon = result["rows"][0]["horizons"]["5"]
        self.assertEqual(horizon["status"], "available")
        self.assertEqual(horizon["end_date"], sessions[4])
        self.assertAlmostEqual(horizon["return_pct"], 5)
        self.assertAlmostEqual(result["rows"][0]["benchmarks"]["SPY"]["5"], 5)

    def test_pagination_offsets_only_passing_hits_and_reports_more_rows(self):
        signal = dt.date(2026, 1, 1)
        bars = {ticker: [{"trade_date": signal + dt.timedelta(days=i), "open": 100, "close": 101,
                          "low": 99, "source": "tradingview", "split_factor": 1} for i in range(1, 64)]
                for ticker in ("ABC", "DEF", "GHI", "SPY", "QQQ")}
        hits = [
            {"ticker": "ABC", "passed": True, "rank": 1, "reasons_json": []},
            {"ticker": "FAIL1", "passed": False, "rank": None, "reasons_json": []},
            {"ticker": "DEF", "passed": True, "rank": 2, "reasons_json": []},
            {"ticker": "FAIL2", "passed": False, "rank": None, "reasons_json": []},
            {"ticker": "GHI", "passed": True, "rank": 3, "reasons_json": []},
        ]
        repo = FakeHistory({"id": 12, "strategy_id": "martin_bar_watchlist_v1", "run_date": signal,
                           "created_at": dt.datetime(2026, 1, 1, 23), "hit_count": 3}, hits, bars)
        with patch("src.webapp.services.martin_bar_watchlist_history._completed_xnys_sessions", return_value=[signal + dt.timedelta(days=i) for i in range(1, 64)]):
            first = build_follow_through_page(repo, 12, limit=2, offset=0)
            second = build_follow_through_page(repo, 12, limit=2, offset=2)
        self.assertEqual([row["ticker"] for row in first["rows"]], ["ABC", "DEF"])
        self.assertTrue(first["has_more"])
        self.assertEqual([row["ticker"] for row in second["rows"]], ["GHI"])
        self.assertFalse(second["has_more"])

    def test_incomplete_recent_horizon_remains_pending(self):
        today = dt.datetime.now(dt.timezone.utc).astimezone(ZoneInfo("America/New_York")).date()
        signal = today - dt.timedelta(days=3)
        dates = [signal + dt.timedelta(days=i) for i in (1, 2, 3)]
        bars = {ticker: [{"trade_date": day, "open": 100, "close": 101, "low": 99,
                          "source": "tradingview", "split_factor": 1} for day in dates]
                for ticker in ("ABC", "SPY", "QQQ")}
        repo = FakeHistory({"id": 13, "strategy_id": "martin_bar_watchlist_v1", "run_date": signal,
                            "created_at": dt.datetime.combine(today, dt.time()), "hit_count": 1},
                           [{"ticker": "ABC", "passed": True, "rank": 1, "reasons_json": []}], bars)
        with patch("src.webapp.services.martin_bar_watchlist_history._completed_xnys_sessions", return_value=dates):
            result = build_follow_through_page(repo, 13)
        self.assertEqual(result["rows"][0]["horizons"]["5"]["status"], "pending_horizon")

    def test_rejects_run_for_another_strategy(self):
        repo = FakeHistory({"id": 1, "strategy_id": "other", "run_date": dt.date.today()}, [], {})
        with self.assertRaises(ValueError):
            build_follow_through_page(repo, 1)


if __name__ == "__main__":
    unittest.main()
