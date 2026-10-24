import datetime as dt
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd

from src.power_gap_volume_watch_screen import build_power_gap_volume_watchlist, run_power_gap_volume_watch
from src.config import AppConfig
from src.screener_catalog import build_screener_catalog


class _Calendar:
    def sessions_in_range(self, start, end):
        return pd.bdate_range(start, end)

    def session_close(self, session):
        return pd.Timestamp(session.date(), tz="UTC") + pd.Timedelta(hours=22)


def latest_completed_session():
    return dt.date(2025, 12, 31)


def frame_with_gap(end_date, count=220, event_index=200):
    dates = pd.bdate_range(end=end_date, periods=count)
    close = [100.0] * count
    open_price = [100.0] * count
    high = [101.0] * count
    low = [99.0] * count
    volume = [1000.0] * count
    open_price[event_index], high[event_index], low[event_index], close[event_index], volume[event_index] = 110, 112, 99, 110, 10000
    return pd.DataFrame({"Open": open_price, "High": high, "Low": low, "Close": close, "Volume": volume}, index=dates)


class PowerGapVolumeWatchScreenTests(unittest.TestCase):
    def test_catalog_registers_daily_history_with_long_coverage(self):
        spec = build_screener_catalog(AppConfig())["power_gap_volume_watch"]
        self.assertIn("daily_bars", spec.required_inputs)
        self.assertGreaterEqual(spec.total_trading_days, 5000)

    def test_volume_only_watchlist_has_no_borrowed_earnings_or_automatic_stop(self):
        hit = {"ticker": "ABC", "signals": ["hvy"], "power_gap_volume_context": {
            "gap_event": {"date": "2025-10-01", "gap_pct": 12, "volume_ratio": 4, "age_trading_sessions": 1},
            "active_signals": ["hvy"], "earnings_context": None,
            "volume_events": {"hve": None, "hvy": {"date": "2025-12-30"}},
        }}
        row = build_power_gap_volume_watchlist([hit])[0]
        self.assertIn("HVY", row["summary"])
        self.assertNotIn("Earnings", row["summary"])
        self.assertNotIn("stop_price", row)
        self.assertEqual(row["event_date"], "2025-12-30")

    def test_provider_failure_leaves_db_technical_gap_hit_present(self):
        cutoff = latest_completed_session()
        frame = frame_with_gap(cutoff, count=300, event_index=280)
        fake_calendar = SimpleNamespace(get_calendar=lambda _: _Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake_calendar}), \
             patch("src.power_gap_volume_watch_screen.load_many_ticker_windows", return_value={"ABC": frame}), \
             patch("src.power_gap_volume_watch_screen.enrich_shortlisted_gaps", side_effect=RuntimeError("provider unavailable")):
            result = run_power_gap_volume_watch(object(), ["ABC"], as_of_date=cutoff)
        self.assertEqual(result["passed_tickers"], 1)
        hit = result["hits"][0]
        self.assertIn("gap", hit["signals"])
        self.assertEqual(hit["power_gap_volume_context"]["earnings_context"]["status"], "lookup_failed")

    def test_batch_database_error_is_limited_to_that_batch(self):
        tickers = [f"T{i}" for i in range(201)]
        fake_calendar = SimpleNamespace(get_calendar=lambda _: _Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake_calendar}), \
             patch("src.power_gap_volume_watch_screen.load_many_ticker_windows", side_effect=[RuntimeError("db unavailable"), {}]):
            result = run_power_gap_volume_watch(object(), tickers, as_of_date=latest_completed_session())
        self.assertEqual(result["total_tickers"], 201)
        self.assertEqual(len(result["failed_tickers"]), 201)
        self.assertIn("daily history batch load failed", result["failed_tickers"][0]["reason"])
        self.assertIn("no daily history returned", result["failed_tickers"][-1]["reason"])

    def test_missing_requested_session_is_coverage_issue_not_successful_empty_scan(self):
        cutoff = latest_completed_session()
        frame = frame_with_gap(cutoff - dt.timedelta(days=1))
        fake_calendar = SimpleNamespace(get_calendar=lambda _: _Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake_calendar}), \
             patch("src.power_gap_volume_watch_screen.load_many_ticker_windows", return_value={"ABC": frame}):
            result = run_power_gap_volume_watch(object(), ["ABC"], as_of_date=cutoff)
        self.assertEqual(result["passed_tickers"], 0)
        self.assertIn("missing required sessions", result["failed_tickers"][0]["reason"])
        self.assertTrue(result["failed_tickers"])

    def test_old_gap_hole_warns_but_valid_gap_hit_is_not_failed(self):
        cutoff = latest_completed_session()
        frame = frame_with_gap(cutoff, count=300, event_index=280)
        frame = frame.drop(frame.index[20])
        fake_calendar = SimpleNamespace(get_calendar=lambda _: _Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake_calendar}), \
             patch("src.power_gap_volume_watch_screen.load_many_ticker_windows", return_value={"ABC": frame}), \
             patch("src.power_gap_volume_watch_screen.enrich_shortlisted_gaps", side_effect=lambda rows, *_args, **_kwargs: rows):
            result = run_power_gap_volume_watch(object(), ["ABC"], as_of_date=cutoff)
        self.assertEqual(result["passed_tickers"], 1)
        self.assertEqual(result["failed_tickers"], [])
        self.assertIn("HVE", result["coverage_warnings"][0]["reasons"][0])

    def test_bad_frame_conversion_isolated_from_good_ticker(self):
        cutoff = latest_completed_session()
        good = frame_with_gap(cutoff)
        bad = good.drop(columns=["Volume"])
        fake_calendar = SimpleNamespace(get_calendar=lambda _: _Calendar())
        with patch.dict(sys.modules, {"exchange_calendars": fake_calendar}), \
             patch("src.power_gap_volume_watch_screen.load_many_ticker_windows", return_value={"BAD": bad, "GOOD": good}), \
             patch("src.power_gap_volume_watch_screen.enrich_shortlisted_gaps", side_effect=lambda rows, *_args, **_kwargs: rows):
            result = run_power_gap_volume_watch(object(), ["BAD", "GOOD"], as_of_date=cutoff)
        self.assertEqual([row["ticker"] for row in result["hits"]], ["GOOD"])
        self.assertIn("conversion failed", result["failed_tickers"][0]["reason"])


if __name__ == "__main__":
    unittest.main()
