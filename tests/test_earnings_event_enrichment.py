from __future__ import annotations

import datetime as dt
import importlib.util
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.config import AppConfig
from src.earnings_enrichment import (
    _build_ainvest_annotation,
    _build_fmp_annotation,
    _parse_yahoo_timestamp,
    _safe_float,
    _xnys_next_session,
    build_earnings_event_contexts,
)


class EarningsEventEnrichmentTests(unittest.TestCase):
    @staticmethod
    def next_session(day):
        day += dt.timedelta(days=1)
        while day.weekday() >= 5 or day == dt.date(2026, 4, 3):
            day += dt.timedelta(days=1)
        return day

    def setUp(self):
        calendar = patch("src.earnings_enrichment._xnys_next_session", side_effect=self.next_session)
        calendar.start()
        self.addCleanup(calendar.stop)

    @staticmethod
    def _row(values):
        return SimpleNamespace(to_dict=lambda: values)

    def test_timestamp_uses_new_york_market_boundaries_and_naive_is_unknown(self) -> None:
        self.assertEqual(_parse_yahoo_timestamp(dt.datetime(2026, 10, 23, 13, 20, tzinfo=dt.timezone.utc)), (dt.date(2026, 10, 23), "before_market"))
        self.assertEqual(_parse_yahoo_timestamp(dt.datetime(2026, 10, 23, 20, 0, tzinfo=dt.timezone.utc)), (dt.date(2026, 10, 23), "after_market"))
        self.assertEqual(_parse_yahoo_timestamp(dt.datetime(2026, 10, 23, 11, 0)), (dt.date(2026, 10, 23), None))
        self.assertIsNone(_safe_float(float("nan")))

    def test_unrelated_fmp_confirmed_date_does_not_supply_release_timing(self) -> None:
        annotation = _build_fmp_annotation(
            "ABC",
            {"date": "2026-10-20", "eps": 0},
            {"date": "2026-10-23", "time": "bmo"},
            {},
            dt.date(2026, 10, 24),
        )
        self.assertEqual(annotation.release_date, "2026-10-20")
        self.assertIsNone(annotation.release_session)

    def test_ainvest_future_history_does_not_become_reported_as_of_past_date(self) -> None:
        annotation = _build_ainvest_annotation(
            "ABC",
            [{"release_date": "2026-10-23", "eps_actual": 0, "eps_forecast": -0.2}],
            None,
            {},
            dt.date(2026, 10, 22),
        )
        self.assertIsNone(annotation.release_date)
        self.assertIsNone(annotation.eps_actual)

    def test_complete_cache_hit_avoids_provider_and_preserves_retrieval_time(self) -> None:
        cached = [{"ticker": "ABC", "earnings_date": dt.date(2026, 10, 23), "eps_actual": 0, "eps_estimate": -0.2,
                   "revenue_actual": None, "revenue_estimate": None, "session_timing": "before_market",
                   "source": "yfinance", "updated_at": dt.datetime(2026, 10, 23, tzinfo=dt.timezone.utc)}]
        with patch("src.earnings_enrichment._load_cached_events", return_value=cached), \
                patch("src.earnings_enrichment.YFinanceEarningsClient", side_effect=AssertionError("cache should be reused")):
            result = build_earnings_event_contexts([{"ticker": "ABC", "signal_date": "2026-10-23"}], AppConfig(), as_of_date=dt.date(2026, 10, 24))
        context = result[0]["earnings_context"]
        self.assertEqual(context["status"], "earnings_confirmed")
        self.assertIn("2026-10-23", context["fetched_at"])

    def test_repeated_ticker_fetches_when_only_one_signal_date_is_cached(self) -> None:
        class Data:
            empty = False

            def iterrows(self):
                return iter([
                    (dt.datetime(2026, 7, 24, 8, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": 0, "EPS Estimate": -0.2})),
                    (dt.datetime(2026, 10, 23, 16, 5, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": -1, "EPS Estimate": -2})),
                ])

        cached = [{"ticker": "ABC", "earnings_date": dt.date(2026, 7, 24), "eps_actual": 0, "eps_estimate": -0.2,
                   "revenue_actual": None, "revenue_estimate": None, "session_timing": "before_market",
                   "source": "yfinance", "updated_at": dt.datetime(2026, 10, 23, tzinfo=dt.timezone.utc)}]
        stock = SimpleNamespace(get_earnings_dates=lambda limit: Data())
        yf = SimpleNamespace(Ticker=lambda ticker: stock)
        rows = [{"ticker": "ABC", "signal_date": "2026-07-24"}, {"ticker": "ABC", "signal_date": "2026-10-26"}]
        with patch("src.earnings_enrichment._load_cached_events", return_value=cached), \
                patch("src.earnings_enrichment.YFinanceEarningsClient", return_value=SimpleNamespace(yf=yf)) as client, \
                patch.dict(os.environ, {"TICKER_SCREENER_DATABASE_URL": ""}, clear=False):
            result = build_earnings_event_contexts(
                rows, AppConfig(), as_of_date=dt.date(2026, 10, 26),
                calendar_next_session=self.next_session,
            )
        self.assertTrue(client.called)
        self.assertEqual([row["earnings_context"]["status"] for row in result], ["earnings_confirmed", "earnings_confirmed"])
        self.assertEqual(result[1]["earnings_context"]["report_date"], "2026-10-23")

    def test_partial_cache_is_upgraded_by_fresh_timing(self) -> None:
        class Data:
            empty = False

            def iterrows(self):
                return iter([(dt.datetime(2026, 10, 23, 8, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": 0, "EPS Estimate": -0.2}))])

        cached = [{"ticker": "ABC", "earnings_date": dt.date(2026, 10, 23), "eps_actual": 0, "eps_estimate": None,
                   "revenue_actual": None, "revenue_estimate": None, "session_timing": None, "source": "fmp", "updated_at": None}]
        yf = SimpleNamespace(Ticker=lambda ticker: SimpleNamespace(get_earnings_dates=lambda limit: Data()))
        with patch("src.earnings_enrichment._load_cached_events", return_value=cached), \
                patch("src.earnings_enrichment._cache_events") as save, \
                patch("src.earnings_enrichment.YFinanceEarningsClient", return_value=SimpleNamespace(yf=yf)):
            result = build_earnings_event_contexts([{"ticker": "ABC", "signal_date": "2026-10-23"}], AppConfig(), as_of_date=dt.date(2026, 10, 24))
        self.assertEqual(result[0]["earnings_context"]["status"], "earnings_confirmed")
        save.assert_called_once()

    def test_per_ticker_provider_failure_keeps_other_matches(self) -> None:
        class Data:
            empty = False

            def iterrows(self):
                return iter([(dt.datetime(2026, 10, 23, 8, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": 0, "EPS Estimate": -0.2}))])

        def ticker(symbol):
            if symbol == "BAD":
                raise RuntimeError("sensitive provider details")
            return SimpleNamespace(get_earnings_dates=lambda limit: Data())

        yf = SimpleNamespace(Ticker=ticker)
        rows = [{"ticker": "GOOD", "signal_date": "2026-10-23"}, {"ticker": "BAD", "signal_date": "2026-10-23"}]
        with patch("src.earnings_enrichment.YFinanceEarningsClient", return_value=SimpleNamespace(yf=yf)):
            result = build_earnings_event_contexts(rows, AppConfig(), as_of_date=dt.date(2026, 10, 24))
        self.assertEqual(result[0]["earnings_context"]["status"], "earnings_confirmed")
        self.assertEqual(result[1]["earnings_context"]["status"], "lookup_failed")

    def test_explicit_ainvest_provider_is_not_routed_to_yahoo(self) -> None:
        rows = [{"ticker": "ABC", "signal_date": "2026-10-23"}]
        with patch.dict(os.environ, {"AINVEST_API_KEY": "test-key"}), \
                patch("src.earnings_enrichment.AInvestEarningsClient") as client, \
                patch("src.earnings_enrichment.YFinanceEarningsClient", side_effect=AssertionError("wrong provider")):
            client.return_value.fetch_history.return_value = [{"release_date": "2026-10-23", "eps_actual": 0, "eps_forecast": -0.2, "session": "bmo"}]
            result = build_earnings_event_contexts(rows, AppConfig(), as_of_date=dt.date(2026, 10, 24), provider="ainvest")
        self.assertEqual(result[0]["earnings_context"]["source"], "ainvest")

    def test_future_signal_is_not_queried_or_confirmed(self) -> None:
        with patch("src.earnings_enrichment.YFinanceEarningsClient", side_effect=AssertionError("future signal must not fetch")):
            result = build_earnings_event_contexts(
                [{"ticker": "ABC", "signal_date": "2026-10-25"}],
                AppConfig(),
                as_of_date=dt.date(2026, 10, 24),
            )
        context = result[0]["earnings_context"]
        self.assertEqual(context["status"], "lookup_failed")
        self.assertEqual(context["match_reason"], "signal_date_after_as_of_cutoff")

    def test_event_context_is_row_scoped_and_zero_eps_is_valid(self) -> None:
        class Data:
            empty = False

            def iterrows(self):
                return iter([(dt.datetime(2026, 10, 23, 8, 0, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": 0, "EPS Estimate": -0.2}))])

        stock = SimpleNamespace(get_earnings_dates=lambda limit: Data())
        yf = SimpleNamespace(Ticker=lambda ticker: stock)
        rows = [{"ticker": "ABC", "signal_date": "2026-10-23"}, {"ticker": "ABC", "signal_date": "2026-10-22"}]
        with patch("src.earnings_enrichment.YFinanceEarningsClient", return_value=SimpleNamespace(yf=yf)), patch.dict(os.environ, {"TICKER_SCREENER_DATABASE_URL": ""}, clear=False):
            result = build_earnings_event_contexts(rows, AppConfig(), as_of_date=dt.date(2026, 10, 26), calendar_next_session=self.next_session)
        self.assertEqual(result[0]["earnings_context"]["status"], "earnings_confirmed")
        self.assertEqual(result[0]["earnings_context"]["eps_actual"], 0)
        self.assertEqual(result[1]["earnings_context"]["status"], "catalyst_unknown")

    def test_after_market_effective_session_uses_injected_exchange_calendar(self) -> None:
        class Data:
            empty = False

            def iterrows(self):
                return iter([(dt.datetime(2026, 10, 23, 16, 5, tzinfo=dt.timezone(dt.timedelta(hours=-4))), EarningsEventEnrichmentTests._row({"Reported EPS": -1, "EPS Estimate": -2}))])

        rows = [{"ticker": "ABC", "signal_date": "2026-10-26"}]
        yf = SimpleNamespace(Ticker=lambda ticker: SimpleNamespace(get_earnings_dates=lambda limit: Data()))
        with patch("src.earnings_enrichment.YFinanceEarningsClient", return_value=SimpleNamespace(yf=yf)), patch.dict(os.environ, {"TICKER_SCREENER_DATABASE_URL": ""}, clear=False):
            result = build_earnings_event_contexts(rows, AppConfig(), as_of_date=dt.date(2026, 10, 26), calendar_next_session=self.next_session)
        context = result[0]["earnings_context"]
        self.assertEqual(context["status"], "earnings_confirmed")
        self.assertEqual(context["report_date"], "2026-10-23")

    @unittest.skipUnless(importlib.util.find_spec("exchange_calendars"), "exchange_calendars unavailable locally")
    def test_xnys_calendar_skips_good_friday(self) -> None:
        self.assertEqual(_xnys_next_session(dt.date(2026, 4, 2)), dt.date(2026, 4, 6))


if __name__ == "__main__":
    unittest.main()
