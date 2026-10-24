from __future__ import annotations

import datetime as dt
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from scripts import enrich_with_earnings as cli
from src.config import AppConfig
from src import earnings_enrichment as enrichment


def next_session(day):
    day += dt.timedelta(days=1)
    while day.weekday() >= 5 or day == dt.date(2026, 4, 3):
        day += dt.timedelta(days=1)
    return day


class EarningsEnrichmentIOTests(unittest.TestCase):
    def test_missing_provider_timestamp_is_not_an_event(self):
        self.assertIsNone(enrichment._parse_yahoo_timestamp(pd.NaT))

    def test_cached_postgres_numeric_values_remain_json_serializable(self):
        event = {"ticker": "ABC", "earnings_date": dt.date(2026, 10, 23),
                 "session_timing": "before_market", "eps_actual": Decimal("0"),
                 "eps_estimate": Decimal("-0.2"), "revenue_actual": Decimal("1200000"),
                 "source": "fmp", "updated_at": dt.datetime(2026, 10, 23, tzinfo=dt.timezone.utc)}
        with patch.object(enrichment, "_load_cached_events", return_value=[event]), \
             patch.object(enrichment, "_cache_events"), \
             patch.object(enrichment, "YFinanceEarningsClient", side_effect=AssertionError("Use complete cache")):
            result = enrichment.build_earnings_event_contexts(
                [{"ticker": "ABC", "signal_date": "2026-10-23"}], AppConfig(),
                as_of_date=dt.date(2026, 10, 24), calendar_next_session=next_session)
        self.assertEqual(result[0]["earnings_context"]["status"], "earnings_confirmed")
        self.assertEqual(json.loads(json.dumps(result, allow_nan=False))[0]["earnings_context"]["eps_actual"], 0)

    def test_before_open_requires_an_actual_exchange_session(self):
        for day, calendar in [(dt.date(2026, 4, 3), next_session),
                              (dt.date(2026, 10, 23), lambda day: None)]:
            with self.subTest(day=day), \
                 patch.object(enrichment, "_load_cached_events", return_value=[{
                     "ticker": "ABC", "earnings_date": day, "session_timing": "before_market",
                     "eps_actual": 1., "source": "fmp"}]), \
                 patch.object(enrichment, "_cache_events"), \
                 patch.object(enrichment, "YFinanceEarningsClient", side_effect=RuntimeError("Unavailable")):
                result = enrichment.build_earnings_event_contexts(
                    [{"ticker": "ABC", "signal_date": str(day)}], AppConfig(),
                    as_of_date=dt.date(2026, 10, 24), calendar_next_session=calendar)
                self.assertFalse(result[0]["earnings_context"]["confirmed"])

    def test_legacy_yahoo_uses_report_dates_and_preserves_historical_timing(self):
        dates = pd.to_datetime(["2026-10-23T12:00:00Z", "2026-10-19T12:00:00Z"])
        frame = pd.DataFrame({"Reported EPS": [2., 1.], "EPS Estimate": [1.8, .8]}, index=dates)
        stock = SimpleNamespace(get_earnings_dates=lambda limit: frame,
                                get_earnings_history=lambda: self.fail("Quarter dates are not release dates"))
        client = enrichment.YFinanceEarningsClient.__new__(enrichment.YFinanceEarningsClient)
        client.yf = SimpleNamespace(Ticker=lambda ticker: stock)
        payload = client.fetch_latest("ABC", as_of_date=dt.date(2026, 10, 20))
        annotation = enrichment._build_yfinance_annotation("ABC", payload, {}, dt.date(2026, 10, 20))
        self.assertEqual(annotation.release_date, "2026-10-19")
        self.assertEqual(annotation.release_session, "before_market")
        self.assertEqual(annotation.eps_actual, 1.)

    def test_future_provider_actuals_do_not_make_future_report_confirmed(self):
        annotation = enrichment._build_fmp_annotation(
            "ABC", {"date": "2026-10-23", "epsActual": 1., "epsEstimated": .8},
            None, {}, dt.date(2026, 10, 20))
        self.assertFalse(annotation.is_reported)
        self.assertEqual(annotation.status, "upcoming")

    def test_event_cli_preserves_both_payload_shapes_and_forwards_cutoff(self):
        row = {"ticker": "ABC", "signal_date": "2026-10-23", "rank": 4}
        for payload in ([row], {"hits": [row], "strategy": "unchanged"}):
            with self.subTest(payload=type(payload).__name__), tempfile.TemporaryDirectory() as tmp:
                source, target = Path(tmp)/"source.json", Path(tmp)/"result.json"
                source.write_text(json.dumps(payload))
                args = SimpleNamespace(input_file=str(source), output_file=str(target), config="unused",
                                       as_of_date="2026-10-24", match_signal_date=True,
                                       signal_date=None, provider="yfinance")
                enriched = [{**row, "earnings_context": {"status": "catalyst_unknown"}}]
                with patch.object(cli, "parse_args", return_value=args), \
                     patch.object(cli, "load_app_config", return_value=AppConfig()), \
                     patch.object(cli, "build_earnings_event_contexts", return_value=enriched) as lookup:
                    self.assertEqual(cli.main(), 0)
                result = json.loads(target.read_text())
                self.assertEqual(result if isinstance(payload, list) else result["hits"], enriched)
                if isinstance(payload, dict):
                    self.assertEqual(result["strategy"], "unchanged")
                self.assertEqual(lookup.call_args.kwargs["as_of_date"], dt.date(2026, 10, 24))


if __name__ == "__main__":
    unittest.main()
