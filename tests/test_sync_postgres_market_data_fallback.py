from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch


class SyncPostgresMarketDataFallbackTests(unittest.TestCase):
    def test_invalid_source_history_is_rejected_before_replacement(self) -> None:
        import pandas as pd
        import scripts.sync_postgres_market_data as script

        invalid_history = pd.DataFrame(
            [{"Date": pd.Timestamp("2026-07-15"), "Open": 10.0, "High": 9.0, "Low": 8.0, "Close": 10.5, "Volume": 1_000}],
        ).set_index("Date")

        with self.assertRaisesRegex(ValueError, "invalid OHLCV range on 2026-07-15"):
            script._validate_history_frame("HPE", invalid_history)

    def test_yfinance_symbol_uses_hyphen_for_share_classes(self) -> None:
        import scripts.sync_postgres_market_data as script

        self.assertEqual(script._yfinance_symbol("BRK.B"), "BRK-B")
        self.assertEqual(script._yfinance_symbol("aapl"), "AAPL")

    def test_replace_daily_bars_checks_coverage_then_uses_ranked_upsert_in_one_commit(self) -> None:
        import scripts.sync_postgres_market_data as script

        calls: list[tuple[str, object]] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def execute(self, sql, params):
                calls.append((sql, params))

            def fetchall(self):
                return [(1,), (2,)]

            def executemany(self, sql, rows):
                calls.append((sql, list(rows)))
                self.rowcount = len(rows)

        class Connection:
            commits = 0
            rollbacks = 0

            def cursor(self):
                return Cursor()

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        connection = Connection()
        applied, skipped = script._replace_daily_bars(connection, [("AAPL", 1), ("AAPL", 2)])

        self.assertEqual((applied, skipped), (2, 0))
        self.assertIn("SELECT trade_date", calls[0][0])
        self.assertNotIn("DELETE FROM daily_bars", " ".join(sql for sql, _ in calls))
        self.assertIn("INSERT INTO daily_bars", calls[1][0])
        self.assertIn("when 'massive' then 2", calls[1][0].lower())
        self.assertIn("when 'yfinance' then 1", calls[1][0].lower())
        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)

    def test_replace_daily_bars_rejects_coverage_loss_before_upsert(self) -> None:
        import scripts.sync_postgres_market_data as script

        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_args): return None
            def execute(self, *_args): pass
            def fetchall(self): return [(3,)]
            def executemany(self, *_args): raise AssertionError("must not write after coverage loss")

        class Connection:
            commits = 0
            rollbacks = 0
            def cursor(self): return Cursor()
            def commit(self): self.commits += 1
            def rollback(self): self.rollbacks += 1

        connection = Connection()
        with self.assertRaisesRegex(script.HistoryCoverageLoss, "History coverage loss"):
            script._replace_daily_bars(connection, [("AAPL", 1), ("AAPL", 2)])
        self.assertEqual(connection.commits, 0)
        self.assertEqual(connection.rollbacks, 1)

    def test_diagnose_missing_ticker_falls_back_to_nasdaq_when_yahoo_is_empty(self) -> None:
        import pandas as pd
        import scripts.sync_postgres_market_data as script

        nasdaq_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-01-25"), "Open": 11.1, "High": 11.1, "Low": 10.2, "Close": 10.88, "Adj Close": 10.88, "Volume": 345221},
                {"Date": pd.Timestamp("2021-01-26"), "Open": 11.3, "High": 11.3, "Low": 10.5901, "Close": 10.63, "Adj Close": 10.63, "Volume": 270742},
            ]
        ).set_index("Date")

        with patch.object(script, "_download_single_history", return_value=(pd.DataFrame(), "yahoo empty")), patch.object(
            script,
            "_download_nasdaq_history",
            return_value=(nasdaq_frame, None),
        ):
            history, outcome = script._diagnose_missing_ticker(
                "NVTS",
                dt.date(2021, 1, 25),
                dt.date(2021, 1, 26),
                30,
                max_retries=1,
                retry_base_seconds=0.0,
                single_ticker_sleep_seconds=0.0,
            )

        self.assertFalse(history.empty)
        self.assertEqual(outcome.status, "synced_full_window")
        self.assertEqual(outcome.source, "nasdaq")

    def test_existing_yahoo_history_is_augmented_with_nasdaq_for_leading_gap(self) -> None:
        import pandas as pd
        import scripts.sync_postgres_market_data as script

        yahoo_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-10-20"), "Open": 13.98, "High": 14.08, "Low": 11.73, "Close": 12.8, "Adj Close": 12.8, "Volume": 1220376},
                {"Date": pd.Timestamp("2021-10-21"), "Open": 13.2, "High": 13.2, "Low": 12.635, "Close": 12.78, "Adj Close": 12.78, "Volume": 714979},
            ]
        ).set_index("Date")
        nasdaq_gap_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-01-25"), "Open": 11.1, "High": 11.1, "Low": 10.2, "Close": 10.88, "Adj Close": 10.88, "Volume": 345221},
                {"Date": pd.Timestamp("2021-01-26"), "Open": 11.3, "High": 11.3, "Low": 10.5901, "Close": 10.63, "Adj Close": 10.63, "Volume": 270742},
            ]
        ).set_index("Date")

        with patch.object(script, "_download_nasdaq_history", return_value=(nasdaq_gap_frame, None)):
            merged, source, errors = script._augment_history_with_nasdaq_fallback(
                "NVTS",
                yahoo_frame,
                dt.date(2021, 1, 25),
                dt.date(2021, 10, 21),
                max_retries=1,
                retry_base_seconds=0.0,
            )

        self.assertFalse(merged.empty)
        self.assertEqual(source, "yfinance+nasdaq")
        self.assertEqual(merged.index.min().date().isoformat(), "2021-01-25")
        self.assertEqual(merged.index.max().date().isoformat(), "2021-10-21")
        self.assertEqual(errors, [])
