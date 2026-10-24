from __future__ import annotations

import copy
import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts import sync_postgres_market_data as sync


class _Connection:
    def __init__(self, dates, *, insert_failure=False, concurrent_date=None):
        self.data = copy.deepcopy(dates)
        self.original = copy.deepcopy(dates)
        self.insert_failure, self.concurrent_date = insert_failure, concurrent_date
        self.calls, self.commits, self.rollbacks = [], 0, 0

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def cursor(self): return _Cursor(self)
    def commit(self):
        self.commits += 1
        self.original = copy.deepcopy(self.data)
    def rollback(self):
        self.rollbacks += 1
        self.data = copy.deepcopy(self.original)


class _Cursor:
    def __init__(self, connection): self.connection, self.rows = connection, []
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, sql, params):
        c, ticker = self.connection, params[0]
        c.calls.append(sql)
        if sql.lstrip().startswith("SELECT"):
            self.rows = [(day,) for day in c.data.get(ticker, [])]
        elif sql.lstrip().startswith("DELETE"):
            if c.concurrent_date is not None:
                c.data.setdefault(ticker, []).append(c.concurrent_date)
                c.original.setdefault(ticker, []).append(c.concurrent_date)
                c.concurrent_date = None
            self.rows = [(day,) for day in c.data.get(ticker, [])]
            c.data[ticker] = []
    def fetchall(self): return self.rows
    def executemany(self, sql, rows):
        c = self.connection
        c.calls.append(sql)
        if c.insert_failure:
            raise RuntimeError("insert failed")
        for row in rows:
            c.data.setdefault(row[0], []).append(row[1])


class MarketHistoryCoverageTests(unittest.TestCase):
    def test_recent_only_download_cannot_delete_existing_history(self):
        dates = [dt.date(2022, 1, 1) + dt.timedelta(days=i) for i in range(1146)]
        connection = _Connection({"NIVF": dates})
        with self.assertRaisesRegex(ValueError, "coverage|discard"):
            sync._replace_daily_bars(connection, [("NIVF", day) for day in dates[-27:]])
        self.assertFalse(any(sql.startswith("DELETE") for sql in connection.calls))
        self.assertEqual(connection.data["NIVF"], dates)
        self.assertEqual(connection.commits, 0)

    def test_equal_count_and_endpoints_do_not_hide_an_interior_gap(self):
        connection = _Connection({"ABC": [1, 2, 4]})
        with self.assertRaisesRegex(ValueError, "coverage|discard"):
            sync._replace_daily_bars(connection, [("ABC", day) for day in [1, 3, 4]])
        self.assertFalse(any(sql.startswith("DELETE") for sql in connection.calls))

    def test_equal_or_expanded_coverage_replaces_atomically(self):
        for replacement in ([1, 2], [1, 2, 3]):
            connection = _Connection({"ABC": [1, 2]})
            self.assertEqual(sync._replace_daily_bars(connection, [("ABC", day) for day in replacement]), (len(replacement), 0))
            self.assertEqual(connection.data["ABC"], replacement)
            self.assertEqual(connection.commits, 1)
            self.assertEqual(connection.rollbacks, 0)

    def test_insert_failure_rolls_back_deleted_history(self):
        connection = _Connection({"ABC": [1, 2]}, insert_failure=True)
        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            sync._replace_daily_bars(connection, [("ABC", 1), ("ABC", 2)])
        self.assertEqual(connection.data["ABC"], [1, 2])
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.commits, 0)

    def test_date_committed_between_guard_and_delete_is_preserved(self):
        connection = _Connection({"ABC": [1, 2]}, concurrent_date=3)
        with self.assertRaisesRegex(ValueError, "coverage|discard"):
            sync._replace_daily_bars(connection, [("ABC", 1), ("ABC", 2)])
        self.assertEqual(connection.data["ABC"], [1, 2, 3])
        self.assertEqual(connection.rollbacks, 1)
        self.assertEqual(connection.commits, 0)

    def test_coverage_failure_keeps_other_ticker_success_and_manifest(self):
        days = [dt.date(2026, 10, day) for day in (20, 21, 22)]
        frame = pd.DataFrame({"Open": 10., "High": 11., "Low": 9., "Close": 10., "Volume": 1000.}, index=pd.to_datetime(days[1:]))
        connection = _Connection({"BAD": days, "GOOD": days[1:]})
        args = SimpleNamespace(start_date=str(days[1]), end_date=str(days[-1]), incremental_days=0,
            database_url="unused", manifest_path="unused.json", ensure_schema=False, source_label="test",
            chunk_size=2, max_retries=1, retry_base_seconds=0, chunk_sleep_seconds=0,
            single_ticker_sleep_seconds=0, stale_after_days=30, batch_size=5000, replace_existing_history=True)
        universe = [SimpleNamespace(symbol=ticker) for ticker in ("BAD", "GOOD")]
        chunk = sync.DownloadChunkResult(["BAD", "GOOD"], {"BAD": frame, "GOOD": frame})
        with patch.object(sync, "parse_args", return_value=args), \
             patch.object(sync, "_load_target_universe", return_value=(None, universe)), \
             patch.object(sync, "load_webapp_config"), \
             patch.object(sync, "_connect", return_value=connection), \
             patch.object(sync, "_upsert_metadata", return_value=2), \
             patch.object(sync, "_download_history", return_value=[chunk]), \
             patch.object(sync, "_augment_history_with_nasdaq_fallback", side_effect=lambda ticker, history, *_args, **_kwargs: (history, "test", [])), \
             patch.object(sync, "_update_metadata_active_flags", return_value=1), \
             patch.object(sync, "_write_manifest") as manifest:
            self.assertEqual(sync.main(), 1)
        self.assertEqual(connection.data["BAD"], days)
        self.assertEqual(connection.data["GOOD"], days[1:])
        outcomes = {outcome.ticker: outcome for outcome in manifest.call_args.kwargs["outcomes"]}
        self.assertEqual(outcomes["BAD"].status, "failed_coverage_loss")
        self.assertEqual(outcomes["BAD"].rows_persisted, 0)
        self.assertTrue(outcomes["BAD"].history_retained)
        self.assertEqual(outcomes["GOOD"].rows_persisted, 2)
        self.assertEqual(manifest.call_args.kwargs["bar_row_count"], 2)


if __name__ == "__main__":
    unittest.main()
