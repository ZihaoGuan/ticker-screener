from __future__ import annotations

import argparse
import unittest
from unittest.mock import MagicMock, patch

import datetime as dt
import json

from scripts.reload_postgres_market_data_date import (
    _build_massive_histories,
    _download_massive_histories,
    _download_yfinance_pass,
    _filter_reload_tickers,
    _load_active_metadata_tickers,
    _replace_trade_date_atomically,
    _validate_replacement_rows,
)


class ReloadPostgresMarketDataDateTests(unittest.TestCase):
    def test_load_active_metadata_tickers_skips_inactive_rows(self) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [("AAPL", True), ("DEAD", False), ("", True)]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        active, inactive_count = _load_active_metadata_tickers(connection)

        self.assertEqual(active, ["AAPL"])
        self.assertEqual(inactive_count, 1)

    def test_filter_reload_tickers_uses_existing_exclusion_policy(self) -> None:
        eligible = _filter_reload_tickers(["AAPL", "ACONW", "MULL", "BRK.B"], {"MULL"})

        self.assertEqual(eligible, ["AAPL", "BRK.B"])

    def test_yfinance_pass_collects_missing_tickers_as_a_batch(self) -> None:
        history = _build_massive_histories(
            {"results": [{"T": "AAPL", "o": 200, "h": 205, "l": 198, "c": 204, "v": 10_000}]},
            dt.date(2026, 10, 9),
            {"AAPL"},
        )["AAPL"]
        chunk = MagicMock(tickers=["AAPL", "MSFT"], history_by_ticker={"AAPL": history}, error=None)
        args = argparse.Namespace(
            chunk_size=100,
            max_retries=4,
            retry_base_seconds=2.0,
            chunk_sleep_seconds=0.0,
            source_label="yfinance",
            batch_size=5000,
        )

        with patch("scripts.reload_postgres_market_data_date._download_history", return_value=iter([chunk])) as download:
            histories, chunks, unresolved = _download_yfinance_pass(
                ["AAPL", "MSFT"], dt.date(2026, 10, 9), args, pass_name="primary", starting_chunk=0
            )

        self.assertEqual(set(histories), {"AAPL"})
        self.assertEqual((chunks, unresolved), (1, ["MSFT"]))
        self.assertEqual(download.call_args.args[0], ["AAPL", "MSFT"])

    def test_replace_trade_date_atomically_rolls_back_on_insert_error(self) -> None:
        cursor = MagicMock()
        cursor.executemany.side_effect = RuntimeError("insert failed")
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        rows = [("AAPL", dt.date(2026, 10, 9), 200.0, 205.0, 198.0, 204.0, 204.0, 10_000, 0.0, 1.0, "massive", dt.datetime.now(dt.timezone.utc))]

        with self.assertRaisesRegex(RuntimeError, "insert failed"):
            _replace_trade_date_atomically(connection, dt.date(2026, 10, 9), rows, 5_000)

        connection.commit.assert_not_called()
        connection.rollback.assert_called_once()

    def test_replace_trade_date_atomically_deletes_and_inserts_before_one_commit(self) -> None:
        cursor = MagicMock()
        cursor.rowcount = 3
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        rows = [
            ("AAPL", dt.date(2026, 10, 9), 200.0, 205.0, 198.0, 204.0, 204.0, 10_000, 0.0, 1.0, "massive", dt.datetime.now(dt.timezone.utc)),
            ("MSFT", dt.date(2026, 10, 9), 500.0, 510.0, 498.0, 505.0, 505.0, 20_000, 0.0, 1.0, "yfinance", dt.datetime.now(dt.timezone.utc)),
        ]

        deleted, applied = _replace_trade_date_atomically(connection, dt.date(2026, 10, 9), rows, 1)

        self.assertEqual((deleted, applied), (3, 2))
        self.assertEqual(cursor.method_calls[0][0], "execute")
        self.assertEqual(cursor.method_calls[1][0], "executemany")
        self.assertEqual(cursor.method_calls[2][0], "executemany")
        self.assertEqual(cursor.method_calls[1].args[1][0][10], "massive")
        self.assertEqual(cursor.method_calls[2].args[1][0][10], "yfinance")
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()

    def test_validate_replacement_rows_rejects_low_coverage_and_duplicate_tickers(self) -> None:
        row = ("AAPL", dt.date(2026, 10, 9), 200.0, 205.0, 198.0, 204.0, 204.0, 10_000, 0.0, 1.0, "massive", dt.datetime.now(dt.timezone.utc))

        with self.assertRaisesRegex(RuntimeError, "coverage"):
            _validate_replacement_rows([], dt.date(2026, 10, 9), minimum_row_count=1)
        with self.assertRaisesRegex(RuntimeError, "Invalid replacement row"):
            _validate_replacement_rows([row, row], dt.date(2026, 10, 9), minimum_row_count=1)

    def test_download_massive_histories_requests_raw_grouped_daily_bars(self) -> None:
        response = MagicMock()
        response.read.return_value = json.dumps(
            {"status": "OK", "results": [{"T": "AAPL", "o": 200, "h": 205, "l": 198, "c": 204, "v": 10_000}]}
        ).encode("utf-8")
        context = MagicMock()
        context.__enter__.return_value = response

        with patch("scripts.reload_postgres_market_data_date.urllib_request.urlopen", return_value=context) as urlopen:
            histories = _download_massive_histories(dt.date(2026, 10, 9), {"AAPL"}, "test-key")

        request = urlopen.call_args.args[0]
        self.assertIn("/v2/aggs/grouped/locale/us/market/stocks/2026-10-09", request.full_url)
        self.assertIn("adjusted=false", request.full_url)
        self.assertEqual(set(histories), {"AAPL"})

    def test_build_massive_histories_keeps_valid_requested_rows(self) -> None:
        histories = _build_massive_histories(
            {
                "results": [
                    {"T": "AAPL", "o": 200, "h": 205, "l": 198, "c": 204, "v": 10_000},
                    {"T": "UNTRACKED", "o": 1, "h": 2, "l": 1, "c": 2, "v": 100},
                    {"T": "BAD", "o": 10, "h": 9, "l": 8, "c": 9, "v": 100},
                ]
            },
            dt.date(2026, 10, 9),
            {"AAPL", "BAD"},
        )

        self.assertEqual(set(histories), {"AAPL"})
        row = histories["AAPL"].iloc[0]
        self.assertEqual(row["Adj Close"], 204)
        self.assertEqual(row["Stock Splits"], 1.0)
        self.assertEqual(histories["AAPL"].index[0].date(), dt.date(2026, 10, 9))

if __name__ == "__main__":
    unittest.main()
