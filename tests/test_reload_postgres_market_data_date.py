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
    _download_tradingview_histories,
    _tradingview_eligible,
    _filter_reload_tickers,
    _load_active_metadata_tickers,
    _minimum_replacement_row_count,
    _replace_trade_date_atomically,
    _rows_for_trade_date,
    _validate_replacement_rows,
    _advance_no_price_observations,
    _is_consecutive_trade_session,
    _provider_incident_detected,
    _replace_and_record_no_price_observations,
    _select_recovery_probes,
)


class ReloadPostgresMarketDataDateTests(unittest.TestCase):
    def test_five_character_r_rights_are_excluded_but_other_lengths_are_not(self) -> None:
        from src.ticker_filters import filter_symbols

        self.assertEqual(filter_symbols(["AACBR", "ABCR", "ABCDEF R"], set()), ["ABCR", "ABCDEF R"])

    def test_no_price_policy_requires_three_distinct_consecutive_trade_sessions(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            registry_path = Path(directory) / "no-price-3d.txt"
            registry_path.write_text("MANUAL # preserve me\n", encoding="utf-8")
            for day in (14, 15):
                _advance_no_price_observations(
                    state_path, registry_path, dt.date(2026, 10, day), {"CWAN"}, set(),
                    probe_tickers=set(), allow_observations=True,
                )
            self.assertNotIn("CWAN", registry_path.read_text(encoding="utf-8"))
            result = _advance_no_price_observations(
                state_path, registry_path, dt.date(2026, 10, 16), {"CWAN"}, set(),
                probe_tickers=set(), allow_observations=True,
            )
            contents = registry_path.read_text(encoding="utf-8")
            self.assertIn("CWAN", contents)
            self.assertIn("MANUAL # preserve me", contents)
            self.assertEqual(result["new_exclusions"], 1)

    def test_no_price_streak_resets_when_trade_sessions_are_not_consecutive(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            registry_path = Path(directory) / "no-price-3d.txt"
            for day in (13, 14, 16):
                _advance_no_price_observations(
                    state_path, registry_path, dt.date(2026, 10, day), {"CWAN"}, set(),
                    probe_tickers=set(), allow_observations=True,
                )
            self.assertNotIn("CWAN", registry_path.read_text(encoding="utf-8"))

    def test_weekend_and_regular_holiday_gaps_still_count_as_consecutive_sessions(self) -> None:
        self.assertTrue(_is_consecutive_trade_session(dt.date(2026, 10, 16), dt.date(2026, 10, 19)))
        self.assertTrue(_is_consecutive_trade_session(dt.date(2026, 11, 25), dt.date(2026, 11, 27)))

    def test_provider_incident_guard_and_failed_reload_do_not_advance_state(self) -> None:
        import tempfile
        from pathlib import Path

        self.assertTrue(_provider_incident_detected(target_count=100, missing_count=10, failed_chunks=0))
        self.assertTrue(_provider_incident_detected(target_count=100, missing_count=1, failed_chunks=1))
        self.assertFalse(_provider_incident_detected(target_count=100, missing_count=0, failed_chunks=1))
        self.assertFalse(_provider_incident_detected(target_count=100, missing_count=1, failed_chunks=0))
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            registry_path = Path(directory) / "no-price-3d.txt"
            result = _advance_no_price_observations(
                state_path, registry_path, dt.date(2026, 10, 16), {"CWAN"}, set(),
                probe_tickers=set(), allow_observations=False,
            )
            self.assertEqual(result["observations"], 0)
            self.assertFalse(state_path.exists())

    def test_tradingview_is_limited_to_latest_completed_session_after_delay_cutoff(self) -> None:
        from zoneinfo import ZoneInfo

        eastern = ZoneInfo("America/New_York")
        friday = dt.date(2026, 10, 16)
        thursday = dt.date(2026, 10, 15)
        self.assertFalse(_tradingview_eligible(friday, dt.datetime(2026, 10, 16, 16, 14, tzinfo=eastern)))
        self.assertFalse(_tradingview_eligible(thursday, dt.datetime(2026, 10, 16, 16, 14, tzinfo=eastern)))
        self.assertTrue(_tradingview_eligible(friday, dt.datetime(2026, 10, 16, 16, 15, tzinfo=eastern)))
        self.assertTrue(_tradingview_eligible(friday, dt.datetime(2026, 10, 19, 8, 0, tzinfo=eastern)))
        self.assertFalse(_tradingview_eligible(dt.date(2026, 10, 19), dt.datetime(2026, 10, 19, 8, 0, tzinfo=eastern)))
        self.assertFalse(_tradingview_eligible(friday, dt.datetime(2026, 10, 19, 10, 0, tzinfo=eastern)))

    def test_tradingview_exchange_disambiguation_and_invalid_rows(self) -> None:
        rows = [
            {"d": ["ABC", "NYSE", 10, 11, 9, 10.5, 100, "delayed"]},
            {"d": ["ABC", "NASDAQ", 20, 21, 19, 20.5, 200, "delayed"]},
            {"d": ["BAD", "NYSE", 10, 9, 8, 9, 100, "delayed"]},
            {"d": [None, "NYSE", 1, 2, 1, 2, 100, "delayed"]},
            {"d": ["NULL", "NYSE", None, 2, 1, 2, 100, "delayed"]},
        ]
        response = MagicMock()
        response.json.return_value = {"data": rows}
        response.raise_for_status.return_value = None
        with patch("scripts.reload_postgres_market_data_date.requests.post", return_value=response):
            histories, invalid, ambiguous, failed = _download_tradingview_histories(
                dt.date(2026, 10, 16), ["ABC", "BAD", "NULL", "MISSING"], {"ABC": "NASDAQ"}
            )
        self.assertEqual(set(histories), {"ABC"})
        self.assertEqual(histories["ABC"].iloc[0]["Close"], 20.5)
        self.assertEqual((invalid, ambiguous, failed), (2, 0, 0))

    def test_tradingview_duplicate_without_unique_exchange_match_is_ambiguous(self) -> None:
        response = MagicMock()
        response.json.return_value = {"data": [
            {"d": ["ABC", "NYSE", 10, 11, 9, 10, 100, "delayed"]},
            {"d": ["ABC", "NASDAQ", 20, 21, 19, 20, 200, "delayed"]},
        ]}
        response.raise_for_status.return_value = None
        with patch("scripts.reload_postgres_market_data_date.requests.post", return_value=response):
            histories, invalid, ambiguous, failed = _download_tradingview_histories(
                dt.date(2026, 10, 16), ["ABC"], {}
            )
        self.assertEqual(histories, {})
        self.assertEqual((invalid, ambiguous, failed), (0, 1, 0))

    def test_tradingview_unique_match_does_not_require_metadata_exchange_match(self) -> None:
        response = MagicMock()
        response.json.return_value = {
            "data": [{"d": ["SPY", "AMEX", 773.24, 773.25, 760.64, 763.25, 64_382_748, "delayed"]}]
        }
        response.raise_for_status.return_value = None
        with patch("scripts.reload_postgres_market_data_date.requests.post", return_value=response):
            histories, invalid, ambiguous, failed = _download_tradingview_histories(
                dt.date(2026, 10, 16), ["SPY"], {"SPY": "ETF"}
            )
        self.assertEqual(set(histories), {"SPY"})
        self.assertEqual((invalid, ambiguous, failed), (0, 0, 0))

    def test_massive_duplicate_and_malformed_rows_do_not_survive(self) -> None:
        histories = _build_massive_histories(
            {"results": [
                {"T": "ABC", "o": 10, "h": 11, "l": 9, "c": 10, "v": 100},
                {"T": "ABC", "o": 10, "h": 11, "l": 9, "c": 10, "v": 100},
                {"T": "BAD", "o": 10, "h": 11, "l": 9, "c": 10, "v": 1.5},
            ]},
            dt.date(2026, 10, 16), {"ABC", "BAD"},
        )
        self.assertEqual(histories, {})

    def test_failed_atomic_replacement_does_not_advance_no_price_state(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            registry_path = Path(directory) / "no-price-3d.txt"
            with patch("scripts.reload_postgres_market_data_date._replace_trade_date_atomically", side_effect=RuntimeError("insert failed")):
                with self.assertRaisesRegex(RuntimeError, "insert failed"):
                    _replace_and_record_no_price_observations(
                        MagicMock(), dt.date(2026, 10, 16), [], 100, state_path, registry_path,
                        {"CWAN"}, set(), probe_tickers=set(), allow_observations=True,
                    )
            self.assertFalse(state_path.exists())

    def test_manual_include_suppresses_auto_exclusion_and_valid_probe_recovers(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            registry_path = Path(directory) / "no-price-3d.txt"
            manual_include = Path(directory) / "manual_include_tickers.txt"
            for day in (14, 15, 16):
                _advance_no_price_observations(
                    state_path, registry_path, dt.date(2026, 10, day), {"CWAN"}, set(),
                    probe_tickers=set(), allow_observations=True,
                )
            manual_include.write_text("CWAN\n", encoding="utf-8")
            self.assertIn("CWAN", registry_path.read_text(encoding="utf-8"))
            from dataclasses import replace
            from src.config import AppConfig
            from src.ticker_filters import load_excluded_tickers

            config = replace(
                AppConfig(),
                manual_included_tickers_file=str(manual_include),
                auto_excluded_tickers_dir=str(registry_path.parent),
                special_security_tickers_file=str(Path(directory) / "missing-special.csv"),
                excluded_tickers_file=str(Path(directory) / "missing-base.txt"),
                manual_excluded_tickers_file=str(Path(directory) / "missing-manual.txt"),
            )
            self.assertNotIn("CWAN", load_excluded_tickers(config))
            probes = _select_recovery_probes(state_path, registry_path, dt.date(2026, 10, 19), max_probes=5)
            self.assertEqual(probes, ["CWAN"])
            result = _advance_no_price_observations(
                state_path, registry_path, dt.date(2026, 10, 19), set(), {"CWAN"},
                probe_tickers={"CWAN"}, allow_observations=True,
            )
            self.assertEqual(result["recoveries"], 1)
            self.assertNotIn("CWAN", registry_path.read_text(encoding="utf-8"))

    def test_rows_for_trade_date_skips_stale_provider_rows(self) -> None:
        requested_date = dt.date(2026, 10, 13)
        matching_row = ("AAPL", requested_date, 200.0, 205.0, 198.0, 204.0, 204.0, 10_000)
        stale_row = ("BHAVR", dt.date(2026, 10, 12), 10.0, 11.0, 10.0, 10.5, 10.5, 100)

        rows, stale_tickers = _rows_for_trade_date([matching_row, stale_row], requested_date)

        self.assertEqual(rows, [matching_row])
        self.assertEqual(stale_tickers, {"BHAVR"})

    def test_validate_replacement_rows_accepts_provider_ohlcv_values(self) -> None:
        row = ("EURKR", dt.date(2026, 10, 13), 0.4181, 0.4499, 0.4181, 0.4499, 0.4499, 361, 0.0, 1.0, "yfinance", dt.datetime.now(dt.timezone.utc))

        _validate_replacement_rows([row], dt.date(2026, 10, 13), minimum_row_count=1)

    def test_minimum_replacement_coverage_uses_eligible_universe_not_partial_existing_rows(self) -> None:
        self.assertEqual(_minimum_replacement_row_count(3_789), 3_411)

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
            histories, chunks, unresolved, failed_chunks = _download_yfinance_pass(
                ["AAPL", "MSFT"], dt.date(2026, 10, 9), args, pass_name="primary", starting_chunk=0
            )

        self.assertEqual(set(histories), {"AAPL"})
        self.assertEqual((chunks, unresolved), (1, ["MSFT"]))
        self.assertEqual(failed_chunks, 0)
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
