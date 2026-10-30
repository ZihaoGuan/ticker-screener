from __future__ import annotations

import unittest
import datetime as dt
import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

from src.webapp.services.martin_forward_test_service import DAILY_TOP5_STRATEGY_ID, MartinForwardTestService, SECTOR_STRATEGY_ID, classify_candidate, order_outcome, position_exit, select_sector_candidates
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository, _REQUIRED_FORWARD_SCHEMA_COLUMNS, _attach_trim_history, _benchmark_comparison, _closed_trade_metrics, _performance_context, _provisional_fallback_dates


class MartinRepositorySchemaTests(unittest.TestCase):
    def test_batched_daily_bar_provenance_uses_one_query_and_preserves_symbol_rows(self):
        rows = [
            {"ticker": "AAA", "source": "tradingview", "split_factor": 1, "trade_date": dt.date(2025, 1, 2),
             "rows": 1, "first_date": dt.date(2025, 1, 2), "last_date": dt.date(2025, 1, 2),
             "latest_split_date": None, "history_first_date": dt.date(2024, 1, 1),
             "unknown_split_rows": 0, "unknown_source_rows": 3},
            {"ticker": "BBB", "source": None, "split_factor": None, "trade_date": dt.date(2025, 1, 2),
             "rows": 1, "first_date": dt.date(2025, 1, 2), "last_date": dt.date(2025, 1, 2),
             "latest_split_date": dt.date(2024, 4, 1), "history_first_date": dt.date(2023, 1, 1),
             "unknown_split_rows": 1, "unknown_source_rows": 1},
        ]
        repository = MartinForwardTestRepository(database_url="")
        with patch.object(repository, "_rows", return_value=rows) as query:
            result = repository.daily_bar_replay_provenance_many(["bbb", "AAA", "AAA"], dt.date(2024, 1, 1), dt.date(2025, 1, 2))
        self.assertEqual(query.call_count, 1)
        self.assertEqual(query.call_args.args[1], (["AAA", "BBB"], dt.date(2025, 1, 2), ["AAA", "BBB"],
                                                   dt.date(2024, 1, 1), dt.date(2025, 1, 2)))
        self.assertEqual(result["AAA"][0]["unknown_source_rows"], 3)
        self.assertEqual(result["BBB"][0]["unknown_split_rows"], 1)

    def test_daily_provenance_operation_count_benchmark(self):
        symbols = [f"TICKER{index}" for index in range(200)]
        repository = MartinForwardTestRepository(database_url="")
        with patch.object(repository, "_rows", return_value=[]) as query:
            for ticker in symbols:
                repository.daily_bar_replay_provenance(ticker, dt.date(2025, 1, 1), dt.date(2025, 12, 31))
            scalar_queries = query.call_count
            query.reset_mock()
            repository.daily_bar_replay_provenance_many(symbols, dt.date(2025, 1, 1), dt.date(2025, 12, 31))
            batch_queries = query.call_count
        self.assertEqual(scalar_queries, 200)
        self.assertEqual(batch_queries, 1)

    def test_batched_candidate_updates_skip_unchanged_rows(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def executemany(self, sql, values): self.sql, self.values = sql, values
        class Connection:
            def __init__(self): self.cursor_instance = Cursor()
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def cursor(self): return self.cursor_instance
        connection = Connection()
        repository = MartinForwardTestRepository(database_url="")
        with patch.object(repository, "_connect", return_value=connection):
            repository.update_candidates([(17, "AAA", {"status": "pending", "reasons": [], "shares": 3})])
        self.assertIn("IS DISTINCT FROM", connection.cursor_instance.sql)
        self.assertEqual(len(connection.cursor_instance.values), 1)
        self.assertEqual(connection.cursor_instance.values[0][3:5], (17, "AAA"))

    class FakeConnection:
        def __init__(self, rows=()): self.rows, self.executions = rows, []
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def cursor(self): return self
        def execute(self, sql, *_args): self.executions.append(sql)
        def fetchall(self): return self.rows

    def test_existing_schema_skips_ddl_for_a_fresh_repository_instance(self):
        rows = [(table, column) for table, columns in _REQUIRED_FORWARD_SCHEMA_COLUMNS.items() for column in columns]
        catalog_connection = self.FakeConnection(rows)
        connect = MagicMock(return_value=catalog_connection)
        with patch.dict(sys.modules, {"psycopg": SimpleNamespace(connect=connect)}):
            MartinForwardTestRepository(database_url="postgresql://schema-ready-fresh-process").ensure_schema()
        self.assertEqual(connect.call_count, 1)
        self.assertIn("default_transaction_read_only=on", connect.call_args.kwargs["options"])
        self.assertEqual(len(catalog_connection.executions), 1)
        self.assertIn("information_schema.columns", catalog_connection.executions[0])

    def test_missing_schema_applies_full_schema(self):
        catalog_connection = self.FakeConnection()
        ddl_connection = self.FakeConnection()
        connect = MagicMock(side_effect=[catalog_connection, ddl_connection])
        with patch.dict(sys.modules, {"psycopg": SimpleNamespace(connect=connect)}):
            MartinForwardTestRepository(database_url="postgresql://schema-missing").ensure_schema()
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(len(ddl_connection.executions), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS martin_forward_runs", ddl_connection.executions[0])


class MartinExecutionActivityTests(unittest.TestCase):
    def test_trim_history_is_derived_from_position_linked_trim_events(self):
        positions = [
            {"id": 10, "payload_json": {"entry_price": 20}},
            {"id": 11, "payload_json": {"trim_history": [{"date": "legacy"}]}},
        ]
        events = [
            {"position_id": 10, "event_type": "trim_3r", "event_date": dt.date(2026, 10, 20),
             "payload_json": {"price": 23, "shares": 30}},
            {"position_id": 11, "event_type": "trim_3r", "event_date": dt.date(2026, 10, 21),
             "payload_json": {"price": 24, "shares": 20}},
            {"position_id": None, "event_type": "trim_3r", "event_date": dt.date(2026, 10, 22),
             "payload_json": {"price": 25, "shares": 10}},
        ]

        _attach_trim_history(positions, events)

        self.assertEqual(positions[0]["payload_json"]["trim_history"], [
            {"date": dt.date(2026, 10, 20), "price": 23, "shares": 30},
        ])
        self.assertEqual(positions[1]["payload_json"]["trim_history"], [{"date": "legacy"}])

    def test_execution_book_activity_is_loaded_beyond_recent_event_window(self):
        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        strategy_id = "martin_range_bar_retest_example"
        position = {"id": 10, "strategy_id": strategy_id, "ticker": "AESI", "status": "closed",
                    "opened_on": dt.date(2026, 3, 25), "closed_on": dt.date(2026, 3, 27),
                    "payload_json": {"trimmed": True}}
        old_trim = {"id": 1, "strategy_id": strategy_id, "position_id": 10, "ticker": "AESI",
                    "event_type": "trim_3r", "event_date": dt.date(2026, 3, 26),
                    "payload_json": {"price": 30, "shares": 20}}
        statements = []

        def fetch(_cursor, sql, params=()):
            statements.append(sql)
            if "SELECT DISTINCT strategy_id" in sql:
                return [{"strategy_id": strategy_id}]
            if "FROM martin_forward_runs" in sql and "ORDER BY session_date DESC" in sql:
                return [{"id": 1, "strategy_id": strategy_id, "session_date": dt.date(2026, 3, 27),
                         "summary_json": {"execution_book_version": "martin_bar_retest_execution_v5"}}]
            if "FROM martin_forward_positions" in sql:
                return [position]
            if "position_id=ANY(%s)" in sql:
                return [old_trim]
            return []

        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value = connection
        with patch.object(repository, "_connect", return_value=connection), \
             patch.object(repository, "_fetch", side_effect=fetch):
            book = repository.context()["books"][strategy_id]

        self.assertEqual(book["events"], [])  # recent-events list remains independently capped
        self.assertEqual(book["activity_events"], [old_trim])
        self.assertEqual(book["positions"][0]["payload_json"]["trim_history"], [
            {"date": dt.date(2026, 3, 26), "price": 30, "shares": 20},
        ])
        activity_query = next(sql for sql in statements if "position_id=ANY(%s)" in sql)
        self.assertIn("event_type=ANY(%s)", activity_query)


class MartinBenchmarkComparisonTests(unittest.TestCase):
    def test_leveraged_and_inverse_catalog_symbols_are_blocked_but_benchmarks_are_not(self) -> None:
        for ticker in ("KORU", "SOXS", "INTW", "MVLL"):
            with self.subTest(ticker=ticker):
                candidate = classify_candidate({"ticker": ticker}, 1)
                self.assertEqual(candidate["status"], "blocked")
                self.assertIn("leveraged_or_inverse_fund", candidate["reasons"])
        for ticker in ("SPY", "QQQ", "TRLV", "ANDG"):
            with self.subTest(ticker=ticker):
                candidate = classify_candidate({"ticker": ticker}, 1)
                self.assertNotIn("leveraged_or_inverse_fund", candidate["reasons"])

    def test_missing_account_market_session_and_latest_etf_bar_remain_gaps(self) -> None:
        history = [{"date": "2026-10-01", "account_equity": 100_000}, {"date": "2026-10-05", "account_equity": 102_000}]
        prices = [{"ticker": ticker, "trade_date": day, "close": close} for ticker, day, close in (
            ("SPY", "2026-10-01", 10), ("QQQ", "2026-10-01", 20), ("SPY", "2026-10-02", 11),
            ("QQQ", "2026-10-02", 21), ("SPY", "2026-10-05", 12))]
        result = _benchmark_comparison(history, prices)
        self.assertEqual(result["missing_dates"], ["2026-10-02", "2026-10-05"])
        self.assertEqual(result["comparison_date"], "2026-10-01")
        self.assertEqual(result["account_history_end"], "2026-10-05")
        for series in result["series"].values():
            self.assertEqual([point["value"] for point in series], [100, None, None])

    def test_partial_or_invalid_baseline_is_never_used(self) -> None:
        prices = [{"ticker": ticker, "trade_date": "2026-10-01", "close": 10} for ticker in ("SPY", "QQQ")]
        history = [{"date": "2026-10-01", "account_equity": 100_000}]
        self.assertEqual(_benchmark_comparison(history, prices, {"2026-10-01"})["status"], "unavailable")
        for invalid in (None, float("nan"), float("inf"), 0, -1):
            with self.subTest(invalid=invalid):
                result = _benchmark_comparison(history, [{**row, "close": invalid} for row in prices])
                self.assertEqual(result["status"], "unavailable")

    def test_repository_context_benchmarks_are_read_only_and_database_errors_are_safe(self) -> None:
        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        connection = MagicMock()
        runs = [{"id": 2, "session_date": dt.date(2026, 10, 1), "summary_json": {"account_equity": 100_000, "cumulative_r": 0, "r_drawdown": 0}},
                {"id": 1, "session_date": dt.date(2026, 10, 1), "summary_json": {"account_equity": 99_000, "cumulative_r": -1, "r_drawdown": -1, "partial": True}}]

        for fail in (False, True):
            statements = []
            def fetch(cursor, sql, params=()):
                statements.append(sql)
                if "FROM daily_bars" in sql:
                    if fail:
                        raise RuntimeError("private-database-detail")
                    return [{"ticker": ticker, "trade_date": dt.date(2026, 10, 1), "close": 10} for ticker in ("SPY", "QQQ")]
                return runs if "FROM martin_forward_runs" in sql and params == ("martin_daily_v1",) else []

            with patch.object(repository, "_connect", return_value=connection), patch.object(repository, "_fetch", side_effect=fetch):
                result = repository.context()
            comparison = result["books"]["account"]["benchmark_comparison"]
            self.assertEqual(comparison["status"], "unavailable" if fail else "available")
            self.assertNotIn("private-database-detail", str(result))
            self.assertTrue(all(statement.strip().startswith("SELECT") for statement in statements))

    def test_negative_and_flat_returns_keep_period_drawdown(self) -> None:
        history = [{"date": "2026-10-01", "account_equity": 100_000}, {"date": "2026-10-02", "account_equity": 90_000}, {"date": "2026-10-03", "account_equity": 100_000}]
        prices = [{"ticker": ticker, "trade_date": day, "close": close} for ticker, day, close in (("SPY", "2026-10-01", 10), ("QQQ", "2026-10-01", 20), ("SPY", "2026-10-02", 9), ("QQQ", "2026-10-02", 22), ("SPY", "2026-10-03", 10), ("QQQ", "2026-10-03", 20))]
        result = _benchmark_comparison(history, prices)
        self.assertAlmostEqual(result["series"]["Account"][-1]["return_pct"], 0)
        self.assertAlmostEqual(result["series"]["Account"][-1]["drawdown_pct"], 0)
        self.assertAlmostEqual(result["series"]["SPY"][1]["return_pct"], -10)
        self.assertAlmostEqual(result["series"]["QQQ"][-1]["return_pct"], 0)
        self.assertAlmostEqual(result["series"]["Account"][1]["drawdown_pct"], -10)

    def test_rebases_common_start_and_handles_unsorted_duplicate_rows(self) -> None:
        history = [{"date": "2026-10-02", "account_equity": 101_000}, {"date": "2026-10-01", "account_equity": 100_000}, {"date": "2026-10-02", "account_equity": 102_000}]
        prices = [{"ticker": "QQQ", "trade_date": "2026-10-02", "close": 22}, {"ticker": "SPY", "trade_date": "2026-10-01", "close": 10}, {"ticker": "QQQ", "trade_date": "2026-10-01", "close": 20}, {"ticker": "SPY", "trade_date": "2026-10-02", "close": 11}, {"ticker": "SPY", "trade_date": "2026-10-02", "close": 12}]
        result = _benchmark_comparison(history, prices)
        self.assertEqual(result["baseline_date"], "2026-10-01")
        self.assertEqual(result["comparison_date"], "2026-10-02")
        self.assertEqual(result["basis"], "price_return_close")
        self.assertAlmostEqual(result["series"]["Account"][-1]["return_pct"], 2)
        self.assertAlmostEqual(result["series"]["SPY"][-1]["return_pct"], 20)
        self.assertAlmostEqual(result["series"]["QQQ"][-1]["return_pct"], 10)

    def test_missing_mid_session_stays_a_gap_and_partial_account_value_is_null(self) -> None:
        history = [{"date": "2026-10-01", "account_equity": 100_000}, {"date": "2026-10-02", "account_equity": 101_000}, {"date": "2026-10-03", "account_equity": 102_000}]
        prices = [{"ticker": ticker, "trade_date": day, "close": close} for ticker, day, close in (("SPY", "2026-10-01", 10), ("QQQ", "2026-10-01", 20), ("QQQ", "2026-10-02", 21), ("SPY", "2026-10-03", 11), ("QQQ", "2026-10-03", 22))]
        result = _benchmark_comparison(history, prices, {"2026-10-02"})
        self.assertIsNone(result["series"]["Account"][1]["value"])
        self.assertIsNone(result["series"]["SPY"][1]["value"])
        self.assertEqual(result["missing_dates"], ["2026-10-02"])

    def test_provisional_fallback_date_is_in_primary_and_excluded_from_strict_series(self) -> None:
        history = [{"date": day, "account_equity": equity} for day, equity in (
            ("2026-10-01", 100_000), ("2026-10-02", 101_000), ("2026-10-05", 103_000))]
        prices = [{"ticker": ticker, "trade_date": day, "close": close} for ticker, day, close in (
            ("SPY", "2026-10-01", 10), ("QQQ", "2026-10-01", 20),
            ("SPY", "2026-10-02", 11), ("QQQ", "2026-10-02", 21),
            ("SPY", "2026-10-05", 12), ("QQQ", "2026-10-05", 22))]
        result = _benchmark_comparison(history, prices, provisional_dates={"2026-10-02"})
        self.assertEqual(result["provisional_dates"], ["2026-10-02"])
        self.assertEqual(result["missing_dates"], [])
        self.assertEqual(result["series"]["Account"][1]["value"], 101)
        strict = result["strict_comparison"]
        self.assertEqual(strict["baseline_date"], "2026-10-01")
        self.assertNotIn("2026-10-02", [point["date"] for point in strict["series"]["Account"]])
        self.assertEqual(strict["comparison_date"], "2026-10-05")

    def test_provisional_date_still_missing_when_account_or_etf_evidence_is_absent(self) -> None:
        history = [{"date": "2026-10-01", "account_equity": 100_000},
                   {"date": "2026-10-02", "account_equity": 101_000}]
        prices = [{"ticker": ticker, "trade_date": day, "close": 10} for ticker, day in (
            ("SPY", "2026-10-01"), ("QQQ", "2026-10-01"), ("SPY", "2026-10-02"))]
        result = _benchmark_comparison(history, prices, provisional_dates={"2026-10-02"})
        self.assertEqual(result["provisional_dates"], [])
        self.assertEqual(result["missing_dates"], ["2026-10-02"])

    def test_provisional_detection_reads_persisted_execution_summary_shape(self) -> None:
        runs = {
            "2026-10-01": {"summary_json": {"partial": True, "intraday_fallback_count": 2,
                "execution_actions": {"intraday_fallbacks": 2, "missing_bars": 0}}},
            "2026-10-02": {"summary_json": {"partial": True, "intraday_fallback_count": 1,
                "execution_actions": {"intraday_fallbacks": 1, "missing_bars": 1}}},
        }
        self.assertEqual(_provisional_fallback_dates(runs, "martin_bar_retest_execution_v3"), {"2026-10-01"})
        self.assertEqual(_provisional_fallback_dates(runs, "martin_bar_retest_execution_v2"), set())

    def test_strict_comparison_rebases_after_provisional_baseline(self) -> None:
        history = [{"date": day, "account_equity": 100_000 + index * 1_000}
                   for index, day in enumerate(("2026-10-01", "2026-10-02", "2026-10-05"))]
        prices = [{"ticker": ticker, "trade_date": day, "close": close} for ticker, day, close in (
            ("SPY", "2026-10-01", 10), ("QQQ", "2026-10-01", 20),
            ("SPY", "2026-10-02", 11), ("QQQ", "2026-10-02", 21),
            ("SPY", "2026-10-05", 12), ("QQQ", "2026-10-05", 22))]
        result = _benchmark_comparison(history, prices, provisional_dates={"2026-10-01"})
        strict = result["strict_comparison"]
        self.assertEqual(strict["baseline_date"], "2026-10-02")
        self.assertNotIn("2026-10-01", [point["date"] for point in strict["series"]["Account"]])


class MartinExecutionBookContextTests(unittest.TestCase):
    def test_apply_event_persists_closed_position_status_and_date_in_payload(self) -> None:
        class Cursor:
            def __init__(self):
                self.statements = []
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, params): self.statements.append((sql, params))
            def fetchone(self): return (1,)

        class Connection:
            def __init__(self): self.db_cursor = Cursor()
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def cursor(self): return self.db_cursor

        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        connection = Connection()
        event_date = dt.date(2026, 10, 23)
        original_payload = {"status": "open", "remaining_shares": 0}
        with patch.object(repository, "_connect", return_value=connection):
            self.assertTrue(repository.apply_event(
                run_id=1, position_id=2, ticker="ABC", event_type="stop", event_date=event_date,
                payload={"reason": "stop"}, position_payload=original_payload, close_position=True,
                strategy_id="replay_book"))

        update_params = next(params for sql, params in connection.db_cursor.statements if "UPDATE martin_forward_positions" in sql)
        saved_payload = json.loads(update_params[0])
        self.assertEqual(saved_payload["status"], "closed")
        self.assertEqual(saved_payload["closed_on"], "2026-10-23")
        self.assertEqual(saved_payload["remaining_shares"], 0)
        self.assertEqual(original_payload, {"status": "open", "remaining_shares": 0})

    def test_apply_event_does_not_rewrite_non_close_position_payload(self) -> None:
        class Cursor:
            def __init__(self): self.statements = []
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, params): self.statements.append((sql, params))
            def fetchone(self): return (1,)
        class Connection:
            def __init__(self): self.db_cursor = Cursor()
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def cursor(self): return self.db_cursor

        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        connection = Connection()
        payload = {"status": "open", "remaining_shares": 12}
        with patch.object(repository, "_connect", return_value=connection):
            self.assertTrue(repository.apply_event(
                run_id=1, position_id=2, ticker="ABC", event_type="trim", event_date=dt.date(2026, 10, 23),
                payload={"reason": "trim"}, position_payload=payload, close_position=False))

        update_params = next(params for sql, params in connection.db_cursor.statements if "UPDATE martin_forward_positions" in sql)
        self.assertEqual(json.loads(update_params[0]), payload)

    def test_pending_orders_can_be_limited_to_orders_available_before_session(self) -> None:
        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        with patch.object(repository, "_rows", return_value=[]) as rows:
            repository.pending_orders(strategy_id="martin_range_bar_retest_v2", before=dt.date(2026, 10, 23))
        query, params = rows.call_args.args
        self.assertIn("r.session_date < %s", query)
        self.assertEqual(params, ("martin_range_bar_retest_v2", dt.date(2026, 10, 23)))

    def test_context_discovers_versioned_replay_books_and_attributes_closed_trade_by_category(self) -> None:
        repository = MartinForwardTestRepository(database_url="postgresql://unused")
        connection = MagicMock()
        execution_id = "martin_range_bar_retest_replay_20261019"
        classification_only_id = "martin_range_bar_retest_classification"
        pivot_id = "rs_leader_30m_pivot_replay_20261019"
        execution_runs = [{"id": 42, "session_date": dt.date(2026, 10, 23), "summary_json": {
            "execution_book_version": "martin_bar_retest_execution_v2", "cumulative_r": 1.5,
            "r_drawdown": 0.0, "realized_pnl": 750.0, "realized_r": 1.5, "unrealized_pnl": 0.0, "unrealized_r": 0.0}}]
        pivot_runs = [{"id": 43, "session_date": dt.date(2026, 10, 23), "summary_json": {
            "execution_book_version": "rs_leader_30m_pivot_execution_v1", "cumulative_r": 0.5,
            "r_drawdown": 0.0, "realized_pnl": 250.0, "realized_r": 0.5, "unrealized_pnl": 0.0, "unrealized_r": 0.0}}]
        closed_trade = {"id": 5, "strategy_id": execution_id, "ticker": "ABC", "status": "closed",
                        "closed_on": dt.date(2026, 10, 23), "payload_json": {"event_category": "gap_mover",
                        "realized_pnl": 750.0, "realized_r": 1.5, "exit_type": "trim_3r"}}
        pivot_trade = {"id": 6, "strategy_id": pivot_id, "ticker": "XYZ", "status": "closed",
                       "closed_on": dt.date(2026, 10, 23), "payload_json": {"event_category": "rs_leader_30m_pivot",
                       "realized_pnl": 250.0, "realized_r": 0.5, "exit_type": "trim_3r"}}
        statements = []

        def fetch(_cursor, sql, params=()):
            statements.append(sql)
            if "SELECT DISTINCT strategy_id" in sql:
                self.assertIn("position(%s in strategy_id)=1", sql)
                self.assertIn("execution_book_version", sql)
                self.assertNotIn("%", sql.replace("%s", ""))
                self.assertEqual(params, ("martin_range_bar_retest_", "martin_bar_retest_execution_v1", "martin_bar_retest_execution_v2", "martin_bar_retest_execution_v3", "martin_bar_retest_execution_v4", "martin_bar_retest_execution_v5",
                    "rs_leader_30m_pivot_replay_", "rs_leader_30m_pivot_execution_v1", "rs_leader_30m_pivot_execution_v2"))
                # Classification-only books remain omitted; the pivot requires its own version marker.
                return [{"strategy_id": execution_id}, {"strategy_id": pivot_id}]
            if "FROM martin_forward_runs" in sql and params:
                return {execution_id: execution_runs, pivot_id: pivot_runs}.get(params[0], [])
            if "FROM martin_forward_candidates" in sql:
                return []
            if "FROM martin_forward_positions" in sql:
                return [pivot_trade] if params == (pivot_id,) else [closed_trade]
            if "FROM martin_forward_events" in sql:
                return []
            return []

        with patch.object(repository, "_connect", return_value=connection), patch.object(repository, "_fetch", side_effect=fetch):
            result = repository.context()

        self.assertIn(execution_id, result["books"])
        self.assertIn(pivot_id, result["books"])
        self.assertNotIn(classification_only_id, result["books"])
        book = result["books"][execution_id]
        self.assertEqual(book["execution_book_version"], "martin_bar_retest_execution_v2")
        self.assertEqual(book["attribution"]["discovery_category"]["gap_mover"]["count"], 1)
        self.assertEqual(book["attribution"]["discovery_category"]["gap_mover"]["realized_pnl"], 750.0)
        pivot = result["books"][pivot_id]
        self.assertEqual(pivot["execution_book_version"], "rs_leader_30m_pivot_execution_v1")
        self.assertEqual(pivot["attribution"]["discovery_category"]["rs_leader_30m_pivot"]["count"], 1)

    def test_missing_baseline_does_not_shift_and_empty_or_invalid_history_is_unavailable(self) -> None:
        history = [{"date": "2026-10-01", "account_equity": 100_000}, {"date": "2026-10-02", "account_equity": 101_000}]
        prices = [{"ticker": "SPY", "trade_date": "2026-10-02", "close": 11}, {"ticker": "QQQ", "trade_date": "2026-10-02", "close": 21}]
        result = _benchmark_comparison(history, prices)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["baseline_date"], "2026-10-01")
        self.assertEqual(_benchmark_comparison([], []) ["status"], "unavailable")
        self.assertEqual(_benchmark_comparison([{"date": "2026-10-01", "account_equity": 0}], prices)["status"], "unavailable")


class MartinForwardTestRulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.row = {"ticker": "ABC", "scanner_ids": ["rs"], "scanners": [{"id": "rs"}], "market_cap": 2_000_000_000, "day_close": 20,
                    "daily_rs_rating": 95, "trade_readiness": "defined_risk", "adr14_pct": 4,
                    "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}

    def test_filters_and_top_hits_max_entry(self) -> None:
        item = classify_candidate(self.row, 1)
        self.assertEqual(item["status"], "pending")
        self.assertAlmostEqual(item["limit_price"], 19 / 0.95)
        self.assertEqual(item["risk_cap_pct"], 0.05)
        for adr in (2, 4, 12):
            self.assertAlmostEqual(classify_candidate({**self.row, "adr14_pct": adr}, 1)["limit_price"], item["limit_price"])
        lower_limit = {**self.row, "trade_plan": {**self.row["trade_plan"], "max_entry_price": 19.8}}
        self.assertEqual(classify_candidate(lower_limit, 1)["limit_price"], 19.8)
        self.assertEqual(classify_candidate({**self.row, "daily_rs_rating": 80}, 1)["status"], "blocked")
        self.assertEqual(classify_candidate({**self.row, "trade_readiness": "leader"}, 1)["status"], "watch")
        self.assertEqual(classify_candidate({**self.row, "trade_readiness": "avoid"}, 1)["status"], "avoid")
        self.assertEqual(classify_candidate({**self.row, "adr14_pct": None}, 1)["status"], "blocked_data")
        self.assertEqual(classify_candidate({**self.row, "scanner_ids": ["unrelated"], "scanners": []}, 1)["status"], "blocked")

    def test_aligned_limit_fills_persisted_joyy_and_gtlb_examples(self) -> None:
        for ticker, stop, maximum, opening in (
            ("JOYY", 80.300003, 84.52631894736842, 83.57),
            ("GTLB", 50.65978335808365, 53.32608774535122, 53.03),
        ):
            with self.subTest(ticker=ticker):
                row = {**self.row, "ticker": ticker, "adr14_pct": 2.02 if ticker == "JOYY" else 5.17,
                       "trade_plan": {"trigger_price": maximum, "stop_price": stop, "max_entry_price": maximum}}
                order = classify_candidate(row, 1)
                self.assertAlmostEqual(order["limit_price"], maximum)
                self.assertEqual(order_outcome(order, {"open": opening, "low": opening}), ("filled", opening, None))

    def test_sector_selector_prioritizes_quadrants_and_limits_alternates(self) -> None:
        rows = [
            {**self.row, "ticker": "WEAK", "sector": "Utilities", "sector_momentum": {"quadrant": "Weakening"}},
            {**self.row, "ticker": "L1", "sector": "Tech", "sector_momentum": {"quadrant": "Leading"}},
            {**self.row, "ticker": "L2", "sector": "Tech", "sector_momentum": {"quadrant": "Leading"}},
            {**self.row, "ticker": "I1", "sector": "Health", "sector_momentum": {"quadrant": "Improving"}},
            {**self.row, "ticker": "NOSECTOR", "sector": None},
        ]
        chosen = select_sector_candidates(rows)
        selected = [item for item in chosen if item.get("selection_tier")]
        self.assertEqual([item["ticker"] for item in selected], ["L1", "I1", "WEAK", "L2"])
        self.assertEqual([item["selection_tier"] for item in selected], ["Primary", "Primary", "Primary", "Alternate"])
        self.assertEqual(next(item for item in chosen if item["ticker"] == "NOSECTOR")["reasons"], ["missing_sector"])

    def test_sector_candidates_carry_snapshot_evidence(self) -> None:
        row = {**self.row, "sector": "Technology", "sector_momentum": {"quadrant": "Leading", "rs_ratio": 102,
              "momentum": 99, "as_of_date": "2026-01-02"}, "adr_suitability": "suitable", "stop_to_adr": 0.5,
              "strike_score": 83, "selection_metadata": {"source": "snapshot"}}
        item = select_sector_candidates([row])[0]
        self.assertEqual(item["sector_rs_ratio"], 102)
        self.assertEqual(item["sector_as_of_date"], "2026-01-02")
        self.assertEqual(item["adr_suitability"], "suitable")
        self.assertEqual(item["selection_metadata"]["source"], "snapshot")
        self.assertEqual(item["selection_metadata"]["selection_reason"], "primary_for_sector")

    def test_order_fill_cancel_expiry_and_missing_bar(self) -> None:
        order = {"stop": 19, "limit_price": 20}
        self.assertEqual(order_outcome(order, {"open": 19, "low": 18}), ("cancelled", None, "opened_at_or_below_stop"))
        self.assertEqual(order_outcome(order, {"open": 19.5, "low": 19}), ("filled", 19.5, None))
        self.assertEqual(order_outcome(order, {"open": 21, "low": 20}), ("filled", 20, None))
        self.assertEqual(order_outcome(order, {"open": 21, "low": 20.1})[0], "expired")
        self.assertEqual(order_outcome(order, {})[0], "pending")

    def test_stop_first_trim_and_ema9_next_open(self) -> None:
        position = {"entry_price": 20, "stop": 19, "initial_r": 1}
        self.assertEqual(position_exit(position, {"open": 20, "low": 18.9, "high": 23}), ("stop", 18.981))
        self.assertEqual(position_exit({**position, "stop": 18}, {"open": 20, "low": 19, "high": 23}), ("trim_3r", 23))
        self.assertEqual(position_exit({**position, "exit_next_open": True}, {"open": 22, "low": 21}), ("ema9_exit", 21.978))


class _ForwardRepo:
    def __init__(self):
        self.runs, self.candidates, self.positions, self.events = {}, [], [], {}
        self.next_id = 1

    def save_session(self, *, session_date, source_run_id, fingerprint, candidates, summary, strategy_id="martin_daily_v1"):
        _ = summary
        key = (strategy_id, session_date, source_run_id, fingerprint)
        if key not in self.runs:
            self.runs[key] = self.next_id
            run_id = self.next_id
            self.next_id += 1
            for item in candidates:
                self.candidates.append({"id": self.next_id, "run_id": run_id, "strategy_id": strategy_id, "ticker": item["ticker"], "rank": item["rank"],
                                        "status": item["status"], "payload_json": dict(item), "session_date": session_date})
                self.next_id += 1
        return self.runs[key]

    def pending_candidates(self, *, before, strategy_id="martin_daily_v1"):
        _ = strategy_id
        return [dict(row) for row in self.candidates if row["strategy_id"] == strategy_id and row["status"] == "pending" and row["session_date"] < before]

    def pending_orders(self, *, strategy_id="martin_daily_v1"):
        _ = strategy_id
        return [dict(ticker=row["ticker"], payload_json=row["payload_json"]) for row in self.candidates if row["strategy_id"] == strategy_id and row["status"] == "pending"]

    def submitted_order_count(self, *, session_date, strategy_id):
        return sum(row["strategy_id"] == strategy_id and row["session_date"] == session_date and "order_date" in row["payload_json"] for row in self.candidates)

    def daily_top5_selected_tickers(self, *, session_date, book_id=DAILY_TOP5_STRATEGY_ID):
        return [row["ticker"] for row in self.candidates if row["strategy_id"] == book_id and row["session_date"] == session_date and row["payload_json"].get("selected_for_book")]

    def open_positions(self, *, strategy_id="martin_daily_v1"):
        _ = strategy_id
        return [dict(row) for row in self.positions if row["strategy_id"] == strategy_id and row["status"] == "open"]

    def candidate_status(self, run_id, ticker):
        row = next((row for row in self.candidates if row["run_id"] == run_id and row["ticker"] == ticker), None)
        return row["status"] if row else None

    def candidate_payload(self, run_id, ticker):
        row = next((row for row in self.candidates if row["run_id"] == run_id and row["ticker"] == ticker), None)
        return row["payload_json"] if row else {}

    def update_candidate(self, run_id, ticker, payload):
        row = next(row for row in self.candidates if row["run_id"] == run_id and row["ticker"] == ticker)
        row["status"], row["payload_json"] = payload["status"], dict(payload)

    def has_pending_order(self, ticker, *, strategy_id="martin_daily_v1"):
        return any(row["ticker"] == ticker and row["strategy_id"] == strategy_id and row["status"] == "pending" for row in self.candidates)

    def reserve_position(self, *, ticker, opened_on, payload, strategy_id="martin_daily_v1"):
        if any(row["ticker"] == ticker and row["strategy_id"] == strategy_id and row["status"] == "open" for row in self.positions):
            return None
        position_id = self.next_id
        self.next_id += 1
        self.positions.append({"id": position_id, "strategy_id": strategy_id, "ticker": ticker, "opened_on": opened_on, "status": "open", "payload_json": dict(payload)})
        return position_id

    def fill_order(self, *, candidate_id, run_id, ticker, event_date, position_payload, event_payload, strategy_id="martin_daily_v1"):
        _ = run_id
        _ = strategy_id
        candidate = next(row for row in self.candidates if row["id"] == candidate_id)
        if candidate["status"] != "pending":
            return None
        position_id = self.reserve_position(ticker=ticker, opened_on=event_date, payload=position_payload, strategy_id=strategy_id)
        if position_id is None:
            candidate["status"] = "blocked"
            return None
        candidate["status"] = "filled"
        self.events[(ticker, "order_filled", event_date, position_id)] = dict(event_payload)
        return position_id

    def apply_event(self, *, run_id, position_id, ticker, event_type, event_date, payload, candidate_id=None,
                    candidate_status=None, position_payload=None, close_position=False, strategy_id="martin_daily_v1"):
        _ = run_id
        _ = strategy_id
        key = (ticker, event_type, event_date, position_id)
        if key in self.events:
            return False
        self.events[key] = dict(payload)
        if candidate_id is not None:
            row = next(item for item in self.candidates if item["id"] == candidate_id)
            row["status"] = candidate_status
        if position_id is not None and position_payload is not None:
            row = next(item for item in self.positions if item["id"] == position_id)
            row["payload_json"] = dict(position_payload)
            if close_position:
                row["status"] = "closed"
        return True

    def set_position_payload(self, position_id, payload):
        row = next(item for item in self.positions if item["id"] == position_id)
        row["payload_json"] = dict(payload)


class MartinForwardTestLifecycleTest(unittest.TestCase):
    def test_policy_uses_separate_replay_book_identity(self) -> None:
        repo = _ForwardRepo()
        day = dt.date(2026, 1, 2)
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day,
            snapshot={"target_trading_date": day.isoformat(), "_snapshot_run_id": 51,
                      "source_fingerprint": "fp", "guru_board": {"rows": []}},
            policy_id=DAILY_TOP5_STRATEGY_ID, book_id="martin_replay_top5_2026")
        self.assertEqual({row["strategy_id"] for row in repo.candidates}, set())
        self.assertEqual(repo.runs and next(iter(repo.runs))[0], "martin_replay_top5_2026")

    def test_daily_top5_is_five_orders_per_session_across_fingerprints(self) -> None:
        repo = _ForwardRepo()
        day = dt.date(2026, 1, 2)
        rows = [{**self._candidate_row(), "ticker": f"T{i:02}", "rank": i} for i in range(1, 8)]
        def snapshot(fingerprint, source_id, candidates):
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source_id, "source_fingerprint": fingerprint, "guru_board": {"rows": candidates}}
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day, snapshot=snapshot("fp1", 51, rows), strategy_id=DAILY_TOP5_STRATEGY_ID)
        for item in repo.candidates:
            if item["strategy_id"] == DAILY_TOP5_STRATEGY_ID and item["payload_json"].get("order_date"):
                item["status"] = "expired"
        service.run_session(as_of_date=day, snapshot=snapshot("fp2", 52, rows), strategy_id=DAILY_TOP5_STRATEGY_ID)
        submitted = [row for row in repo.candidates if row["strategy_id"] == DAILY_TOP5_STRATEGY_ID and row["payload_json"].get("order_date")]
        self.assertEqual(len(submitted), 5)
        self.assertTrue(all(row["payload_json"]["shares"] == 100 for row in submitted))
        self.assertEqual({row["ticker"] for row in submitted}, {"T01", "T02", "T03", "T04", "T05"})
        self.assertEqual(sum(row["status"] == "expired" for row in submitted), 5)
        self.assertTrue(all(row["payload_json"]["entry_policy_version"] for row in submitted))
        self.assertTrue(all(row["payload_json"]["selection_date"] == day.isoformat() for row in submitted))

    def test_daily_top5_selects_after_excluding_existing_holding_tickers(self) -> None:
        repo = _ForwardRepo()
        day = dt.date(2026, 1, 2)
        for ticker in ("T01", "T02", "T03", "T04", "T05"):
            repo.positions.append({"id": repo.next_id, "strategy_id": DAILY_TOP5_STRATEGY_ID, "ticker": ticker,
                                   "status": "open", "opened_on": day, "payload_json": {"entry_price": 20, "remaining_shares": 100}})
            repo.next_id += 1
        rows = [{**self._candidate_row(), "ticker": f"T{i:02}", "rank": i} for i in range(1, 8)]
        snapshot = {"target_trading_date": day.isoformat(), "_snapshot_run_id": 61, "source_fingerprint": "holdings",
                    "guru_board": {"rows": rows}}
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day, snapshot=snapshot, strategy_id=DAILY_TOP5_STRATEGY_ID)
        submitted = [row["ticker"] for row in repo.candidates if row["strategy_id"] == DAILY_TOP5_STRATEGY_ID and row["payload_json"].get("order_date")]
        self.assertEqual(submitted, ["T06", "T07"])
        self.assertEqual(next(row for row in repo.candidates if row["ticker"] == "T01")["status"], "blocked_open_position")

    def test_daily_top5_allows_more_than_five_concurrent_holdings(self) -> None:
        repo = _ForwardRepo()
        day1, day2, day3 = dt.date(2026, 1, 2), dt.date(2026, 1, 5), dt.date(2026, 1, 6)
        dates = pd.to_datetime([day1, day2, day3])
        frames = {f"T{i:02}": pd.DataFrame({"Open": [20, 20, 20], "High": [20.5, 20.5, 20.5],
                  "Low": [19.5, 19.5, 19.5], "Close": [20, 20, 20]}, index=dates) for i in range(1, 7)}
        def load(tickers, as_of, *_args, **_kwargs):
            return {ticker: frames[ticker].loc[frames[ticker].index.date <= as_of] for ticker in tickers}
        def snapshot(day, source, tickers):
            rows = [{**self._candidate_row(), "ticker": ticker} for ticker in tickers]
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source,
                    "source_fingerprint": f"fp{source}", "guru_board": {"rows": rows}}
        service = MartinForwardTestService(repo, bar_loader=load)
        service.run_session(as_of_date=day1, snapshot=snapshot(day1, 91, [f"T{i:02}" for i in range(1, 6)]), strategy_id=DAILY_TOP5_STRATEGY_ID)
        service.run_session(as_of_date=day2, snapshot=snapshot(day2, 92, ["T06"]), strategy_id=DAILY_TOP5_STRATEGY_ID)
        service.run_session(as_of_date=day3, snapshot=None, strategy_id=DAILY_TOP5_STRATEGY_ID)
        self.assertEqual(sum(row["status"] == "open" for row in repo.positions), 6)
        self.assertEqual(sum(row["strategy_id"] == DAILY_TOP5_STRATEGY_ID and row["payload_json"].get("order_date") == day1.isoformat() for row in repo.candidates), 5)
        self.assertEqual(sum(row["strategy_id"] == DAILY_TOP5_STRATEGY_ID and row["payload_json"].get("order_date") == day2.isoformat() for row in repo.candidates), 1)

    @staticmethod
    def _candidate_row():
        return {"scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
                "adr14_pct": 4, "trade_readiness": "defined_risk",
                "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}

    def test_session_reserves_then_fills_same_day_stops_and_retries_idempotently(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.date_range("2025-12-19", periods=12, freq="B")
        frame = pd.DataFrame({"Open": [20.0] * 11 + [19.3], "High": [20.5] * 11 + [23.0],
                              "Low": [19.5] * 11 + [18.8], "Close": [20.0] * 11 + [19.0]}, index=dates)
        def load_bars(tickers, as_of, days, **kwargs):
            _ = tickers, days, kwargs
            return {"ABC": frame.loc[frame.index.date <= as_of]}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snap1 = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 11, "source_fingerprint": "fp1", "guru_board": {"rows": [row]}}
        snap2 = {"target_trading_date": date2.isoformat(), "_snapshot_run_id": 12, "source_fingerprint": "fp2", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snap1)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertIsInstance(repo.candidates[0]["payload_json"]["shares"], int)
        self.assertLessEqual(repo.candidates[0]["payload_json"]["risk_dollars"], 500)
        self.assertLessEqual(repo.candidates[0]["payload_json"]["allocation_dollars"], 20_000)
        service.run_session(as_of_date=date2, snapshot=snap2)
        self.assertEqual(repo.candidates[0]["status"], "filled")
        self.assertEqual(repo.positions[0]["status"], "closed")
        self.assertEqual(repo.positions[0]["payload_json"]["entry_policy_version"], "top_hits_max_entry_5pct_v1")
        self.assertIn(("ABC", "stop", date2, repo.positions[0]["id"]), repo.events)
        event_count, position_count = len(repo.events), len(repo.positions)
        service.run_session(as_of_date=date2, snapshot=snap2)
        self.assertEqual(len(repo.events), event_count)
        self.assertEqual(len(repo.positions), position_count)

    def test_account_capacity_waits_and_exit_releases_slot_for_next_session_order(self) -> None:
        day = dt.date(2026, 1, 2)
        row = {"ticker": "CAND", **self._candidate_row()}
        snapshot = {"target_trading_date": day.isoformat(), "_snapshot_run_id": 71,
                    "source_fingerprint": "capacity", "guru_board": {"rows": [row]}}
        full_repo = _ForwardRepo()
        for index in range(5):
            full_repo.positions.append({"id": index + 1, "strategy_id": "martin_daily_v1", "ticker": f"OPEN{index}",
                "status": "open", "opened_on": day, "payload_json": {"entry_price": 20, "stop": 19,
                "initial_r": 1, "shares": 100, "remaining_shares": 100}})
        service = MartinForwardTestService(full_repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day, snapshot=snapshot)
        waiting = next(item for item in full_repo.candidates if item["ticker"] == "CAND")
        self.assertEqual(waiting["status"], "waiting_capacity")
        self.assertEqual(waiting["payload_json"]["queue_rank"], 1)
        self.assertTrue(waiting["payload_json"]["selected_for_book"])

        releasing_repo = _ForwardRepo()
        dates = pd.to_datetime([day])
        frames = {f"OPEN{index}": pd.DataFrame({"Open": [20], "High": [20.5], "Low": [18 if index == 0 else 19.5],
                    "Close": [19]}, index=dates) for index in range(5)}
        for index in range(5):
            releasing_repo.positions.append({"id": index + 1, "strategy_id": "martin_daily_v1", "ticker": f"OPEN{index}",
                "status": "open", "opened_on": day, "payload_json": {"entry_price": 20, "stop": 19,
                "initial_r": 1, "shares": 100, "remaining_shares": 100}})
        def load(tickers, *_args, **_kwargs):
            return {ticker: frames[ticker] for ticker in tickers if ticker in frames}
        service = MartinForwardTestService(releasing_repo, bar_loader=load)
        service.run_session(as_of_date=day, snapshot=snapshot)
        candidate = next(item for item in releasing_repo.candidates if item["ticker"] == "CAND")
        self.assertEqual(candidate["status"], "pending")
        self.assertTrue(candidate["payload_json"]["selected_for_book"])
        self.assertEqual(sum(item["status"] == "open" for item in releasing_repo.positions), 4)

    def test_missing_exact_bar_marks_pending_order_and_preserves_policy_levels(self) -> None:
        repo = _ForwardRepo()
        day1, day2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        row = {"ticker": "ABC", **self._candidate_row()}
        def snapshot(day, source):
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source,
                    "source_fingerprint": f"fp{source}", "guru_board": {"rows": [row]}}
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day1, snapshot=snapshot(day1, 81))
        order = repo.candidates[0]["payload_json"]
        limit, stop = order["limit_price"], order["stop"]
        service.run_session(as_of_date=day2, snapshot=None)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertEqual(repo.candidates[0]["payload_json"]["market_data_state"], "awaiting_data")
        self.assertEqual(repo.candidates[0]["payload_json"]["limit_price"], limit)
        self.assertEqual(repo.candidates[0]["payload_json"]["stop"], stop)
        self.assertEqual(repo.candidates[0]["payload_json"]["entry_policy_version"], "top_hits_max_entry_5pct_v1")

        later_day = dt.date(2026, 1, 6)
        later_frame = pd.DataFrame({"Open": [19.5], "High": [20], "Low": [19.2], "Close": [19.8]}, index=pd.to_datetime([later_day]))
        service.bar_loader = lambda *_args, **_kwargs: {"ABC": later_frame}
        service.run_session(as_of_date=later_day, snapshot=None)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertEqual(repo.candidates[0]["payload_json"]["awaiting_session_date"], day2.isoformat())
        self.assertEqual(repo.positions, [])

    def test_sector_book_isolated_and_uses_normalized_r_sizing(self) -> None:
        repo = _ForwardRepo()
        day = dt.date(2026, 1, 2)
        snapshot = {"target_trading_date": day.isoformat(), "_snapshot_run_id": 88, "source_fingerprint": "two-books",
                    "guru_board": {"rows": [{"ticker": "ABC", "sector": "Technology", "sector_momentum": {"quadrant": "Leading"},
                        "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95, "adr14_pct": 4,
                        "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}]}}
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        service.run_session(as_of_date=day, snapshot=snapshot)
        service.run_session(as_of_date=day, snapshot=snapshot, strategy_id=SECTOR_STRATEGY_ID)
        account_candidates = [row for row in repo.candidates if row["strategy_id"] == "martin_daily_v1"]
        sector = next(row for row in repo.candidates if row["strategy_id"] == SECTOR_STRATEGY_ID)
        self.assertEqual(len(account_candidates), 1)
        self.assertTrue(account_candidates[0]["payload_json"]["selected_for_book"])
        self.assertEqual(sector["payload_json"]["shares"], 100)
        self.assertEqual(sector["payload_json"]["sizing_mode"], "normalized_r")
        self.assertTrue(sector["payload_json"]["selected_for_book"])
        self.assertEqual(sector["payload_json"]["selection_tier"], "Primary")

    def test_position_trim_ema9_schedule_and_next_open_exit(self) -> None:
        repo = _ForwardRepo()
        ticker = "ABC"
        date1, date2, date3 = dt.date(2026, 1, 2), dt.date(2026, 1, 5), dt.date(2026, 1, 6)
        dates = pd.bdate_range("2025-12-19", "2026-01-06")
        frame = pd.DataFrame({"Open": [20.0] * len(dates), "High": [20.5] * len(dates),
                              "Low": [19.5] * len(dates), "Close": [20.0] * len(dates)}, index=dates)
        frame.loc[pd.Timestamp(date1), ["High", "Close"]] = [23.0, 20.0]
        frame.loc[pd.Timestamp(date2), ["Close"]] = 19.0
        frame.loc[pd.Timestamp(date3), ["Open", "High", "Low", "Close"]] = [18.5, 19.0, 18.0, 18.8]
        repo.positions.append({"id": 77, "strategy_id": "martin_daily_v1", "ticker": ticker, "status": "open", "opened_on": date1,
                               "payload_json": {"entry_price": 20.0, "stop": 19.0, "initial_r": 1.0,
                                                "shares": 100, "remaining_shares": 100, "trimmed": False,
                                                "exit_next_open": False}})
        def load_bars(tickers, as_of, days, **kwargs):
            _ = tickers, days, kwargs
            return {ticker: frame.loc[frame.index.date <= as_of]}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        def empty_snapshot(day, source_id):
            return {"target_trading_date": day.isoformat(), "_snapshot_run_id": source_id,
                    "source_fingerprint": f"fp{source_id}", "guru_board": {"rows": []}}
        service.run_session(as_of_date=date1, snapshot=empty_snapshot(date1, 21))
        self.assertEqual(repo.positions[0]["payload_json"]["remaining_shares"], 90)
        self.assertIn((ticker, "trim_3r", date1, 77), repo.events)
        service.run_session(as_of_date=date2, snapshot=empty_snapshot(date2, 22))
        self.assertTrue(repo.positions[0]["payload_json"]["exit_next_open"])
        self.assertIn((ticker, "ema9_exit_scheduled", date2, 77), repo.events)
        service.run_session(as_of_date=date3, snapshot=empty_snapshot(date3, 23))
        self.assertEqual(repo.positions[0]["status"], "closed")
        self.assertAlmostEqual(repo.positions[0]["payload_json"]["exit_price"], 18.5 * .999)
        self.assertEqual(repo.positions[0]["payload_json"].get("initial_shares", repo.positions[0]["payload_json"]["shares"]), 100)
        self.assertEqual(repo.positions[0]["payload_json"]["remaining_shares"], 0)
        self.assertAlmostEqual(repo.positions[0]["payload_json"]["realized_pnl"], 30 + (18.5 * .999 - 20) * 90)
        self.assertAlmostEqual(repo.positions[0]["payload_json"]["realized_r"], repo.positions[0]["payload_json"]["realized_pnl"] / 100)
        self.assertIn((ticker, "ema9_exit", date3, 77), repo.events)

    def test_missing_bar_leaves_pending_order_intact(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snap = lambda day, source: {"target_trading_date": day.isoformat(), "_snapshot_run_id": source,
                                    "source_fingerprint": f"fp{source}", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snap(date1, 31))
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual(repo.candidates[0]["status"], "pending")
        self.assertEqual(repo.positions, [])

    def test_next_bar_cancels_gap_below_stop_and_expires_unreached_limit(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.to_datetime([date1, date2])
        frames = {
            "AAA": pd.DataFrame({"Open": [20, 18], "High": [20.5, 19], "Low": [19.5, 17], "Close": [20, 18]}, index=dates),
            "BBB": pd.DataFrame({"Open": [20, 22], "High": [20.5, 23], "Low": [19.5, 20.1], "Close": [20, 22]}, index=dates),
        }
        def load_bars(tickers, as_of, days, **kwargs):
            _ = days, kwargs
            return {ticker: frames[ticker].loc[frames[ticker].index.date <= as_of] for ticker in tickers}
        service = MartinForwardTestService(repo, bar_loader=load_bars)
        row = lambda ticker: {"ticker": ticker, "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20,
                              "daily_rs_rating": 95, "adr14_pct": 4, "trade_readiness": "defined_risk",
                              "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snapshot = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 41,
                    "source_fingerprint": "gap-test", "guru_board": {"rows": [row("AAA"), row("BBB")]}}
        service.run_session(as_of_date=date1, snapshot=snapshot)
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual({item["ticker"]: item["status"] for item in repo.candidates}, {"AAA": "cancelled", "BBB": "expired"})
        self.assertIn(("AAA", "order_cancelled", date2, None), repo.events)
        self.assertIn(("BBB", "order_expired", date2, None), repo.events)

    def test_leveraged_pending_order_is_cancelled_before_bar_execution(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        repo.candidates.append({"id": 7, "run_id": 6, "strategy_id": "martin_daily_v1", "ticker": "MVLL",
            "rank": 1, "status": "pending", "session_date": date1,
            "payload_json": {"ticker": "MVLL", "limit_price": 10, "stop": 9}})
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {})
        result = service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual(repo.candidates[0]["status"], "cancelled")
        self.assertIn(("MVLL", "order_cancelled", date2, None), repo.events)
        self.assertEqual(repo.events[("MVLL", "order_cancelled", date2, None)]["reason"], "leveraged_or_inverse_fund")
        self.assertEqual(result["cancelled"], 1)

    def test_entry_day_three_r_does_not_trim(self) -> None:
        repo = _ForwardRepo()
        date1, date2 = dt.date(2026, 1, 2), dt.date(2026, 1, 5)
        dates = pd.to_datetime([date1, date2])
        frame = pd.DataFrame({"Open": [20, 19.3], "High": [20.5, 24], "Low": [19.5, 19.2], "Close": [20, 22]}, index=dates)
        service = MartinForwardTestService(repo, bar_loader=lambda *_args, **_kwargs: {"ABC": frame})
        row = {"ticker": "ABC", "scanner_ids": ["rs"], "market_cap": 2e9, "close": 20, "daily_rs_rating": 95,
               "adr14_pct": 4, "trade_readiness": "defined_risk", "trade_plan": {"trigger_price": 20, "stop_price": 19, "max_entry_price": 21}}
        snapshot = {"target_trading_date": date1.isoformat(), "_snapshot_run_id": 51,
                    "source_fingerprint": "no-entry-trim", "guru_board": {"rows": [row]}}
        service.run_session(as_of_date=date1, snapshot=snapshot)
        service.run_session(as_of_date=date2, snapshot=None)
        self.assertEqual(repo.positions[0]["payload_json"]["remaining_shares"], repo.positions[0]["payload_json"]["shares"])
        self.assertFalse(repo.positions[0]["payload_json"]["trimmed"])
        self.assertFalse(any(key[1] == "trim_3r" for key in repo.events))


    def test_server_performance_history_calculates_realized_drawdown(self) -> None:
        positions = [
            {"status": "closed", "closed_on": "2026-01-05", "payload_json": {"realized_r": 1.5, "realized_pnl": 750,
             "sector": "Technology", "sector_quadrant": "Leading", "selection_tier": "Primary", "adr_suitability": "suitable", "exit_type": "trim_3r"}},
            {"status": "closed", "closed_on": "2026-01-06", "payload_json": {"realized_r": -1, "realized_pnl": -500,
             "sector": "Utilities", "sector_quadrant": "Weakening", "selection_tier": "Primary", "adr_suitability": "caution", "exit_type": "stop"}},
        ]
        book = _performance_context("martin_daily_v1", [], [], positions, [])
        self.assertEqual(book["history"][-1]["cumulative_r"], 0.5)
        self.assertEqual(book["history"][-1]["account_equity"], 100_250)
        self.assertEqual(book["history"][-1]["r_drawdown"], -1)
        self.assertEqual(book["attribution"]["sector"]["Technology"]["count"], 1)

    def test_closed_trade_performance_metrics_use_trade_r(self) -> None:
        positions = [
            {"status": "closed", "closed_on": "2026-01-05", "payload_json": {"realized_r": 2, "realized_pnl": 1000, "holding_sessions": 4}},
            {"status": "closed", "closed_on": "2026-01-06", "payload_json": {"realized_r": -1, "realized_pnl": -500, "holding_sessions": 6}},
            {"status": "open", "payload_json": {"realized_r": 1, "realized_pnl": 500, "holding_sessions": 3}},
        ]
        metrics = _performance_context("martin_daily_v1", [], [], positions, [])["metrics"]
        self.assertEqual(metrics["expectancy_r"], 0.5)
        self.assertEqual(metrics["win_rate_pct"], 50)
        self.assertEqual(metrics["profit_factor_r"], 2)
        self.assertEqual(metrics["average_holding_sessions"], 5)
        self.assertEqual(metrics["realized_r"], 2)
        self.assertEqual(metrics["realized_pnl"], 1000)
        self.assertEqual(metrics["gross_losses"], 500)

    def test_closed_trade_metrics_ignore_open_partial_positions(self) -> None:
        metrics = _closed_trade_metrics([
            {"realized_r": 2, "holding_sessions": 4},
            {"realized_r": -1, "holding_sessions": 6},
        ])
        self.assertEqual(metrics["expectancy_r"], 0.5)
        self.assertEqual(metrics["win_rate_pct"], 50)
        self.assertEqual(metrics["profit_factor_r"], 2)
        self.assertEqual(metrics["average_holding_sessions"], 5)


if __name__ == "__main__":
    unittest.main()
