from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch

from src.webapp.repositories.history_repository import HistoryRepository


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection") -> None:
        self.connection = connection
        self._rows: list[tuple[object, ...]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        normalized_sql = " ".join(sql.split())
        self.connection.executed_sql.append((normalized_sql, params))
        if "FROM information_schema.columns" in normalized_sql:
            self._rows = list(self.connection.schema_checks.pop(0))
            return
        if "SELECT pg_advisory_lock" in normalized_sql:
            self.connection.lock_calls += 1
            self._rows = []
            return
        if "SELECT pg_advisory_unlock" in normalized_sql:
            self.connection.unlock_calls += 1
            self._rows = []
            return
        if normalized_sql.startswith("CREATE INDEX IF NOT EXISTS"):
            self._rows = []
            return
        if self.connection.raise_on_schema_apply:
            raise RuntimeError("schema apply failed")
        self.connection.schema_apply_calls += 1
        self._rows = []

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _FakeTransaction:
    def __enter__(self) -> "_FakeTransaction":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConnection:
    def __init__(
        self,
        *,
        schema_checks: list[list[tuple[object, ...]]],
        raise_on_schema_apply: bool = False,
    ) -> None:
        self.schema_checks = list(schema_checks)
        self.raise_on_schema_apply = raise_on_schema_apply
        self.autocommit = False
        self.executed_sql: list[tuple[str, tuple[object, ...] | None]] = []
        self.lock_calls = 0
        self.unlock_calls = 0
        self.schema_apply_calls = 0

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    def commit(self) -> None:
        return None


def _ready_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for table_name, columns in HistoryRepository._REQUIRED_HISTORY_SCHEMA_COLUMNS.items():
        for column_name in columns:
            rows.append((table_name, column_name))
    return rows


class HistoryRepositoryEnsureSchemaTests(unittest.TestCase):
    def test_list_scheduled_job_runs_includes_remote_and_github_only(self) -> None:
        connection = _FakeConnection(schema_checks=[])
        repository = HistoryRepository(database_url="postgres://example")
        repository._connect = lambda: connection  # type: ignore[method-assign]

        with patch.object(repository, "_rows_to_dicts", return_value=[]):
            repository.list_scheduled_job_runs(limit=42)

        sql, params = connection.executed_sql[0]
        self.assertIn("IN ('remote', 'github')", sql)
        self.assertIn("scheduled_job_id", sql)
        self.assertEqual(params, (42,))

    def test_ensure_schema_skips_full_schema_apply_when_history_schema_exists(self) -> None:
        connection = _FakeConnection(schema_checks=[_ready_rows()])
        repository = HistoryRepository(database_url="postgres://example")
        fake_psycopg = types.SimpleNamespace(connect=lambda _: connection)

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            repository.ensure_schema()

        self.assertTrue(repository._schema_ready)
        self.assertEqual(connection.lock_calls, 0)
        self.assertEqual(connection.unlock_calls, 0)
        self.assertEqual(connection.schema_apply_calls, 0)

    def test_find_screen_run_matches_dated_artifact_stem(self) -> None:
        connection = _FakeConnection(schema_checks=[])
        repository = HistoryRepository(database_url="postgres://example")
        repository._connect = lambda: connection  # type: ignore[method-assign]

        with patch.object(repository, "_rows_to_dicts", return_value=[{"id": 5664}]):
            payload = repository.find_screen_run_by_watchlist_stem(
                "ema9_20_crossover_targets_2026-10-11"
            )

        self.assertEqual(payload, {"id": 5664})
        sql, params = connection.executed_sql[0]
        self.assertIn("strategy_id = %s AND run_date = %s", sql)
        self.assertIn("ema9_20_crossover_targets", params or ())
        self.assertIn("2026-10-11", params or ())

    def test_claim_remote_job_applies_priority_dependencies_pool_and_global_limit(self) -> None:
        connection = _FakeConnection(schema_checks=[])
        repository = HistoryRepository(database_url="postgres://example")
        repository._connect = lambda: connection  # type: ignore[method-assign]

        with patch.object(repository, "_rows_to_dicts", return_value=[]):
            repository.claim_remote_job_run(
                worker_name="worker-a",
                code_version="image-a",
                worker_pools=("general",),
            )

        sql, params = connection.executed_sql[0]
        self.assertIn("pg_advisory_xact_lock", sql)
        self.assertIn("jsonb_array_elements_text", sql)
        self.assertIn("dependency_job_ids", sql)
        self.assertIn("parallel_limit", sql)
        self.assertIn("queue_priority", sql)
        self.assertIn("worker_pool", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params[1:3], ("worker-a", "image-a"))
        self.assertEqual(params[3:5], (["general"], ["general"]))

    def test_claim_scheduled_github_job_requires_successful_dependencies_and_no_prior_dispatch(self) -> None:
        connection = _FakeConnection(schema_checks=[])
        repository = HistoryRepository(database_url="postgres://example")
        repository._connect = lambda: connection  # type: ignore[method-assign]

        with patch.object(repository, "_rows_to_dicts", return_value=[]):
            repository.claim_ready_scheduled_github_job_run()

        sql, params = connection.executed_sql[0]
        self.assertIn("execution_mode', '') = 'github'", sql)
        self.assertIn("scheduled_job_id", sql)
        self.assertIn("github_run_id", sql)
        self.assertIn("latest_provider.status = 'success'", sql)
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertEqual(params[0], repository._REMOTE_QUEUE_CLAIM_ADVISORY_LOCK_KEY)

    def test_ensure_schema_unlocks_after_schema_apply_failure(self) -> None:
        connection = _FakeConnection(schema_checks=[[], []], raise_on_schema_apply=True)
        repository = HistoryRepository(database_url="postgres://example")
        fake_psycopg = types.SimpleNamespace(connect=lambda _: connection)

        with patch.dict(sys.modules, {"psycopg": fake_psycopg}):
            with self.assertRaisesRegex(RuntimeError, "schema apply failed"):
                repository.ensure_schema()

        self.assertFalse(repository._schema_ready)
        self.assertEqual(connection.lock_calls, 1)
        self.assertEqual(connection.unlock_calls, 1)
        self.assertEqual(connection.schema_apply_calls, 0)


if __name__ == "__main__":
    unittest.main()
