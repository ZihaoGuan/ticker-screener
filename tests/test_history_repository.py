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


def _ready_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for table_name, columns in HistoryRepository._REQUIRED_HISTORY_SCHEMA_COLUMNS.items():
        for column_name in columns:
            rows.append((table_name, column_name))
    return rows


class HistoryRepositoryEnsureSchemaTests(unittest.TestCase):
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
