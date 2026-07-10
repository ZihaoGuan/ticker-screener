from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.webapp.repositories.my_picks_repository import MyPicksRepository


class _FakeCursor:
    description = [("id",), ("ticker",)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str) -> None:
        self.last_sql = sql

    def fetchall(self):
        return [(1, "NVDA")]


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _FakeCursor()


class MyPicksRepositoryTests(unittest.TestCase):
    def test_connect_can_skip_schema_bootstrap(self) -> None:
        repository = MyPicksRepository(database_url="postgres://example")
        repository.ensure_schema = MagicMock()

        with patch.dict("sys.modules", {"psycopg": SimpleNamespace(connect=MagicMock(return_value="connection"))}):
            connection = repository._connect(ensure_schema=False)

        repository.ensure_schema.assert_not_called()
        self.assertEqual(connection, "connection")

    def test_list_picks_uses_read_only_connect_without_schema_bootstrap(self) -> None:
        repository = MyPicksRepository(database_url="postgres://example")

        with patch.object(repository, "_connect", return_value=_FakeConnection()) as mock_connect:
            rows = repository.list_picks()

        mock_connect.assert_called_once_with(ensure_schema=False)
        self.assertEqual(rows[0]["ticker"], "NVDA")


if __name__ == "__main__":
    unittest.main()
