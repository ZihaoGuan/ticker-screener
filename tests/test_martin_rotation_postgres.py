"""Read-only integration checks against an existing application test database."""
from __future__ import annotations

import os
import unittest

from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository, _SessionConnection
from src.webapp.services.martin_rotation import ROTATION_STRATEGY_ID


@unittest.skipUnless(os.environ.get("TEST_POSTGRES_DATABASE_URL"), "Set TEST_POSTGRES_DATABASE_URL for real SQL checks")
class RotationPostgresTests(unittest.TestCase):
    def test_latest_session_and_rotation_exit_dates(self):
        import psycopg

        with psycopg.connect(os.environ["TEST_POSTGRES_DATABASE_URL"]) as connection:
            connection.execute("SET TRANSACTION READ ONLY")
            repository = MartinForwardTestRepository(database_url=os.environ["TEST_POSTGRES_DATABASE_URL"])
            # Borrow this read-only transaction, avoiding schema setup or writes.
            repository._session_connection = _SessionConnection(connection)
            cases = [
                (repository.latest_session_date, "SELECT max(session_date) FROM martin_forward_runs WHERE strategy_id=%s"),
                (repository.last_rotation_exit, "SELECT max(event_date) FROM martin_forward_events WHERE strategy_id=%s AND event_type='rotation_exit'"),
            ]
            for method, query in cases:
                with self.subTest(method=method.__name__), connection.transaction():
                    expected = connection.execute(query, (ROTATION_STRATEGY_ID,)).fetchone()[0]
                    self.assertEqual(method(ROTATION_STRATEGY_ID), expected)


if __name__ == "__main__":
    unittest.main()
