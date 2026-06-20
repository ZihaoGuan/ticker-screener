from __future__ import annotations

import datetime as dt
import unittest

from src.ratings.repository import RatingsRepository


class _FakeCursor:
    def __init__(self, scripted_results: list[dict[str, object]]) -> None:
        self.scripted_results = list(scripted_results)
        self.current_result: dict[str, object] | None = None
        self.executed_sql: list[str] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        del params
        if not self.scripted_results:
            raise AssertionError("No scripted result left for execute call.")
        self.executed_sql.append(sql)
        self.current_result = self.scripted_results.pop(0)

    def fetchone(self):
        if self.current_result is None:
            raise AssertionError("fetchone called before execute.")
        return self.current_result.get("fetchone")

    def fetchall(self):
        if self.current_result is None:
            raise AssertionError("fetchall called before execute.")
        return self.current_result.get("fetchall", [])


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> _FakeConnection:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


class RatingsRepositoryRankChangeTests(unittest.TestCase):
    def test_list_top_rating_snapshots_adds_rank_change_fields(self) -> None:
        cursor = _FakeCursor(
            [
                {"fetchone": (dt.date(2026, 6, 13),)},
                {"fetchall": [("Technology",)]},
                {"fetchone": (dt.date(2026, 6, 6),)},
                {"fetchall": [("ok", 3), ("missing_metrics", 1)]},
                {
                    "fetchall": [
                        ("NVDA", dt.date(2026, 6, 13), "Technology", "Semiconductors", 98.2, 15.0, 16.0, 17.0, 18.0, "A", "A", "A", "A", "ok", None, 1),
                        ("MSFT", dt.date(2026, 6, 13), "Technology", "Software", 96.4, 14.0, 15.0, 16.0, 17.0, "A", "A", "A", "B", "ok", None, 2),
                        ("PLTR", dt.date(2026, 6, 13), "Technology", "Software", 93.0, 12.0, 13.0, 14.0, 15.0, "B", "A", "B", "B", "ok", None, 3),
                    ]
                },
                {"fetchall": [("MSFT", 1), ("NVDA", 2), ("AMD", 3)]},
            ]
        )
        repository = RatingsRepository()
        repository._connect = lambda: _FakeConnection(cursor)

        payload = repository.list_top_rating_snapshots(as_of_date=dt.date(2026, 6, 13), limit=25, rating_status="ok")

        self.assertEqual(payload["as_of_date"], "2026-06-13")
        self.assertEqual(payload["previous_as_of_date"], "2026-06-06")
        self.assertEqual(payload["status_counts"], {"missing_metrics": 1, "ok": 3})
        self.assertEqual(payload["sector_options"], ["Technology"])
        self.assertEqual(payload["rows"][0]["current_rank"], 1)
        self.assertEqual(payload["rows"][0]["previous_rank"], 2)
        self.assertEqual(payload["rows"][0]["rank_change"], "up")
        self.assertEqual(payload["rows"][0]["rank_delta"], 1)
        self.assertEqual(payload["rows"][1]["rank_change"], "down")
        self.assertEqual(payload["rows"][1]["rank_delta"], -1)
        self.assertEqual(payload["rows"][2]["rank_change"], "new")
        self.assertIsNone(payload["rows"][2]["previous_rank"])
        self.assertIsNone(payload["rows"][2]["rank_delta"])

    def test_load_latest_ticker_rating_bundle_includes_top_200_fa_rank(self) -> None:
        cursor = _FakeCursor(
            [
                {
                    "fetchone": (
                        dt.date(2026, 6, 13),
                        "NVDA",
                        "Technology",
                        "Semiconductors",
                        100.0,
                        120.0,
                        25.0,
                        1.8,
                        20.0,
                        15.0,
                        30.0,
                        22.0,
                        33.0,
                        70.0,
                        11.0,
                        18.0,
                        35.0,
                        28.0,
                        22.0,
                        44.0,
                        50.0,
                        8.0,
                        14.0,
                        21.0,
                        55.0,
                        33.0,
                        3.2,
                        4.1,
                        "finviz",
                        "ok",
                        None,
                        15.0,
                        16.0,
                        17.0,
                        18.0,
                        98.2,
                        "A",
                        "A",
                        "A",
                        "A",
                        "ok",
                        None,
                        [],
                        [],
                    )
                },
                {"fetchone": (17,)},
            ]
        )
        repository = RatingsRepository()
        repository._connect = lambda: _FakeConnection(cursor)

        payload = repository.load_latest_ticker_rating_bundle("NVDA")

        assert payload is not None
        self.assertEqual(payload["fundamental_rank"]["current_rank"], 17)
        self.assertEqual(payload["fundamental_rank"]["list_limit"], 200)

    def test_list_top_rating_snapshots_supports_sector_filter(self) -> None:
        cursor = _FakeCursor(
            [
                {"fetchone": (dt.date(2026, 6, 13),)},
                {"fetchall": [("Technology",)]},
                {"fetchone": (dt.date(2026, 6, 6),)},
                {"fetchall": [("ok", 2)]},
                {
                    "fetchall": [
                        ("NVDA", dt.date(2026, 6, 13), "Technology", "Semiconductors", 98.2, 15.0, 16.0, 17.0, 18.0, "A", "A", "A", "A", "ok", None, 1),
                        ("MSFT", dt.date(2026, 6, 13), "Technology", "Software", 96.4, 14.0, 15.0, 16.0, 17.0, "A", "A", "A", "B", "ok", None, 2),
                    ]
                },
                {"fetchall": [("NVDA", 2), ("MSFT", 1)]},
            ]
        )
        repository = RatingsRepository()
        repository._connect = lambda: _FakeConnection(cursor)

        payload = repository.list_top_rating_snapshots(
            as_of_date=dt.date(2026, 6, 13),
            limit=25,
            rating_status="ok",
            sector="Technology",
        )

        self.assertEqual([row["ticker"] for row in payload["rows"]], ["NVDA", "MSFT"])
        self.assertEqual(payload["status_counts"], {"ok": 2})
        self.assertEqual(payload["sector_options"], ["Technology"])

    def test_list_top_rating_snapshots_qualifies_previous_rank_columns(self) -> None:
        cursor = _FakeCursor(
            [
                {"fetchone": (dt.date(2026, 6, 13),)},
                {"fetchall": [("Technology",)]},
                {"fetchone": (dt.date(2026, 6, 6),)},
                {"fetchall": [("ok", 1)]},
                {
                    "fetchall": [
                        ("NVDA", dt.date(2026, 6, 13), "Technology", "Semiconductors", 98.2, 15.0, 16.0, 17.0, 18.0, "A", "A", "A", "A", "ok", None, 1),
                    ]
                },
                {"fetchall": [("NVDA", 2)]},
            ]
        )
        repository = RatingsRepository()
        repository._connect = lambda: _FakeConnection(cursor)

        repository.list_top_rating_snapshots(as_of_date=dt.date(2026, 6, 13), limit=25, rating_status="ok")

        previous_rank_sql = cursor.executed_sql[-1]
        self.assertIn("SELECT\n                            r.ticker,\n                            r.overall_rating", previous_rank_sql)
        self.assertIn("WHERE r.as_of_date = %s", previous_rank_sql)
        self.assertIn("COALESCE(r.rating_status, '')", previous_rank_sql)

    def test_list_top_technical_rating_snapshots_adds_rank_change_fields(self) -> None:
        cursor = _FakeCursor(
            [
                {"fetchone": (dt.date(2026, 6, 13),)},
                {"fetchall": [("Communication Services",), ("Consumer Cyclical",)]},
                {"fetchone": (dt.date(2026, 6, 6),)},
                {"fetchall": [("ok", 2)]},
                {
                    "fetchall": [
                        ("TSLA", dt.date(2026, 6, 13), "Consumer Cyclical", "Auto Manufacturers", 91.5, 18.0, 17.0, 16.0, 15.0, 14.0, "Leading", "ok", None, ["tight"], 1),
                        ("META", dt.date(2026, 6, 13), "Communication Services", "Internet Content", 90.4, 18.0, 17.0, 16.0, 15.0, 14.0, "Leading", "ok", None, [], 2),
                    ]
                },
                {"fetchall": [("TSLA", 1), ("META", 2)]},
            ]
        )
        repository = RatingsRepository()
        repository._connect = lambda: _FakeConnection(cursor)

        payload = repository.list_top_technical_rating_snapshots(as_of_date=dt.date(2026, 6, 13), limit=25, technical_status="ok")

        self.assertEqual(payload["as_of_date"], "2026-06-13")
        self.assertEqual(payload["previous_as_of_date"], "2026-06-06")
        self.assertEqual(payload["sector_options"], ["Communication Services", "Consumer Cyclical"])
        self.assertEqual(payload["rows"][0]["rank_change"], "same")
        self.assertEqual(payload["rows"][0]["rank_delta"], 0)
        self.assertEqual(payload["rows"][1]["rank_change"], "same")
        self.assertEqual(payload["rows"][1]["flags"], [])


if __name__ == "__main__":
    unittest.main()
