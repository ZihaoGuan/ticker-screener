import datetime as dt
from contextlib import nullcontext
import sys
import types
import unittest
from unittest.mock import patch

from scripts import replay_martin_bar_retest_range as replay
from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository


class FakeRepo:
    def __init__(self):
        self.calls = []
        self.states = {}
        self.saved = []
        self.latest = None

    def latest_gap_watch_states(self, book):
        self.calls.append(("states", book))
        return self.states

    def save_session(self, **kwargs):
        self.saved.append(kwargs)
        self.latest = kwargs["session_date"]
        self.states = {item["ticker"]: item for item in kwargs["candidates"]}

    def latest_session_date(self, *_): return self.latest
    def session_run_id(self, *_): return None
    def atomic_session(self, *_): return nullcontext()


class Cursor:
    def __init__(self, connection): self.connection = connection
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, *_args): pass
    def fetchone(self): return (True,)


class Connection:
    def __init__(self): self.commits = 0; self.rollbacks = 0
    def cursor(self): return Cursor(self)
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1


class SaveCursor:
    def __init__(self):
        self.calls = []
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, sql, params=()): self.calls.append((sql, params))
    def fetchall(self): return []
    def fetchone(self): return (1,)


class SaveConnection:
    def __init__(self): self.db_cursor = SaveCursor()
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def cursor(self): return self.db_cursor


class MartinBarRetestRangeTests(unittest.TestCase):
    def test_service_repository_save_serializes_datetime_fields_as_iso8601(self):
        day = dt.date(2026, 10, 19)
        published_at = dt.datetime(2026, 10, 20, 13, 30, tzinfo=dt.timezone.utc)
        connection = SaveConnection()
        class Repo(MartinForwardTestRepository):
            def _connect(self): return connection
            def session_run_id(self, *_args): return None
            def latest_session_date(self, *_args): return None
            def latest_gap_watch_states(self, *_args): return {}
        repo = Repo()
        source = {"target_trading_date": day.isoformat(), "rules_version": replay.SOURCE_ID,
                  "source_run_id": 7110, "published_at": published_at,
                  "replay_provenance": {"publication": published_at},
                  "hits": [{"ticker": "ABC", "discovery_categories": ["leader_30_session"]}]}

        class Calendar:
            def next_session(self, _day): return dt.datetime(2026, 10, 20)
            def session_open(self, _day): return dt.datetime(2026, 10, 20, 13, 30, tzinfo=dt.timezone.utc)
        fake_exchange_calendars = types.SimpleNamespace(get_calendar=lambda *_a, **_k: Calendar())

        with patch("src.webapp.services.martin_bar_retest_service.session_dates", lambda *_: [day - dt.timedelta(days=1), day]), \
                patch("src.webapp.services.martin_bar_retest_service.require_completed_session", lambda *_: None), \
                patch.dict(sys.modules, {"exchange_calendars": fake_exchange_calendars}):
            MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {}).run_session(
                as_of_date=day, source_record=source, dry_run=False)

        run_params = next(params for sql, params in connection.db_cursor.calls if "INSERT INTO martin_forward_runs" in sql)
        candidate_params = next(params for sql, params in connection.db_cursor.calls if "INSERT INTO martin_forward_candidates" in sql)
        self.assertIn('"publication": "2026-10-20T13:30:00+00:00"', run_params[4])
        self.assertIn('2026-10-20T13:30:00+00:00', candidate_params[5])

    def test_service_publication_timing_uses_next_session_at_calendar_boundary(self):
        day = dt.date(2026, 10, 19)

        class Calendar:
            def next_session(self, _day):
                import pandas as pd
                return pd.Timestamp("2026-10-20")

            def session_open(self, _day):
                import pandas as pd
                return pd.Timestamp("2026-10-20T13:30:00Z")

            def sessions_in_range(self, *_args):
                raise AssertionError("service should not query through the calendar end date")

        fake_exchange_calendars = types.SimpleNamespace(get_calendar=lambda *_args, **_kwargs: Calendar())
        source = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1",
                  "published_at": "2026-10-19T12:00:00Z", "hits": []}
        with patch.dict(sys.modules, {"exchange_calendars": fake_exchange_calendars}), \
                patch("src.webapp.services.martin_bar_retest_service.session_dates", lambda *_: [day]), \
                patch("src.webapp.services.martin_bar_retest_service.require_completed_session", lambda *_: None):
            result = MartinBarRetestService(FakeRepo(), bar_loader=lambda *_a, **_k: {}).run_session(
                as_of_date=day, source_record=source)

        self.assertFalse(result["retrospective_source_scan"])

    def test_publication_timing_handles_weekend_after_calendar_end(self):
        class Calendar:
            def next_session(self, day):
                session = dt.date.fromisoformat(day)
                return dt.datetime.combine(session + dt.timedelta(days=3 if session.weekday() == 4 else 1),
                                           dt.time())

            def session_open(self, day):
                import pandas as pd
                return pd.Timestamp(f"{day}T13:30:00Z")

            def sessions_in_range(self, _start, end):
                if dt.date.fromisoformat(end).weekday() >= 5:
                    raise AssertionError("range end falls on a weekend outside the calendar")
                raise AssertionError("publication timing should use next_session directly")

        fake_exchange_calendars = types.SimpleNamespace(get_calendar=lambda *_args, **_kwargs: Calendar())
        published_at = "2026-10-25T00:00:00Z"
        with patch.dict(sys.modules, {"exchange_calendars": fake_exchange_calendars}):
            results = [replay.publication_timing(day, published_at)
                       for day in replay.EXPECTED_SOURCES]

        self.assertEqual([retrospective for retrospective, _ in results],
                         [True, True, True, True, False])
        self.assertEqual(results[-1][1], "saved_before_next_open")

    def test_execution_only_run_keeps_next_session_sequential(self):
        day26, day27 = dt.date(2026, 10, 26), dt.date(2026, 10, 27)
        repo = FakeRepo()
        replay.save_execution_only_run(repo, book_id="martin_range_bar_retest_seq", day=day26)
        source = {"target_trading_date": day27.isoformat(), "rules_version": replay.SOURCE_ID,
                  "source_run_id": 8001, "hits": []}
        with patch("src.webapp.services.martin_bar_retest_service.session_dates",
                   lambda day, *_: [day26, day27]), \
                patch("src.webapp.services.martin_bar_retest_service.require_completed_session", lambda *_: None):
            result = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {}).run_session(
                as_of_date=day27, source_record=source, dry_run=False,
                book_id="martin_range_bar_retest_seq")
        self.assertEqual(repo.latest, day27)
        self.assertEqual(result["session_date"], day27.isoformat())

    def test_service_separates_policy_from_book_and_reads_prior_from_book(self):
        day = dt.date(2026, 10, 23)
        repo = FakeRepo()
        repo.states = {"ABC": {"ticker": "ABC", "event_identity": {"event_date": "2026-10-20"},
                                "event_date": "2026-10-20", "category": "leader_30_session", "status": "watch"}}
        source = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1", "hits": []}
        with patch("src.webapp.services.martin_bar_retest_service.session_dates", lambda *_: [day]), \
                patch("src.webapp.services.martin_bar_retest_service.require_completed_session", lambda *_: None):
            result = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {}).run_session(
                as_of_date=day, source_record=source, dry_run=False, policy_id="martin_bar_retest_v1",
                book_id="martin_range_bar_retest_test")
        self.assertEqual(result["strategy_id"], "martin_bar_retest_v1")
        self.assertEqual(repo.calls, [("states", "martin_range_bar_retest_test")])
        self.assertEqual(repo.saved[0]["strategy_id"], "martin_range_bar_retest_test")
        self.assertEqual(repo.saved[0]["summary"]["rule_version"], "martin_bar_retest_v1")
        with self.assertRaisesRegex(ValueError, "Unsupported Martin Bar Retest policy"):
            MartinBarRetestService(repo).run_session(as_of_date=day, source_record=source,
                                                      policy_id="arbitrary_policy", book_id="range")

    def test_saved_sources_require_exact_ids_and_classify_publication_timing(self):
        dates = list(replay.EXPECTED_SOURCES)
        sources = {day: {"source_run_id": rid, "hit_count": count, "hits": [{}] * count,
                         "strategy_id": replay.SOURCE_ID, "rules_version": replay.SOURCE_ID,
                         "created_at": "2026-10-25T00:00:00Z", "source_fingerprint": f"fp-{rid}"}
                   for day, (rid, count) in replay.EXPECTED_SOURCES.items()}
        with patch("scripts.run_martin_bar_retest._load_source", lambda _history, day: sources.get(day)), \
                patch.object(replay, "publication_timing", side_effect=lambda day, _published:
                             (day != dates[-1], "published_after_next_open" if day != dates[-1] else "saved_before_next_open")):
            result = replay.load_range_sources(object(), dates)
            self.assertEqual(list(result), dates)
            self.assertFalse(result[dates[-1]]["replay_provenance"]["retrospective"])
            self.assertEqual(result[dates[-1]]["replay_provenance"]["source_mode"], "saved_before_next_open")
            del sources[dates[2]]
            with self.assertRaisesRegex(ValueError, "Missing persisted source"):
                replay.load_range_sources(object(), dates)

    def test_actual_service_carries_watch_through_zero_hit_session(self):
        days = list(replay.EXPECTED_SOURCES)
        repo = FakeRepo()
        sessions_to_day = {day: days[:index + 1] for index, day in enumerate(days)}
        sources = {day: {"target_trading_date": day.isoformat(), "rules_version": replay.SOURCE_ID,
                         "source_run_id": replay.EXPECTED_SOURCES[day][0],
                         "hits": ([{"ticker": "ABC", "discovery_categories": ["leader_30_session"]}]
                                  if index == 0 else [])}
                   for index, day in enumerate(days)}
        with patch("src.webapp.services.martin_bar_retest_service.session_dates",
                   lambda day, *_: sessions_to_day[day]), \
                patch("src.webapp.services.martin_bar_retest_service.require_completed_session", lambda *_: None):
            service = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {})
            results = [service.run_session(as_of_date=day, source_record=sources[day], dry_run=False,
                        policy_id=replay.POLICY_ID, book_id="martin_range_bar_retest_carry") for day in days]
        self.assertTrue(results[1]["zero_hit_day"])
        self.assertEqual(results[1]["active_event_count"], 1)
        self.assertEqual(results[1]["candidates"][0]["event_date"], days[0].isoformat())
        self.assertEqual(len(repo.saved), 5)
        self.assertTrue(all(record["strategy_id"] == "martin_range_bar_retest_carry" for record in repo.saved))

    def test_dry_run_rolls_back_and_failure_rolls_back_all(self):
        days = list(replay.EXPECTED_SOURCES)
        sources = {day: {"source_run_id": replay.EXPECTED_SOURCES[day][0]} for day in days}
        connection = Connection()
        with patch.object(replay, "check_empty_book", lambda *_: None), \
                patch.object(replay, "run_range", lambda *_a, **_k: [{"date": d.isoformat()} for d in days]):
            replay.execute_range(connection, sessions=days, sources=sources, book_id="martin_range_bar_retest_t",
                                 database_url="test", apply=False)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 1))

        def fail(*_args, **_kwargs): raise ValueError("session failed")
        with patch.object(replay, "check_empty_book", lambda *_: None), patch.object(replay, "run_range", fail):
            with self.assertRaisesRegex(ValueError, "session failed"):
                replay.execute_range(connection, sessions=days, sources=sources, book_id="martin_range_bar_retest_t",
                                     database_url="test", apply=True)
        self.assertEqual((connection.commits, connection.rollbacks), (0, 2))

    def test_existing_book_refusal_and_range_identity_validation(self):
        class ExistingConnection:
            def __init__(self): self.tables = []
            def cursor(self): return self
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, _params): self.tables.append(sql.split("FROM ")[1].split()[0])
            def fetchone(self): return (1 if len(self.tables) == 2 else 0,)

        connection = ExistingConnection()
        with self.assertRaisesRegex(ValueError, "already has history"):
            replay.check_empty_book(connection, "martin_range_bar_retest_test")
        self.assertEqual(connection.tables, ["martin_forward_runs", "martin_forward_positions"])
        with self.assertRaises(SystemExit):
            replay.main(["--from-date", "2026-10-20", "--to-date", "2026-10-23", "--book-id", "martin_range_bar_retest_x"])
        with self.assertRaises(SystemExit):
            replay.main(["--from-date", "2026-10-19", "--to-date", "2026-10-23", "--book-id", "martin_bar_retest_v1"])


if __name__ == "__main__":
    unittest.main()
