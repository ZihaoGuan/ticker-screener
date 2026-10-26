import datetime as dt
from contextlib import nullcontext
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import subprocess
import types
import unittest
from unittest.mock import patch

from scripts import replay_martin_bar_retest_range as replay
from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository

SOURCE_DAYS = [dt.date(2026, 10, day) for day in range(19, 24)]


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


class PerformanceCursor:
    def __init__(self, rows): self.rows = rows
    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, *_args): pass
    def fetchall(self): return self.rows


class PerformanceConnection:
    def __init__(self, rows): self.rows = rows
    def cursor(self): return PerformanceCursor(self.rows)


class MartinBarRetestRangeTests(unittest.TestCase):
    def test_run_range_uses_replay_repository_with_outer_transaction(self):
        from unittest.mock import patch

        class Cursor:
            description = []
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, *_args): pass
            def fetchall(self): return []

        class Connection:
            def cursor(self): return Cursor()

        day = dt.date(2026, 10, 19)
        source = {"source_run_id": 71, "target_trading_date": day.isoformat(),
                  "rules_version": "martin_bar_watchlist_v1", "hits": [],
                  "replay_provenance": {"source_mode": "published_after_next_open"}}
        with patch("src.webapp.services.martin_bar_retest_service.session_dates", return_value=[day - dt.timedelta(days=1), day]), \
             patch("src.webapp.services.martin_bar_retest_service.require_completed_session"), \
             patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository._rows", return_value=[]), \
             patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository.save_session", return_value=9), \
             patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository.set_run_metadata"), \
             patch("src.webapp.services.martin_bar_retest_execution.MartinBarRetestExecutionService") as execution, \
             patch("scripts.replay_martin_bar_retest_range.continuation_mark", return_value={"mark_coverage": {"status": "complete"}}):
            execution.return_value.run_session.return_value = {"missing_bars": 0, "scheduled": 0}
            result = replay.run_range(Connection(), sessions=[day], sources={day: source},
                                      book_id="martin_range_atomic_regression", database_url="test")
        self.assertEqual(result[0]["date"], day.isoformat())

    def test_daily_mark_uses_close_and_reports_missing_position_coverage(self):
        class MarkRepo:
            def _rows(self, sql, _params):
                if "SELECT status,ticker" in sql:
                    return [{"status": "open", "ticker": "ABC", "payload_json": {"entry_price": 10, "remaining_shares": 5,
                             "initial_r_per_share": 1, "initial_shares": 5, "realized_pnl": 2, "realized_r": .4}},
                            {"status": "open", "ticker": "MISS", "payload_json": {"entry_price": 10, "remaining_shares": 5,
                             "initial_r_per_share": 1, "initial_shares": 5}}]
                return []
        frame = __import__("pandas").DataFrame([{"Open": 11, "High": 13, "Low": 10, "Close": 12, "Volume": 100}],
                 index=__import__("pandas").to_datetime(["2026-10-26"]))
        with patch("src.market_data_access.load_many_ticker_windows", lambda *_a, **_k: {"ABC": frame}):
            mark = replay.continuation_mark(MarkRepo(), "martin_range_bar_retest_test", dt.date(2026, 10, 26), "test")
        self.assertIsNone(mark["unrealized_pnl"])
        self.assertIsNone(mark["cumulative_r"])
        self.assertEqual(mark["realized_pnl"], 2)
        self.assertEqual(mark["mark_coverage"], {"open_positions": 2, "marked_positions": 1,
                         "missing_tickers": ["MISS"], "status": "partial"})

    def test_continuation_uses_next_session_and_missing_source_settles_without_new_candidates(self):
        day = dt.date(2026, 10, 23)
        class Repo:
            latest = day
            def __init__(self, **_kwargs): self.saved = []; self.metadata = []
            def latest_session_date(self, _book): return self.latest
            def session_run_id(self, _book, _day): return None
            def atomic_session(self, _book): return nullcontext()
            def save_session(self, **kwargs): self.saved.append(kwargs); return 91
            def _rows(self, sql, _params):
                return [{"summary_json": {"execution_book_version": "martin_bar_retest_execution_v1"}}] if "SELECT summary_json FROM martin_forward_runs WHERE strategy_id=%s ORDER BY" in sql else []
            def set_run_metadata(self, run_id, payload): self.metadata.append((run_id, payload))
        class Execution:
            def __init__(self, *_args, **_kwargs): self.book_id = None
            def run_session(self, **kwargs):
                self.kwargs = kwargs
                return {"missing_bars": 1, "scheduled": 0}
        repo_instances = []
        class RepoFactory(Repo):
            def __init__(self, **kwargs): super().__init__(**kwargs); repo_instances.append(self)
        with patch.object(replay, "xnys_sessions", lambda start, _end: [dt.date(2026, 10, 26)]), \
                patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository", RepoFactory), \
                patch("src.webapp.services.martin_bar_retest_execution.MartinBarRetestExecutionService", Execution), \
                patch("src.webapp.services.martin_rotation.require_completed_session"), \
                patch("scripts.run_martin_bar_retest._load_source", return_value=None), \
                patch.object(replay, "continuation_mark", return_value={"account_equity": 101000, "cumulative_r": 1,
                    "mark_coverage": {"status": "complete"}}):
            result = replay.continue_book(book_id="martin_range_bar_retest_live", database_url="test", history=object())
        repo = repo_instances[-1]
        self.assertEqual(result["session_date"], "2026-10-26")
        self.assertEqual(repo.saved[0]["source_run_id"], 0)
        self.assertEqual(repo.metadata[0][1]["source_mode"], "execution_only_no_saved_scan")
        self.assertIsNone(result["account_equity"])
        self.assertIsNone(result["cumulative_r"])
        self.assertTrue(repo.metadata[0][1]["partial"])

    def test_continuation_is_idempotent_for_existing_next_session(self):
        class Repo:
            def __init__(self, **_kwargs): pass
            def latest_session_date(self, _book): return dt.date(2026, 10, 23)
            def session_run_id(self, _book, _day): return 55
            def _rows(self, *_args): return [{"summary_json": {"execution_book_version": "martin_bar_retest_execution_v1"}}]
        with patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository", Repo), \
                patch.object(replay, "xnys_sessions", lambda *_args: [dt.date(2026, 10, 26)]), \
                patch("src.webapp.services.martin_rotation.require_completed_session"):
            result = replay.continue_book(book_id="martin_range_bar_retest_live", database_url="test", history=object())
        self.assertEqual(result["status"], "already_processed")
        self.assertEqual(result["run_id"], 55)

    def test_repeated_invocation_after_latest_processed_is_noop_until_next_close(self):
        class Repo:
            def __init__(self, **_kwargs): pass
            def latest_session_date(self, _book): return dt.date(2026, 10, 26)
            def session_run_id(self, _book, day): return 61 if day == dt.date(2026, 10, 26) else None
            def _rows(self, *_args): return [{"summary_json": {"execution_book_version": "martin_bar_retest_execution_v1"}}]
        with patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository", Repo), \
                patch.object(replay, "xnys_sessions", lambda *_args: [dt.date(2026, 10, 27)]), \
                patch("src.webapp.services.martin_rotation.require_completed_session", side_effect=ValueError("Rotation strategy requires a completed exchange session")):
            result = replay.continue_book(book_id="martin_range_bar_retest_live", database_url="test", history=object())
        self.assertEqual(result["status"], "not_ready")
        self.assertEqual(result["session_date"], "2026-10-26")
        self.assertEqual(result["next_session_date"], "2026-10-27")
        self.assertEqual(result["reason"], "awaiting_next_completed_session")

    def test_continuation_refuses_unversioned_book_before_writes(self):
        class Repo:
            def __init__(self, **_kwargs): pass
            def latest_session_date(self, _book): return dt.date(2026, 10, 23)
            def _rows(self, *_args): return [{"summary_json": {"execution_book_version": "other"}}]
        with patch("src.webapp.repositories.martin_forward_test_repository.MartinForwardTestRepository", Repo):
            with self.assertRaisesRegex(ValueError, "not a versioned"):
                replay.continue_book(book_id="martin_range_bar_retest_other", database_url="test", history=object())

    def test_continue_cli_without_apply_is_explicitly_preflight_only(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = replay.continue_main(["--book-id", "martin_range_bar_retest_live"])
        self.assertEqual(result, 0)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["mode"], "preflight_only_no_writes")
        self.assertFalse(payload["executed"])

    def test_continue_script_path_supports_help_without_package_import_error(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "continue_martin_bar_retest.py"
        result = subprocess.run([sys.executable, str(script), "--help"], cwd=root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--book-id", result.stdout)
        self.assertNotIn("ModuleNotFoundError", result.stderr)

    def test_book_performance_uses_database_status_over_stale_payload(self):
        connection = PerformanceConnection([
            ("open", "ABC", {"status": "closed", "ticker": "STALE", "realized_pnl": 0}),
            ("closed", "XYZ", {"status": "open", "ticker": "STALE", "realized_pnl": 12.5}),
        ])

        performance = replay.book_performance(connection, "martin_range_bar_retest_test")

        self.assertEqual(performance["open_positions"], 1)
        self.assertEqual(performance["closed_positions"], 1)
        self.assertEqual(performance["open_tickers"], ["ABC"])

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
                       for day in SOURCE_DAYS]

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

    def test_saved_sources_require_provenance_and_classify_publication_timing(self):
        dates = SOURCE_DAYS
        sources = {day: {"source_run_id": index + 1, "hit_count": index, "hits": [{}] * index,
                         "strategy_id": replay.SOURCE_ID, "rules_version": replay.SOURCE_ID,
                         "created_at": "2026-10-25T00:00:00Z", "source_fingerprint": f"fp-{index + 1}"}
                   for index, day in enumerate(dates)}
        class History:
            def list_runs(self, **kwargs):
                day = kwargs["start_date"]
                source = sources.get(day)
                return ([{"run_date": day.isoformat(), "id": source["source_run_id"]}] if source else [])
        with patch("scripts.run_martin_bar_retest._load_source", lambda _history, day: sources.get(day)), \
                patch.object(replay, "publication_timing", side_effect=lambda day, _published:
                             (day != dates[-1], "published_after_next_open" if day != dates[-1] else "saved_before_next_open")):
            result = replay.load_range_sources(History(), dates)
            self.assertEqual(list(result), dates)
            self.assertFalse(result[dates[-1]]["replay_provenance"]["retrospective"])
            self.assertEqual(result[dates[-1]]["replay_provenance"]["source_mode"], "saved_before_next_open")
            del sources[dates[2]]
            with self.assertRaisesRegex(ValueError, "Missing persisted source"):
                replay.load_range_sources(History(), dates)

    def test_general_historical_range_requires_one_persisted_source_per_session(self):
        dates = [dt.date(2026, 9, 23) + dt.timedelta(days=index) for index in range(31)
                 if (dt.date(2026, 9, 23) + dt.timedelta(days=index)).weekday() < 5]
        source_rows = {day: {"source_run_id": index + 100, "hit_count": 0, "hits": [],
                    "strategy_id": replay.SOURCE_ID, "rules_version": replay.SOURCE_ID,
                    "created_at": "2026-10-25T00:00:00Z", "source_fingerprint": f"fp-{index}"}
                       for index, day in enumerate(dates)}

        class History:
            duplicate = False
            def list_runs(self, **kwargs):
                day = kwargs["start_date"]
                source = source_rows.get(day)
                rows = ([{"run_date": day.isoformat(), "id": source["source_run_id"]}] if source else [])
                return rows + rows if self.duplicate and rows else rows

        history = History()
        with patch.object(replay, "xnys_sessions", side_effect=lambda start, end: dates
                          if (start, end) == (dt.date(2026, 9, 23), dt.date(2026, 10, 23)) else []), \
                patch("scripts.run_martin_bar_retest._load_source", lambda _history, day: source_rows.get(day)), \
                patch.object(replay, "publication_timing", return_value=(True, "published_after_next_open")):
            sessions = replay.xnys_sessions(dt.date(2026, 9, 23), dt.date(2026, 10, 23))
            self.assertEqual(len(sessions), 23)
            loaded = replay.load_range_sources(history, sessions)
            self.assertEqual(list(loaded), dates)
            del source_rows[dates[4]]
            with self.assertRaisesRegex(ValueError, "Missing persisted source"):
                replay.load_range_sources(history, sessions)
            source_rows[dates[4]] = {"source_run_id": 104, "hit_count": 0, "hits": [],
                "strategy_id": replay.SOURCE_ID, "rules_version": replay.SOURCE_ID,
                "created_at": "2026-10-25T00:00:00Z", "source_fingerprint": "fp-4"}
            history.duplicate = True
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                replay.load_range_sources(history, sessions)

    def test_actual_service_carries_watch_through_zero_hit_session(self):
        days = SOURCE_DAYS
        repo = FakeRepo()
        sessions_to_day = {day: days[:index + 1] for index, day in enumerate(days)}
        sources = {day: {"target_trading_date": day.isoformat(), "rules_version": replay.SOURCE_ID,
                         "source_run_id": index + 1,
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
        days = SOURCE_DAYS
        sources = {day: {"source_run_id": index + 1} for index, day in enumerate(days)}
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

    def test_run_range_records_backdated_mode_and_preserves_default(self):
        class ReplayRepo:
            def __init__(self): self.metadata = []
            def set_run_metadata(self, run_id, payload): self.metadata.append((run_id, payload))
            def _rows(self, *_args): return []

        class Execution:
            def __init__(self, repository, **_kwargs): self.repository = repository; self.book_id = None
            def run_session(self, **kwargs):
                self.repository.execution_calls.append(kwargs)
                return {"missing_bars": 0, "scheduled": 0}

        class Service:
            def __init__(self, _repository, **_kwargs): pass
            def run_session(self, **_kwargs):
                return {"status": "complete", "run_id": 31, "candidates": [],
                        "source_hit_count": 1, "new_event_count": 0, "active_event_count": 0,
                        "zero_hit_day": False, "retrospective_source_scan": True}

        repositories = []
        class RepoFactory(ReplayRepo):
            def __init__(self, _connection):
                super().__init__(); self.execution_calls = []; repositories.append(self)

        class PerformanceCursor:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, *_args): pass
            def fetchall(self): return []

        class PerformanceConnection:
            def cursor(self): return PerformanceCursor()

        day = dt.date(2026, 10, 19)
        sources = {day: {"source_run_id": 7110, "replay_provenance": {"source_mode": "published_after_next_open"}}}
        for enabled in (False, True):
            with patch("src.webapp.services.martin_bar_retest_service.MartinBarRetestService", Service), \
                    patch("src.webapp.services.martin_bar_retest_execution.MartinBarRetestExecutionService", Execution), \
                    patch("src.webapp.services.martin_rs_replay.ReplayRepository", RepoFactory):
                results = replay.run_range(PerformanceConnection(), sessions=[day], sources=sources,
                    book_id="martin_range_bar_retest_mode_test", database_url="test",
                    simulate_backdated_buys=enabled)
            repository = repositories[-1]
            self.assertEqual(repository.execution_calls[0]["simulate_backdated_buys"], enabled)
            metadata = repository.metadata[0][1]
            self.assertEqual(metadata["simulate_backdated_buys"], enabled)
            self.assertEqual(metadata["backdated_buy_basis"], "hypothetical_backdated_simulation" if enabled else "forward_safe")
            self.assertEqual(results[0]["backdated_buy_simulation"], enabled)
            self.assertEqual(results[0]["fill_basis"], "hypothetical_backdated_simulation" if enabled else "forward_safe")

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
            replay.main(["--from-date", "2026-10-24", "--to-date", "2026-10-23", "--book-id", "martin_range_bar_retest_x"])
        with self.assertRaises(SystemExit):
            replay.main(["--from-date", "2026-10-19", "--to-date", "2026-10-23", "--book-id", "martin_bar_retest_v1"])


if __name__ == "__main__":
    unittest.main()
