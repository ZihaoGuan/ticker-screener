#!/usr/bin/env python3
"""Replay saved Martin Bar Watchlist scans across the Oct 19-23, 2026 XNYS sessions."""
from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FROM_DATE = dt.date(2026, 10, 19)
TO_DATE = dt.date(2026, 10, 23)
POLICY_ID = "martin_bar_retest_v1"
SOURCE_ID = "martin_bar_watchlist_v1"
EXPECTED_SOURCES = {
    dt.date(2026, 10, 19): (7110, 45),
    dt.date(2026, 10, 20): (7111, 79),
    dt.date(2026, 10, 21): (7112, 85),
    dt.date(2026, 10, 22): (7113, 137),
    dt.date(2026, 10, 23): (7107, 96),
}


def xnys_sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=start - dt.timedelta(days=10), end=end + dt.timedelta(days=10))
    return [label.date() for label in calendar.sessions_in_range(start.isoformat(), end.isoformat())]


def publication_timing(day: dt.date, published_at: Any) -> tuple[bool, str]:
    import exchange_calendars as xcals
    import pandas as pd
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=5), end=day + dt.timedelta(days=20))
    following = next(label.date() for label in calendar.sessions_in_range(
        day.isoformat(), (day + dt.timedelta(days=20)).isoformat()) if label.date() > day)
    published = pd.Timestamp(published_at)
    if published.tzinfo is None:
        published = published.tz_localize("UTC")
    retrospective = published >= calendar.session_open(following.isoformat())
    return retrospective, ("published_after_next_open" if retrospective else "saved_before_next_open")


def load_range_sources(history: Any, sessions: list[dt.date]) -> dict[dt.date, dict[str, Any]]:
    from scripts.run_martin_bar_retest import _load_source

    expected_sessions = list(EXPECTED_SOURCES)
    if sessions != expected_sessions:
        raise ValueError("This replay requires all five XNYS sessions from 2026-10-19 through 2026-10-23.")
    sources = {}
    for day in sessions:
        expected_id, expected_hits = EXPECTED_SOURCES[day]
        source = _load_source(history, day)
        if source is None:
            raise ValueError(f"Missing persisted source run for {day} (expected run {expected_id}).")
        if int(source.get("source_run_id") or 0) != expected_id:
            raise ValueError(f"Wrong persisted source run for {day}: expected {expected_id}, got {source.get('source_run_id')}.")
        if int(source.get("hit_count") or 0) != expected_hits or len(source.get("hits") or []) != expected_hits:
            raise ValueError(f"Source run {expected_id} hit count/provenance mismatch for {day}.")
        if source.get("strategy_id") != SOURCE_ID or source.get("rules_version") != SOURCE_ID:
            raise ValueError(f"Source run {expected_id} has invalid strategy or rules provenance.")
        if not source.get("created_at") or not source.get("source_fingerprint"):
            raise ValueError(f"Source run {expected_id} is missing publication or fingerprint provenance.")
        retrospective, timing = publication_timing(day, source["created_at"])
        source["replay_provenance"] = {
            "origin": "historical_replay",
            "source_mode": timing,
            "retrospective": retrospective,
            "source_run_id": expected_id,
            "created_after_session_close": True,
            "limitation": "Saved screen run used as historical input; the creation timestamp does not prove point-in-time universe or data availability.",
        }
        sources[day] = source
    return sources


def check_empty_book(connection: Any, book_id: str) -> None:
    with connection.cursor() as cursor:
        for table in ("martin_forward_runs", "martin_forward_positions", "martin_forward_events"):
            cursor.execute(f"SELECT count(*) FROM {table} WHERE strategy_id=%s", (book_id,))
            if cursor.fetchone()[0]:
                raise ValueError(f"Book {book_id} already has history; replay refuses to modify it.")


def run_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
              book_id: str, database_url: str) -> list[dict[str, Any]]:
    from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
    from src.webapp.services.martin_rs_replay import ReplayRepository

    service = MartinBarRetestService(ReplayRepository(connection), database_url=database_url)
    results = []
    for day in sessions:
        source = sources[day]
        result = service.run_session(as_of_date=day, source_record=source, dry_run=False,
                                     policy_id=POLICY_ID, book_id=book_id)
        if result.get("status") in {"missing_source", "blocked_source"} or result.get("skipped"):
            raise ValueError(f"Replay blocked or duplicated on {day}: {result.get('reason') or result.get('status')}.")
        statuses = Counter(str(item.get("status") or "unknown") for item in result.get("candidates", []))
        reasons = Counter(str(reason) for item in result.get("candidates", [])
                          for reason in (item.get("reasons") or []))
        results.append({"date": day.isoformat(), "source_run_id": source["source_run_id"],
                        "source_hit_count": result["source_hit_count"],
                        "new_event_count": result["new_event_count"],
                        "active_event_count": result["active_event_count"],
                        "zero_hit_day": result["zero_hit_day"],
                        "retrospective_source_scan": result["retrospective_source_scan"],
                        "source_publication_timing": source["replay_provenance"]["source_mode"],
                        "candidate_status_counts": dict(statuses), "reason_counts": dict(reasons),
                        "candidate_count": len(result.get("candidates", [])),
                        "status": "complete"})
    return results


def execute_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
                  book_id: str, database_url: str, apply: bool) -> list[dict[str, Any]]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{book_id}",))
            if not cursor.fetchone()[0]:
                raise RuntimeError(f"Replay book {book_id} is currently locked.")
        check_empty_book(connection, book_id)
        results = run_range(connection, sessions=sessions, sources=sources, book_id=book_id,
                            database_url=database_url)
        if apply:
            connection.commit()
        else:
            connection.rollback()
        return results
    except Exception:
        connection.rollback()
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay the saved Oct 19-23, 2026 Martin Bar Retest scans into a fresh isolated book.")
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--apply", action="store_true", help="Commit all five sessions atomically; default simulates then rolls back.")
    args = parser.parse_args(argv)
    if args.from_date != FROM_DATE or args.to_date != TO_DATE:
        parser.error("this replay requires --from-date 2026-10-19 and --to-date 2026-10-23")
    if not args.book_id.startswith("martin_range_bar_retest_") or args.book_id == POLICY_ID:
        parser.error("--book-id must start with martin_range_bar_retest_ and be separate from the policy ID")

    from src.webapp.config import load_webapp_config
    from src.webapp.repositories.history_repository import HistoryRepository
    from src.webapp.services.screener_history_service import ScreenerHistoryService
    import psycopg

    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    sessions = xnys_sessions(args.from_date, args.to_date)
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=True))
    try:
        sources = load_range_sources(history, sessions)
    except Exception as exc:
        print(json.dumps({"ready": False, "book_id": args.book_id, "reason": "source_preflight_failed", "detail": str(exc)}))
        return 2

    with psycopg.connect(config.database_url) as connection:
        results = execute_range(connection, sessions=sessions, sources=sources, book_id=args.book_id,
                                database_url=config.database_url, apply=args.apply)
    print(json.dumps({"ready": True, "policy_id": POLICY_ID, "book_id": args.book_id,
        "from_date": args.from_date.isoformat(), "to_date": args.to_date.isoformat(),
        "mode": "apply" if args.apply else "dry_run_rolled_back", "saved": bool(args.apply),
        "source_mode": "persisted_saved_screen_runs", "sessions": results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
