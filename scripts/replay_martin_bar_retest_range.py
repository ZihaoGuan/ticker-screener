#!/usr/bin/env python3
"""Replay saved Martin Bar Watchlist scans across the Oct 19-23, 2026 XNYS sessions."""
from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import hashlib
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
    following = calendar.next_session(day.isoformat()).date()
    published = pd.Timestamp(published_at)
    if published.tzinfo is None:
        published = published.tz_localize("UTC")
    retrospective = published >= calendar.session_open(following.isoformat())
    return retrospective, ("published_after_next_open" if retrospective else "saved_before_next_open")


def load_range_sources(history: Any, sessions: list[dt.date]) -> dict[dt.date, dict[str, Any]]:
    from scripts.run_martin_bar_retest import _load_source

    expected_sessions = list(EXPECTED_SOURCES)
    if sessions[:len(expected_sessions)] != expected_sessions:
        raise ValueError("This replay requires all five XNYS sessions from 2026-10-19 through 2026-10-23.")
    sources = {}
    for day in sessions:
        source = _load_source(history, day)
        expected = EXPECTED_SOURCES.get(day)
        if source is None and expected:
            raise ValueError(f"Missing persisted source run for {day} (expected run {expected[0]}).")
        if source is None:
            continue  # Execution-only completed sessions can settle orders without a new scan.
        if expected:
            expected_id, expected_hits = expected
            if int(source.get("source_run_id") or 0) != expected_id:
                raise ValueError(f"Wrong persisted source run for {day}: expected {expected_id}, got {source.get('source_run_id')}.")
            if int(source.get("hit_count") or 0) != expected_hits or len(source.get("hits") or []) != expected_hits:
                raise ValueError(f"Source run {expected_id} hit count/provenance mismatch for {day}.")
        if source.get("strategy_id") != SOURCE_ID or source.get("rules_version") != SOURCE_ID:
            raise ValueError(f"Source run {source.get('source_run_id')} has invalid strategy or rules provenance.")
        if int(source.get("hit_count") or 0) != len(source.get("hits") or []):
            raise ValueError(f"Source run {source.get('source_run_id')} hit count does not match loaded hits.")
        if not source.get("created_at") or not source.get("source_fingerprint"):
            raise ValueError(f"Source run {source.get('source_run_id')} is missing publication or fingerprint provenance.")
        retrospective, timing = publication_timing(day, source["created_at"])
        source["replay_provenance"] = {
            "origin": "historical_replay",
            "source_mode": timing,
            "retrospective": retrospective,
            "source_run_id": source.get("source_run_id"),
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


def save_execution_only_run(repository: Any, *, book_id: str, day: dt.date) -> int | None:
    """Keep zero-scan execution sessions in the book's sequential session history."""
    return repository.save_session(session_date=day, source_run_id=0,
        fingerprint=hashlib.sha256(f"{book_id}:{day}:execution-only".encode()).hexdigest(),
        candidates=[], summary={"strategy_id": POLICY_ID, "book_id": book_id,
            "rule_version": "martin_bar_retest_execution_v1", "session_date": day.isoformat(),
            "source_run_id": None, "source_mode": "execution_only_no_saved_scan", "zero_hit_day": None,
            "execution_mode": "historical_replay"}, strategy_id=book_id)


def book_performance(connection: Any, book_id: str) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT status,ticker,payload_json FROM martin_forward_positions WHERE strategy_id=%s", (book_id,))
        rows = cursor.fetchall()
    positions = [{**(row[2] or {}), "status": row[0], "ticker": row[1]} for row in rows]
    fills_by_category: dict[str, int] = {}
    for position in positions:
        category = str(position.get("event_category") or position.get("category") or "unknown")
        fills_by_category[category] = fills_by_category.get(category, 0) + 1
    return {"open_positions": sum(row["status"] == "open" for row in positions),
            "open_tickers": sorted(str(row.get("ticker") or "").upper() for row in positions if row["status"] == "open"),
            "closed_positions": sum(row["status"] == "closed" for row in positions),
            "realized_pnl": sum(float(row.get("realized_pnl") or 0) for row in positions),
            "fills_by_discovery_category": fills_by_category}


def run_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
              book_id: str, database_url: str, simulate_backdated_buys: bool = False) -> list[dict[str, Any]]:
    from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
    from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService
    from src.webapp.services.martin_rs_replay import ReplayRepository

    repository = ReplayRepository(connection)
    service = MartinBarRetestService(repository, database_url=database_url)
    execution = MartinBarRetestExecutionService(repository, database_url=database_url)
    execution.book_id = book_id
    results = []
    for day in sessions:
        source = sources.get(day)
        if source is None:
            from src.webapp.services.martin_rotation import require_completed_session
            require_completed_session(day)
            run_id = save_execution_only_run(repository, book_id=book_id, day=day)
            actions = execution.run_session(day=day, run_id=run_id, candidates=[], source_on_time=False)
            if actions["missing_bars"]:
                raise ValueError(f"Execution replay is missing required bars on {day}; the full range was rolled back.")
            repository.set_run_metadata(run_id, {"execution_book_version": "martin_bar_retest_execution_v1",
                "execution_only": True, "execution_actions": actions,
                "simulate_backdated_buys": simulate_backdated_buys,
                "backdated_buy_basis": None})
            results.append({"date": day.isoformat(), "source_run_id": None, "source_hit_count": 0,
                "new_event_count": 0, "active_event_count": 0, "zero_hit_day": None,
                "retrospective_source_scan": None, "source_publication_timing": "no_scan_execution_only",
                "execution_actions": actions, "category_cohorts": {}, "candidate_status_counts": {},
                "reason_counts": {}, "candidate_count": 0, "status": "execution_only"})
            continue
        result = service.run_session(as_of_date=day, source_record=source, dry_run=False,
                                     policy_id=POLICY_ID, book_id=book_id)
        if result.get("status") in {"missing_source", "blocked_source"} or result.get("skipped"):
            raise ValueError(f"Replay blocked or duplicated on {day}: {result.get('reason') or result.get('status')}.")
        actions = execution.run_session(day=day, run_id=result.get("run_id"), candidates=result.get("candidates", []),
                                        source_on_time=result.get("retrospective_source_scan") is False,
                                        simulate_backdated_buys=simulate_backdated_buys)
        if actions["missing_bars"]:
            raise ValueError(f"Execution replay is missing required bars on {day}; the full range was rolled back.")
        cohorts = {category: sum(item.get("event_category") == category for item in result.get("candidates", []))
                   for category in sorted({str(item.get("event_category") or "unknown") for item in result.get("candidates", [])})}
        if result.get("run_id") is not None:
            repository.set_run_metadata(result["run_id"], {"execution_book_version": "martin_bar_retest_execution_v1",
                "execution_actions": actions, "execution_source_on_time": result.get("retrospective_source_scan") is False,
                "simulate_backdated_buys": simulate_backdated_buys,
                "backdated_buy_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and result.get("retrospective_source_scan") else "forward_safe",
                "source_reconstructed_later": bool(result.get("retrospective_source_scan")),
                "category_cohorts": cohorts, "execution_account_limits": {"base_equity": 100000,
                "risk_per_trade": 500, "max_name_value": 20000, "max_committed_risk": 2500, "max_positions": 8}})
        statuses = Counter(str(item.get("status") or "unknown") for item in result.get("candidates", []))
        reasons = Counter(str(reason) for item in result.get("candidates", [])
                          for reason in (item.get("reasons") or []))
        results.append({"date": day.isoformat(), "source_run_id": source["source_run_id"],
                        "source_hit_count": result["source_hit_count"],
                        "new_event_count": result["new_event_count"],
                        "active_event_count": result["active_event_count"],
                        "zero_hit_day": result["zero_hit_day"],
                        "retrospective_source_scan": result["retrospective_source_scan"],
                        "backdated_buy_simulation": bool(simulate_backdated_buys and result["retrospective_source_scan"]),
                        "source_reconstructed_later": bool(result["retrospective_source_scan"]),
                        "fill_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and result["retrospective_source_scan"] else "forward_safe",
                        "source_publication_timing": source["replay_provenance"]["source_mode"],
                        "candidate_status_counts": dict(statuses), "reason_counts": dict(reasons),
                        "execution_actions": actions,
                        "category_cohorts": cohorts,
                        "candidate_count": len(result.get("candidates", [])),
                        "status": "complete"})
    if results:
        results[-1]["book_performance"] = book_performance(connection, book_id)
    return results


def execute_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
                  book_id: str, database_url: str, apply: bool, simulate_backdated_buys: bool = False) -> list[dict[str, Any]]:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{book_id}",))
            if not cursor.fetchone()[0]:
                raise RuntimeError(f"Replay book {book_id} is currently locked.")
        check_empty_book(connection, book_id)
        results = run_range(connection, sessions=sessions, sources=sources, book_id=book_id,
                            database_url=database_url, simulate_backdated_buys=simulate_backdated_buys)
        if apply:
            connection.commit()
        else:
            connection.rollback()
        return results
    except Exception:
        connection.rollback()
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay saved Martin Bar Retest scans and subsequent execution sessions into a fresh isolated book.")
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--apply", action="store_true", help="Commit the full session range atomically; default simulates then rolls back.")
    parser.add_argument("--simulate-backdated-buys", action="store_true",
        help="Allow confirmed signals from scans saved after next-session open to schedule hypothetical historical fills.")
    args = parser.parse_args(argv)
    if args.from_date != FROM_DATE or args.to_date < TO_DATE:
        parser.error("this replay requires --from-date 2026-10-19 and --to-date on or after 2026-10-23")
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
                                database_url=config.database_url, apply=args.apply,
                                simulate_backdated_buys=args.simulate_backdated_buys)
    print(json.dumps({"ready": True, "policy_id": POLICY_ID, "execution_policy_id": "martin_bar_retest_execution_v1", "book_id": args.book_id,
        "from_date": args.from_date.isoformat(), "to_date": args.to_date.isoformat(),
        "mode": "apply" if args.apply else "dry_run_rolled_back", "saved": bool(args.apply),
        "simulate_backdated_buys": args.simulate_backdated_buys,
        "backdated_buy_basis": "hypothetical_backdated_simulation" if args.simulate_backdated_buys else "forward_safe",
        "source_mode": "saved_screen_runs_with_execution_only_sessions", "sessions": results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
