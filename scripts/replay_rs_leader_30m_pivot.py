#!/usr/bin/env python3
"""Replay saved Martin watchlist runs into an isolated RS Leader 30m Pivot book."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.replay_martin_bar_retest_range import check_empty_book, load_range_sources, xnys_sessions
from src.webapp.services.rs_leader_30m_pivot import EXECUTION_VERSION, POLICY_ID, execute_pivot_range


def validate_bundle_book_id(audit: dict, book_id: str) -> None:
    if audit.get("book_id") != book_id:
        raise ValueError(f"frozen_30m_book_id_mismatch: bundle={audit.get('book_id')!r} replay={book_id!r}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--intraday-dir", required=True,
                        help="Immutable 30m TradingView bundle directory. Invalid or absent bars block entry.")
    parser.add_argument("--apply", action="store_true", help="Commit the whole replay; default simulates then rolls back.")
    parser.add_argument("--simulate-backdated-buys", action="store_true",
                        help="Allow hypothetical entries from scans saved after the next-session open.")
    args = parser.parse_args(argv)
    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    if not args.book_id.startswith("rs_leader_30m_pivot_replay_"):
        parser.error("--book-id must start with rs_leader_30m_pivot_replay_")
    if not Path(args.intraday_dir).is_dir():
        parser.error("--intraday-dir must be an existing frozen 30m bundle directory")
    from src.webapp.config import load_webapp_config
    from src.webapp.repositories.history_repository import HistoryRepository
    from src.webapp.services.screener_history_service import ScreenerHistoryService
    from src.ticker_filters import leveraged_or_inverse_provenance, set_replay_fund_exclusions, reset_replay_fund_exclusions
    import psycopg
    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    sessions = xnys_sessions(args.from_date, args.to_date)
    try:
        audit = json.loads((Path(args.intraday_dir) / "audit.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        parser.error(f"--intraday-dir requires a valid audit.json manifest: {exc}")
    try:
        validate_bundle_book_id(audit, args.book_id)
    except ValueError as exc:
        parser.error(str(exc))
    if bool(audit.get("simulate_backdated_buys", False)) != args.simulate_backdated_buys:
        parser.error("freeze and replay --simulate-backdated-buys modes must match")
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=True))
    sources = load_range_sources(history, sessions)
    for source in sources.values():
        source.setdefault("replay_provenance", {}).update({"execution_version": EXECUTION_VERSION,
            "source_strategy_id": "martin_bar_watchlist_v1",
            "simulate_backdated_buys": args.simulate_backdated_buys,
            "backdated_buy_basis": "hypothetical_backdated_simulation" if args.simulate_backdated_buys else "forward_safe"})
    exclusions = leveraged_or_inverse_provenance()
    token = set_replay_fund_exclusions(exclusions["symbols"])
    try:
        # Ensure DDL completes before the replay's repeatable-read transaction.
        from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
        MartinForwardTestRepository(database_url=config.database_url).ensure_schema()
        with psycopg.connect(config.database_url) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{args.book_id}",))
                    if not cursor.fetchone()[0]:
                        raise RuntimeError(f"Replay book {args.book_id} is currently locked.")
                check_empty_book(connection, args.book_id)
                results = execute_pivot_range(connection, sessions=sessions, sources=sources,
                    book_id=args.book_id, database_url=config.database_url, intraday_dir=args.intraday_dir,
                    simulate_backdated_buys=args.simulate_backdated_buys)
                if args.apply:
                    connection.commit()
                else:
                    connection.rollback()
            except Exception:
                connection.rollback()
                raise
    finally:
        reset_replay_fund_exclusions(token)
    print(json.dumps({"ready": True, "policy_id": POLICY_ID, "execution_version": EXECUTION_VERSION,
        "book_id": args.book_id, "from_date": args.from_date.isoformat(), "to_date": args.to_date.isoformat(),
        "mode": "apply" if args.apply else "dry_run_rolled_back", "saved": bool(args.apply),
        "simulate_backdated_buys": args.simulate_backdated_buys,
        "backdated_buy_basis": "hypothetical_backdated_simulation" if args.simulate_backdated_buys else "forward_safe",
        "source_mode": "persisted_martin_bar_watchlist_v1_runs", "sessions": results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
