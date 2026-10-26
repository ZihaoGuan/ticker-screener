#!/usr/bin/env python3
"""Replay one Martin policy over dated XNYS inputs in an isolated paper book."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import datetime as dt
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.martin_daily_gap_retest import REPLAY_V4_STRATEGY_ID

POLICIES = {
    "account": "martin_daily_v1",
    "sector": "martin_sector_book_v1",
    "top5": "martin_daily_top5_v1",
    "rs_expanded": "martin_rs_expanded_v1",
    "rotation": "martin_rs_expanded_rotation_v1",
    "gap_retest": REPLAY_V4_STRATEGY_ID,
}


def xnys_calendar(start: dt.date, end: dt.date):
    import exchange_calendars as xcals
    return xcals.get_calendar("XNYS", start=start - dt.timedelta(days=15), end=end + dt.timedelta(days=15))


def xnys_sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    calendar = xnys_calendar(start, end)
    return [label.date() for label in calendar.sessions_in_range(start.isoformat(), end.isoformat())]


def load_saved_top_hits(cursor: Any, day: dt.date, *, calendar: Any, now: dt.datetime) -> dict[str, Any]:
    from src.webapp.services.martin_rs_replay import choose_snapshot, timestamp
    session = day.isoformat()
    close = timestamp(calendar.session_close(session))
    next_open = timestamp(calendar.session_open(calendar.next_session(session)))
    cursor.execute("""SELECT id,run_date,created_at,result_summary_json
        FROM screen_runs WHERE strategy_id='scanner_top_hits_snapshot' AND deleted_at IS NULL
        AND run_date=%s ORDER BY id""", (day,))
    records = cursor.fetchall()
    snapshot = choose_snapshot(records, day, close, next_open, now)
    if snapshot is None:
        return {"blocked": "no_valid_point_in_time_top_hits_for_exact_session", "session_date": session}
    summary = snapshot
    payload = {**summary, "target_trading_date": session,
               "provenance": {"screen_run_id": summary["_snapshot_run_id"],
                              "generated_at": summary["_original_generated_at"]}}
    if not payload.get("source_fingerprint"):
        import hashlib
        payload["source_fingerprint"] = hashlib.sha256(
            json.dumps(summary, sort_keys=True, default=str).encode()).hexdigest()
    return {"snapshot": payload, "provenance": payload["provenance"]}


def _row_source_ids(row: dict[str, Any]) -> list[str]:
    result = set(str(value) for value in row.get("scanner_ids", []) if value)
    for item in row.get("scanners", []) if isinstance(row.get("scanners"), list) else []:
        if isinstance(item, dict):
            value = item.get("id") or item.get("strategy_id")
            if value:
                result.add(str(value))
    plan = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
    if plan.get("source_strategy_id"):
        result.add(str(plan["source_strategy_id"]))
    return sorted(result)


def compare_reconstructed_snapshot(saved_snapshot: dict[str, Any], reconstructed: dict[str, Any], *,
                                   policy_id: str, max_diffs: int = 50) -> dict[str, Any]:
    """Compare saved and reconstructed Top Hits decision fields, without replaying orders."""
    from src.webapp.services.martin_forward_test_service import classify_candidate

    saved_rows = (saved_snapshot.get("guru_board") or {}).get("rows") or []
    reconstructed_rows = reconstructed.get("rows") or []
    saved_by_ticker = {str(row.get("ticker") or "").upper(): {**row, "rank": row.get("rank", rank)}
        for rank, row in enumerate(saved_rows, start=1) if row.get("ticker")}
    rebuilt_by_ticker = {str(row.get("ticker") or "").upper(): {**row, "rank": row.get("rank", rank)}
        for rank, row in enumerate(reconstructed_rows, start=1) if row.get("ticker")}
    saved_members, rebuilt_members = set(saved_by_ticker), set(rebuilt_by_ticker)
    membership_diffs = []
    for ticker in sorted(saved_members ^ rebuilt_members):
        source = saved_by_ticker.get(ticker) or rebuilt_by_ticker.get(ticker) or {}
        membership_diffs.append({"ticker": ticker,
            "present_in": "saved" if ticker in saved_members else "reconstructed",
            "source_ids": _row_source_ids(source)})

    def value(row: dict[str, Any], field: str) -> Any:
        plan = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
        strike = row.get("strike_zone") if isinstance(row.get("strike_zone"), dict) else {}
        if field == "eligibility":
            return classify_candidate(row, int(row.get("rank") or 1))["status"]
        if field == "avoid":
            return row.get("trade_readiness") == "avoid" or strike.get("state") == "avoid"
        if field == "strike_score":
            return row.get("strike_score", strike.get("score"))
        if field in {"entry", "stop", "max_entry"}:
            key = {"entry": "trigger_price", "stop": "stop_price", "max_entry": "max_entry_price"}[field]
            return plan.get(key)
        if field == "rank":
            return row.get("rank")
        return row.get(field)

    compared_fields = ("eligibility", "avoid", "trade_readiness", "entry", "stop", "max_entry",
                      "adr14_pct", "strike_score", "scanner_count", "rank")
    differences = []
    field_counts: dict[str, int] = {}
    total_diff_count = 0
    for ticker in sorted(saved_members & rebuilt_members):
        saved, rebuilt = saved_by_ticker[ticker], rebuilt_by_ticker[ticker]
        changed = {}
        for field in compared_fields:
            saved_value, rebuilt_value = value(saved, field), value(rebuilt, field)
            if isinstance(saved_value, float) and isinstance(rebuilt_value, (float, int)):
                equal = abs(saved_value - float(rebuilt_value)) < 1e-6
            else:
                equal = saved_value == rebuilt_value
            if not equal:
                changed[field] = {"saved": saved_value, "reconstructed": rebuilt_value}
                field_counts[field] = field_counts.get(field, 0) + 1
        if changed:
            total_diff_count += 1
            if len(differences) < max_diffs:
                differences.append({"ticker": ticker, "fields": changed,
                    "source_ids": {"saved": _row_source_ids(saved),
                                   "reconstructed": _row_source_ids(rebuilt)}})
    return {
        "session_date": reconstructed.get("session_date"),
        "policy_id": policy_id,
        "saved_snapshot_run_id": saved_snapshot.get("_snapshot_run_id"),
        "focus_membership": {"saved_count": len(saved_members), "reconstructed_count": len(rebuilt_members),
            "mismatch_count": len(membership_diffs), "mismatches": membership_diffs[:max_diffs],
            "truncated_count": max(0, len(membership_diffs) - max_diffs)},
        "decision_field_mismatches": {"ticker_mismatch_count": total_diff_count,
            "field_counts": field_counts, "differences": differences,
            "truncated_count": max(0, total_diff_count - len(differences))},
        "orders_fills": {"status": "not_compared",
            "reason": "reconstruction_blocked" if not reconstructed.get("ready") else "calibration_scope_input_parity_only"},
    }


def build_reconstruction_calibration_report(sessions: list[dt.date], *, saved_snapshots: dict[dt.date, dict[str, Any]],
                                            reconstructed: dict[dt.date, dict[str, Any]],
                                            policy_id: str) -> dict[str, Any]:
    comparisons = []
    no_snapshot = []
    for day in sessions:
        saved = saved_snapshots.get(day, {})
        if not isinstance(saved.get("snapshot"), dict):
            no_snapshot.append({"session_date": day.isoformat(),
                "reason": saved.get("blocked") or "no_valid_saved_top_hits_snapshot"})
            continue
        comparisons.append(compare_reconstructed_snapshot(saved["snapshot"], reconstructed[day],
                                                          policy_id=policy_id))
    return {"policy_id": policy_id, "compared_session_count": len(comparisons),
        "sessions_without_valid_saved_snapshot": no_snapshot, "comparisons": comparisons}


def _check_book_empty(connection, book_id: str) -> None:
    with connection.cursor() as cursor:
        for table in ("martin_forward_runs", "martin_forward_positions", "martin_forward_events"):
            cursor.execute(f"SELECT count(*) FROM {table} WHERE strategy_id=%s", (book_id,))
            if cursor.fetchone()[0]:
                raise ValueError(f"Book {book_id} already has history; range replay refuses to append or overwrite it.")


def _replay_sessions(connection: Any, sessions: list[dt.date], dated: dict[dt.date, dict[str, Any]],
                     sources: dict[dt.date, dict[str, Any]], *, policy: str, book_id: str,
                     database_url: str, from_date: dt.date, to_date: dt.date) -> list[dict[str, Any]]:
    from src.webapp.services.martin_forward_test_service import MartinForwardTestService
    from src.webapp.services.martin_gap_retest_service import MartinGapRetestService
    from src.webapp.services.martin_rs_replay import ReplayRepository

    repository = ReplayRepository(connection)
    results = []
    if policy == "gap_retest":
        service = MartinGapRetestService(repository, database_url=database_url)
        for day in sessions:
            result = service.run_session(as_of_date=day, source_record=sources[day], dry_run=False,
                policy_id=REPLAY_V4_STRATEGY_ID, book_id=book_id, historical_replay=True)
            if result.get("run_id") is None or result.get("partial"):
                raise ValueError(f"Gap Retest replay incomplete on {day}; transaction rolled back.")
            results.append({"date": day.isoformat(), "run_id": result.get("run_id"),
                            "candidate_count": result.get("candidate_count"), "status": result.get("status"),
                            "source_fingerprint": sources[day]["source_fingerprint"]})
    else:
        service = MartinForwardTestService(repository, database_url=database_url)
        for day in sessions:
            snapshot = dated[day]["snapshot"]
            if not isinstance(snapshot.get("replay_provenance"), dict):
                snapshot["replay_provenance"] = {"origin": "historical_replay", "source_mode": "saved_point_in_time_snapshot",
                    "from_date": from_date.isoformat(), "to_date": to_date.isoformat(), "source_run_id": snapshot["_snapshot_run_id"]}
            result = service.run_session(as_of_date=day, snapshot=snapshot,
                                         policy_id=POLICIES[policy], book_id=book_id)
            if result.get("run_id") is None or result.get("partial"):
                raise ValueError(f"{policy} replay incomplete on {day}; transaction rolled back.")
            results.append({"date": day.isoformat(), "run_id": result.get("run_id"),
                            "candidate_count": result.get("candidate_count"), "partial": result.get("partial"),
                            "source_run_id": snapshot.get("_snapshot_run_id"),
                            "source_mode": (snapshot.get("replay_provenance") or {}).get("source_mode"),
                            "manifest_digest": (snapshot.get("replay_provenance") or {}).get("manifest_digest"),
                            "source_fingerprint": snapshot.get("source_fingerprint")})
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Replay one Martin policy across sequential XNYS sessions.")
    parser.add_argument("--policy", required=True, choices=sorted(POLICIES))
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True, help="Fresh persistence identity, separate from policy.")
    parser.add_argument("--input-source", choices=("saved", "reconstructed"), default="saved",
                        help="Top Hits source mode; reconstructed requires complete dated evidence.")
    parser.add_argument("--apply", action="store_true", help="Commit the full isolated range; default runs and rolls back the same service simulation.")
    args = parser.parse_args(argv)
    from src.config import load_app_config
    from src.webapp.config import load_webapp_config
    import psycopg

    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    if args.input_source == "reconstructed" and args.policy == "gap_retest":
        parser.error("--input-source reconstructed applies only to the five Top Hits policies")
    if not args.book_id.startswith("martin_range_") or args.book_id in set(POLICIES.values()):
        parser.error("--book-id must start with martin_range_ and identify a new isolated book")
    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    calendar = xnys_calendar(args.from_date, args.to_date)
    sessions = [label.date() for label in calendar.sessions_in_range(args.from_date.isoformat(), args.to_date.isoformat())]
    if not sessions:
        parser.error("date range contains no XNYS sessions")
    now = dt.datetime.now(dt.timezone.utc)
    from src.ticker_filters import leveraged_or_inverse_provenance
    exclusion_provenance = leveraged_or_inverse_provenance()

    dated: dict[dt.date, dict[str, Any]] = {}
    sources: dict[dt.date, dict[str, Any]] = {}
    details: dict[str, Any] = {}
    calibration_report: dict[str, Any] | None = None
    missing: list[str] = []
    if args.policy == "gap_retest":
        from scripts.replay_martin_gap_retest import reconstruct_sources
        try:
            with redirect_stdout(sys.stderr):
                records, details = reconstruct_sources(args.from_date, args.to_date,
                    database_url=config.database_url, config=load_app_config())
        except ValueError as exc:
            print(json.dumps({"ready": False, "policy": args.policy, "book_id": args.book_id,
                "reason": "gap_source_reconstruction_failed", "detail": str(exc)}))
            return 2
        sources = {dt.date.fromisoformat(row["target_trading_date"]): row for row in records}
        missing = [day.isoformat() for day in sessions if day not in sources]
    elif args.input_source == "reconstructed":
        from src.webapp.services.martin_reconstructed_inputs import reconstruct_price_focus_session
        with psycopg.connect(config.database_url, row_factory=psycopg.rows.dict_row) as read_connection:
            with read_connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                reconstructed = {}
                saved_calibration_inputs = {}
                for index, day in enumerate(sessions, start=1):
                    print(f"Reconstructing {day} ({index}/{len(sessions)})", file=sys.stderr, flush=True)
                    next_open = calendar.session_open(calendar.next_session(day.isoformat()))
                    with redirect_stdout(sys.stderr):
                        reconstructed[day] = reconstruct_price_focus_session(
                            cursor, day, next_open=next_open, database_url=config.database_url,
                            app_config=load_app_config(), policy_id=POLICIES[args.policy])
                    saved_calibration_inputs[day] = load_saved_top_hits(
                        cursor, day, calendar=calendar, now=now)
                    print(f"Reconstructed {day}: candidates={reconstructed[day].get('candidate_count')} "
                          f"ready={reconstructed[day].get('ready')}", file=sys.stderr, flush=True)
                calibration_report = build_reconstruction_calibration_report(
                    sessions, saved_snapshots=saved_calibration_inputs,
                    reconstructed=reconstructed, policy_id=POLICIES[args.policy])
        blockers = {day.isoformat(): reconstructed[day] for day in sessions
                    if not reconstructed[day]["ready"]}
        if blockers:
            print(json.dumps({"ready": False, "policy": args.policy, "book_id": args.book_id,
                "input_source": args.input_source, "blocked_sessions": blockers,
                "calibration": calibration_report,
                "reason": "reconstructed_inputs_not_certifiable"}, indent=2, default=str))
            return 2
        dated = {day: {"snapshot": {"target_trading_date": day.isoformat(),
                   "guru_board": {"rows": reconstructed[day]["rows"]},
                   "source_fingerprint": reconstructed[day]["source_fingerprint"],
                   "replay_provenance": {"origin": "historical_replay",
                       "source_mode": "martin_reconstructed_inputs_v1",
                       "manifest_version": reconstructed[day]["manifest_version"],
                       "manifest_digest": reconstructed[day]["manifest_digest"],
                       "code_fingerprint": reconstructed[day]["code_fingerprint"],
                       "config_fingerprint": reconstructed[day]["config_fingerprint"],
                       "data_cutoff": reconstructed[day]["data_cutoff"],
                       "source_fingerprint": reconstructed[day]["source_fingerprint"],
                       "reconstruction_report": reconstructed[day]}}}
                  for day in sessions}
    else:
        with psycopg.connect(config.database_url, row_factory=psycopg.rows.dict_row) as read_connection:
            with read_connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                dated = {day: load_saved_top_hits(cursor, day, calendar=calendar, now=now) for day in sessions}
        missing = [day.isoformat() for day in sessions if "snapshot" not in dated[day]]
    if missing:
        print(json.dumps({"ready": False, "policy": args.policy, "book_id": args.book_id,
            "blocked_sessions": missing, "reason": "exact_dated_input_missing_or_invalid", **details}, indent=2, default=str))
        return 2

    for entry in dated.values():
        snapshot = entry.get("snapshot") if isinstance(entry, dict) else None
        if isinstance(snapshot, dict):
            provenance = snapshot.setdefault("replay_provenance", {})
            provenance["fund_exclusions"] = {"version": exclusion_provenance["version"],
                "sha256": exclusion_provenance["sha256"]}
    for source in sources.values():
        if isinstance(source, dict):
            source.setdefault("replay_provenance", {})["fund_exclusions"] = {
                "version": exclusion_provenance["version"], "sha256": exclusion_provenance["sha256"]}

    with psycopg.connect(config.database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{args.book_id}",))
            if not cursor.fetchone()[0]:
                raise RuntimeError(f"Replay book {args.book_id} is currently locked.")
        _check_book_empty(connection, args.book_id)
        from src.ticker_filters import set_replay_fund_exclusions, reset_replay_fund_exclusions
        exclusion_token = set_replay_fund_exclusions(exclusion_provenance["symbols"])
        try:
            with redirect_stdout(sys.stderr):
                results = _replay_sessions(connection, sessions, dated, sources, policy=args.policy,
                    book_id=args.book_id, database_url=config.database_url, from_date=args.from_date, to_date=args.to_date)
        finally:
            reset_replay_fund_exclusions(exclusion_token)
        if args.apply:
            connection.commit()
        else:
            connection.rollback()
    print(json.dumps({"ready": True, "policy": args.policy, "book_id": args.book_id,
        "input_source": args.input_source,
        "mode": "apply" if args.apply else "dry_run_rolled_back", "sessions": results,
        "saved": bool(args.apply), "calibration": calibration_report, **details}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
