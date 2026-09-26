#!/usr/bin/env python3
"""Build the complete Top Hits read-model after scanner data is ready."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import sys
from contextlib import contextmanager
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.watchlist_service import WatchlistService
from src.webapp.services.snapshot_preflight import check_required_scheduled_jobs


@contextmanager
def _snapshot_build_lock(lock_path: Path):
    """Ensure overlapping scheduler ticks never build the same snapshot twice."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _is_within_debounce_window(*, latest_source_at: str, now: dt.datetime, debounce_seconds: int) -> bool:
    if not latest_source_at or debounce_seconds <= 0:
        return False
    try:
        parsed = dt.datetime.fromisoformat(latest_source_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return (now - parsed.astimezone(dt.timezone.utc)).total_seconds() < debounce_seconds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Top Hits snapshot after its scheduled prerequisites complete.")
    parser.add_argument("--required-job-id", action="append", default=[], help="Scheduled job ID required to finish successfully today (repeatable).")
    parser.add_argument("--required-job-group", action="append", default=[], help="Scheduled prerequisite group (repeatable).")
    parser.add_argument("--skip-if-current", action="store_true", help="Do not rebuild when today's completed snapshot already exists.")
    parser.add_argument("--allow-incremental-refresh", action="store_true", help="Refresh a completed same-date snapshot after an eligible scanner reruns without waiting for unrelated prerequisites.")
    parser.add_argument("--debounce-seconds", type=int, default=300, help="Wait after the newest source run before rebuilding (default: 300).")
    args = parser.parse_args(argv)
    config = load_webapp_config()
    service = WatchlistService(
        artifacts_dir=config.artifacts_dir,
        database_url=config.database_url,
        market_data_source=config.market_data_source,
    )
    now = dt.datetime.now(dt.timezone.utc)
    lock_path = config.artifacts_dir / "status" / "build_scanner_top_hits_snapshot.lock"
    with _snapshot_build_lock(lock_path) as acquired:
        if not acquired:
            print("SNAPSHOT_WAITING: Top Hits refresh is already building in another scheduler process.")
            return 0
        board_payload = service.get_scanner_board(now=now)
        refresh_state = service.get_scanner_top_hits_snapshot_refresh_state(now=now, board_payload=board_payload)
        source_changed = bool(refresh_state["source_changed"])
        if args.skip_if_current and refresh_state["has_current_snapshot"] and not source_changed:
            print("SNAPSHOT_CURRENT: Top Hits already reflects the current scanner source fingerprint.")
            return 0
        if source_changed and _is_within_debounce_window(
            latest_source_at=str(refresh_state["latest_source_at"]),
            now=now,
            debounce_seconds=max(0, int(args.debounce_seconds)),
        ):
            print("SNAPSHOT_WAITING: Top Hits detected updated scanner inputs; waiting for the five-minute debounce window.")
            return 0
        preflight = check_required_scheduled_jobs(
            status_dir=config.artifacts_dir / "status",
            required_job_ids=args.required_job_id,
            required_job_groups=args.required_job_group,
            project_root=PROJECT_ROOT,
            now=now,
        )
        incremental_refresh = bool(
            args.allow_incremental_refresh
            and refresh_state["has_current_snapshot"]
            and source_changed
            and not preflight.active_job_ids
        )
        if refresh_state["has_current_snapshot"] and source_changed and preflight.active_job_ids:
            print(
                "SNAPSHOT_WAITING: Top Hits detected updated scanner inputs; "
                f"waiting for active source jobs {', '.join(preflight.active_job_ids)}."
            )
            return 0
        if not preflight.ready and not incremental_refresh:
            print(
                "SNAPSHOT_WAITING: Top Hits retained previous snapshot; "
                f"waiting for {', '.join(preflight.pending_job_ids)} for {preflight.target_date}."
            )
            return 0
        if incremental_refresh and not preflight.ready:
            print("Top Hits incremental refresh: updating the completed snapshot from changed scanner inputs.")
        payload = service.persist_scanner_top_hits_snapshot(now=now, board_payload=board_payload)
    if payload is None:
        print("Top Hits snapshot skipped: database is not configured.")
        return 1
    print(f"Top Hits snapshot persisted: {len(payload.get('rows') or [])} overlapping tickers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
