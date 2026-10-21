"""One readiness gate for scheduled and manual Top Hits publication."""
from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from zoneinfo import ZoneInfo

from .snapshot_preflight import check_required_scheduled_jobs
from ...market_data_access import load_daily_bars_frame_from_db

_NEW_YORK = ZoneInfo("America/New_York")


@contextmanager
def _snapshot_build_lock(lock_path: Path):
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
    parsed = _timestamp(latest_source_at)
    return bool(parsed and debounce_seconds > 0 and (now - parsed).total_seconds() < debounce_seconds)


def _timestamp(value: str) -> dt.datetime | None:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.replace(tzinfo=dt.timezone.utc) if parsed.tzinfo is None else parsed


def _configured_options(project_root: Path) -> dict[str, Any]:
    try:
        payload = json.loads((project_root / "config" / "scheduled_jobs.json").read_text(encoding="utf-8"))
        for job in payload.get("jobs", []):
            if job.get("action_id") == "build_scanner_top_hits_snapshot":
                return dict(job.get("options") or {})
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return {}


def refresh_scanner_top_hits(service: Any, *, project_root: Path, now: dt.datetime,
                            options: dict[str, Any] | None = None) -> dict[str, str]:
    """Retain the previous publication until today's post-close inputs are ready."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    local_now = now.astimezone(_NEW_YORK)
    target = local_now.date()
    status_dir = service.repository.artifacts_dir / "status"
    configured = _configured_options(project_root)
    resolved = {**configured, **(options or {})}
    for key in ("required_job_ids", "required_job_groups"):
        resolved[key] = list(dict.fromkeys([*(configured.get(key) or []), *((options or {}).get(key) or [])]))

    def outcome(status: str, message: str) -> dict[str, str]:
        result = {"status": status, "target_date": target.isoformat(), "message": message,
                  "checked_at": now.astimezone(dt.timezone.utc).isoformat()}
        status_dir.mkdir(parents=True, exist_ok=True)
        # Separate from the worker-owned status file, which treats a deliberate wait as exit 0.
        with tempfile.NamedTemporaryFile(mode="w", dir=status_dir, suffix=".tmp", delete=False, encoding="utf-8") as stream:
            json.dump(result, stream)
            temporary = stream.name
        os.replace(temporary, status_dir / "scanner_top_hits_refresh.json")
        return result

    if target.weekday() >= 5 or local_now.time() < dt.time(16):
        return outcome("waiting", f"Waiting for a completed trading session on {target}; retaining the previous Top Hits snapshot.")
    required_ids = resolved.get("required_job_ids") or []
    required_groups = resolved.get("required_job_groups") or []
    if not required_ids and not required_groups:
        return outcome("waiting", f"Waiting for configured snapshot prerequisites for {target}; retaining the previous Top Hits snapshot.")
    with _snapshot_build_lock(status_dir / "build_scanner_top_hits_snapshot.lock") as acquired:
        if not acquired:
            return outcome("waiting", f"Waiting for another Top Hits build for {target} to finish.")
        preflight = check_required_scheduled_jobs(status_dir=status_dir, required_job_ids=required_ids,
            required_job_groups=required_groups, project_root=project_root, now=now)
        if not preflight.ready:
            return outcome("waiting", f"Waiting for {target} prerequisites: {', '.join(preflight.pending_job_ids)}; retaining the previous Top Hits snapshot.")
        benchmark = load_daily_bars_frame_from_db(service.benchmark_ticker, target, target, database_url=service.database_url)
        if benchmark is None or benchmark.empty:
            return outcome("waiting", f"Waiting for the benchmark session bar dated {target}; retaining the previous Top Hits snapshot.")
        board = service.get_scanner_board(now=now, target_date=target)
        # Do not mistake pre-close/ad-hoc scanner runs for the finished daily batch.
        post_close_daily_source = any(
            str(card.get("timeframe") or "").lower() != "weekly"
            and str(card.get("sort_date") or "") == target.isoformat()
            and (card.get("has_run") or card.get("available"))
            and (captured := _timestamp(str(card.get("captured_at") or ""))) is not None
            and captured.astimezone(_NEW_YORK).date() == target
            and captured.astimezone(_NEW_YORK).time() >= dt.time(16)
            for card in board.get("cards", []) if isinstance(card, dict)
        )
        if not post_close_daily_source:
            return outcome("waiting", f"Waiting for post-close daily scanner results dated {target}; retaining the previous Top Hits snapshot.")
        state = service.get_scanner_top_hits_snapshot_refresh_state(now=now, board_payload=board)
        if resolved.get("skip_if_current", True) and state["has_current_snapshot"] and not state["source_changed"]:
            return outcome("current", f"Top Hits for {target} is unchanged; the saved snapshot already reflects these scanner inputs.")
        if state["source_changed"] and _is_within_debounce_window(latest_source_at=str(state["latest_source_at"]),
                now=now, debounce_seconds=max(0, int(resolved.get("debounce_seconds", 300)))):
            return outcome("waiting", f"Waiting for updated {target} scanner inputs to settle (debounce); retaining the previous Top Hits snapshot.")
        try:
            payload = service.persist_scanner_top_hits_snapshot(now=now, board_payload=board)
            if payload is None:
                raise RuntimeError("Top Hits snapshot cannot be saved: database is not configured.")
        except Exception:
            outcome("failed", f"Top Hits rebuild failed for {target}; check the job log.")
            raise
        return outcome("rebuilt", f"Top Hits snapshot rebuilt for {target}: {len(payload.get('rows') or [])} overlapping tickers.")
