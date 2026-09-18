from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .run_service import RunService


_NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class SnapshotPreflight:
    target_date: str
    required_job_ids: tuple[str, ...]
    pending_job_ids: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.pending_job_ids


def check_required_scheduled_jobs(
    *,
    status_dir: Path,
    required_job_ids: list[str] | tuple[str, ...],
    required_job_groups: list[str] | tuple[str, ...] = (),
    project_root: Path | None = None,
    now: dt.datetime | None = None,
) -> SnapshotPreflight:
    """Require successful prerequisite jobs completed on today's NY market date.

    Status files are deliberately used here instead of a request-path database
    query: the scheduler owns them, and a missing/failed prerequisite must keep
    the prior completed API snapshot intact.
    """
    reference_now = now or dt.datetime.now(dt.timezone.utc)
    if reference_now.tzinfo is None:
        reference_now = reference_now.replace(tzinfo=dt.timezone.utc)
    target_date = reference_now.astimezone(_NEW_YORK).date().isoformat()
    clean_ids = list(dict.fromkeys(str(item).strip() for item in required_job_ids if str(item).strip()))
    for group_name in dict.fromkeys(str(item).strip() for item in required_job_groups if str(item).strip()):
        group_job_ids = _resolve_required_job_group(project_root=project_root, group_name=group_name)
        clean_ids.extend(group_job_ids or [f"group:{group_name}"])
    clean_ids = list(dict.fromkeys(clean_ids))
    pending: list[str] = []
    for job_id in clean_ids:
        payload = _load_status(status_dir / f"{job_id}.json")
        if not _completed_successfully_for_date(payload, target_date=target_date):
            pending.append(job_id)
    return SnapshotPreflight(
        target_date=target_date,
        required_job_ids=tuple(clean_ids),
        pending_job_ids=tuple(pending),
    )


def _load_status(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _resolve_required_job_group(*, project_root: Path | None, group_name: str) -> list[str]:
    """Resolve scheduler-owned prerequisite groups to concrete status files."""
    if group_name != "daily_scanner_batch" or project_root is None:
        return []
    try:
        payload = json.loads((project_root / "config" / "scheduled_jobs.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    jobs = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(jobs, list):
        return []
    run_service = RunService(project_root=project_root)
    job_ids: list[str] = []
    for job in jobs:
        if not isinstance(job, dict) or not bool(job.get("enabled", True)):
            continue
        if str(job.get("cron_tz") or "America/New_York") != "America/New_York":
            continue
        if str(job.get("cron_expr") or "").strip() != "0 18 * * 1-5":
            continue
        if run_service._job_type_for_action(str(job.get("action_id") or "")) != "screen_run":
            continue
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            job_ids.append(job_id)
    return list(dict.fromkeys(job_ids))


def _completed_successfully_for_date(payload: dict[str, Any] | None, *, target_date: str) -> bool:
    if not payload or str(payload.get("status") or "").lower() != "success":
        return False
    finished_at = str(payload.get("last_finished_at") or "").strip()
    if not finished_at:
        return False
    try:
        parsed = dt.datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(_NEW_YORK).date().isoformat() == target_date
