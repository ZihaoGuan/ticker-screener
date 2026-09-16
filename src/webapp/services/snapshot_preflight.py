from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


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
    clean_ids = tuple(dict.fromkeys(str(item).strip() for item in required_job_ids if str(item).strip()))
    pending: list[str] = []
    for job_id in clean_ids:
        payload = _load_status(status_dir / f"{job_id}.json")
        if not _completed_successfully_for_date(payload, target_date=target_date):
            pending.append(job_id)
    return SnapshotPreflight(
        target_date=target_date,
        required_job_ids=clean_ids,
        pending_job_ids=tuple(pending),
    )


def _load_status(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


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
