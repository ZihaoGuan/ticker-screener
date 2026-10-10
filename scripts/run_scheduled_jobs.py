#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
import sys
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService
from src.webapp.config import load_webapp_config


ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
STATUS_DIR = ARTIFACTS_DIR / "status"
STATE_FILE = STATUS_DIR / "scheduler-state.json"
_PERSISTED_SCREEN_RUN_PATTERN = re.compile(r"Persisted screen run id=(\d+)")
_SNAPSHOT_WAITING_PATTERN = re.compile(r"^SNAPSHOT_WAITING:\s*(.+)$", re.MULTILINE)
_SNAPSHOT_CURRENT_PATTERN = re.compile(r"^SNAPSHOT_CURRENT:\s*(.+)$", re.MULTILINE)


def _load_state() -> dict[str, str]:
    if not STATE_FILE.exists():
        return {}
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(payload: dict[str, str]) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_scheduler_status(
    *,
    job_id: str,
    job_label: str,
    status: str,
    message: str,
    artifact_file: str = "",
    persisted_to_db: bool | None = None,
    screen_run_id: int | None = None,
    persistence_message: str | None = None,
) -> None:
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    status_path = STATUS_DIR / f"{job_id}.json"
    payload = {
        "job_id": job_id,
        "job_label": job_label,
        "status": status,
        "last_started_at": None,
        "last_finished_at": None,
        "exit_code": None,
        "log_file": "",
        "artifact_file": artifact_file or None,
        "message": message,
        "persisted_to_db": persisted_to_db,
        "screen_run_id": screen_run_id,
        "persistence_message": persistence_message,
    }
    tmp_path = status_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(status_path)


def _update_scheduler_persistence_status(
    *,
    job_id: str,
    persisted_to_db: bool | None,
    screen_run_id: int | None,
    persistence_message: str,
) -> None:
    status_path = STATUS_DIR / f"{job_id}.json"
    if not status_path.exists():
        return
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    payload["persisted_to_db"] = persisted_to_db
    payload["screen_run_id"] = screen_run_id
    payload["persistence_message"] = persistence_message
    tmp_path = status_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(status_path)


def _sync_scheduler_persistence_from_status(job_id: str) -> None:
    status_path = STATUS_DIR / f"{job_id}.json"
    if not status_path.exists():
        return
    try:
        payload = json.loads(status_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    status = str(payload.get("status") or "")
    if status in {"queued", "running"}:
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=None,
            screen_run_id=None,
            persistence_message="Persistence pending.",
        )
        return
    log_file = str(payload.get("log_file") or "").strip()
    if status == "success" and log_file:
        try:
            log_text = Path(log_file).read_text(encoding="utf-8")
        except Exception:
            log_text = ""
        waiting_match = _SNAPSHOT_WAITING_PATTERN.search(log_text)
        if waiting_match:
            payload["status"] = "waiting"
            payload["message"] = waiting_match.group(1).strip()
            payload["persisted_to_db"] = None
            payload["screen_run_id"] = None
            payload["persistence_message"] = "Snapshot build deferred; prior completed snapshot remains active."
            tmp_path = status_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            tmp_path.replace(status_path)
            return
        current_match = _SNAPSHOT_CURRENT_PATTERN.search(log_text)
        if current_match:
            payload["message"] = current_match.group(1).strip()
            payload["persistence_message"] = "Snapshot is already current; no rebuild was needed."
            tmp_path = status_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            tmp_path.replace(status_path)
            return
    if status != "success":
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=False,
            screen_run_id=None,
            persistence_message="Job failed before DB persistence completed.",
        )
        return
    log_file = str(payload.get("log_file") or "").strip()
    if not log_file:
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=False,
            screen_run_id=None,
            persistence_message="Missing log file; could not confirm DB persistence.",
        )
        return
    log_path = Path(log_file)
    if not log_path.exists():
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=False,
            screen_run_id=None,
            persistence_message="Log file missing; could not confirm DB persistence.",
        )
        return
    try:
        log_text = log_path.read_text(encoding="utf-8")
    except Exception:
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=False,
            screen_run_id=None,
            persistence_message="Could not read log file to confirm DB persistence.",
        )
        return
    match = _PERSISTED_SCREEN_RUN_PATTERN.search(log_text)
    if match:
        screen_run_id = int(match.group(1))
        _update_scheduler_persistence_status(
            job_id=job_id,
            persisted_to_db=True,
            screen_run_id=screen_run_id,
            persistence_message=f"Persisted screen run id={screen_run_id}.",
        )
        return
    _update_scheduler_persistence_status(
        job_id=job_id,
        persisted_to_db=False,
        screen_run_id=None,
        persistence_message="No persisted screen run id found in job log.",
    )


def _matches_field(field: str, value: int, *, minimum: int, maximum: int) -> bool:
    if field == "*":
        return True
    for part in field.split(","):
        if "/" in part:
            base, step_text = part.split("/", 1)
            step = int(step_text)
            if step <= 0:
                continue
            if base == "*":
                start = minimum
                end = maximum
            elif "-" in base:
                start_text, end_text = base.split("-", 1)
                start = int(start_text)
                end = int(end_text)
            else:
                start = int(base)
                end = maximum
            if start <= value <= end and (value - start) % step == 0:
                return True
            continue
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            if int(start_text) <= value <= int(end_text):
                return True
            continue
        if int(part) == value:
            return True
    return False


def _cron_matches(cron_expr: str, current: dt.datetime) -> bool:
    minute, hour, day, month, weekday = cron_expr.split()
    cron_weekday = (current.weekday() + 1) % 7
    return (
        _matches_field(minute, current.minute, minimum=0, maximum=59)
        and _matches_field(hour, current.hour, minimum=0, maximum=23)
        and _matches_field(day, current.day, minimum=1, maximum=31)
        and _matches_field(month, current.month, minimum=1, maximum=12)
        and _matches_field(weekday, cron_weekday, minimum=0, maximum=7)
    )


def _artifact_path_for_job(job: dict[str, object], *, local_now: dt.datetime) -> str:
    action_id = str(job.get("action_id") or "")
    date_label = local_now.date().isoformat()
    if action_id == "weekly_rs":
        return str(PROJECT_ROOT / "artifacts" / "watchlists" / f"weekly_rs_new_high_{date_label}.json")
    return ""


def _resolve_template_value(value: object, *, local_now: dt.datetime) -> object:
    if isinstance(value, str):
        replacements = {
            "{{local_date}}": local_now.date().isoformat(),
            "{{local_date_minus_7}}": (local_now.date() - dt.timedelta(days=7)).isoformat(),
            "{{local_date_minus_14}}": (local_now.date() - dt.timedelta(days=14)).isoformat(),
            "{{local_date_minus_100}}": (local_now.date() - dt.timedelta(days=100)).isoformat(),
            "{{local_date_minus_465}}": (local_now.date() - dt.timedelta(days=465)).isoformat(),
            "{{local_date_plus_7}}": (local_now.date() + dt.timedelta(days=7)).isoformat(),
            "{{local_date_plus_14}}": (local_now.date() + dt.timedelta(days=14)).isoformat(),
        }
        resolved = value
        for token, replacement in replacements.items():
            resolved = resolved.replace(token, replacement)
        return resolved
    if isinstance(value, list):
        return [_resolve_template_value(item, local_now=local_now) for item in value]
    if isinstance(value, dict):
        return {str(key): _resolve_template_value(item, local_now=local_now) for key, item in value.items()}
    return value


def main() -> int:
    web_config = load_webapp_config()
    run_service = RunService(project_root=PROJECT_ROOT, database_url=web_config.database_url)
    schedule_service = ScheduledJobService(project_root=PROJECT_ROOT, run_service=run_service)
    recovery = run_service.recover_remote_jobs()
    if recovery["requeued"]:
        print(f"remote recovery: requeued={recovery['requeued']}")
    actions = {
        action.action_id: action
        for action in run_service._actions.values()
    }
    state = _load_state()
    state_changed = False
    time_snapshots: dict[str, dt.datetime] = {}

    for job in schedule_service.list_jobs():
        if not job.get("enabled"):
            continue
        action_id = str(job.get("action_id") or "")
        action = actions.get(action_id)
        if action is None:
            continue
        cron_tz = str(job.get("cron_tz") or "America/New_York")
        local_now = time_snapshots.get(cron_tz)
        if local_now is None:
            local_now = dt.datetime.now(ZoneInfo(cron_tz)).replace(second=0, microsecond=0)
            time_snapshots[cron_tz] = local_now
        if not _cron_matches(str(job.get("cron_expr") or ""), local_now):
            continue
        slot_key = f"{job['job_id']}@{local_now.isoformat()}"
        if state.get(str(job["job_id"])) == slot_key:
            continue

        artifact_path = _artifact_path_for_job(job, local_now=local_now)
        resolved_options = _resolve_template_value(job.get("options") or {}, local_now=local_now)
        queue_options = dict(resolved_options) if isinstance(resolved_options, dict) else {}
        queue_options.update(
            {
                "execution_mode": "remote",
                "scheduled_job_id": str(job["job_id"]),
                "scheduled_job_label": str(job["job_label"]),
                "scheduled_artifact_file": artifact_path,
            }
        )
        remote_job_id = run_service.launch(action_id, options=queue_options, trigger_source="scheduler")
        _write_scheduler_status(
            job_id=str(job["job_id"]),
            job_label=str(job["job_label"]),
            status="queued",
            message=f"Queued worker job {remote_job_id}.",
            artifact_file=artifact_path,
            persisted_to_db=None,
            screen_run_id=None,
            persistence_message="Waiting for worker completion.",
        )
        state[str(job["job_id"])] = slot_key
        state_changed = True
        print(f"queued scheduled job {job['job_id']} ({action_id}) at {local_now.isoformat()} {cron_tz}")

    if state_changed:
        _save_state(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
