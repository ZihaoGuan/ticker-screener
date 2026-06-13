#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
import time
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
DEPLOY_DIR = PROJECT_ROOT / "deploy"
WRAPPER_SCRIPT = PROJECT_ROOT / "scripts" / "run_with_status.sh"


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
    }
    tmp_path = status_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(status_path)


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
    run_service = RunService(project_root=PROJECT_ROOT, database_url=load_webapp_config().database_url)
    schedule_service = ScheduledJobService(project_root=PROJECT_ROOT, run_service=run_service)
    max_parallel_jobs = schedule_service.get_max_parallel_jobs()
    recovery = run_service.recover_remote_jobs(max_local_fallbacks=max_parallel_jobs)
    if recovery["requeued"] or recovery["local_fallback_started"]:
        print(
            f"remote recovery: requeued={recovery['requeued']} "
            f"local_fallback_started={recovery['local_fallback_started']}"
        )
    actions = {
        action.action_id: action
        for action in run_service._actions.values()
        if action.action_id != "sync_postgres_market_data"
    }
    state = _load_state()
    any_run = False
    time_snapshots: dict[str, dt.datetime] = {}
    due_runs: list[tuple[dict[str, object], str, dt.datetime, dict[str, str], list[str], Path]] = []

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

        env = dict(os.environ)
        artifact_path = _artifact_path_for_job(job, local_now=local_now)
        if artifact_path:
            env["TICKER_SCREENER_STATUS_ARTIFACT"] = artifact_path
        resolved_options = _resolve_template_value(job.get("options") or {}, local_now=local_now)
        if isinstance(resolved_options, dict) and str(resolved_options.get("execution_mode") or "local").strip().lower() == "remote":
            job_id = run_service.launch(action_id, options=resolved_options, trigger_source="scheduler")
            _write_scheduler_status(
                job_id=str(job["job_id"]),
                job_label=str(job["job_label"]),
                status="queued",
                message=f"Queued remote job {job_id} for worker execution.",
                artifact_file=str(env.get("TICKER_SCREENER_STATUS_ARTIFACT") or ""),
            )
            print(f"queued scheduled remote job {job['job_id']} ({action_id}) at {local_now.isoformat()} {cron_tz}")
            continue
        command_tail = run_service.build_command(action_id, resolved_options if isinstance(resolved_options, dict) else {})
        if os.path.exists("/.dockerenv"):
            command = [
                str(WRAPPER_SCRIPT),
                str(job["job_id"]),
                str(job["job_label"]),
                "--",
                *command_tail,
            ]
            run_cwd = PROJECT_ROOT
        else:
            command = [
                str(WRAPPER_SCRIPT),
                str(job["job_id"]),
                str(job["job_label"]),
                "--",
                "docker-compose",
                "exec",
                "-T",
                "web",
                "python",
                *command_tail[1:],
            ]
            run_cwd = DEPLOY_DIR
        due_runs.append((job, slot_key, local_now, env, command, run_cwd))
        state[str(job["job_id"])] = slot_key
        any_run = True

    if any_run:
        _save_state(state)
        pending_runs = list(due_runs)
        running_processes: list[tuple[dict[str, object], subprocess.Popen[bytes]]] = []
        exit_code = 0

        for index, (job, _, _, env, _, _) in enumerate(pending_runs):
            if index >= max_parallel_jobs:
                _write_scheduler_status(
                    job_id=str(job["job_id"]),
                    job_label=str(job["job_label"]),
                    status="queued",
                    message="Queued behind other scheduled jobs for this time slot.",
                    artifact_file=str(env.get("TICKER_SCREENER_STATUS_ARTIFACT") or ""),
                )

        while pending_runs or running_processes:
            while pending_runs and len(running_processes) < max_parallel_jobs:
                job, _, local_now, env, command, run_cwd = pending_runs.pop(0)
                action_id = str(job.get("action_id") or "")
                cron_tz = str(job.get("cron_tz") or "America/New_York")
                print(f"running scheduled job {job['job_id']} ({action_id}) at {local_now.isoformat()} {cron_tz}")
                process = subprocess.Popen(command, cwd=run_cwd, env=env)
                running_processes.append((job, process))

            next_running: list[tuple[dict[str, object], subprocess.Popen[bytes]]] = []
            for job, process in running_processes:
                return_code = process.poll()
                if return_code is None:
                    next_running.append((job, process))
                    continue
                if return_code != 0:
                    print(f"scheduled job {job['job_id']} exited with {return_code}")
                    exit_code = return_code if exit_code == 0 else exit_code
            running_processes = next_running
            if running_processes:
                time.sleep(1)
        return exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
