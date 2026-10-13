from __future__ import annotations

import json
import re
import statistics
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.config import load_app_config
from src.screener_catalog import build_screener_catalog

from .run_service import RunService


_CRON_FIELD_PATTERN = re.compile(r"^[\d\*,/\-]+$")
_COMMON_TIMEZONES = (
    "America/New_York",
    "Pacific/Auckland",
    "UTC",
)
_DEFAULT_MAX_PARALLEL_JOBS = 5
_DEFAULT_QUEUE_PRIORITY = 50
_DEFAULT_WORKER_POOL = "any"
_POLICY_UNSET = object()


class ScheduledJobService:
    def __init__(self, *, project_root: Path, run_service: RunService) -> None:
        self.project_root = project_root
        self.run_service = run_service
        self.artifacts_dir = run_service.artifacts_dir
        self.config_path = project_root / "config" / "scheduled_jobs.json"

    def get_context(self) -> dict[str, Any]:
        jobs = self.list_jobs()
        self._attach_queue_states(jobs)
        return {
            "jobs": jobs,
            "available_actions": self._available_actions(jobs),
            "common_timezones": list(_COMMON_TIMEZONES),
            "scheduler_command": f"cd {self.project_root / 'deploy'} && {self.project_root / 'scripts' / 'run_scheduled_jobs.py'}",
            "max_parallel_jobs": self.get_max_parallel_jobs(),
        }

    def _attach_queue_states(self, jobs: list[dict[str, Any]]) -> None:
        """Attach the queue's current truth, rather than inferring it from status files.

        Status files are useful history, but are not authoritative while a remote
        job is waiting for dependencies or for a compatible worker.
        """
        repository = getattr(self.run_service, "history_repository", None)
        list_runs = getattr(repository, "list_remote_job_runs", None)
        list_workers = getattr(repository, "list_remote_workers", None)
        if not callable(list_runs):
            return
        try:
            runs = list_runs(limit=max(100, len(jobs) * 12))
            workers = list_workers() if callable(list_workers) else []
        except Exception:
            return
        by_job_id = {str(job["job_id"]): job for job in jobs}
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for run in runs:
            payload = run.get("request_payload") if isinstance(run.get("request_payload"), dict) else {}
            options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
            schedule_id = str(options.get("scheduled_job_id") or "").strip()
            key = str(payload.get("dependency_key") or "").strip()
            if not schedule_id or not key:
                continue
            pair = (schedule_id, key)
            if pair not in latest:
                latest[pair] = run

        for job in jobs:
            local_now = datetime.now(ZoneInfo(str(job["cron_tz"])))
            key = local_now.date().isoformat()
            current = latest.get((str(job["job_id"]), key))
            execution_mode = str((job.get("options") or {}).get("execution_mode") or "remote").lower()
            dependencies = [by_job_id[item] for item in job.get("depends_on_job_ids", []) if item in by_job_id]
            dependency_runs = [latest.get((str(item["job_id"]), key)) for item in dependencies]
            failed = next((run for run in dependency_runs if run and str(run.get("status")) in {"failed", "interrupted"}), None)
            unfinished = next((item for item, run in zip(dependencies, dependency_runs) if not run or str(run.get("status")) != "success"), None)
            state = "independent" if not dependencies else "ready"
            blocking_job_id = ""
            if current and str(current.get("status")) == "running":
                state = "running"
            elif current and str(current.get("status")) == "failed":
                result = current.get("result_payload") if isinstance(current.get("result_payload"), dict) else {}
                state = "expired" if "daily cutoff" in str(result.get("message") or "").lower() else "blocked"
            elif failed:
                state = "blocked"
                blocking_job_id = str((failed.get("request_payload") or {}).get("options", {}).get("scheduled_job_id") or "")
            elif unfinished:
                state = "waiting"
                blocking_job_id = str(unfinished["job_id"])
            elif current and str(current.get("status")) == "queued" and execution_mode != "github" and not _has_compatible_worker(workers, str(job.get("worker_pool") or "any")):
                state = "no-worker"
            elif current and str(current.get("status")) == "queued":
                state = "ready"
            job["queue_state"] = {
                "state": state,
                "job_run_id": current.get("id") if current else None,
                "blocking_job_id": blocking_job_id or None,
            }

    def get_action_activity(self, *, run_jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return card-safe activity for every configured or recently run action."""
        configured_jobs = self.list_jobs()
        by_action: dict[str, dict[str, Any]] = {}
        for job in configured_jobs:
            action_id = str(job["action_id"])
            activity = by_action.setdefault(action_id, _empty_action_activity(action_id))
            activity["scheduled_count"] += 1
            status = self._scheduled_status(job)
            entry = _activity_entry(status, estimated_duration_seconds=job.get("estimated_duration_seconds"))
            if entry is None:
                continue
            if entry["status"] in {"queued", "running"}:
                activity["scheduled_current"] = _newer_activity(activity["scheduled_current"], entry)
            else:
                activity["scheduled_last"] = _newer_activity(activity["scheduled_last"], entry)

        for job in run_jobs:
            action_id = str(job.get("action_id") or "").strip()
            if not action_id:
                continue
            activity = by_action.setdefault(action_id, _empty_action_activity(action_id))
            entry = _activity_entry(job)
            if entry is None:
                continue
            target = "scheduled" if str(job.get("trigger_source") or "manual") == "scheduler" else "adhoc"
            state_key = f"{target}_{'current' if entry['status'] in {'queued', 'running'} else 'last'}"
            activity[state_key] = _newer_activity(activity[state_key], entry)
        return sorted(by_action.values(), key=lambda item: str(item["action_id"]))

    def list_jobs(self) -> list[dict[str, Any]]:
        payload = self._load_jobs()
        jobs = payload.get("jobs", [])
        if not isinstance(jobs, list):
            return []
        normalized: list[dict[str, Any]] = []
        for item in jobs:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "job_id": str(item.get("job_id") or "").strip(),
                    "job_label": str(item.get("job_label") or "").strip(),
                    "action_id": str(item.get("action_id") or "").strip(),
                    "cron_expr": str(item.get("cron_expr") or "").strip(),
                    "cron_tz": str(item.get("cron_tz") or "America/New_York").strip() or "America/New_York",
                    "enabled": bool(item.get("enabled", True)),
                    "options": item.get("options") if isinstance(item.get("options"), dict) else {},
                    "priority": _normalize_queue_priority(item.get("priority")),
                    "depends_on_job_ids": _normalize_dependency_job_ids(item.get("depends_on_job_ids")),
                    "worker_pool": _normalize_worker_pool(item.get("worker_pool")),
                }
            )
        jobs = [item for item in normalized if item["job_id"] and item["action_id"] and item["cron_expr"]]
        estimates = {job["job_id"]: self._estimate_job_duration(job["job_id"]) for job in jobs}
        for job in jobs:
            job.update(estimates[job["job_id"]])
        return jobs

    def upsert_job(
        self,
        *,
        job_id: str,
        job_label: str,
        action_id: str,
        cron_expr: str,
        cron_tz: str,
        enabled: bool,
        options: dict[str, Any] | None = None,
        priority: object = _POLICY_UNSET,
        depends_on_job_ids: object = _POLICY_UNSET,
        worker_pool: object = _POLICY_UNSET,
    ) -> dict[str, Any]:
        clean_job_id = _normalize_job_id(job_id)
        clean_job_label = str(job_label or "").strip()
        clean_action_id = str(action_id or "").strip()
        clean_cron_expr = _normalize_cron_expr(cron_expr)
        clean_cron_tz = str(cron_tz or "").strip() or "America/New_York"
        if not clean_job_id:
            raise ValueError("job_id is required.")
        if not clean_job_label:
            raise ValueError("job_label is required.")
        if clean_action_id not in {item["id"] for item in self._available_actions()}:
            raise ValueError(f"Unknown action_id: {clean_action_id}")
        _validate_cron_expr(clean_cron_expr)
        clean_options = dict(options or {})
        action = self.run_service._actions.get(clean_action_id)
        if action is None:
            raise ValueError(f"Unknown action_id: {clean_action_id}")
        normalized_options = self.run_service._normalize_options(action, clean_options)
        # Preserve action-specific schedule options verbatim (some are not CLI
        # fields), while storing this scheduler-owned list in its canonical form.
        for option_key in ("required_job_ids", "required_job_groups"):
            if option_key in normalized_options:
                clean_options[option_key] = normalized_options[option_key]

        payload = self._load_jobs()
        jobs = [item for item in payload.get("jobs", []) if isinstance(item, dict)]
        existing_job = next(
            (item for item in jobs if str(item.get("job_id") or "").strip() == clean_job_id),
            None,
        )
        clean_priority = (
            _normalize_queue_priority((existing_job or {}).get("priority"))
            if priority is _POLICY_UNSET
            else _validate_queue_priority(priority)
        )
        clean_dependencies = (
            _normalize_dependency_job_ids((existing_job or {}).get("depends_on_job_ids"))
            if depends_on_job_ids is _POLICY_UNSET
            else _validate_dependency_job_ids(depends_on_job_ids)
        )
        clean_worker_pool = (
            _normalize_worker_pool((existing_job or {}).get("worker_pool"))
            if worker_pool is _POLICY_UNSET
            else _validate_worker_pool(worker_pool)
        )
        next_job = {
            "job_id": clean_job_id,
            "job_label": clean_job_label,
            "action_id": clean_action_id,
            "cron_expr": clean_cron_expr,
            "cron_tz": clean_cron_tz,
            "enabled": bool(enabled),
            "options": clean_options,
            # Older editors do not submit queue policy. Preserve it in that case;
            # callers that do submit it receive strict validation below.
            "priority": clean_priority,
            "depends_on_job_ids": clean_dependencies,
            "worker_pool": clean_worker_pool,
        }
        replaced = False
        next_jobs: list[dict[str, Any]] = []
        for item in jobs:
            if str(item.get("job_id") or "").strip() == clean_job_id:
                next_jobs.append(next_job)
                replaced = True
            else:
                next_jobs.append(item)
        if not replaced:
            next_jobs.append(next_job)
        _validate_schedule_dependencies(next_jobs)
        payload["jobs"] = sorted(next_jobs, key=lambda item: str(item.get("job_label") or item.get("job_id") or ""))
        self._write_jobs(payload)
        return next_job

    def delete_job(self, *, job_id: str) -> None:
        clean_job_id = _normalize_job_id(job_id)
        if not clean_job_id:
            raise ValueError("job_id is required.")
        payload = self._load_jobs()
        jobs = [item for item in payload.get("jobs", []) if isinstance(item, dict)]
        dependents = [
            str(item.get("job_label") or item.get("job_id") or "")
            for item in jobs
            if clean_job_id in _normalize_dependency_job_ids(item.get("depends_on_job_ids"))
        ]
        if dependents:
            raise ValueError(f"Cannot delete {clean_job_id}; required by: {', '.join(dependents)}.")
        next_jobs = [item for item in jobs if str(item.get("job_id") or "").strip() != clean_job_id]
        payload["jobs"] = next_jobs
        self._write_jobs(payload)

    def run_now(self, *, job_id: str) -> dict[str, Any]:
        clean_job_id = _normalize_job_id(job_id)
        job = next((item for item in self.list_jobs() if item["job_id"] == clean_job_id), None)
        if job is None:
            raise ValueError("Scheduled job was not found.")
        if not job["enabled"]:
            raise ValueError("Cannot run a disabled schedule.")
        local_now = datetime.now(ZoneInfo(str(job["cron_tz"]))).replace(second=0, microsecond=0)
        remote_job_id = self.enqueue_job(job=job, local_now=local_now, trigger_source="manual")
        return {"job_id": remote_job_id, "scheduled_job_id": clean_job_id}

    def best_effort_skip_reason(self, job: dict[str, Any]) -> str:
        """Return why a low-priority scheduled occurrence should be dropped.

        Top Hits runs every five minutes and is derived data, so retaining a prior
        snapshot is preferable to building a queue while workers are occupied.
        Manual runs deliberately bypass this admission check.
        """
        if str(job.get("action_id") or "") != "build_scanner_top_hits_snapshot":
            return ""
        options = job.get("options") if isinstance(job.get("options"), dict) else {}
        if str(options.get("execution_mode") or "remote").strip().lower() != "remote":
            return ""
        repository = getattr(self.run_service, "history_repository", None)
        has_active = getattr(repository, "has_active_remote_job_run", None)
        list_workers = getattr(repository, "list_remote_workers", None)
        if not callable(has_active) or not callable(list_workers):
            return "worker capacity is unavailable"
        try:
            if has_active(action_id=str(job["action_id"]), code_version=self.run_service.code_version):
                return "another Top Hits snapshot is already queued or running"
            workers = list_workers()
        except Exception:
            return "worker capacity is unavailable"
        if not _has_idle_compatible_worker(
            workers,
            str(job.get("worker_pool") or "any"),
            code_version=self.run_service.code_version,
        ):
            return "no compatible worker is idle"
        return ""

    def enqueue_job(self, *, job: dict[str, Any], local_now: datetime, trigger_source: str = "scheduler") -> str:
        action_id = str(job["action_id"])
        if action_id not in self.run_service._actions:
            raise ValueError(f"Unknown action_id: {action_id}")
        resolved_options = _resolve_template_value(job.get("options") or {}, local_now=local_now)
        queue_options = dict(resolved_options) if isinstance(resolved_options, dict) else {}
        artifact_path = _artifact_path_for_job(self.artifacts_dir, job, local_now=local_now)
        queue_options.setdefault("execution_mode", "remote")
        queue_options.update(
            {
                "scheduled_job_id": str(job["job_id"]),
                "scheduled_job_label": str(job["job_label"]),
                "scheduled_artifact_file": artifact_path,
            }
        )
        deadline = local_now.replace(hour=23, minute=59, second=59, microsecond=0)
        remote_job_id = self.run_service.launch(
            action_id,
            options=queue_options,
            trigger_source=trigger_source,
            queue_policy={
                "queue_priority": job.get("priority", _DEFAULT_QUEUE_PRIORITY),
                "dependency_job_ids": job.get("depends_on_job_ids", []),
                "dependency_key": local_now.date().isoformat(),
                "dependency_expires_at": deadline.isoformat(),
                "worker_pool": job.get("worker_pool", _DEFAULT_WORKER_POOL),
                "parallel_limit": self.get_max_parallel_jobs(),
            },
        )
        self._write_status(
            job_id=str(job["job_id"]),
            job_label=str(job["job_label"]),
            message=f"Queued worker job {remote_job_id}.",
            artifact_file=artifact_path,
        )
        return remote_job_id

    def _write_status(self, *, job_id: str, job_label: str, message: str, artifact_file: str) -> None:
        status_dir = self.artifacts_dir / "status"
        status_dir.mkdir(parents=True, exist_ok=True)
        status_path = status_dir / f"{job_id}.json"
        payload = {
            "job_id": job_id, "job_label": job_label, "status": "queued", "last_started_at": None,
            "last_finished_at": None, "exit_code": None, "log_file": "", "artifact_file": artifact_file or None,
            "message": message, "persisted_to_db": None, "screen_run_id": None,
            "persistence_message": "Waiting for worker completion.",
        }
        tmp_path = status_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(status_path)

    def get_max_parallel_jobs(self) -> int:
        payload = self._load_jobs()
        value = payload.get("max_parallel_jobs")
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return _DEFAULT_MAX_PARALLEL_JOBS
        return max(1, min(20, parsed))

    def update_max_parallel_jobs(self, value: int) -> int:
        parsed = int(value)
        if parsed < 1 or parsed > 20:
            raise ValueError("max_parallel_jobs must be between 1 and 20.")
        payload = self._load_jobs()
        payload["max_parallel_jobs"] = parsed
        self._write_jobs(payload)
        return parsed

    def _available_actions(self, jobs: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        filter_catalog = self.run_service._get_filter_catalog()
        screener_catalog = build_screener_catalog(load_app_config())
        required_inputs_by_action = {
            action_id: spec.required_inputs for action_id, spec in screener_catalog.items()
        }
        actions = self.run_service.list_actions() + [
            {
                "id": action.action_id,
                "label": action.label,
                "bias_group": action.bias_group,
                "bullish_subgroup": action.bullish_subgroup,
                "fields": [
                    {
                        "id": field.field_id,
                        "label": field.label,
                        "type": field.field_type,
                        "placeholder": field.placeholder,
                        "help_text": field.help_text,
                        "options": self.run_service._field_options(
                            field,
                            filter_catalog,
                        ),
                    }
                    for field in action.fields
                ],
            }
            for action in self.run_service._actions.values()
            if action.action_id == "sync_postgres_market_data"
        ]
        job_estimates = jobs if jobs is not None else self.list_jobs()
        estimates_by_action: dict[str, list[int]] = {}
        sample_counts_by_action: dict[str, int] = {}
        for job in job_estimates:
            duration = job.get("estimated_duration_seconds")
            if isinstance(duration, int) and duration > 0:
                action_id = str(job["action_id"])
                estimates_by_action.setdefault(action_id, []).append(duration)
                sample_counts_by_action[action_id] = sample_counts_by_action.get(action_id, 0) + int(job.get("estimate_sample_count") or 0)
        return [
            {
                "id": item["id"],
                "label": item["label"],
                "bias_group": item.get("bias_group") or "other",
                "bullish_subgroup": item.get("bullish_subgroup") or "",
                "fields": item.get("fields", []),
                "execution_modes": item.get("execution_modes") or list(self.run_service.execution_modes_for_action(str(item["id"]))),
                "requires_daily_bars": "daily_bars" in required_inputs_by_action.get(str(item["id"]), ()),
                **_summarize_durations(
                    estimates_by_action.get(str(item["id"]), []),
                    sample_count=sample_counts_by_action.get(str(item["id"]), 0),
                ),
            }
            for item in actions
        ]

    def _estimate_job_duration(self, job_id: str) -> dict[str, Any]:
        log_dir = self.artifacts_dir / "status" / "logs"
        if not log_dir.is_dir():
            return _summarize_durations([])
        active_log = self._active_log_file(job_id)
        durations: list[int] = []
        for log_path in sorted(log_dir.glob(f"{job_id}-*.log"), reverse=True)[:12]:
            if active_log and log_path == active_log:
                continue
            started_at = _started_at_from_log_name(job_id, log_path.name)
            if started_at is None:
                continue
            duration = int(log_path.stat().st_mtime - started_at.timestamp())
            if 0 < duration <= 12 * 60 * 60:
                durations.append(duration)
        return _summarize_durations(durations)

    def _active_log_file(self, job_id: str) -> Path | None:
        status_path = self.artifacts_dir / "status" / f"{job_id}.json"
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(status, dict) or str(status.get("status") or "").lower() not in {"queued", "running", "waiting"}:
            return None
        log_file = status.get("log_file")
        return Path(log_file) if isinstance(log_file, str) and log_file else None

    def _scheduled_status(self, job: dict[str, Any]) -> dict[str, Any] | None:
        status_path = self.artifacts_dir / "status" / f"{job['job_id']}.json"
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        payload = dict(payload)
        payload["job_id"] = str(payload.get("job_id") or job["job_id"])
        payload["label"] = str(payload.get("job_label") or job["job_label"])
        payload["started_at"] = str(payload.get("last_started_at") or "")
        payload["finished_at"] = str(payload.get("last_finished_at") or "")
        payload["success_count"] = int(payload.get("success_count") or 0)
        status = str(payload.get("status") or "unknown").lower()
        if status in {"queued", "running", "waiting"} and _is_stale_status(payload, estimated_duration_seconds=job.get("estimated_duration_seconds")):
            payload["status"] = "interrupted"
            payload["message"] = "No completion signal was recorded within the expected run window."
        return payload

    def _load_jobs(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return {"jobs": []}
        try:
            payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        except Exception:
            return {"jobs": []}
        return payload if isinstance(payload, dict) else {"jobs": []}

    def _write_jobs(self, payload: dict[str, Any]) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _started_at_from_log_name(job_id: str, filename: str) -> datetime | None:
    prefix = f"{job_id}-"
    if not filename.startswith(prefix) or not filename.endswith(".log"):
        return None
    try:
        return datetime.strptime(filename[len(prefix) : -4], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _summarize_durations(durations: list[int], *, sample_count: int | None = None) -> dict[str, Any]:
    if not durations:
        return {"estimated_duration_seconds": None, "estimate_sample_count": 0}
    return {
        "estimated_duration_seconds": int(round(statistics.median(durations))),
        "estimate_sample_count": sample_count if sample_count is not None else len(durations),
    }


def _empty_action_activity(action_id: str) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "adhoc_current": None,
        "adhoc_last": None,
        "scheduled_current": None,
        "scheduled_last": None,
        "scheduled_count": 0,
    }


def _activity_entry(payload: dict[str, Any] | None, *, estimated_duration_seconds: Any = None) -> dict[str, Any] | None:
    if not payload:
        return None
    status = str(payload.get("status") or "unknown").lower()
    if status == "waiting":
        status = "queued"
    if status not in {"queued", "running", "success", "failed", "cancelled", "interrupted"}:
        return None
    return {
        "job_id": str(payload.get("job_id") or ""),
        "label": str(payload.get("label") or payload.get("job_label") or ""),
        "status": status,
        "started_at": str(payload.get("started_at") or payload.get("last_started_at") or ""),
        "finished_at": str(payload.get("finished_at") or payload.get("last_finished_at") or ""),
        "success_count": int(payload.get("success_count") or 0),
        "screen_run_id": payload.get("screen_run_id"),
        "estimated_duration_seconds": estimated_duration_seconds if isinstance(estimated_duration_seconds, int) else None,
        "message": str(payload.get("message") or ""),
    }


def _newer_activity(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if current is None:
        return candidate
    current_time = str(current.get("finished_at") or current.get("started_at") or "")
    candidate_time = str(candidate.get("finished_at") or candidate.get("started_at") or "")
    return candidate if candidate_time >= current_time else current


def _is_stale_status(payload: dict[str, Any], *, estimated_duration_seconds: Any) -> bool:
    started_at = str(payload.get("last_started_at") or payload.get("started_at") or "")
    try:
        started = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    expected = estimated_duration_seconds if isinstance(estimated_duration_seconds, int) else 0
    threshold_seconds = max(30 * 60, expected * 3)
    return (datetime.now(UTC) - started.astimezone(UTC)).total_seconds() > threshold_seconds


def _normalize_job_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    return normalized.strip("_")


def _normalize_cron_expr(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_queue_priority(value: object) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError):
        return _DEFAULT_QUEUE_PRIORITY
    return min(100, max(0, priority))


def _normalize_dependency_job_ids(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted({job_id for item in value if (job_id := str(item or "").strip())})


def _normalize_worker_pool(value: object) -> str:
    pool = str(value or "").strip().lower()
    return pool if re.fullmatch(r"[a-z0-9_-]+", pool or "") else _DEFAULT_WORKER_POOL


def _validate_queue_priority(value: object) -> int:
    try:
        priority = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("priority must be between 0 and 100.") from exc
    if not 0 <= priority <= 100:
        raise ValueError("priority must be between 0 and 100.")
    return priority


def _validate_dependency_job_ids(value: object) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("depends_on_job_ids must be a list.")
    return _normalize_dependency_job_ids(value)


def _validate_worker_pool(value: object) -> str:
    pool = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-z0-9_-]+", pool or ""):
        raise ValueError("worker_pool must be non-empty and use only lowercase letters, numbers, underscores, or hyphens.")
    return pool


def _has_compatible_worker(workers: list[dict[str, Any]], worker_pool: str) -> bool:
    requested = _normalize_worker_pool(worker_pool)
    for worker in workers:
        if not bool(worker.get("is_healthy")):
            continue
        metadata = worker.get("metadata_json") if isinstance(worker.get("metadata_json"), dict) else {}
        pools = metadata.get("worker_pools") if isinstance(metadata.get("worker_pools"), list) else []
        normalized_pools = {_normalize_worker_pool(item) for item in pools}
        if requested == "any" or "any" in normalized_pools or requested in normalized_pools:
            return True
    return False


def _has_idle_compatible_worker(
    workers: list[dict[str, Any]],
    worker_pool: str,
    *,
    code_version: str,
) -> bool:
    requested = _normalize_worker_pool(worker_pool)
    for worker in workers:
        if not bool(worker.get("is_healthy")):
            continue
        if str(worker.get("status") or "").strip().lower() != "idle":
            continue
        if worker.get("current_job_run_id") is not None:
            continue
        metadata = worker.get("metadata_json") if isinstance(worker.get("metadata_json"), dict) else {}
        worker_version = str(metadata.get("code_version") or "").strip()
        if code_version and worker_version and worker_version != code_version:
            continue
        pools = metadata.get("worker_pools") if isinstance(metadata.get("worker_pools"), list) else []
        normalized_pools = {_normalize_worker_pool(item) for item in pools}
        if requested == "any" or "any" in normalized_pools or requested in normalized_pools:
            return True
    return False


def _validate_schedule_dependencies(jobs: list[dict[str, Any]]) -> None:
    by_id = {str(item.get("job_id") or "").strip(): item for item in jobs}
    for job_id, job in by_id.items():
        dependencies = _normalize_dependency_job_ids(job.get("depends_on_job_ids"))
        if job_id in dependencies:
            raise ValueError(f"Schedule {job_id} cannot depend on itself.")
        for dependency_id in dependencies:
            provider = by_id.get(dependency_id)
            if provider is None:
                raise ValueError(f"Schedule {job_id} depends on unknown schedule: {dependency_id}.")
            if bool(job.get("enabled", True)) and not bool(provider.get("enabled", True)):
                raise ValueError(f"Enabled schedule {job_id} cannot depend on disabled schedule: {dependency_id}.")
            if str(job.get("cron_tz") or "America/New_York") != str(provider.get("cron_tz") or "America/New_York"):
                raise ValueError(f"Schedule {job_id} must use the same timezone as dependency {dependency_id}.")
            consumer_days = _simple_cron_days(str(job.get("cron_expr") or ""))
            provider_days = _simple_cron_days(str(provider.get("cron_expr") or ""))
            if consumer_days is not None and provider_days is not None and not consumer_days.intersection(provider_days):
                raise ValueError(f"Schedule {job_id} never shares a run day with dependency {dependency_id}.")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(job_id: str) -> None:
        if job_id in visiting:
            raise ValueError("Schedule dependencies cannot contain a cycle.")
        if job_id in visited:
            return
        visiting.add(job_id)
        for dependency_id in _normalize_dependency_job_ids(by_id[job_id].get("depends_on_job_ids")):
            visit(dependency_id)
        visiting.remove(job_id)
        visited.add(job_id)

    for job_id in by_id:
        visit(job_id)


def _validate_cron_expr(value: str) -> None:
    parts = value.split()
    if len(parts) != 5:
        raise ValueError("cron_expr must have 5 fields: minute hour day month weekday")
    for part in parts:
        if not _CRON_FIELD_PATTERN.match(part):
            raise ValueError(f"Unsupported cron field: {part}")


def _simple_cron_days(cron_expr: str) -> set[int] | None:
    parts = cron_expr.split()
    if len(parts) != 5:
        return None
    weekday = parts[4]
    if weekday == "*":
        return set(range(7))
    if weekday == "1-5":
        return {1, 2, 3, 4, 5}
    if weekday in {"0", "7"}:
        return {0}
    if weekday == "6":
        return {6}
    return None


def _artifact_path_for_job(artifacts_dir: Path, job: dict[str, Any], *, local_now: datetime) -> str:
    if str(job.get("action_id") or "") == "weekly_rs":
        return str(artifacts_dir / "watchlists" / f"weekly_rs_new_high_{local_now.date().isoformat()}.json")
    return ""


def _resolve_template_value(value: object, *, local_now: datetime) -> object:
    if isinstance(value, str):
        replacements = {
            "{{local_date}}": local_now.date().isoformat(),
            "{{local_date_minus_7}}": (local_now.date() - timedelta(days=7)).isoformat(),
            "{{local_date_minus_14}}": (local_now.date() - timedelta(days=14)).isoformat(),
            "{{local_date_minus_100}}": (local_now.date() - timedelta(days=100)).isoformat(),
            "{{local_date_minus_465}}": (local_now.date() - timedelta(days=465)).isoformat(),
            "{{local_date_plus_7}}": (local_now.date() + timedelta(days=7)).isoformat(),
            "{{local_date_plus_14}}": (local_now.date() + timedelta(days=14)).isoformat(),
        }
        return re.sub(r"\{\{[^}]+\}\}", lambda match: replacements.get(match.group(0), match.group(0)), value)
    if isinstance(value, list):
        return [_resolve_template_value(item, local_now=local_now) for item in value]
    if isinstance(value, dict):
        return {str(key): _resolve_template_value(item, local_now=local_now) for key, item in value.items()}
    return value
