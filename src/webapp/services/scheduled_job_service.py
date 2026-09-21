from __future__ import annotations

import json
import re
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .run_service import RunService


_CRON_FIELD_PATTERN = re.compile(r"^[\d\*,/\-]+$")
_COMMON_TIMEZONES = (
    "America/New_York",
    "Pacific/Auckland",
    "UTC",
)
_DEFAULT_MAX_PARALLEL_JOBS = 5


class ScheduledJobService:
    def __init__(self, *, project_root: Path, run_service: RunService) -> None:
        self.project_root = project_root
        self.run_service = run_service
        self.config_path = project_root / "config" / "scheduled_jobs.json"

    def get_context(self) -> dict[str, Any]:
        jobs = self.list_jobs()
        return {
            "jobs": jobs,
            "available_actions": self._available_actions(jobs),
            "common_timezones": list(_COMMON_TIMEZONES),
            "scheduler_command": f"cd {self.project_root / 'deploy'} && {self.project_root / 'scripts' / 'run_scheduled_jobs.py'}",
            "max_parallel_jobs": self.get_max_parallel_jobs(),
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
        next_job = {
            "job_id": clean_job_id,
            "job_label": clean_job_label,
            "action_id": clean_action_id,
            "cron_expr": clean_cron_expr,
            "cron_tz": clean_cron_tz,
            "enabled": bool(enabled),
            "options": clean_options,
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
        payload["jobs"] = sorted(next_jobs, key=lambda item: str(item.get("job_label") or item.get("job_id") or ""))
        self._write_jobs(payload)
        return next_job

    def delete_job(self, *, job_id: str) -> None:
        clean_job_id = _normalize_job_id(job_id)
        if not clean_job_id:
            raise ValueError("job_id is required.")
        payload = self._load_jobs()
        jobs = [item for item in payload.get("jobs", []) if isinstance(item, dict)]
        next_jobs = [item for item in jobs if str(item.get("job_id") or "").strip() != clean_job_id]
        payload["jobs"] = next_jobs
        self._write_jobs(payload)

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
                **_summarize_durations(
                    estimates_by_action.get(str(item["id"]), []),
                    sample_count=sample_counts_by_action.get(str(item["id"]), 0),
                ),
            }
            for item in actions
        ]

    def _estimate_job_duration(self, job_id: str) -> dict[str, Any]:
        log_dir = self.project_root / "artifacts" / "status" / "logs"
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
        status_path = self.project_root / "artifacts" / "status" / f"{job_id}.json"
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(status, dict) or str(status.get("status") or "").lower() not in {"queued", "running", "waiting"}:
            return None
        log_file = status.get("log_file")
        return Path(log_file) if isinstance(log_file, str) and log_file else None

    def _scheduled_status(self, job: dict[str, Any]) -> dict[str, Any] | None:
        status_path = self.project_root / "artifacts" / "status" / f"{job['job_id']}.json"
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


def _validate_cron_expr(value: str) -> None:
    parts = value.split()
    if len(parts) != 5:
        raise ValueError("cron_expr must have 5 fields: minute hour day month weekday")
    for part in parts:
        if not _CRON_FIELD_PATTERN.match(part):
            raise ValueError(f"Unsupported cron field: {part}")
