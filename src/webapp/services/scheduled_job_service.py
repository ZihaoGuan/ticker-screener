from __future__ import annotations

import json
import re
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
        return {
            "jobs": self.list_jobs(),
            "available_actions": self._available_actions(),
            "common_timezones": list(_COMMON_TIMEZONES),
            "scheduler_command": f"cd {self.project_root / 'deploy'} && {self.project_root / 'scripts' / 'run_scheduled_jobs.py'}",
            "max_parallel_jobs": self.get_max_parallel_jobs(),
        }

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
        return [item for item in normalized if item["job_id"] and item["action_id"] and item["cron_expr"]]

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
        self.run_service._normalize_options(action, clean_options)

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
                if not next_job["options"] and isinstance(item.get("options"), dict):
                    next_job["options"] = dict(item.get("options") or {})
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

    def _available_actions(self) -> list[dict[str, Any]]:
        return [
            {"id": item["id"], "label": item["label"], "fields": item.get("fields", [])}
            for item in (
                self.run_service.list_actions()
                + [
                    {
                        "id": action.action_id,
                        "label": action.label,
                        "fields": [
                            {
                                "id": field.field_id,
                                "label": field.label,
                                "type": field.field_type,
                                "placeholder": field.placeholder,
                                "help_text": field.help_text,
                                "options": self.run_service._field_options(
                                    field,
                                    self.run_service._build_filter_option_catalog(),
                                ),
                            }
                            for field in action.fields
                        ],
                    }
                    for action in self.run_service._actions.values()
                    if action.action_id == "sync_postgres_market_data"
                ]
            )
        ]

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
