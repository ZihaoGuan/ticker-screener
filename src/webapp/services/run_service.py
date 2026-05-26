from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import subprocess
import sys
import threading
import uuid
from typing import Any


@dataclass(frozen=True)
class RunAction:
    action_id: str
    label: str
    script_path: str
    supports_limit: bool = True


class RunService:
    _actions = {
        "rs": RunAction("rs", "Run RS", "scripts/run_rs_screen.py"),
        "vcp": RunAction("vcp", "Run VCP", "scripts/run_vcp_screen.py"),
        "cup_handle": RunAction("cup_handle", "Run Cup Handle", "scripts/run_cup_handle_screen.py"),
    }
    _jobs_lock = threading.Lock()
    _jobs: list[dict[str, Any]] = []
    _jobs_by_id: dict[str, dict[str, Any]] = {}

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def list_actions(self) -> list[dict[str, Any]]:
        return [
            {
                "id": action.action_id,
                "label": action.label,
                "command": f"{sys.executable} {action.script_path}",
                "supports_limit": action.supports_limit,
            }
            for action in self._actions.values()
        ]

    def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._jobs_lock:
            return [dict(item) for item in self._jobs[:limit]]

    def launch(self, action_id: str, *, limit: int | None = None) -> str:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")

        job_id = uuid.uuid4().hex[:12]
        started_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        command = [sys.executable, action.script_path]
        if action.supports_limit and limit is not None:
            command.extend(["--limit", str(limit)])

        job = {
            "job_id": job_id,
            "action_id": action_id,
            "label": action.label,
            "status": "running",
            "command": " ".join(command),
            "started_at": started_at,
            "finished_at": "",
            "return_code": None,
            "log_tail": "Starting...\n",
        }

        with self._jobs_lock:
            self._jobs.insert(0, job)
            self._jobs_by_id[job_id] = job
            del self._jobs[50:]

        thread = threading.Thread(target=self._run_job, args=(job_id, command), daemon=True)
        thread.start()
        return job_id

    def _run_job(self, job_id: str, command: list[str]) -> None:
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        log_lines: list[str] = []
        assert process.stdout is not None
        for line in process.stdout:
            log_lines.append(line.rstrip())
            log_lines = log_lines[-80:]
            with self._jobs_lock:
                job = self._jobs_by_id[job_id]
                job["log_tail"] = "\n".join(log_lines)

        return_code = process.wait()
        finished_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            job["status"] = "success" if return_code == 0 else "failed"
            job["return_code"] = return_code
            job["finished_at"] = finished_at
            job["log_tail"] = "\n".join(log_lines) if log_lines else job["log_tail"]
