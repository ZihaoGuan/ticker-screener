from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
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
    extra_args: tuple[str, ...] = ()


class RunService:
    _progress_pattern = re.compile(r"\[(\d{1,6})/(\d{1,6})\]")
    _actions = {
        "rs": RunAction("rs", "Run RS", "scripts/run_rs_screen.py"),
        "vcp": RunAction("vcp", "Run VCP", "scripts/run_vcp_screen.py"),
        "cup_handle": RunAction("cup_handle", "Run Cup Handle", "scripts/run_cup_handle_screen.py"),
        "gap_fill": RunAction("gap_fill", "Run Gap Fill", "scripts/run_gap_fill_screen.py"),
        "weekly_htf_pullback": RunAction(
            "weekly_htf_pullback",
            "Run Weekly HTF Pullback",
            "scripts/run_weekly_htf_pullback_screen.py",
        ),
        "htf_8w_runup": RunAction("htf_8w_runup", "Run HTF 8W Runup", "scripts/run_htf_runup_screen.py"),
        "weekly_rs": RunAction("weekly_rs", "Run Weekly RS", "scripts/run_weekly_rs_screen.py"),
        "near_200ma": RunAction("near_200ma", "Run Near 200MA", "scripts/run_near_200ma_screen.py"),
        "lost_21ema": RunAction("lost_21ema", "Run Lost 21EMA", "scripts/run_lost_21ema_screen.py"),
        "legacy_peg": RunAction(
            "legacy_peg",
            "Run Legacy PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "legacy"),
        ),
        "sean_peg": RunAction(
            "sean_peg",
            "Run Sean PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "sean-peg"),
        ),
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
                "command": " ".join([sys.executable, action.script_path, *action.extra_args]).strip(),
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
        command.extend(action.extra_args)
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
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Starting…",
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
                self._update_progress(job, log_lines)

        return_code = process.wait()
        finished_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            job["status"] = "success" if return_code == 0 else "failed"
            job["return_code"] = return_code
            job["finished_at"] = finished_at
            job["log_tail"] = "\n".join(log_lines) if log_lines else job["log_tail"]
            if return_code == 0:
                job["progress_percent"] = 100
                job["progress_label"] = "Completed"
            elif job.get("progress_percent") is None:
                job["progress_label"] = "Failed"

    def _update_progress(self, job: dict[str, Any], log_lines: list[str]) -> None:
        current = None
        total = None
        last_line = ""
        for line in reversed(log_lines):
            match = self._progress_pattern.search(line)
            if match:
                current = int(match.group(1))
                total = int(match.group(2))
                last_line = line
                break

        if current is None or total is None or total <= 0:
            return

        percent = max(0, min(100, round((current / total) * 100)))
        job["progress_current"] = current
        job["progress_total"] = total
        job["progress_percent"] = percent
        detail = "screening" if "screening" in last_line.lower() else "processing"
        job["progress_label"] = f"{current}/{total} {detail}"
