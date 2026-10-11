from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from .screener_history_service import ScreenerHistoryService
from src.webapp.repositories.history_repository import HistoryRepository


class ExternalJobResultService:
    def __init__(
        self,
        *,
        repository: HistoryRepository,
        history: ScreenerHistoryService,
        artifacts_dir: Path,
        artifact_base_url: str,
    ) -> None:
        self.repository = repository
        self.history = history
        self.artifacts_dir = artifacts_dir
        self.artifact_base_url = artifact_base_url.rstrip("/")

    def complete(self, *, job_run_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        row = self.repository.get_job_run(job_run_id)
        if row is None:
            raise ValueError("Unknown external job.")
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        if request_payload.get("execution_mode") != "github" or result_payload.get("executor") != "github_actions":
            raise ValueError("Job is not assigned to GitHub Actions.")
        expected_run_id = int(result_payload.get("github_run_id") or 0)
        received_run_id = int(payload.get("github_run_id") or 0)
        if not expected_run_id or expected_run_id != received_run_id:
            raise ValueError("GitHub workflow run does not own this job.")
        status = str(payload.get("status") or "").lower()
        if status == "failed":
            message = str(payload.get("message") or "GitHub workflow failed.")
            self.repository.patch_job_run_result(
                job_run_id,
                result_payload_patch={
                    "progress_label": "GitHub failed",
                    "message": message,
                    "github_conclusion": "failure",
                },
                status="failed",
                finished_at=str(payload.get("finished_at") or "") or None,
            )
            self._write_scheduled_status(
                request_payload=request_payload,
                status="failed",
                message=message,
                artifact_file="",
                screen_run_id=None,
            )
            return {"status": "failed"}
        if status != "success":
            raise ValueError("External completion status must be success or failed.")
        if str(row.get("status") or "") not in {"queued", "running"}:
            raise ValueError("Job is no longer eligible to publish results.")
        manifest_url = str(payload.get("manifest_url") or "").strip()
        manifest = self._read_json_url(manifest_url)
        if int(manifest.get("job_run_id") or 0) != job_run_id or int(manifest.get("github_run_id") or 0) != expected_run_id:
            raise ValueError("Artifact manifest does not match this job.")
        action_id = str(request_payload.get("action_id") or "")
        if str(manifest.get("action_id") or "") != action_id:
            raise ValueError("Artifact manifest action does not match this job.")
        files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
        saved: dict[str, Path] = {}
        for name in ("raw", "watchlist", "summary"):
            item = files.get(name) if isinstance(files.get(name), dict) else {}
            saved[name] = self._download_file(job_run_id=job_run_id, name=name, url=str(item.get("url") or ""), expected_sha256=str(item.get("sha256") or ""))
        summary = self._read_json_file(saved["summary"])
        raw = self._read_json_file(saved["raw"])
        summary["raw_results_file"] = str(saved["raw"])
        summary["watchlist_file"] = str(saved["watchlist"])
        saved["summary"].write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
        screen_run_id = self.history.persist_screen_run(strategy_id=action_id, options=options, summary_payload=summary, raw_payload=raw, job_run_id=job_run_id)
        self.repository.patch_job_run_result(
            job_run_id,
            result_payload_patch={
                "progress_label": "Completed on GitHub",
                "message": "GitHub artifacts validated and persisted.",
                "screen_run_id": screen_run_id,
                "summary_file": str(saved["summary"]),
                "raw_results_file": str(saved["raw"]),
                "watchlist_file": str(saved["watchlist"]),
                "github_conclusion": "success",
            },
            status="success",
            artifact_path=str(saved["summary"]),
        )
        self._write_scheduled_status(
            request_payload=request_payload,
            status="success",
            message="GitHub artifacts validated and persisted.",
            artifact_file=str(saved["summary"]),
            screen_run_id=screen_run_id,
        )
        return {"status": "success", "screen_run_id": screen_run_id}

    def heartbeat(self, *, job_run_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        row = self.repository.get_job_run(job_run_id)
        if row is None:
            raise ValueError("Unknown external job.")
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        if result_payload.get("executor") != "github_actions" or int(result_payload.get("github_run_id") or 0) != int(payload.get("github_run_id") or 0):
            raise ValueError("GitHub workflow run does not own this job.")
        if str(row.get("status") or "") not in {"queued", "running"}:
            raise ValueError("Job is no longer eligible to run.")
        self.repository.patch_job_run_result(
            job_run_id,
            result_payload_patch={
                "progress_label": "Running on GitHub",
                "message": "GitHub workflow is running.",
                "github_heartbeat_at": str(payload.get("at") or ""),
            },
            status="running",
        )
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        self._write_scheduled_status(
            request_payload=request_payload,
            status="running",
            message="GitHub workflow is running.",
            artifact_file="",
            screen_run_id=None,
        )
        return {"status": "running"}

    def _write_scheduled_status(
        self,
        *,
        request_payload: dict[str, Any],
        status: str,
        message: str,
        artifact_file: str,
        screen_run_id: int | None,
    ) -> None:
        options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
        scheduled_job_id = str(options.get("scheduled_job_id") or "").strip()
        if not scheduled_job_id:
            return
        status_dir = self.artifacts_dir / "status"
        status_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "job_id": scheduled_job_id,
            "job_label": str(options.get("scheduled_job_label") or scheduled_job_id),
            "status": status,
            "last_started_at": None,
            "last_finished_at": None,
            "exit_code": 0 if status == "success" else (1 if status == "failed" else None),
            "log_file": "",
            "artifact_file": artifact_file or options.get("scheduled_artifact_file") or None,
            "message": message,
            "persisted_to_db": True if screen_run_id is not None else (False if status == "failed" else None),
            "screen_run_id": screen_run_id,
            "persistence_message": (
                f"Persisted screen run id={screen_run_id}."
                if screen_run_id is not None
                else ("Job failed before DB persistence completed." if status == "failed" else "Persistence pending.")
            ),
        }
        status_path = status_dir / f"{scheduled_job_id}.json"
        tmp_path = status_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(status_path)

    def _allowed_url(self, value: str) -> str:
        if not self.artifact_base_url or not value.startswith(self.artifact_base_url + "/"):
            raise ValueError("External artifact URL is not allowed.")
        return value

    def _read_json_url(self, url: str) -> dict[str, Any]:
        data = self._download(self._allowed_url(url))
        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("External artifact manifest is invalid.") from exc
        if not isinstance(value, dict):
            raise ValueError("External artifact manifest must be an object.")
        return value

    def _download_file(self, *, job_run_id: int, name: str, url: str, expected_sha256: str) -> Path:
        data = self._download(self._allowed_url(url))
        if len(expected_sha256) != 64 or hashlib.sha256(data).hexdigest() != expected_sha256.lower():
            raise ValueError(f"External {name} artifact checksum mismatch.")
        target = self.artifacts_dir / "external-jobs" / str(job_run_id) / f"{name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return target

    @staticmethod
    def _download(url: str) -> bytes:
        with urlopen(url, timeout=30) as response:
            data = response.read(10 * 1024 * 1024 + 1)
        if len(data) > 10 * 1024 * 1024:
            raise ValueError("External artifact exceeds 10 MB limit.")
        return data

    @staticmethod
    def _read_json_file(path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("External artifact JSON is invalid.") from exc
        if not isinstance(value, dict):
            raise ValueError("External artifact must be an object.")
        return value
