from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from .screener_history_service import ScreenerHistoryService
from src.ratings.repository import RatingsRepository
from src.ratings.technical_indicator_transfer import ACTION_ID as TECHNICAL_RATINGS_ACTION, MAX_COMPRESSED_BYTES, read_export
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
        existing_screen_run_id = int(result_payload.get("screen_run_id") or 0)
        if existing_screen_run_id:
            return {"status": "success", "screen_run_id": existing_screen_run_id}
        action_id = str(request_payload.get("action_id") or "")
        if action_id == TECHNICAL_RATINGS_ACTION and result_payload.get("technical_rating_count") is not None and row.get("status") == "success":
            return {"status": "success", "technical_rating_count": int(result_payload["technical_rating_count"])}
        finished_at = self._completion_time(payload.get("finished_at"))
        if status == "failed":
            message = str(payload.get("message") or "GitHub workflow failed.")
            if self.repository.fallback_github_job_run(job_run_id=job_run_id, github_run_id=expected_run_id, message=message):
                return {"status": "queued", "execution_mode": "remote"}
            self.repository.patch_job_run_result(
                job_run_id,
                result_payload_patch={
                    "progress_label": "GitHub failed",
                    "message": message,
                    "github_conclusion": "failure",
                },
                status="failed",
                finished_at=finished_at,
            )
            self._write_scheduled_status(
                request_payload=request_payload,
                status="failed",
                message=message,
                artifact_file="",
                screen_run_id=None,
                finished_at=finished_at,
            )
            return {"status": "failed"}
        if status != "success":
            raise ValueError("External completion status must be success or failed.")
        if str(row.get("status") or "") not in {"queued", "running"}:
            raise ValueError("Job is no longer eligible to publish results.")
        manifest_url = str(payload.get("manifest_url") or "").strip()
        if action_id == TECHNICAL_RATINGS_ACTION:
            attempt = int(result_payload.get("github_attempt") or 1)
            expected_manifest_url = f"{self.artifact_base_url}/external-jobs/{job_run_id}/attempt-{attempt}/manifest.json"
            if manifest_url != expected_manifest_url:
                raise ValueError("Technical rating manifest URL does not match this job.")
        manifest = self._read_json_url(manifest_url)
        if type(manifest.get("job_run_id")) is not int or type(manifest.get("github_run_id")) is not int or manifest["job_run_id"] != job_run_id or manifest["github_run_id"] != expected_run_id:
            raise ValueError("Artifact manifest does not match this job.")
        if str(manifest.get("action_id") or "") != action_id:
            raise ValueError("Artifact manifest action does not match this job.")
        files = manifest.get("files") if isinstance(manifest.get("files"), dict) else {}
        if action_id == TECHNICAL_RATINGS_ACTION:
            return self._complete_technical_ratings(
                job_run_id=job_run_id, request_payload=request_payload, files=files,
                manifest_url=manifest_url, finished_at=finished_at,
            )
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
        persisted = self.history.get_run(screen_run_id, include_hits=False) if screen_run_id is not None else None
        success_count = int(persisted.get("hit_count") or 0) if isinstance(persisted, dict) else 0
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
                "success_count": success_count,
            },
            status="success",
            artifact_path=str(saved["summary"]),
            finished_at=finished_at,
        )
        self._write_scheduled_status(
            request_payload=request_payload,
            status="success",
            message="GitHub artifacts validated and persisted.",
            artifact_file=str(saved["summary"]),
            screen_run_id=screen_run_id,
            finished_at=finished_at,
        )
        return {"status": "success", "screen_run_id": screen_run_id}

    def _complete_technical_ratings(
        self, *, job_run_id: int, request_payload: dict[str, Any], files: dict[str, Any],
        manifest_url: str, finished_at: str,
    ) -> dict[str, Any]:
        if set(files) != {"ratings"} or not isinstance(files["ratings"], dict):
            raise ValueError("Technical rating manifest files are invalid.")
        item = files["ratings"]
        expected_url = manifest_url.removesuffix("manifest.json") + "ratings.json.gz"
        if set(item) != {"url", "sha256", "size"} or item["url"] != expected_url:
            raise ValueError("Technical rating artifact URL is invalid.")
        size = item["size"]
        digest = item["sha256"]
        if type(size) is not int or not 0 < size <= MAX_COMPRESSED_BYTES or not isinstance(digest, str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Technical rating artifact metadata is invalid.")
        data = self._download(self._allowed_url(expected_url))
        if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Technical rating artifact checksum mismatch.")
        options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
        as_of_date, tickers, ratings = read_export(data, expected_date=str(options.get("as_of_date") or ""))
        requested_tickers = options.get("tickers")
        if requested_tickers:
            if not isinstance(requested_tickers, list) or not set(tickers) <= {str(ticker).upper() for ticker in requested_tickers}:
                raise ValueError("Technical rating ticker scope exceeds job request.")
        requested_limit = options.get("limit")
        if requested_limit is not None and len(tickers) > int(requested_limit):
            raise ValueError("Technical rating ticker count exceeds job limit.")
        count = RatingsRepository(self.repository.database_url).replace_technical_indicator_rating_snapshots(
            as_of_date, ratings, tickers=tickers,
        )
        if count != len(ratings):
            raise ValueError("Technical rating persistence did not save all rows.")
        self.repository.patch_job_run_result(
            job_run_id,
            result_payload_patch={
                "progress_label": "Completed on GitHub",
                "message": "GitHub technical ratings validated and persisted.",
                "technical_rating_count": count,
                "external_manifest_url": manifest_url,
                "github_conclusion": "success",
                "success_count": sum(item.technical_status == "ok" for item in ratings),
            },
            status="success",
            artifact_path=manifest_url,
            finished_at=finished_at,
        )
        self._write_scheduled_status(
            request_payload=request_payload, status="success",
            message=f"Persisted {count} technical indicator ratings.",
            artifact_file=manifest_url, screen_run_id=None, finished_at=finished_at,
            persisted_to_db=True,
        )
        return {"status": "success", "technical_rating_count": count}

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
                "started_at": str(payload.get("at") or ""),
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
        finished_at: str | None = None,
        persisted_to_db: bool | None = None,
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
            "last_finished_at": finished_at,
            "exit_code": 0 if status == "success" else (1 if status == "failed" else None),
            "log_file": "",
            "artifact_file": artifact_file or options.get("scheduled_artifact_file") or None,
            "message": message,
            "persisted_to_db": persisted_to_db if persisted_to_db is not None else (True if screen_run_id is not None else (False if status == "failed" else None)),
            "screen_run_id": screen_run_id,
            "persistence_message": (
                f"Persisted screen run id={screen_run_id}."
                if screen_run_id is not None
                else ("Ratings persisted to DB." if persisted_to_db else ("Job failed before DB persistence completed." if status == "failed" else "Persistence pending."))
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
        request = Request(url, headers={"User-Agent": "ticker-screener/1.0"})
        with urlopen(request, timeout=30) as response:
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

    @staticmethod
    def _now_iso() -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")

    @classmethod
    def _completion_time(cls, value: Any) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return cls._now_iso()
        try:
            dt.datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError:
            return cls._now_iso()
        return candidate
