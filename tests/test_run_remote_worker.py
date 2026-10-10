from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

import scripts.run_remote_worker as module


class _RunService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir


class _HistoryRepository:
    def __init__(self) -> None:
        self.patches: list[dict[str, object]] = []

    def patch_job_run_result(self, _job_run_id: int, **kwargs: object) -> None:
        self.patches.append(dict(kwargs))

    def is_remote_job_cancel_requested(self, _job_run_id: int) -> bool:
        return False

    def heartbeat_remote_worker(self, **_kwargs: object) -> None:
        return None


class _FailingJobRunService(_RunService):
    database_url = ""

    def __init__(self, artifacts_dir: Path) -> None:
        super().__init__(artifacts_dir)
        self.history_repository = _HistoryRepository()
        self.completed_job: dict[str, object] = {}

    def build_command(self, _action_id: str, _options: dict[str, object], *, normalized: bool) -> list[str]:
        assert normalized
        return [
            sys.executable,
            "-c",
            "import sys; [print(f'line-{i}') for i in range(100)]; print('FINAL_EXCEPTION'); sys.exit(1)",
        ]

    def _extract_progress(self, _lines: list[str]) -> dict[str, object]:
        return {"current": None, "total": None, "percent": None, "label": "", "success_count": 0}

    def _update_artifacts(self, _job: dict[str, object], _line: str) -> None:
        return None

    def _load_summary_metadata(self, _job: dict[str, object]) -> None:
        return None

    def finalize_completed_job(self, job: dict[str, object]) -> None:
        self.completed_job = dict(job)


class RunRemoteWorkerTests(unittest.TestCase):
    def test_run_claimed_job_drains_output_after_short_process_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _FailingJobRunService(Path(temp_dir))

            exit_code = module._run_claimed_job(
                service,  # type: ignore[arg-type]
                {
                    "id": 809,
                    "job_name": "Run Finviz Target Price +50%",
                    "trigger_source": "manual",
                    "request_payload": {"action_id": "finviz_target_price_50", "options": {}},
                },
                worker_name="test-worker",
                heartbeat_seconds=5.0,
            )

            log_text = Path(str(service.completed_job["log_file"])).read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertIn("FINAL_EXCEPTION", log_text)
        self.assertIn("FINAL_EXCEPTION", str(service.completed_job["log_tail"]))

    def test_write_scheduled_status_keeps_worker_log_and_persisted_screen_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _RunService(Path(temp_dir))
            state = {
                "started_at": "2026-10-10T00:00:00+00:00",
                "return_code": 0,
                "log_file": "/artifacts/status/logs/weekly-rs.log",
                "watchlist_file": "/artifacts/watchlists/weekly-rs.json",
                "message": "Completed on worker primary-worker.",
                "screen_run_id": 321,
            }

            module._write_scheduled_status(
                service,  # type: ignore[arg-type]
                options={"scheduled_job_id": "weekly_rs", "scheduled_job_label": "Weekly RS"},
                state=state,
                status="success",
                finished_at="2026-10-10T00:01:00+00:00",
            )

            payload = json.loads((Path(temp_dir) / "status" / "weekly_rs.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["log_file"], state["log_file"])
        self.assertTrue(payload["persisted_to_db"])
        self.assertEqual(payload["screen_run_id"], 321)
