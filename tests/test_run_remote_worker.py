from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import scripts.run_remote_worker as module


class _RunService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir


class RunRemoteWorkerTests(unittest.TestCase):
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
