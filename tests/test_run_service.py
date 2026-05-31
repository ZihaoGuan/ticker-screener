from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.webapp.services.run_service import RunService


class _DummyProcess:
    def __init__(self) -> None:
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True


class RunServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        RunService._jobs = []
        RunService._jobs_by_id = {}
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        self.service = RunService(project_root=self.project_root)

    def test_update_progress_tracks_percent_and_success_count(self) -> None:
        job = {
            "success_count": 0,
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": None,
        }

        self.service._update_progress(
            job,
            [
                "[1/10] screening AAPL | passed=0",
                "[4/10] screening NVDA | passed=2",
            ],
        )

        self.assertEqual(job["success_count"], 2)
        self.assertEqual(job["progress_current"], 4)
        self.assertEqual(job["progress_total"], 10)
        self.assertEqual(job["progress_percent"], 40)
        self.assertEqual(job["progress_label"], "4/10 screening")

    def test_load_summary_metadata_reads_passed_tickers_and_watchlist_file(self) -> None:
        summary_path = self.project_root / "artifacts" / "raw" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(
            json.dumps({"passed_tickers": 7, "watchlist_file": "/tmp/watchlist.json"}),
            encoding="utf-8",
        )
        job = {"summary_file": str(summary_path), "success_count": 0, "watchlist_file": ""}

        self.service._load_summary_metadata(job)

        self.assertEqual(job["success_count"], 7)
        self.assertEqual(job["watchlist_file"], "/tmp/watchlist.json")

    def test_cancel_marks_job_and_terminates_process(self) -> None:
        process = _DummyProcess()
        job = {
            "job_id": "job-1",
            "action_id": "rs",
            "label": "Run RS",
            "status": "running",
            "command": "python scripts/run_rs_screen.py",
            "started_at": "2026-05-31T00:00:00+00:00",
            "finished_at": "",
            "return_code": None,
            "log_tail": "Starting...\n",
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Starting…",
            "success_count": 0,
            "watchlist_file": "",
            "summary_file": "",
            "cancel_requested": False,
            "_started_monotonic": 10.0,
            "_process": process,
        }
        RunService._jobs = [job]
        RunService._jobs_by_id = {"job-1": job}

        payload = self.service.cancel("job-1")

        self.assertTrue(process.terminated)
        self.assertTrue(job["cancel_requested"])
        self.assertTrue(payload["cancel_requested"])
        self.assertEqual(payload["job_id"], "job-1")
        self.assertIn("Cancellation requested", job["log_tail"])

    def test_launch_sync_job_applies_start_date_end_date_chunk_size_and_tickers(self) -> None:
        captured: dict[str, object] = {}

        def fake_run_job(job_id: str, command: list[str], env: dict[str, str]) -> None:
            captured["job_id"] = job_id
            captured["command"] = command
            captured["env"] = env

        self.service._run_job = fake_run_job  # type: ignore[method-assign]

        job_id = self.service.launch(
            "sync_postgres_market_data",
            options={
                "start_date": "2020-01-01",
                "end_date": "2026-05-31",
                "chunk_size": "55",
                "tickers": "AAPL NVDA",
            },
        )

        self.assertEqual(job_id, captured["job_id"])
        command = captured["command"]
        self.assertIn("scripts/sync_postgres_market_data.py", command)
        self.assertIn("--start-date", command)
        self.assertIn("2020-01-01", command)
        self.assertIn("--end-date", command)
        self.assertIn("2026-05-31", command)
        self.assertIn("--chunk-size", command)
        self.assertIn("55", command)
        tickers_index = command.index("--tickers")
        self.assertEqual(command[tickers_index + 1 : tickers_index + 3], ["AAPL", "NVDA"])


if __name__ == "__main__":
    unittest.main()
