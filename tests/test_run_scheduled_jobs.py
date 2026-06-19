from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import scripts.run_scheduled_jobs as module


class RunScheduledJobsTests(unittest.TestCase):
    def test_sync_scheduler_persistence_from_status_marks_success_when_log_contains_screen_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "job.log"
                log_path.write_text("hello\nPersisted screen run id=321\nbye\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "weekly_rs_close.json"
                status_path.write_text(
                    json.dumps(
                        {
                            "job_id": "weekly_rs_close",
                            "job_label": "Weekly RS Close",
                            "status": "success",
                            "log_file": str(log_path),
                        }
                    ),
                    encoding="utf-8",
                )

                module._sync_scheduler_persistence_from_status("weekly_rs_close")

                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertTrue(bool(payload["persisted_to_db"]))
        self.assertEqual(payload["screen_run_id"], 321)
        self.assertEqual(payload["persistence_message"], "Persisted screen run id=321.")

    def test_sync_scheduler_persistence_from_status_marks_failure_when_log_lacks_screen_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "job.log"
                log_path.write_text("hello\njob done\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "weekly_rs_close.json"
                status_path.write_text(
                    json.dumps(
                        {
                            "job_id": "weekly_rs_close",
                            "job_label": "Weekly RS Close",
                            "status": "success",
                            "log_file": str(log_path),
                        }
                    ),
                    encoding="utf-8",
                )

                module._sync_scheduler_persistence_from_status("weekly_rs_close")

                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertFalse(bool(payload["persisted_to_db"]))
        self.assertIsNone(payload["screen_run_id"])
        self.assertEqual(payload["persistence_message"], "No persisted screen run id found in job log.")


if __name__ == "__main__":
    unittest.main()
