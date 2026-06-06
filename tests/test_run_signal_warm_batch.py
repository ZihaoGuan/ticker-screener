from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_signal_warm_batch


class _FakeProcess:
    def __init__(self, lines: list[str], return_code: int = 0) -> None:
        self.stdout = iter(lines)
        self._return_code = return_code

    def wait(self) -> int:
        return self._return_code


class RunSignalWarmBatchTests(unittest.TestCase):
    def test_streaming_subprocess_updates_child_log_live(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            updates: list[tuple[int | None, str, dict[str, object] | None]] = []
            run_service = SimpleNamespace(
                history_repository=SimpleNamespace(
                    update_job_run=lambda job_run_id, status=None, result_payload=None, **kwargs: updates.append(
                        (job_run_id, status, result_payload)
                    )
                )
            )
            log_file = Path(temp_dir) / "child.log"
            process = _FakeProcess(
                [
                    "boot line\n",
                    "[1/3] processing rs 2026-06-05 | passed=2\n",
                    "detail a\n",
                    "detail b\n",
                    "Wrote run summary to /tmp/summary.json\n",
                ]
            )

            with patch.object(run_signal_warm_batch.subprocess, "Popen", return_value=process):
                return_code, combined, summary_path = run_signal_warm_batch._run_streaming_subprocess(
                    command=["python", "fake.py"],
                    process_env={},
                    log_file=log_file,
                    run_service=run_service,
                    child_job_run_id=88,
                    strategy_id="rs",
                    run_date="2026-06-05",
                )

            self.assertEqual(return_code, 0)
            self.assertEqual(summary_path, "/tmp/summary.json")
            self.assertIn("passed=2", combined)
            self.assertTrue(log_file.exists())
            self.assertIn("detail b", log_file.read_text(encoding="utf-8"))
            self.assertGreaterEqual(len(updates), 2)
            first_payload = updates[0][2] or {}
            last_payload = updates[-1][2] or {}
            self.assertEqual(first_payload.get("log_file"), str(log_file))
            self.assertEqual(last_payload.get("success_count"), 2)
            self.assertIn("Wrote run summary to /tmp/summary.json", str(last_payload.get("log_tail") or ""))


if __name__ == "__main__":
    unittest.main()
