from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "upgrade_compose_worker.sh"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def _run_script(
    tmp_path: Path,
    *,
    active_jobs: int,
    orphaned_jobs: int = 0,
    healthy_workers: int = 4,
) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "calls.log"
    _write_executable(
        bin_dir / "docker",
        "#!/bin/sh\n"
        "echo \"$*|$TICKER_SCREENER_IMAGE_TAG\" >> \"$CALL_LOG\"\n"
        "if [ \"$1 $2\" = \"compose version\" ]; then exit 1; fi\n"
        "if [ \"$1 $2\" = \"image inspect\" ]; then exit 0; fi\n"
        "if [ \"$1\" = \"rm\" ]; then exit 0; fi\n"
        "exit 99\n",
    )
    _write_executable(
        bin_dir / "docker-compose",
        "#!/bin/sh\n"
        "echo \"$*|$TICKER_SCREENER_IMAGE_TAG\" >> \"$CALL_LOG\"\n"
        "if [ \"$1\" = \"exec\" ]; then\n"
        "  case \"$*\" in\n"
        "    *'SELECT COUNT(*) FROM cancelled'*) printf '%s\\n' \"$ORPHANED_JOBS\" ;;\n"
        "    *primary-worker*) printf '%s\\n' \"$HEALTHY_WORKERS\" ;;\n"
        "    *) printf '%s\\n' \"$ACTIVE_JOBS\" ;;\n"
        "  esac\n"
        "  exit 0\n"
        "fi\n"
        "if [ \"$1 $2 $3 $4 $5 $6\" = \"ps -q worker worker_parallel worker_parallel_2 worker_parallel_3\" ]; then printf '%s\\n' worker-container-id parallel-worker-container-id parallel-worker-2-container-id parallel-worker-3-container-id; exit 0; fi\n"
        "if [ \"$1\" = \"up\" ]; then exit 0; fi\n"
        "exit 99\n",
    )
    _write_executable(bin_dir / "sleep", "#!/bin/sh\nexit 0\n")
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "CALL_LOG": str(call_log),
            "ACTIVE_JOBS": str(active_jobs),
            "ORPHANED_JOBS": str(orphaned_jobs),
            "HEALTHY_WORKERS": str(healthy_workers),
        }
    )
    return subprocess.run(
        [str(SCRIPT), "80663d4"],
        cwd=PROJECT_ROOT / "deploy",
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


class UpgradeComposeWorkerTest(unittest.TestCase):
    def test_recreates_only_worker_after_drain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            result = _run_script(tmp_path, active_jobs=0)

            self.assertEqual(result.returncode, 0, result.stderr)
            calls = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(calls), 11, calls)
            self.assertEqual(calls[0], "compose version|")
            self.assertEqual(calls[1], "image inspect ticker-screener:80663d4|")
            self.assertIn("SELECT COUNT(*) FROM cancelled", calls[2])
            self.assertIn("FROM job_runs", calls[3])
            self.assertEqual(calls[4], "ps -q worker worker_parallel worker_parallel_2 worker_parallel_3|")
            self.assertEqual(calls[5], "rm -f worker-container-id|")
            self.assertEqual(calls[6], "rm -f parallel-worker-container-id|")
            self.assertEqual(calls[7], "rm -f parallel-worker-2-container-id|")
            self.assertEqual(calls[8], "rm -f parallel-worker-3-container-id|")
            self.assertEqual(
                calls[9],
                "up -d --no-deps worker worker_parallel worker_parallel_2 worker_parallel_3|80663d4",
            )
            self.assertIn("FROM remote_workers", calls[10])

    def test_refuses_worker_upgrade_until_drain_is_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            result = _run_script(tmp_path, active_jobs=2)

            self.assertEqual(result.returncode, 1)
            self.assertIn("Worker drain is incomplete", result.stderr)
            calls = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(calls), 4, calls)
            self.assertEqual(calls[0], "compose version|")
            self.assertEqual(calls[1], "image inspect ticker-screener:80663d4|")
            self.assertIn("SELECT COUNT(*) FROM cancelled", calls[2])
            self.assertIn("FROM job_runs", calls[3])

    def test_records_orphaned_previous_version_cancellation_before_releasing_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = _run_script(Path(tmp_dir), active_jobs=0, orphaned_jobs=2)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Cancelled 2 orphaned previous-version remote job(s).", result.stdout)
            self.assertIn("All four workers are healthy", result.stdout)
            calls = (Path(tmp_dir) / "calls.log").read_text(encoding="utf-8")
            self.assertIn("no_compatible_healthy_worker", calls)
            self.assertIn("INTERVAL '10 minutes'", calls)

    def test_requires_all_compose_workers_to_report_target_image_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result = _run_script(Path(tmp_dir), active_jobs=0, healthy_workers=3)

            self.assertEqual(result.returncode, 1)
            self.assertIn("did not report healthy", result.stderr)


if __name__ == "__main__":
    unittest.main()
