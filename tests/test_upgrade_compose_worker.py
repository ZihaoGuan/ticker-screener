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


def _run_script(tmp_path: Path, *, active_jobs: int) -> subprocess.CompletedProcess[str]:
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
        "if [ \"$1\" = \"exec\" ]; then printf '%s\\n' \"$ACTIVE_JOBS\"; exit 0; fi\n"
        "if [ \"$1 $2 $3 $4\" = \"ps -q worker worker_parallel\" ]; then printf '%s\\n' worker-container-id parallel-worker-container-id; exit 0; fi\n"
        "if [ \"$1\" = \"up\" ]; then exit 0; fi\n"
        "exit 99\n",
    )
    env = os.environ.copy()
    env.update({"PATH": f"{bin_dir}:{env['PATH']}", "CALL_LOG": str(call_log), "ACTIVE_JOBS": str(active_jobs)})
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
            self.assertEqual(len(calls), 7)
            self.assertEqual(calls[0], "compose version|")
            self.assertEqual(calls[1], "image inspect ticker-screener:80663d4|")
            self.assertIn("exec -T db", calls[2])
            self.assertEqual(calls[3], "ps -q worker worker_parallel|")
            self.assertEqual(calls[4], "rm -f worker-container-id|")
            self.assertEqual(calls[5], "rm -f parallel-worker-container-id|")
            self.assertEqual(calls[6], "up -d --no-deps worker worker_parallel|80663d4")

    def test_refuses_worker_upgrade_until_drain_is_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            result = _run_script(tmp_path, active_jobs=2)

            self.assertEqual(result.returncode, 1)
            self.assertIn("Worker drain is incomplete", result.stderr)
            calls = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(calls), 3)
            self.assertEqual(calls[0], "compose version|")
            self.assertEqual(calls[1], "image inspect ticker-screener:80663d4|")
            self.assertIn("exec -T db", calls[2])


if __name__ == "__main__":
    unittest.main()
