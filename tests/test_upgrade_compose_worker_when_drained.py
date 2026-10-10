from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "upgrade_compose_worker_when_drained.sh"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def _run_script(tmp_path: Path, *, web_image: str, succeed_on_attempt: int) -> tuple[subprocess.CompletedProcess[str], int]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    attempts_file = tmp_path / "attempts"
    _write_executable(
        bin_dir / "docker",
        "#!/bin/sh\n"
        "if [ \"$1 $2\" = \"compose version\" ]; then exit 1; fi\n"
        "if [ \"$1\" = \"inspect\" ]; then printf '%s\\n' \"$WEB_IMAGE\"; exit 0; fi\n"
        "exit 99\n",
    )
    _write_executable(
        bin_dir / "docker-compose",
        "#!/bin/sh\n"
        "if [ \"$1 $2 $3\" = \"ps -q web\" ]; then printf '%s\\n' web-container-id; exit 0; fi\n"
        "exit 99\n",
    )
    _write_executable(bin_dir / "flock", "#!/bin/sh\nexit 0\n")
    _write_executable(bin_dir / "sleep", "#!/bin/sh\nexit 0\n")
    upgrade_script = tmp_path / "upgrade.sh"
    _write_executable(
        upgrade_script,
        "#!/bin/sh\n"
        "attempts=0\n"
        "[ ! -f \"$ATTEMPTS_FILE\" ] || attempts=$(cat \"$ATTEMPTS_FILE\")\n"
        "attempts=$((attempts + 1))\n"
        "printf '%s\\n' \"$attempts\" > \"$ATTEMPTS_FILE\"\n"
        "[ \"$attempts\" -ge \"$SUCCEED_ON_ATTEMPT\" ]\n",
    )
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "WEB_IMAGE": web_image,
            "ATTEMPTS_FILE": str(attempts_file),
            "SUCCEED_ON_ATTEMPT": str(succeed_on_attempt),
            "TICKER_SCREENER_DEPLOY_DIR": str(tmp_path),
            "TICKER_SCREENER_WORKER_UPGRADE_STATUS_DIR": str(tmp_path / "status"),
            "TICKER_SCREENER_WORKER_UPGRADE_SCRIPT": str(upgrade_script),
            "TICKER_SCREENER_WORKER_UPGRADE_POLL_SECONDS": "0",
        }
    )
    result = subprocess.run(
        [str(SCRIPT), "80663d4"],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    attempts = int(attempts_file.read_text(encoding="utf-8")) if attempts_file.exists() else 0
    return result, attempts


class UpgradeComposeWorkerWhenDrainedTest(unittest.TestCase):
    def test_retries_until_previous_version_jobs_drain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result, attempts = _run_script(
                Path(tmp_dir), web_image="ticker-screener:80663d4", succeed_on_attempt=2
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(attempts, 2)
            self.assertIn("waiting for previous-version jobs", result.stdout)
            self.assertIn("completed after drain", result.stdout)

    def test_exits_without_release_when_web_version_was_superseded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            result, attempts = _run_script(
                Path(tmp_dir), web_image="ticker-screener:95d1755", succeed_on_attempt=1
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(attempts, 0)
            self.assertIn("superseded", result.stdout)


if __name__ == "__main__":
    unittest.main()
