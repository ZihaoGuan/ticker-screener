from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "deploy_compose_services.sh"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


def _run_script(tmp_path: Path, *, compose_failure: str) -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    call_log = tmp_path / "calls.log"
    state_file = tmp_path / "state"

    _write_executable(
        bin_dir / "docker",
        "#!/bin/sh\n"
        "if [ \"$1 $2\" = \"compose version\" ]; then exit 1; fi\n"
        "exit 99\n",
    )
    _write_executable(
        bin_dir / "docker-compose",
        "#!/bin/sh\n"
        "echo \"$*\" >> \"$CALL_LOG\"\n"
        "if [ \"$1\" = \"rm\" ]; then exit 0; fi\n"
        "if [ ! -f \"$STATE_FILE\" ]; then\n"
        "  touch \"$STATE_FILE\"\n"
        "  printf '%s\\n' \"$COMPOSE_FAILURE\" >&2\n"
        "  exit 1\n"
        "fi\n"
        "exit 0\n",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "CALL_LOG": str(call_log),
            "STATE_FILE": str(state_file),
            "COMPOSE_FAILURE": compose_failure,
        }
    )
    return subprocess.run(
        [str(SCRIPT), "up", "-d", "--force-recreate", "--no-deps", "web", "caddy"],
        cwd=PROJECT_ROOT / "deploy",
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


class DeployComposeServicesTest(unittest.TestCase):
    def test_legacy_compose_recovers_from_container_config_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            result = _run_script(tmp_path, compose_failure="KeyError: 'ContainerConfig'")

            self.assertEqual(result.returncode, 0, result.stderr)
            calls = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                calls,
                [
                    "up -d --force-recreate --no-deps web caddy",
                    "rm -sf web caddy",
                    "up -d --force-recreate --no-deps web caddy",
                ],
            )

    def test_legacy_compose_does_not_retry_unrelated_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            result = _run_script(tmp_path, compose_failure="unrelated compose failure")

            self.assertEqual(result.returncode, 1)
            calls = (tmp_path / "calls.log").read_text(encoding="utf-8").splitlines()
            self.assertEqual(calls, ["up -d --force-recreate --no-deps web caddy"])


if __name__ == "__main__":
    unittest.main()
