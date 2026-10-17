from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts" / "cleanup_compose_releases.sh"


def _write_executable(path: Path, contents: str) -> None:
    path.write_text(contents, encoding="utf-8")
    path.chmod(0o755)


class CleanupComposeReleasesTest(unittest.TestCase):
    def test_removes_only_stale_application_releases_and_preserves_one_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            bin_dir = tmp_path / "bin"
            bin_dir.mkdir()
            call_log = tmp_path / "calls.log"
            _write_executable(
                bin_dir / "docker",
                "#!/bin/sh\n"
                "echo \"$*\" >> \"$CALL_LOG\"\n"
                "case \"$1 $2\" in\n"
                "  'system df') exit 0 ;;\n"
                "  'ps -aq')\n"
                "    case \"$*\" in *'service=web'*'status=exited'*) printf '%s\\n' exited-web ;; *'status=exited'*) ;; *) printf '%s\\n' running-web ;; esac\n"
                "    exit 0 ;;\n"
                "  'inspect -f')\n"
                "    case \"$4\" in\n"
                "      exited-web) printf '%s\\n' ticker-screener:older ;;\n"
                "      running-web) printf '%s\\n' sha256:current ;;\n"
                "    esac\n"
                "    exit 0 ;;\n"
                "  'image inspect') printf '%s\\n' sha256:current; exit 0 ;;\n"
                "  'image ls')\n"
                "    printf '%s\\n' 'ticker-screener:current sha256:current' 'ticker-screener:rollback-one sha256:rollback-one' 'ticker-screener:rollback-two sha256:rollback-two' 'ticker-screener:older sha256:older'\n"
                "    exit 0 ;;\n"
                "  'image rm'|'image prune'|'builder prune'|'rm exited-web') exit 0 ;;\n"
                "esac\n"
                "exit 99\n",
            )
            env = os.environ.copy()
            env.update({"PATH": f"{bin_dir}:{env['PATH']}", "CALL_LOG": str(call_log)})
            result = subprocess.run(
                [str(SCRIPT), "80663d4"],
                cwd=PROJECT_ROOT,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            calls = call_log.read_text(encoding="utf-8")
            self.assertIn("rm exited-web", calls)
            self.assertIn("image rm ticker-screener:older", calls)
            self.assertIn("image rm ticker-screener:rollback-two", calls)
            self.assertNotIn("image rm ticker-screener:current", calls)
            self.assertNotIn("image rm ticker-screener:rollback-one", calls)
            self.assertNotIn("volume", calls)
            self.assertIn("image prune -f", calls)
            self.assertIn("builder prune -f --filter until=24h", calls)


if __name__ == "__main__":
    unittest.main()
