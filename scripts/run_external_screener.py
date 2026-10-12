#!/usr/bin/env python3
"""Run one GitHub-eligible screener and record its standard artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.services.run_service import RunService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a portable screener for GitHub Actions.")
    parser.add_argument("--action-id", required=True)
    parser.add_argument("--options-json", required=True)
    parser.add_argument("--artifacts-file", required=True)
    return parser.parse_args()


def _artifact_paths(summary_path: Path) -> dict[str, str]:
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Screener did not write a valid run summary.") from exc
    if not isinstance(summary, dict):
        raise ValueError("Screener run summary must be an object.")
    paths = {
        "raw": str(summary.get("raw_results_file") or "").strip(),
        "watchlist": str(summary.get("watchlist_file") or "").strip(),
        "summary": str(summary_path),
    }
    missing = [name for name, value in paths.items() if not value or not Path(value).is_file()]
    if missing:
        raise ValueError(f"Screener did not produce required artifacts: {', '.join(missing)}.")
    return paths


def run(*, action_id: str, options: dict[str, Any], artifacts_file: Path) -> dict[str, str]:
    service = RunService(project_root=PROJECT_ROOT)
    if "github" not in service.execution_modes_for_action(action_id):
        raise ValueError(f"GitHub execution is not enabled for {action_id}.")
    command = service.build_command(action_id, options)
    completed = subprocess.run(command, cwd=PROJECT_ROOT, text=True, capture_output=True, check=False)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode:
        raise RuntimeError(f"Screener exited with code {completed.returncode}.")
    summary_markers = [
        line.removeprefix("Wrote run summary to ").strip()
        for line in completed.stdout.splitlines()
        if line.startswith("Wrote run summary to ")
    ]
    if not summary_markers:
        raise ValueError("Screener did not report a run summary.")
    paths = _artifact_paths(Path(summary_markers[-1]))
    artifacts_file.write_text(json.dumps(paths), encoding="utf-8")
    return paths


def main() -> int:
    args = parse_args()
    try:
        options = json.loads(args.options_json)
    except json.JSONDecodeError as exc:
        raise SystemExit("options_json must be valid JSON.") from exc
    if not isinstance(options, dict):
        raise SystemExit("options_json must be a JSON object.")
    try:
        run(action_id=args.action_id, options=options, artifacts_file=Path(args.artifacts_file))
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
