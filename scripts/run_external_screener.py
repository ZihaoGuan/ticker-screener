#!/usr/bin/env python3
"""Run one GitHub-eligible screener and record its standard artifacts."""

from __future__ import annotations

import argparse
import json
import os
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


def _validate_database_requirements(options: dict[str, Any], environment: dict[str, str]) -> None:
    market_data_source = str(options.get("market_data_source") or "").strip().lower()
    if market_data_source == "database-first" and not environment.get("TICKER_SCREENER_DATABASE_URL", "").strip():
        raise ValueError(
            "GitHub database-first execution requires TICKER_SCREENER_DATABASE_URL. "
            "Keep this schedule on a remote worker until private database access is configured."
        )


def _validate_nonempty_database_universe(summary_path: Path) -> None:
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Screener did not write a valid run summary.") from exc
    if not isinstance(summary, dict):
        raise ValueError("Screener run summary must be an object.")

    source = str(summary.get("source") or "").strip().lower()
    total_tickers = summary.get("total_tickers")
    if source != "database-universe" or total_tickers is None:
        return
    try:
        is_empty = int(total_tickers) == 0
    except (TypeError, ValueError) as exc:
        raise ValueError("Screener run summary has an invalid total_tickers value.") from exc
    if is_empty:
        raise ValueError(
            "Screener received an empty database universe; refusing to publish an empty GitHub result."
        )


def run(*, action_id: str, options: dict[str, Any], artifacts_file: Path) -> dict[str, str]:
    environment = os.environ.copy()
    if options.get("market_data_source"):
        environment["TICKER_SCREENER_MARKET_DATA_SOURCE"] = str(options["market_data_source"])
    _validate_database_requirements(options, environment)

    service = RunService(project_root=PROJECT_ROOT)
    if "github" not in service.execution_modes_for_action(action_id):
        raise ValueError(f"GitHub execution is not enabled for {action_id}.")
    command = service.build_command(action_id, options)
    completed = subprocess.run(command, cwd=PROJECT_ROOT, text=True, capture_output=True, check=False, env=environment)
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
    summary_path = Path(summary_markers[-1])
    _validate_nonempty_database_universe(summary_path)
    paths = _artifact_paths(summary_path)
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
