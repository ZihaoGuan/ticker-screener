#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONITOR_SCRIPT = PROJECT_ROOT / "src" / "ibd_distribution_day_monitor" / "ibd_monitor.py"
DEFAULT_CONFIG = PROJECT_ROOT / "src" / "ibd_distribution_day_monitor" / "default.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run IBD Distribution Day Monitor for dashboard artifacts.")
    parser.add_argument(
        "--as-of-date",
        help="Optional as-of date override. Defaults to today's date.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    as_of_date = str(args.as_of_date or "").strip() or dt.date.today().isoformat()
    output_dir = PROJECT_ROOT / "artifacts" / "reports" / "ibd_distribution_day_monitor" / as_of_date
    output_dir.mkdir(parents=True, exist_ok=True)

    if not MONITOR_SCRIPT.exists():
        print(f"ERROR: monitor script not found: {MONITOR_SCRIPT}", file=sys.stderr)
        return 1

    command = [
        sys.executable,
        str(MONITOR_SCRIPT),
        "--config",
        str(DEFAULT_CONFIG),
        "--as-of",
        as_of_date,
        "--output-dir",
        str(output_dir),
    ]
    print(f"Running IBD Distribution Day Monitor into {output_dir}")
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT))
    if completed.returncode == 0:
        print(f"IBD monitor artifacts directory: {output_dir}")
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
