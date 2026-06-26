#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ANALYZER_SCRIPT = PROJECT_ROOT / "trading-skills" / "skills" / "market-breadth-analyzer" / "scripts" / "market_breadth_analyzer.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run cached market breadth analysis for dashboard artifacts.")
    parser.add_argument(
        "--date-label",
        help="Optional artifact folder label. Defaults to today's date.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    date_label = str(args.date_label or "").strip() or dt.date.today().isoformat()
    output_dir = PROJECT_ROOT / "artifacts" / "reports" / "market_breadth" / date_label
    output_dir.mkdir(parents=True, exist_ok=True)

    if not ANALYZER_SCRIPT.exists():
        print(f"ERROR: analyzer script not found: {ANALYZER_SCRIPT}", file=sys.stderr)
        return 1

    command = [
        sys.executable,
        str(ANALYZER_SCRIPT),
        "--output-dir",
        str(output_dir),
    ]
    print(f"Running market breadth analyzer into {output_dir}")
    completed = subprocess.run(command, cwd=str(PROJECT_ROOT))
    if completed.returncode == 0:
        print(f"Market breadth artifacts directory: {output_dir}")
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
