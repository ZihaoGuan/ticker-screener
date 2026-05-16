#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RENDERER_PATH = PROJECT_ROOT / "vendor" / "trade_master_signals" / "render_sector_rotation_rrg.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render an RRG-style sector rotation map.")
    parser.add_argument("--output-dir", help="Directory for rendered files.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--period", default="3y")
    parser.add_argument("--trail-weeks", type=int, default=12)
    parser.add_argument("--ratio-window", type=int, default=10)
    parser.add_argument("--momentum-window", type=int, default=4)
    parser.add_argument("--universe", choices=("sector", "industry"), default="industry")
    parser.add_argument("--tickers", nargs="*")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else PROJECT_ROOT / "artifacts" / "output" / "sector_rotation_rrg"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        str(RENDERER_PATH),
        "--output-dir",
        str(output_dir),
        "--benchmark",
        args.benchmark,
        "--period",
        args.period,
        "--trail-weeks",
        str(args.trail_weeks),
        "--ratio-window",
        str(args.ratio_window),
        "--momentum-window",
        str(args.momentum_window),
        "--universe",
        args.universe,
    ]
    if args.tickers:
        command.extend(["--tickers", *args.tickers])

    subprocess.run(command, check=True)
    print(f"Rendered RRG output to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
