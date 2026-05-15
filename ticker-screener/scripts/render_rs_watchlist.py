#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RENDERER_PATH = PROJECT_ROOT / "vendor" / "trade_master_signals" / "render_watchlist_candles.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render RS watchlist charts.")
    parser.add_argument("--watchlist-file", required=True, help="Path to the watchlist JSON file.")
    parser.add_argument("--output-dir", help="Directory for rendered charts.")
    parser.add_argument("--benchmark", default="SPY")
    parser.add_argument("--period", default="18mo")
    parser.add_argument("--lookback", type=int, default=120)
    parser.add_argument("--split-pages", type=int, default=4)
    parser.add_argument("--montage-columns", type=int, default=2)
    parser.add_argument("--card-width", type=int, default=700)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    watchlist_path = Path(args.watchlist_file).resolve()
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else PROJECT_ROOT / "artifacts" / "output" / watchlist_path.stem
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        str(RENDERER_PATH),
        "--watchlist-file",
        str(watchlist_path),
        "--output-dir",
        str(output_dir),
        "--benchmark",
        args.benchmark,
        "--period",
        args.period,
        "--lookback",
        str(args.lookback),
        "--split-pages",
        str(args.split_pages),
        "--montage-columns",
        str(args.montage_columns),
        "--card-width",
        str(args.card_width),
    ]
    subprocess.run(command, check=True)
    print(f"Rendered charts to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
