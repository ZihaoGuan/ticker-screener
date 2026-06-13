#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Finviz fundamentals sync, sector baselines, and ticker ratings.")
    parser.add_argument("--as-of-date", default=today_label())
    parser.add_argument("--limit", type=int)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--include-sectors", nargs="+")
    parser.add_argument("--resume-from", default="")
    parser.add_argument("--delay-min-seconds", type=float, default=0.15)
    parser.add_argument("--delay-max-seconds", type=float, default=0.4)
    parser.add_argument("--batch-size-before-rest", type=int, default=500)
    parser.add_argument("--rest-seconds", type=float, default=5.0)
    parser.add_argument("--overwrite-policy", default="skip-existing", choices=("latest-date", "replace-date", "skip-existing"))
    parser.add_argument("--retry-failed-from-manifest", action="store_true")
    parser.add_argument("--circuit-breaker-consecutive-503", type=int, default=25)
    parser.add_argument("--min-sector-peers", type=int, default=20)
    parser.add_argument("--min-category-metrics", type=float, default=1.0)
    parser.add_argument("--database-url", default="")
    return parser.parse_args()


def _run(command: list[str]) -> None:
    print("running:", " ".join(command), flush=True)
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True)


def _format_elapsed(seconds: float) -> str:
    rounded = max(0, int(round(seconds)))
    minutes, secs = divmod(rounded, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _run_stage(stage_number: int, stage_total: int, stage_label: str, command: list[str]) -> None:
    print(f"Stage {stage_number}/{stage_total}: {stage_label}", flush=True)
    started = time.monotonic()
    _run(command)
    print(
        f"Stage {stage_number}/{stage_total} complete: {stage_label} · elapsed={_format_elapsed(time.monotonic() - started)}",
        flush=True,
    )


def main() -> int:
    args = parse_args()
    shared = ["--as-of-date", str(args.as_of_date)]
    if args.database_url:
        shared.extend(["--database-url", args.database_url])

    sync_command = [sys.executable, "scripts/sync_finviz_fundamentals.py", *shared]
    if args.limit is not None:
        sync_command.extend(["--limit", str(args.limit)])
    if args.tickers:
        sync_command.append("--tickers")
        sync_command.extend(args.tickers)
    if args.include_sectors:
        sync_command.append("--include-sectors")
        sync_command.extend(args.include_sectors)
    if args.resume_from:
        sync_command.extend(["--resume-from", args.resume_from])
    if args.retry_failed_from_manifest:
        sync_command.append("--retry-failed-from-manifest")
    sync_command.extend(
        [
            "--delay-min-seconds",
            str(args.delay_min_seconds),
            "--delay-max-seconds",
            str(args.delay_max_seconds),
            "--batch-size-before-rest",
            str(args.batch_size_before_rest),
            "--rest-seconds",
            str(args.rest_seconds),
            "--overwrite-policy",
            str(args.overwrite_policy),
            "--circuit-breaker-consecutive-503",
            str(args.circuit_breaker_consecutive_503),
        ]
    )
    pipeline_started = time.monotonic()
    _run_stage(1, 3, "Sync Finviz Fundamentals", sync_command)
    baseline_command = [sys.executable, "scripts/build_sector_rating_baselines.py", *shared]
    if args.include_sectors:
        baseline_command.append("--include-sectors")
        baseline_command.extend(args.include_sectors)
    _run_stage(2, 3, "Build Sector Rating Baselines", baseline_command)
    rating_command = [
        sys.executable,
        "scripts/build_ticker_ratings.py",
        *shared,
        "--min-sector-peers",
        str(args.min_sector_peers),
        "--min-category-metrics",
        str(args.min_category_metrics),
    ]
    if args.include_sectors:
        rating_command.append("--include-sectors")
        rating_command.extend(args.include_sectors)
    _run_stage(
        3,
        3,
        "Build Ticker Ratings",
        rating_command,
    )
    print(f"Pipeline complete · elapsed={_format_elapsed(time.monotonic() - pipeline_started)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
