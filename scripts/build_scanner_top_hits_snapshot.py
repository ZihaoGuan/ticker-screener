#!/usr/bin/env python3
"""Build the complete Top Hits read-model after scanner data is ready."""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.watchlist_service import WatchlistService
from src.webapp.services.scanner_top_hits_refresh import refresh_scanner_top_hits


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Top Hits snapshot after its scheduled prerequisites complete.")
    parser.add_argument("--required-job-id", action="append", default=None, help="Scheduled job ID required to finish successfully today (repeatable).")
    parser.add_argument("--required-job-group", action="append", default=None, help="Scheduled prerequisite group (repeatable).")
    parser.add_argument("--skip-if-current", action="store_true", default=None, help="Do not rebuild when today's completed snapshot already exists.")
    parser.add_argument("--allow-incremental-refresh", action="store_true", default=None, help="Compatibility flag; all configured prerequisites must still be ready.")
    parser.add_argument("--debounce-seconds", type=int, default=None, help="Wait after the newest source run before rebuilding (default: 300).")
    args = parser.parse_args(argv)
    config = load_webapp_config()
    service = WatchlistService(
        artifacts_dir=config.artifacts_dir,
        database_url=config.database_url,
        market_data_source=config.market_data_source,
    )
    now = dt.datetime.now(dt.timezone.utc)
    options = {key: value for key, value in {
        "required_job_ids": args.required_job_id, "required_job_groups": args.required_job_group,
        "skip_if_current": args.skip_if_current, "debounce_seconds": args.debounce_seconds,
    }.items() if value is not None}
    result = refresh_scanner_top_hits(service, project_root=PROJECT_ROOT, now=now, options=options)
    print(f"SNAPSHOT_{result['status'].upper()}: {result['message']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
