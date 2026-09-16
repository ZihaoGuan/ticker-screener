#!/usr/bin/env python3
"""Build the complete Top Hits read-model after scanner data is ready."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.watchlist_service import WatchlistService
from src.webapp.services.snapshot_preflight import check_required_scheduled_jobs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the Top Hits snapshot after its scheduled prerequisites complete.")
    parser.add_argument("--required-job-id", action="append", default=[], help="Scheduled job ID required to finish successfully today (repeatable).")
    args = parser.parse_args(argv)
    config = load_webapp_config()
    preflight = check_required_scheduled_jobs(
        status_dir=config.artifacts_dir / "status",
        required_job_ids=args.required_job_id,
    )
    if not preflight.ready:
        print(
            "SNAPSHOT_WAITING: Top Hits retained previous snapshot; "
            f"waiting for {', '.join(preflight.pending_job_ids)} for {preflight.target_date}."
        )
        return 0
    service = WatchlistService(
        artifacts_dir=config.artifacts_dir,
        database_url=config.database_url,
        market_data_source=config.market_data_source,
    )
    payload = service.persist_scanner_top_hits_snapshot()
    if payload is None:
        print("Top Hits snapshot skipped: database is not configured.")
        return 1
    print(f"Top Hits snapshot persisted: {len(payload.get('rows') or [])} overlapping tickers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
