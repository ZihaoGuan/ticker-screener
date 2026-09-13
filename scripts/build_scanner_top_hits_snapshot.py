#!/usr/bin/env python3
"""Build the complete Top Hits read-model after scanner data is ready."""
from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.watchlist_service import WatchlistService


def main() -> int:
    config = load_webapp_config()
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
