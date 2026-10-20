#!/usr/bin/env python3
from __future__ import annotations

import json
import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_forward_test_service import MartinForwardTestService, SECTOR_STRATEGY_ID
from src.webapp.services.watchlist_service import WatchlistService


def main() -> int:
    parser = argparse.ArgumentParser(description="Process one exact Martin Focus forward-test session.")
    parser.add_argument("--as-of-date", help="Trading session date; defaults to the latest persisted Top Hits snapshot date.")
    args = parser.parse_args()
    config = load_webapp_config()
    watchlist = WatchlistService(artifacts_dir=config.artifacts_dir, database_url=config.database_url,
                                 market_data_source="database-first")
    snapshot = watchlist._load_latest_persisted_scanner_top_hits_payload()
    date_text = args.as_of_date or (snapshot or {}).get("target_trading_date")
    if not date_text:
        print("No as-of date or persisted scanner_top_hits_snapshot; no session processed.")
        return 0
    service = MartinForwardTestService(MartinForwardTestRepository(database_url=config.database_url), database_url=config.database_url)
    day = dt.date.fromisoformat(date_text)
    result = {"account": service.run_session(as_of_date=day, snapshot=snapshot),
              "sector_opportunity": service.run_session(as_of_date=day, snapshot=snapshot, strategy_id=SECTOR_STRATEGY_ID)}
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
