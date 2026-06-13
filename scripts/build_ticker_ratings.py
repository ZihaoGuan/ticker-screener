#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import today_label
from src.ratings.calculator import build_ticker_rating
from src.ratings.constants import MIN_SECTOR_PEERS_DEFAULT
from src.ratings.repository import RatingsRepository
from src.webapp.config import load_webapp_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ticker ratings from fundamentals snapshots and sector baselines.")
    parser.add_argument("--as-of-date", default=today_label())
    parser.add_argument("--min-sector-peers", type=int, default=MIN_SECTOR_PEERS_DEFAULT)
    parser.add_argument("--min-category-metrics", type=float, default=1.0)
    parser.add_argument("--database-url", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = (args.database_url or load_webapp_config().database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(str(args.as_of_date))
    repository = RatingsRepository(database_url)
    snapshots = repository.load_fundamentals_for_date(as_of_date)
    baselines = repository.load_sector_baselines_for_date(as_of_date)
    ratings = [
        build_ticker_rating(
            snapshot,
            baselines_by_metric=baselines.get(str(snapshot.sector or ""), {}),
            min_sector_peers=args.min_sector_peers,
        )
        for snapshot in snapshots
    ]
    count = repository.replace_rating_snapshots(as_of_date, ratings)
    ok_count = len([item for item in ratings if item.rating_status == "ok"])
    print(f"ticker_ratings={count}", flush=True)
    print(f"ticker_ratings_ok={ok_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
