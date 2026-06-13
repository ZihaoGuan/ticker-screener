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
from src.ratings.baselines import build_sector_baselines
from src.ratings.repository import RatingsRepository
from src.webapp.config import load_webapp_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build per-sector rating baselines from Finviz fundamentals snapshots.")
    parser.add_argument("--as-of-date", default=today_label())
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
    baselines = build_sector_baselines(snapshots, as_of_date=as_of_date)
    count = repository.replace_sector_metric_baselines(as_of_date, baselines)
    print(f"sector_metric_baselines={count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
