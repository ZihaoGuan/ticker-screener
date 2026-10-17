#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.daily_screening_features import build_daily_screening_features
from src.market_data_access import _connect, resolve_database_url
from src.ticker_filters import load_excluded_tickers


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the daily Postgres screening-feature snapshot.")
    parser.add_argument("--trade-date", help="Trade date (YYYY-MM-DD); defaults to the latest available daily-bars date.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    args = parser.parse_args()
    database_url = resolve_database_url()
    date = dt.date.fromisoformat(args.trade_date) if args.trade_date else None
    if date is None:
        connection = _connect(database_url)
        if connection is None:
            raise RuntimeError("Postgres database is unavailable.")
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) FROM daily_bars")
                latest = cursor.fetchone()[0]
        if latest is None:
            raise RuntimeError("daily_bars has no available trade date.")
        date = latest
    result = build_daily_screening_features(
        date,
        database_url=database_url,
        excluded_tickers=load_excluded_tickers(load_app_config(args.config)),
    )
    print("Daily screening features built: " + ", ".join(f"{key}={value}" for key, value in result.items()), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
