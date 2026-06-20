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
from src.market_data_access import load_many_ticker_windows
from src.ratings.repository import RatingsRepository
from src.ratings.technical_indicator import build_multi_timeframe_technical_indicator_ratings
from src.webapp.config import load_webapp_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build TradingView-style multi-timeframe technical indicator ratings from daily bars.")
    parser.add_argument("--as-of-date", default=today_label())
    parser.add_argument("--tickers", nargs="+", help="Only rebuild ratings for selected tickers.")
    parser.add_argument("--include-sectors", nargs="+", help="Only rebuild ratings for selected sectors.")
    parser.add_argument("--limit", type=int, default=None, help="Optional universe cap for faster rebuilds.")
    parser.add_argument("--database-url", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    web_config = load_webapp_config()
    database_url = (args.database_url or web_config.database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(str(args.as_of_date))
    repository = RatingsRepository(database_url)
    selected_tickers = tuple(str(item).strip().upper() for item in (args.tickers or []) if str(item).strip())
    selected_sectors = tuple(str(item).strip() for item in (args.include_sectors or []) if str(item).strip())
    target_tickers = repository.list_active_tickers(
        tickers=selected_tickers or None,
        sectors=selected_sectors or None,
        limit=args.limit,
    )
    print(
        "loading_technical_indicator_universe "
        f"as_of_date={as_of_date.isoformat()} "
        f"tickers={len(target_tickers)} "
        f"include_sectors={','.join(selected_sectors) or '-'}",
        flush=True,
    )
    if not target_tickers:
        repository.replace_technical_indicator_rating_snapshots(as_of_date, [], tickers=[])
        print("technical_indicator_ratings=0", flush=True)
        print("technical_indicator_ratings_ok=0", flush=True)
        return 0

    frame_map = load_many_ticker_windows(
        target_tickers,
        as_of_date,
        5200,
        database_url=database_url,
    )
    ratings = []
    ok_count = 0
    total = len(target_tickers)
    for index, ticker in enumerate(target_tickers, start=1):
        per_timeframe = build_multi_timeframe_technical_indicator_ratings(
            ticker,
            frame_map.get(ticker),
            as_of_date=as_of_date,
        )
        ratings.extend(per_timeframe)
        ok_count += sum(1 for item in per_timeframe if item.technical_status == "ok")
        status_summary = ",".join(f"{item.timeframe}:{item.technical_status}" for item in per_timeframe)
        print(f"[{index}/{total}] technical_indicator_rating {ticker} statuses={status_summary}", flush=True)

    count = repository.replace_technical_indicator_rating_snapshots(
        as_of_date,
        ratings,
        tickers=target_tickers,
    )
    print(f"technical_indicator_ratings={count}", flush=True)
    print(f"technical_indicator_ratings_ok={ok_count}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
