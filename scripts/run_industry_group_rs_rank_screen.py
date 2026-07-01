#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured

from src.artifact_paths import build_screener_artifact_paths
from src.config import today_label
from src.industry_group_rs_rank_screen import run_industry_group_rs_rank_screen
from src.market_data_access import resolve_database_url
from src.ratings.repository import RatingsRepository
from src.ticker_filters import filter_symbols


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run industry-group RS rank screener from persisted technical ratings.")
    parser.add_argument("--database-url", default="")
    parser.add_argument("--limit", type=int, help="Optional max ticker count to evaluate.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker subset.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    parser.add_argument("--minimum-rank", type=float, default=90.0, help="Minimum industry-group RS rank to keep.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    database_url = resolve_database_url(args.database_url)
    if not database_url:
        raise RuntimeError("No database URL configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    requested_tickers = filter_symbols(args.tickers, set()) if args.tickers else None
    if requested_tickers is None:
        requested_tickers = RatingsRepository(database_url).list_active_tickers(limit=args.limit)
    elif args.limit:
        requested_tickers = requested_tickers[: max(1, int(args.limit))]

    result = run_industry_group_rs_rank_screen(database_url=database_url, as_of_date=as_of_date, tickers=requested_tickers, minimum_rank=float(args.minimum_rank))
    watchlist = [item.to_dict() for item in result.hits]

    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id="industry_group_rs_rank", date_label=date_label)
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(summary_path, {
        "strategy_id": "industry_group_rs_rank",
        "date_label": date_label,
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "minimum_rank": float(args.minimum_rank),
        "source": "technical-rating-snapshots",
        "total_tickers": result.total_tickers,
        "passed_tickers": result.passed_tickers,
        "failed_tickers": result.failed_tickers,
        "raw_results_file": str(raw_path),
        "watchlist_file": str(watchlist_path),
    })

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")
    persisted_run_id = persist_screen_run_artifacts_if_configured(args=args, summary_path=summary_path)
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
