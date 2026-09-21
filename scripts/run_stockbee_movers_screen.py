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
from src.config import load_app_config, today_label
from src.market_data_access import load_active_universe_from_db, resolve_database_url
from src.stockbee_movers_screen import STOCKBEE_MOVER_PROFILES, run_stockbee_mover_screen
from src.stockbee_movers_watchlist_builder import build_stockbee_movers_watchlist
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a lightweight Stockbee mover scanner.")
    parser.add_argument("--profile", choices=tuple(STOCKBEE_MOVER_PROFILES), required=True)
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the candidate set for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the active universe.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    add_universe_filter_args(parser)
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    database_url = resolve_database_url()
    if args.tickers:
        tickers = [UniverseTicker(symbol=symbol) for symbol in filter_symbols(args.tickers, excluded)]
    else:
        tickers = load_active_universe_from_db(as_of_date=as_of_date, limit=args.limit, database_url=database_url)
        tickers = filter_universe_tickers(tickers, excluded)
        tickers = filter_universe_by_criteria(tickers, build_filter_criteria_from_args(args))

    result = run_stockbee_mover_screen(config, tickers, profile=args.profile, as_of_date=as_of_date, database_url=database_url)
    watchlist = build_stockbee_movers_watchlist(result.hits)
    paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=args.profile, date_label=date_label)
    _write_json(paths.raw_results_path, result.to_dict())
    _write_json(paths.watchlist_path, watchlist)
    _write_json(paths.summary_path, {
        "strategy_id": args.profile, "date_label": date_label,
        "as_of_date": as_of_date.isoformat() if as_of_date else None,
        "total_tickers": result.total_tickers, "passed_tickers": result.passed_tickers,
        "failed_tickers": result.failed_tickers,
        "raw_results_file": str(paths.raw_results_path), "watchlist_file": str(paths.watchlist_path),
    })
    run_id = persist_screen_run_artifacts_if_configured(args=args, summary_path=paths.summary_path)
    print(f"Wrote {args.profile}: {result.passed_tickers}/{result.total_tickers} tickers")
    if run_id is not None:
        print(f"Persisted screen run id={run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
