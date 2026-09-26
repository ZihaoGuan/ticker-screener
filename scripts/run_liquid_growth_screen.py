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
from src.liquid_growth_screen import LIQUID_GROWTH_STRATEGY_ID, load_liquid_growth_universe, run_liquid_growth_screen
from src.liquid_growth_watchlist_builder import build_liquid_growth_watchlist
from src.market_data_access import resolve_database_url
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the reported-data Liquid Growth (TML) screener.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--date-label")
    parser.add_argument("--as-of-date")
    add_universe_filter_args(parser)
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    database_url = resolve_database_url()
    excluded = load_excluded_tickers(config)
    if args.tickers:
        tickers = [UniverseTicker(symbol=symbol) for symbol in filter_symbols(args.tickers, excluded)]
        source = "manual-tickers"
    else:
        tickers = load_liquid_growth_universe(as_of_date=as_of_date, limit=args.limit, database_url=database_url)
        tickers = filter_universe_by_criteria(filter_universe_tickers(tickers, excluded), build_filter_criteria_from_args(args))
        source = "database-universe"
    result = run_liquid_growth_screen(config, tickers, as_of_date=as_of_date, database_url=database_url)
    paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=LIQUID_GROWTH_STRATEGY_ID, date_label=date_label)
    _write_json(paths.raw_results_path, result.to_dict())
    _write_json(paths.watchlist_path, build_liquid_growth_watchlist(result.hits))
    _write_json(paths.summary_path, {"strategy_id": LIQUID_GROWTH_STRATEGY_ID, "date_label": date_label, "as_of_date": as_of_date.isoformat() if as_of_date else None, "source": source, "total_tickers": result.total_tickers, "passed_tickers": result.passed_tickers, "failed_tickers": result.failed_tickers, "raw_results_file": str(paths.raw_results_path), "watchlist_file": str(paths.watchlist_path)})
    persisted_run_id = persist_screen_run_artifacts_if_configured(args=args, summary_path=paths.summary_path)
    print(f"Liquid Growth: {result.passed_tickers}/{result.total_tickers}; summary {paths.summary_path}", flush=True)
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
