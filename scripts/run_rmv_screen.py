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
from src.rmv_screen import run_rmv_screen
from src.rmv_watchlist_builder import build_rmv_watchlist
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker, load_universe
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Relative Measured Volatility tightness screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--date-label")
    parser.add_argument("--as-of-date")
    add_universe_filter_args(parser)
    args = parser.parse_args()
    config = load_app_config(args.config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    excluded = load_excluded_tickers(config)
    tickers = [UniverseTicker(symbol=symbol) for symbol in filter_symbols(args.tickers, excluded)] if args.tickers else filter_universe_by_criteria(filter_universe_tickers(load_universe(config, limit=args.limit), excluded), build_filter_criteria_from_args(args))
    result = run_rmv_screen(config, tickers, as_of_date=as_of_date)
    paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id="rmv_tightness", date_label=args.date_label or today_label(as_of_date))
    paths.raw_results_path.parent.mkdir(parents=True, exist_ok=True)
    paths.raw_results_path.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
    paths.watchlist_path.write_text(json.dumps(build_rmv_watchlist(result.hits), indent=2), encoding="utf-8")
    paths.summary_path.write_text(json.dumps({"strategy_id": "rmv_tightness", "date_label": args.date_label or today_label(as_of_date), "as_of_date": as_of_date.isoformat() if as_of_date else None, "total_tickers": result.total_tickers, "passed_tickers": result.passed_tickers, "failed_tickers": result.failed_tickers, "raw_results_file": str(paths.raw_results_path), "watchlist_file": str(paths.watchlist_path)}, indent=2), encoding="utf-8")
    persisted = persist_screen_run_artifacts_if_configured(args=args, summary_path=paths.summary_path)
    print(f"RMV Tightness: {result.passed_tickers}/{result.total_tickers}", flush=True)
    if persisted is not None:
        print(f"Persisted screen run id={persisted}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
