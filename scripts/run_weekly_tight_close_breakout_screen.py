#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config, today_label
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker, load_universe
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria
from src.weekly_tight_close_breakout_watchlist_builder import build_weekly_tight_close_breakout_watchlist
from src.weekly_tight_close_screen import run_weekly_tight_close_breakout_screen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the weekly tight close breakout screener.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the candidate set for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the configured universe.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    add_universe_filter_args(parser)
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list[UniverseTicker]:
    return [UniverseTicker(symbol=normalized) for normalized in filter_symbols(symbols, excluded)]


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    filter_criteria = build_filter_criteria_from_args(args)
    tickers = _manual_tickers(args.tickers, excluded) if args.tickers else load_universe(config, limit=args.limit)
    if not args.tickers:
        tickers = filter_universe_by_criteria(tickers, filter_criteria)

    result = run_weekly_tight_close_breakout_screen(config, tickers, as_of_date=as_of_date)
    watchlist = build_weekly_tight_close_breakout_watchlist(result.hits)
    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id="weekly_tight_close_breakout", date_label=date_label)
    _write_json(artifact_paths.raw_results_path, result.to_dict())
    _write_json(artifact_paths.watchlist_path, watchlist)
    _write_json(
        artifact_paths.summary_path,
        {
            "strategy_id": "weekly_tight_close_breakout",
            "date_label": date_label,
            "as_of_date": as_of_date.isoformat() if as_of_date else None,
            "total_tickers": result.total_tickers,
            "passed_tickers": result.passed_tickers,
            "failed_tickers": result.failed_tickers,
            "raw_results_file": str(artifact_paths.raw_results_path),
            "watchlist_file": str(artifact_paths.watchlist_path),
        },
    )

    print(f"Wrote raw results to {artifact_paths.raw_results_path}")
    print(f"Wrote watchlist to {artifact_paths.watchlist_path}")
    print(f"Wrote run summary to {artifact_paths.summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
