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

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured

from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config, override_config, today_label
from src.htf_runup_screen import run_htf_runup_screen
from src.htf_runup_watchlist_builder import build_htf_runup_watchlist
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker, load_universe
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the 8-week 100% runup screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the universe for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of full exchange universe.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    add_universe_filter_args(parser)
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list[UniverseTicker]:
    deduped: list[UniverseTicker] = []
    for normalized in filter_symbols(symbols, excluded):
        deduped.append(UniverseTicker(symbol=normalized))
    return deduped


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    if args.limit:
        config = override_config(config, max_tickers=args.limit)
    excluded = load_excluded_tickers(config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    filter_criteria = build_filter_criteria_from_args(args)

    if args.tickers:
        universe = _manual_tickers(args.tickers, excluded)
    else:
        universe = load_universe(config, limit=args.limit)
        universe = filter_universe_by_criteria(universe, filter_criteria)

    result = run_htf_runup_screen(config, universe, as_of_date=as_of_date)
    watchlist = build_htf_runup_watchlist(result.hits)

    artifact_paths = build_screener_artifact_paths(
        PROJECT_ROOT / "artifacts",
        strategy_id="eight_week_100_runup",
        date_label=date_label,
    )
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "strategy_id": "eight_week_100_runup",
            "date_label": date_label,
            "as_of_date": as_of_date.isoformat() if as_of_date else None,
            "signal_profile": "eight_week_100_runup",
            "total_tickers": result.total_tickers,
            "passed_tickers": result.passed_tickers,
            "failed_tickers": result.failed_tickers,
            "raw_results_file": str(raw_path),
            "watchlist_file": str(watchlist_path),
        },
    )

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")
    persisted_run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=summary_path,
    )
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
