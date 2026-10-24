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

from scripts._screen_run_persistence import _merge_watchlist_fields, persist_screen_run_artifacts_if_configured
from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config
from src.power_gap_volume_watch_screen import build_power_gap_volume_watchlist, run_power_gap_volume_watch
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker, load_universe
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria
from src.webapp.config import load_webapp_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the database-first Power Gap & Volume Watch daily screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--date-label")
    parser.add_argument("--as-of-date")
    parser.add_argument("--earnings-provider")
    add_universe_filter_args(parser)
    args = parser.parse_args()
    config = load_app_config(args.config)
    cutoff = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    excluded = load_excluded_tickers(config)
    if args.tickers:
        universe = [UniverseTicker(symbol=ticker) for ticker in filter_symbols(args.tickers, excluded)]
    else:
        universe = filter_universe_by_criteria(
            filter_universe_tickers(load_universe(config, limit=args.limit), excluded),
            build_filter_criteria_from_args(args),
        )
    result = run_power_gap_volume_watch(config, universe, as_of_date=cutoff, provider=args.earnings_provider)
    watchlist = build_power_gap_volume_watchlist(result["hits"])
    _merge_watchlist_fields(result, watchlist)
    date_label = args.date_label or result["run_date"]
    paths = build_screener_artifact_paths(load_webapp_config().artifacts_dir, strategy_id="power_gap_volume_watch", date_label=date_label)
    for path, payload in ((paths.raw_results_path, result), (paths.watchlist_path, watchlist),
                          (paths.summary_path, {"strategy_id": "power_gap_volume_watch", "rules_version": result["rules_version"],
                                                "date_label": date_label, "as_of_date": result["run_date"],
                                                "source": "manual-tickers" if args.tickers else "exchange-universe",
                                                "total_tickers": result["total_tickers"], "passed_tickers": result["passed_tickers"],
                                                "failed_tickers": result["failed_tickers"],
                                                "raw_results_file": str(paths.raw_results_path),
                                                "watchlist_file": str(paths.watchlist_path)})):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"Power Gap & Volume Watch: {result['passed_tickers']}/{result['total_tickers']} hits; "
          f"{len(result['failed_tickers'])} ticker/data issues; as of {result['run_date']}", flush=True)
    print(f"Wrote raw results to {paths.raw_results_path}", flush=True)
    print(f"Wrote watchlist to {paths.watchlist_path}", flush=True)
    print(f"Wrote run summary to {paths.summary_path}", flush=True)
    persisted = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=paths.summary_path,
        option_overrides={"market_data_source": "database-first"},
    )
    if persisted is not None:
        print(f"Persisted screen run id={persisted}", flush=True)
    # Partial component coverage must not discard valid hits, but a run with no
    # hits and blocked required observations is not a successful empty scan.
    return 2 if result["failed_tickers"] and not result["passed_tickers"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
