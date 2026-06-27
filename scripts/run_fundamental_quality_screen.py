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
from src.fundamental_quality_screen import run_fundamental_quality_screen
from src.ticker_filters import filter_symbols, load_excluded_tickers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fundamental quality screen using Finviz prefilter plus local annual metrics.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Optional max Finviz-prefiltered candidates to evaluate.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker subset to keep from Finviz prefilter results.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", dest="as_of_date", help="Reference date for result labeling (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--reference-date", help="Reference date for result labeling (YYYY-MM-DD). Defaults to today.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    date_label = args.date_label or today_label()
    reference_date_text = args.as_of_date or args.reference_date
    reference_date = dt.date.fromisoformat(reference_date_text) if reference_date_text else dt.date.today()
    requested_tickers = filter_symbols(args.tickers, excluded) if args.tickers else None

    result = run_fundamental_quality_screen(
        as_of_date=reference_date,
        limit=args.limit,
        tickers=requested_tickers,
    )
    watchlist = [item.to_dict() for item in result.hits]

    artifact_paths = build_screener_artifact_paths(
        PROJECT_ROOT / "artifacts",
        strategy_id="fundamental_quality",
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
            "strategy_id": "fundamental_quality",
            "date_label": date_label,
            "reference_date": reference_date.isoformat(),
            "prefilter_source": result.prefilter_source,
            "annual_fundamentals_provider": result.annual_fundamentals_provider,
            "filters": result.filters,
            "requested_tickers": list(requested_tickers or []),
            "total_prefilter_candidates": result.total_prefilter_candidates,
            "evaluated_candidates": result.evaluated_candidates,
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
