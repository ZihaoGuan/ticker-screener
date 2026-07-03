#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured

from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config, today_label
from src.finviz_pattern_scanner import (
    FINVIZ_PATTERN_OPTIONS,
    build_finviz_pattern_strategy_id,
    resolve_finviz_pattern_filter,
    resolve_finviz_pattern_label,
    run_finviz_pattern_scanner,
)
from src.ticker_filters import filter_symbols, load_excluded_tickers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run live Finviz chart-pattern scanner.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--pattern", choices=[value for value, _label in FINVIZ_PATTERN_OPTIONS], required=True)
    parser.add_argument("--limit", type=int, help="Optional max rows to keep after Finviz scan.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker subset to keep from Finviz results.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    date_label = args.date_label or today_label()
    requested_tickers = filter_symbols(args.tickers, excluded) if args.tickers else None
    pattern = str(args.pattern).strip().lower()
    pattern_label = resolve_finviz_pattern_label(pattern)
    strategy_id = build_finviz_pattern_strategy_id(pattern)

    result = run_finviz_pattern_scanner(pattern=pattern, limit=args.limit, tickers=requested_tickers)
    watchlist = list(result.get("hits") or [])

    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=strategy_id, date_label=date_label)
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    _write_json(raw_path, result)
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "strategy_id": strategy_id,
            "screen_name": f"Finviz Pattern: {pattern_label}",
            "source": "finviz.screener",
            "pattern": pattern,
            "pattern_label": pattern_label,
            "finviz_filter": resolve_finviz_pattern_filter(pattern),
            "date_label": date_label,
            "requested_tickers": list(requested_tickers or []),
            "total_candidates": result.get("total_candidates", 0),
            "returned_candidates": result.get("returned_candidates", 0),
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
