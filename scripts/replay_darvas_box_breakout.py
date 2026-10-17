#!/usr/bin/env python3
"""Replay Darvas rejection telemetry on explicit historical dates."""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.darvas_box_screen import run_darvas_box_breakout_screen
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker, load_universe


def aggregate_replay_results(results: list[dict[str, object]]) -> dict[str, object]:
    """Sum date-level production result telemetry without recalculating predicates."""
    reasons: dict[str, int] = {}
    totals = {"total_tickers": 0, "passed_tickers": 0, "strategy_rejected": 0, "data_failures": 0}
    for result in results:
        raw_reasons = result.get("rejection_counts", {})
        if isinstance(raw_reasons, dict):
            for reason, count in raw_reasons.items():
                reasons[str(reason)] = reasons.get(str(reason), 0) + int(count)
        accounting = result.get("accounting", {})
        if isinstance(accounting, dict):
            for field in totals:
                totals[field] += int(accounting.get(field, 0))
    return {"date_count": len(results), **totals, "rejection_counts": reasons}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Darvas rejection telemetry across explicit dates.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--dates", nargs="+", required=True, help="Historical as-of dates (YYYY-MM-DD).")
    parser.add_argument("--tickers", nargs="+", help="Explicit symbols; defaults to configured universe.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", type=Path, required=True, help="JSON output path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dates = [dt.date.fromisoformat(value) for value in args.dates]
    if len(set(dates)) != len(dates):
        raise SystemExit("--dates must not contain duplicates")
    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    tickers = (
        [UniverseTicker(symbol=symbol) for symbol in filter_symbols(args.tickers, excluded)]
        if args.tickers else load_universe(config, limit=args.limit)
    )
    # Historical telemetry must be deterministic. Do not turn missing database
    # history into network-dependent Yahoo requests; classify it in the run.
    results = [
        run_darvas_box_breakout_screen(config, tickers, as_of_date=as_of, allow_fallback=False).to_dict()
        for as_of in dates
    ]
    payload = {"strategy_id": "darvas_box_breakout", "as_of_dates": [value.isoformat() for value in dates],
               "results": results, "aggregate": aggregate_replay_results(results)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
