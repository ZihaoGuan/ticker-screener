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
from src.config import load_app_config, today_label
from src.market_data_access import load_active_universe_from_db, resolve_database_url
from src.stockbee_momentum_burst_screen import DEFAULT_MARKET_GATE, run_stockbee_momentum_burst_screen
from src.stockbee_momentum_burst_watchlist_builder import build_stockbee_momentum_burst_watchlist
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def _log(message: str) -> None:
    print(message, flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Stockbee momentum burst screener from local DB market data.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit candidate set for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of DB-backed active universe.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical as-of date for replay mode (YYYY-MM-DD).")
    parser.add_argument(
        "--market-gate",
        choices=("allowed", "neutral", "restrictive"),
        default=DEFAULT_MARKET_GATE,
        help="Market regime alignment for score weighting.",
    )
    add_universe_filter_args(parser)
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list[UniverseTicker]:
    return [UniverseTicker(symbol=symbol) for symbol in filter_symbols(symbols, excluded)]


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)
    database_url = resolve_database_url()
    filter_criteria = build_filter_criteria_from_args(args)

    if args.tickers:
        tickers = _manual_tickers(args.tickers, excluded)
    else:
        tickers = load_active_universe_from_db(
            as_of_date=as_of_date,
            limit=args.limit,
            database_url=database_url,
        )
        tickers = filter_universe_tickers(tickers, excluded)
        tickers = filter_universe_by_criteria(tickers, filter_criteria)

    _log(
        f"prepared stockbee universe: total={len(tickers)} "
        f"source={'manual-tickers' if args.tickers else 'database-universe'} market_gate={args.market_gate}"
    )
    result = run_stockbee_momentum_burst_screen(
        config,
        tickers,
        as_of_date=as_of_date,
        market_gate=args.market_gate,
        database_url=database_url,
    )
    watchlist_hits = [hit for hit in result.hits if hit.rating in {"A", "A-"}]
    watchlist = build_stockbee_momentum_burst_watchlist(watchlist_hits)
    _log(
        f"watchlist filter applied: raw_hits={len(result.hits)} "
        f"scanner_entries_a_minus_or_better={len(watchlist_hits)}"
    )

    artifact_paths = build_screener_artifact_paths(
        PROJECT_ROOT / "artifacts",
        strategy_id="stockbee_momentum_burst",
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
            "strategy_id": "stockbee_momentum_burst",
            "date_label": date_label,
            "as_of_date": as_of_date.isoformat() if as_of_date else None,
            "source": "manual-tickers" if args.tickers else "database-universe",
            "market_gate": args.market_gate,
            "total_tickers": result.total_tickers,
            "passed_tickers": result.passed_tickers,
            "watchlist_passed_tickers": len(watchlist_hits),
            "failed_tickers": result.failed_tickers,
            "rejected_count": len(result.rejected_tickers),
            "raw_results_file": str(raw_path),
            "watchlist_file": str(watchlist_path),
        },
    )

    _log(f"Wrote raw results to {raw_path}")
    _log(f"Wrote watchlist to {watchlist_path}")
    _log(f"Wrote run summary to {summary_path}")
    persisted_run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=summary_path,
    )
    if persisted_run_id is not None:
        _log(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
