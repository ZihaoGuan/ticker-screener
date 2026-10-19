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
from src.market_data_access import db_frame_has_recent_coverage, load_active_universe_from_db, load_many_ticker_windows, resolve_database_url
from src.qullamaggie_screen import evaluate_qullamaggie_ep, evaluate_qullamaggie_parabolic_short
from src.ticker_filters import filter_symbols, filter_universe_tickers, load_excluded_tickers
from src.universe import UniverseTicker
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria


def main(strategy_id: str, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=f"Run the daily-data {strategy_id} candidate scan.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--tickers", nargs="+")
    parser.add_argument("--date-label")
    parser.add_argument("--as-of-date")
    add_universe_filter_args(parser)
    args = parser.parse_args(argv)
    config = load_app_config(args.config)
    as_of = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.date.today()
    date_label = args.date_label or today_label(as_of)
    database_url = resolve_database_url(None)
    criteria = build_filter_criteria_from_args(args)
    excluded = load_excluded_tickers(config)
    universe = (
        [UniverseTicker(symbol=symbol) for symbol in filter_symbols(args.tickers, excluded)]
        if args.tickers
        else filter_universe_by_criteria(filter_universe_tickers(load_active_universe_from_db(as_of_date=as_of, limit=args.limit, database_url=database_url), excluded), criteria)
    )
    frames = load_many_ticker_windows([item.symbol for item in universe], as_of, 380, database_url=database_url)
    evaluate = evaluate_qullamaggie_ep if strategy_id == "qullamaggie_ep" else evaluate_qullamaggie_parabolic_short
    hits = []
    failures = []
    for ticker in universe:
        frame = frames.get(ticker.symbol.upper())
        if not db_frame_has_recent_coverage(frame, as_of):
            error = "missing DB daily bars" if frame is None or frame.empty else f"stale DB daily bars (latest {frame.index.max()})"
            failures.append({"ticker": ticker.symbol, "error": error})
            continue
        try:
            evidence = evaluate(frame)
            if evidence:
                hits.append({"ticker": ticker.symbol.upper(), "signal_date": as_of.isoformat(), **evidence})
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
    paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=strategy_id, date_label=date_label)
    paths.raw_results_path.parent.mkdir(parents=True, exist_ok=True)
    paths.raw_results_path.write_text(json.dumps({"run_date": as_of.isoformat(), "total_tickers": len(universe), "passed_tickers": len(hits), "failed_tickers": failures, "hits": hits}, indent=2), encoding="utf-8")
    paths.watchlist_path.write_text(json.dumps(hits, indent=2), encoding="utf-8")
    paths.summary_path.write_text(json.dumps({"strategy_id": strategy_id, "date_label": date_label, "as_of_date": as_of.isoformat(), "total_tickers": len(universe), "passed_tickers": len(hits), "failed_tickers": failures, "raw_results_file": str(paths.raw_results_path), "watchlist_file": str(paths.watchlist_path)}, indent=2), encoding="utf-8")
    run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=paths.summary_path,
        option_overrides={"market_data_source": "database"},
    )
    if run_id is not None:
        print(f"Persisted screen run id={run_id}")
    return 0
