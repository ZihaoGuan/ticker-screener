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
from src.gamma_squeeze_screen import default_gamma_squeeze_universe, run_gamma_squeeze_screen
from src.gamma_squeeze_watchlist_builder import build_gamma_squeeze_watchlist
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker


STRATEGY_ID = "gamma_squeeze"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run gamma squeeze screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit default options universe for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--as-of-date", help="Historical label date for replay mode (YYYY-MM-DD).")
    parser.add_argument("--min-squeeze-score", type=float, default=65.0, help="Minimum score required to pass.")
    parser.add_argument("--timeout-seconds", type=int, default=20, help="Per-symbol CBOE request timeout.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_tickers(symbols: list[str], *, excluded: set[str]) -> list[UniverseTicker]:
    return [UniverseTicker(symbol=ticker) for ticker in filter_symbols(symbols, excluded)]


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else None
    date_label = args.date_label or today_label(as_of_date)

    if args.tickers:
        universe = _manual_tickers(args.tickers, excluded=excluded)
    else:
        universe = default_gamma_squeeze_universe(config, limit=args.limit)

    result = run_gamma_squeeze_screen(
        config,
        universe,
        as_of_date=as_of_date,
        min_squeeze_score=float(args.min_squeeze_score),
        timeout_seconds=max(1, int(args.timeout_seconds)),
    )
    watchlist = build_gamma_squeeze_watchlist(result.hits)

    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id=STRATEGY_ID, date_label=date_label)
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "strategy_id": STRATEGY_ID,
            "date_label": date_label,
            "as_of_date": as_of_date.isoformat() if as_of_date else None,
            "signal_profile": STRATEGY_ID,
            "min_squeeze_score": float(args.min_squeeze_score),
            "total_tickers": result.total_tickers,
            "passed_tickers": result.passed_tickers,
            "failed_tickers": len(result.failed_tickers),
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
        option_overrides={"min_squeeze_score": float(args.min_squeeze_score)},
    )
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
