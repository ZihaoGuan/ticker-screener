#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, override_config, today_label
from src.lost_21ema_screen import run_lost_21ema_screen
from src.lost_21ema_watchlist_builder import build_lost_21ema_watchlist
from src.ticker_filters import filter_symbols, load_excluded_tickers
from src.universe import UniverseTicker, load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the recent 21 EMA loss to 50D MA weakness screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the candidate set for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the configured universe.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_tickers(symbols: list[str], excluded: set[str]) -> list[UniverseTicker]:
    tickers: list[UniverseTicker] = []
    for normalized in filter_symbols(symbols, excluded):
        tickers.append(UniverseTicker(symbol=normalized))
    return tickers


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    if args.limit:
        config = override_config(config, max_tickers=args.limit)
    excluded = load_excluded_tickers(config)
    date_label = args.date_label or today_label()
    tickers = _manual_tickers(args.tickers, excluded) if args.tickers else load_universe(config, limit=args.limit)

    result = run_lost_21ema_screen(config, tickers)
    watchlist = build_lost_21ema_watchlist(result.hits)

    raw_path = PROJECT_ROOT / "artifacts" / "raw" / f"lost_21ema_{date_label}.json"
    watchlist_path = PROJECT_ROOT / "artifacts" / "watchlists" / f"lost_21ema_{date_label}.json"
    summary_path = PROJECT_ROOT / "artifacts" / "raw" / f"lost_21ema_run_summary_{date_label}.json"

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "date_label": date_label,
            "source": "manual-tickers" if args.tickers else "exchange-universe",
            "total_tickers": result.total_tickers,
            "passed_tickers": result.passed_tickers,
            "failed_tickers": result.failed_tickers,
            "testing_50d_support_count": sum(1 for item in result.hits if item.support_state == "testing_50d_support"),
            "lost_50d_support_count": sum(1 for item in result.hits if item.support_state == "lost_50d_support"),
            "raw_results_file": str(raw_path),
            "watchlist_file": str(watchlist_path),
        },
    )

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
