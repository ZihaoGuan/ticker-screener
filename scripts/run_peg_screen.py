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

from src.config import load_app_config, today_label
from src.cookstock_bridge import load_configured_cookstock
from src.peg_screen import EarningsEvent, run_peg_screen
from src.peg_watchlist_builder import build_peg_watchlist
from src.universe import load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the PEG screener.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the candidate set for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the configured source.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument(
        "--source",
        choices=("universe", "earnings-watchlist"),
        default="universe",
        help="Candidate source. Default scans the configured exchange universe.",
    )
    parser.add_argument(
        "--reference-date",
        help="Reference date for next-week earnings watchlist (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _manual_events(symbols: list[str]) -> list[EarningsEvent]:
    deduped: list[EarningsEvent] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = symbol.upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(EarningsEvent(ticker=normalized))
    return deduped


def _universe_events(config_path: str, limit: int | None) -> list[EarningsEvent]:
    config = load_app_config(config_path)
    universe = load_universe(config, limit=limit)
    return [
        EarningsEvent(
            ticker=item.symbol,
            sector=item.sector,
            exchange=item.exchange,
        )
        for item in universe
    ]


def _next_week_events(config_path: str, reference_date: dt.date | None, limit: int | None) -> list[EarningsEvent]:
    config = load_app_config(config_path)
    cookstock = load_configured_cookstock(config)
    raw_events = cookstock.fetch_next_week_earnings_watchlist(reference_date=reference_date)
    events = [
        EarningsEvent(
            ticker=str(item["ticker"]).upper(),
            earnings_date=str(item.get("event_date")) if item.get("event_date") else None,
            summary=str(item.get("summary")) if item.get("summary") else None,
            sector=None,
            exchange=None,
        )
        for item in raw_events
    ]
    if limit is not None:
        return events[:limit]
    return events


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    date_label = args.date_label or today_label()
    reference_date = dt.date.fromisoformat(args.reference_date) if args.reference_date else None

    if args.tickers:
        earnings_events = _manual_events(args.tickers)
    elif args.source == "earnings-watchlist":
        earnings_events = _next_week_events(args.config, reference_date, args.limit)
    else:
        earnings_events = _universe_events(args.config, args.limit)

    result = run_peg_screen(config, earnings_events)
    watchlist = build_peg_watchlist(result.hits)

    raw_path = PROJECT_ROOT / "artifacts" / "raw" / f"peg_earnings_gap_{date_label}.json"
    watchlist_path = PROJECT_ROOT / "artifacts" / "watchlists" / f"peg_earnings_gap_{date_label}.json"
    summary_path = PROJECT_ROOT / "artifacts" / "raw" / f"peg_run_summary_{date_label}.json"

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "date_label": date_label,
            "source": args.source if not args.tickers else "manual-tickers",
            "reference_date": str(reference_date) if reference_date else None,
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
