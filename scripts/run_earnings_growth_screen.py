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
from src.earnings_growth_screen import run_earnings_growth_screen
from src.earnings_growth_watchlist_builder import build_earnings_growth_watchlist
from src.pre_earnings_screen import PreEarningsEvent
from src.universe import load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the next-week earnings growth screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the next-week candidate set.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of the earnings watchlist.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument(
        "--reference-date",
        help="Reference date for next-week earnings watchlist (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _sector_by_symbol(config_path: str) -> dict[str, tuple[str | None, str | None]]:
    config = load_app_config(config_path)
    universe = load_universe(config)
    return {item.symbol: (item.sector, item.exchange) for item in universe}


def _manual_events(symbols: list[str], sector_map: dict[str, tuple[str | None, str | None]] | None = None) -> list[PreEarningsEvent]:
    events: list[PreEarningsEvent] = []
    seen: set[str] = set()
    lookup = sector_map or {}
    for symbol in symbols:
        ticker = str(symbol).upper().strip()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        sector, exchange = lookup.get(ticker, (None, None))
        events.append(
            PreEarningsEvent(
                ticker=ticker,
                sector=sector,
                exchange=exchange,
            )
        )
    return events


def _next_week_events(config_path: str, reference_date: dt.date | None, limit: int | None) -> list[PreEarningsEvent]:
    config = load_app_config(config_path)
    universe = load_universe(config)
    sector_map = {item.symbol: (item.sector, item.exchange) for item in universe}
    cookstock = load_configured_cookstock(config)
    raw_events = cookstock.fetch_next_week_earnings_watchlist(reference_date=reference_date)

    events: list[PreEarningsEvent] = []
    seen: set[str] = set()
    for item in raw_events:
        ticker = str(item["ticker"]).upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        sector, exchange = sector_map.get(ticker, (None, None))
        events.append(
            PreEarningsEvent(
                ticker=ticker,
                earnings_date=str(item.get("event_date")) if item.get("event_date") else None,
                summary=str(item.get("summary")) if item.get("summary") else None,
                sector=sector,
                exchange=exchange,
            )
        )
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
        events = _manual_events(args.tickers)
        source_label = "manual-tickers"
    else:
        events = _next_week_events(args.config, reference_date, args.limit)
        source_label = "next-week-earnings"

    result = run_earnings_growth_screen(config, events, as_of_date=reference_date or dt.date.today())
    watchlist = build_earnings_growth_watchlist(result.hits)

    raw_path = PROJECT_ROOT / "artifacts" / "raw" / f"earnings_growth_{date_label}.json"
    watchlist_path = PROJECT_ROOT / "artifacts" / "watchlists" / f"earnings_growth_{date_label}.json"
    summary_path = PROJECT_ROOT / "artifacts" / "raw" / f"earnings_growth_run_summary_{date_label}.json"

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "date_label": date_label,
            "source": source_label,
            "reference_date": str(reference_date) if reference_date else None,
            "earnings_provider": result.earnings_provider,
            "financials_provider": result.financials_provider,
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
