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
from src.cookstock_bridge import load_configured_cookstock
from src.pre_earnings_screen import PreEarningsEvent, run_pre_earnings_screen
from src.pre_earnings_watchlist_builder import build_pre_earnings_watchlist
from src.ticker_filters import filter_pre_earnings_events, filter_symbols, load_excluded_tickers
from src.universe import load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the pre-earnings focus screener.")
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


def _manual_events(
    symbols: list[str],
    sector_by_symbol: dict[str, tuple[str | None, str | None]],
    excluded: set[str],
) -> list[PreEarningsEvent]:
    deduped: list[PreEarningsEvent] = []
    for normalized in filter_symbols(symbols, excluded):
        sector, exchange = sector_by_symbol.get(normalized, (None, None))
        deduped.append(
            PreEarningsEvent(
                ticker=normalized,
                sector=sector,
                exchange=exchange,
            )
        )
    return deduped


def _sector_by_symbol(config_path: str) -> dict[str, tuple[str | None, str | None]]:
    config = load_app_config(config_path)
    excluded = load_excluded_tickers(config)
    universe = load_universe(config)
    return {
        item.symbol: (item.sector, item.exchange)
        for item in universe
    }


def _next_week_events(
    config_path: str,
    reference_date: dt.date | None,
    limit: int | None,
) -> list[PreEarningsEvent]:
    config = load_app_config(config_path)
    universe = load_universe(config)
    sector_by_symbol = {
        item.symbol: (item.sector, item.exchange)
        for item in universe
    }
    cookstock = load_configured_cookstock(config)
    raw_events = cookstock.fetch_next_week_earnings_watchlist(reference_date=reference_date)

    events: list[PreEarningsEvent] = []
    seen: set[str] = set()
    for item in raw_events:
        ticker = str(item["ticker"]).upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        sector, exchange = sector_by_symbol.get(ticker, (None, None))
        events.append(
            PreEarningsEvent(
                ticker=ticker,
                earnings_date=str(item.get("event_date")) if item.get("event_date") else None,
                summary=str(item.get("summary")) if item.get("summary") else None,
                sector=sector,
                exchange=exchange,
            )
        )
    events = filter_pre_earnings_events(events, excluded)
    if limit is not None:
        return events[:limit]
    return events


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config(args.config)
    excluded = load_excluded_tickers(config)
    date_label = args.date_label or today_label()
    reference_date = dt.date.fromisoformat(args.reference_date) if args.reference_date else None

    if args.tickers:
        sector_by_symbol = _sector_by_symbol(args.config)
        events = _manual_events(args.tickers, sector_by_symbol, excluded)
        source_label = "manual-tickers"
    else:
        events = _next_week_events(args.config, reference_date, args.limit)
        source_label = "next-week-earnings"

    result = run_pre_earnings_screen(config, events)
    watchlist = build_pre_earnings_watchlist(result.hits)

    artifact_paths = build_screener_artifact_paths(PROJECT_ROOT / "artifacts", strategy_id="pre_earnings_focus", date_label=date_label)
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    _write_json(
        summary_path,
        {
            "strategy_id": "pre_earnings_focus",
            "date_label": date_label,
            "source": source_label,
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
    persisted_run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=summary_path,
    )
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
