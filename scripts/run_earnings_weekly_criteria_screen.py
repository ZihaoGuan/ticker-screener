#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import replace
import json
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.earnings_growth_screen import run_earnings_growth_screen
from src.earnings_growth_watchlist_builder import build_earnings_growth_watchlist
from src.pre_earnings_screen import PreEarningsEvent
from src.ticker_filters import filter_pre_earnings_events, load_excluded_tickers
from src.universe import load_universe
from src.cookstock_bridge import load_configured_cookstock
from src.webapp.services.earnings_calendar_service import CRITERIA_STRATEGY_ID
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.config import load_webapp_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the next-week earnings weekly criteria screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the next-week candidate set.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--reference-date", help="Reference date for next-week earnings watchlist (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--skip-persist", action="store_true", help="Skip persisting the screen run into the webapp DB.")
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _next_week_events(config_path: str, reference_date: dt.date | None, limit: int | None) -> list[PreEarningsEvent]:
    config = load_app_config(config_path)
    excluded = load_excluded_tickers(config)
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
    events = filter_pre_earnings_events(events, excluded)
    if limit is not None:
        return events[:limit]
    return events


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    base_config = load_app_config(args.config)
    config = replace(
        base_config,
        earnings_growth_min_institutional_ownership_pct=10.0,
        earnings_growth_min_revenue_yoy_pct=100.0,
        earnings_growth_eps_improving_quarters=4,
        earnings_growth_min_move_occurrences=0,
        earnings_growth_min_move_pct=0.0,
        earnings_growth_min_quarter_revenue=0.0,
    )
    date_label = args.date_label or today_label()
    reference_date = dt.date.fromisoformat(args.reference_date) if args.reference_date else None
    run_date = reference_date or dt.date.today()

    events = _next_week_events(args.config, reference_date, args.limit)
    result = run_earnings_growth_screen(config, events, as_of_date=run_date)
    watchlist = build_earnings_growth_watchlist(result.hits)

    raw_path = PROJECT_ROOT / "artifacts" / "raw" / f"{CRITERIA_STRATEGY_ID}_{date_label}.json"
    watchlist_path = PROJECT_ROOT / "artifacts" / "watchlists" / f"{CRITERIA_STRATEGY_ID}_{date_label}.json"
    summary_path = PROJECT_ROOT / "artifacts" / "raw" / f"{CRITERIA_STRATEGY_ID}_run_summary_{date_label}.json"

    _write_json(raw_path, result.to_dict())
    _write_json(watchlist_path, watchlist)
    summary_payload = {
        "date_label": date_label,
        "as_of_date": run_date.isoformat(),
        "source": "next-week-earnings",
        "reference_date": str(reference_date) if reference_date else None,
        "earnings_provider": result.earnings_provider,
        "financials_provider": result.financials_provider,
        "total_tickers": result.total_tickers,
        "passed_tickers": result.passed_tickers,
        "failed_tickers": len(result.failed_tickers),
        "raw_results_file": str(raw_path),
        "watchlist_file": str(watchlist_path),
    }
    _write_json(summary_path, summary_payload)

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")

    if not args.skip_persist:
        webapp_config = load_webapp_config()
        history_service = ScreenerHistoryService(
            database_url=webapp_config.database_url,
            artifacts_dir=webapp_config.artifacts_dir,
        )
        if history_service.is_configured():
            run_id = history_service.persist_screen_run(
                strategy_id=CRITERIA_STRATEGY_ID,
                options={
                    "limit": args.limit,
                    "reference_date": run_date.isoformat(),
                    "source": "next-week-earnings",
                    "market_data_source": "internet",
                },
                summary_payload=summary_payload,
                raw_payload=result.to_dict(),
            )
            print(f"Persisted screen run id={run_id}")
        else:
            print("warning: webapp database not configured; skipped persistence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
