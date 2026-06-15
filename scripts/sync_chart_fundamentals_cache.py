#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.cookstock_bridge import load_configured_cookstock
from src.ratings.repository import RatingsRepository
from src.webapp.config import load_webapp_config
from src.webapp.services.watchlist_service import (
    _load_yahoo_earnings_and_holders_playwright,
    _load_yahoo_implied_move_playwright,
    _merge_chart_fundamentals_cache_fields,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh DB-backed chart fundamentals cache for focused tickers.")
    parser.add_argument("--as-of-date", default=today_label(), help="Cache snapshot date label YYYY-MM-DD.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list. If provided, skip rating/earnings selection.")
    parser.add_argument("--fundamental-limit", type=int, default=200, help="Top fundamental rating ticker count.")
    parser.add_argument("--technical-limit", type=int, default=200, help="Top technical rating ticker count.")
    parser.add_argument("--upcoming-weeks", type=int, default=2, help="How many upcoming earnings weeks to include (0-3).")
    parser.add_argument("--earnings-limit", type=int, default=8, help="How many earnings rows to persist per ticker.")
    parser.add_argument(
        "--overwrite-policy",
        default="skip-existing",
        choices=("latest-date", "replace-date", "skip-existing"),
        help="Skip or replace existing same/newer cache rows.",
    )
    parser.add_argument("--database-url", default="", help="Optional Postgres connection string.")
    return parser.parse_args()


def _manual_tickers(symbols: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        normalized = str(item).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _top_rating_tickers(repository: RatingsRepository, *, limit: int, as_of_date: dt.date | None) -> list[str]:
    if limit <= 0:
        return []
    payload = repository.list_top_rating_snapshots(as_of_date=as_of_date, limit=limit, rating_status="ok")
    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not rows and as_of_date is not None:
        payload = repository.list_top_rating_snapshots(as_of_date=None, limit=limit, rating_status="ok")
        rows = payload.get("rows") if isinstance(payload, dict) else []
    return [str(row.get("ticker") or "").strip().upper() for row in rows if isinstance(row, dict) and str(row.get("ticker") or "").strip()]


def _top_technical_tickers(repository: RatingsRepository, *, limit: int, as_of_date: dt.date | None) -> list[str]:
    if limit <= 0:
        return []
    payload = repository.list_top_technical_rating_snapshots(as_of_date=as_of_date, limit=limit, technical_status="ok")
    rows = payload.get("rows") if isinstance(payload, dict) else []
    if not rows and as_of_date is not None:
        payload = repository.list_top_technical_rating_snapshots(as_of_date=None, limit=limit, technical_status="ok")
        rows = payload.get("rows") if isinstance(payload, dict) else []
    return [str(row.get("ticker") or "").strip().upper() for row in rows if isinstance(row, dict) and str(row.get("ticker") or "").strip()]


def _upcoming_earnings_tickers(*, config_path: str | None, anchor_date: dt.date, upcoming_weeks: int) -> list[str]:
    normalized_weeks = max(0, min(3, int(upcoming_weeks)))
    if normalized_weeks <= 0:
        return []
    config = load_app_config(config_path)
    cookstock = load_configured_cookstock(config)
    ordered: list[str] = []
    seen: set[str] = set()
    week_start = anchor_date - dt.timedelta(days=anchor_date.weekday() + 1) if anchor_date.weekday() != 6 else anchor_date
    for week_offset in range(normalized_weeks):
        start_date = week_start + dt.timedelta(days=week_offset * 7)
        end_date = start_date + dt.timedelta(days=6)
        raw_events = cookstock.fetch_earnings_calendar_watchlist(start_date, end_date)
        for item in raw_events:
            ticker = str(item.get("ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            ordered.append(ticker)
    return ordered


def _dedupe_tickers(groups: list[list[str]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for ticker in group:
            normalized = str(ticker).strip().upper()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
    return ordered


def main() -> int:
    args = parse_args()
    database_url = (args.database_url or load_webapp_config().database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(str(args.as_of_date))
    repository = RatingsRepository(database_url)

    if args.tickers:
        target_tickers = _manual_tickers(args.tickers)
    else:
        target_tickers = _dedupe_tickers(
            [
                _top_rating_tickers(repository, limit=max(0, int(args.fundamental_limit)), as_of_date=as_of_date),
                _top_technical_tickers(repository, limit=max(0, int(args.technical_limit)), as_of_date=as_of_date),
                _upcoming_earnings_tickers(config_path=None, anchor_date=as_of_date, upcoming_weeks=args.upcoming_weeks),
            ]
        )

    if not target_tickers:
        print("No target tickers resolved.", flush=True)
        return 0

    latest_dates = repository.load_latest_chart_fundamentals_dates(target_tickers)
    print(
        "sync_chart_fundamentals_cache "
        f"tickers={len(target_tickers)} "
        f"fundamental_limit={args.fundamental_limit} "
        f"technical_limit={args.technical_limit} "
        f"upcoming_weeks={args.upcoming_weeks} "
        f"overwrite_policy={args.overwrite_policy}",
        flush=True,
    )

    refreshed = 0
    skipped = 0
    for index, ticker in enumerate(target_tickers, start=1):
        latest_date = latest_dates.get(ticker)
        if args.overwrite_policy == "skip-existing" and latest_date == as_of_date:
            skipped += 1
            print(f"[{index}/{len(target_tickers)}] {ticker} skipped_existing as_of_date={as_of_date.isoformat()}", flush=True)
            continue
        if args.overwrite_policy == "latest-date" and latest_date is not None and latest_date >= as_of_date:
            skipped += 1
            print(f"[{index}/{len(target_tickers)}] {ticker} skipped_latest existing_date={latest_date.isoformat()}", flush=True)
            continue

        cached_entry = repository.load_latest_chart_fundamentals_cache_entry(ticker)
        earnings_rows, holders_pct, revenue_yoy_pct, earnings_yoy_pct, browser_diagnostics = _load_yahoo_earnings_and_holders_playwright(
            ticker,
            earnings_limit=max(1, int(args.earnings_limit)),
        )
        implied_move, options_diagnostics = _load_yahoo_implied_move_playwright(ticker)
        merged_payload = _merge_chart_fundamentals_cache_fields(
            cached_entry,
            earnings_rows=earnings_rows,
            holders_pct=holders_pct,
            revenue_yoy_pct=revenue_yoy_pct,
            earnings_yoy_pct=earnings_yoy_pct,
            implied_move=implied_move,
        )
        repository.ensure_ticker_metadata_stub(ticker, source="chart-fundamentals-cache")
        repository.upsert_chart_fundamentals_cache_entry(
            ticker=ticker,
            as_of_date=as_of_date,
            earnings_eps_history=list(merged_payload["earnings_eps_history"]),
            holders_float_held_by_institutions_pct=merged_payload["holders_float_held_by_institutions_pct"],
            revenue_yoy_pct=merged_payload["revenue_yoy_pct"],
            earnings_yoy_pct=merged_payload["earnings_yoy_pct"],
            implied_move=merged_payload["implied_move"],
            source_summary={
                "source": "yahoo-playwright",
                "diagnostics": {
                    "earnings": browser_diagnostics["earnings"],
                    "holders": browser_diagnostics["holders"],
                    "statistics": browser_diagnostics["statistics"],
                    "options": options_diagnostics,
                },
            },
        )
        refreshed += 1
        print(
            f"[{index}/{len(target_tickers)}] {ticker} refreshed "
            f"earnings={len(merged_payload['earnings_eps_history'])} "
            f"holders={'yes' if merged_payload['holders_float_held_by_institutions_pct'] is not None else 'no'} "
            f"rev_yoy={'yes' if merged_payload['revenue_yoy_pct'] is not None else 'no'} "
            f"earn_yoy={'yes' if merged_payload['earnings_yoy_pct'] is not None else 'no'} "
            f"implied_move={'yes' if merged_payload['implied_move'] is not None else 'no'}",
            flush=True,
        )

    print(
        f"completed refreshed={refreshed} skipped={skipped} total={len(target_tickers)} as_of_date={as_of_date.isoformat()}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
