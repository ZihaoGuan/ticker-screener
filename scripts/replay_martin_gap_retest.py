#!/usr/bin/env python3
"""Replay reconstructed Power Gap events into the isolated Oct 19 gap-retest book."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from typing import Any

import exchange_calendars as xcals
import pandas as pd

from src.config import load_app_config
from src.market_data_access import load_many_ticker_windows
from src.power_gap_volume_watch import detect_many
from src.martin_daily_gap_retest import REPLAY_STRATEGY_ID
from src.ticker_filters import filter_universe_tickers, load_excluded_tickers
from src.universe import load_universe
from src.universe_filters import add_universe_filter_args, build_filter_criteria_from_args, filter_universe_by_criteria
from src.webapp.config import load_webapp_config
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_gap_retest_service import MartinGapRetestService

BOOK_ID = REPLAY_STRATEGY_ID
START = dt.date(2026, 10, 19)
DEFAULT_END = dt.date(2026, 10, 23)


def reconstruct_sources(start: dt.date, end: dt.date, *, database_url: str, config: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    calendar = xcals.get_calendar("XNYS", start=(start - dt.timedelta(days=500)).isoformat(), end=end.isoformat())
    sessions = [label.date() for label in calendar.sessions_in_range(start.isoformat(), end.isoformat())]
    if not sessions or sessions[0] != start:
        raise ValueError(f"{start} is not an XNYS session")
    if sessions[-1] != end:
        raise ValueError(f"{end} is not an XNYS session")
    parser = argparse.ArgumentParser(add_help=False)
    add_universe_filter_args(parser)
    args = parser.parse_args([])
    symbols = [item.symbol for item in filter_universe_by_criteria(
        filter_universe_tickers(load_universe(config), load_excluded_tickers(config)),
        build_filter_criteria_from_args(args))]
    sources: list[dict[str, Any]] = []
    data_issues = 0
    for offset in range(0, len(symbols), 200):
        batch = symbols[offset:offset + 200]
        frames = load_many_ticker_windows(batch, end, 326, database_url=database_url)
        rows = {ticker: [{"date": idx.date().isoformat(), "open": row["Open"], "high": row["High"],
                          "low": row["Low"], "close": row["Close"], "volume": row["Volume"],
                          "adjustment_basis": "stored_daily_bars_ohlcv"}
                         for idx, row in frame.iterrows()] for ticker, frame in frames.items()}
        observed = [dt.date.fromisoformat(row["date"]) for values in rows.values() for row in values]
        expected = ([label.date() for label in calendar.sessions_in_range(
            pd.Timestamp(min(observed) - dt.timedelta(days=7)), pd.Timestamp(end.isoformat()))] if observed else [])
        for session in sessions:
            candidates = []
            for result in detect_many(rows, session, expected):
                if result.get("coverage", {}).get("issues"):
                    data_issues += 1
                context = result.get("power_gap_volume_context") or {}
                if "gap" in result.get("signals", []) and context.get("gap_event"):
                    candidates.append({"ticker": result["ticker"], "hit_payload_json": {
                        "ticker": result["ticker"], "power_gap_volume_context": context}})
            candidates.sort(key=lambda row: row["ticker"])
            fingerprint = hashlib.sha256(json.dumps(candidates, sort_keys=True, default=str).encode()).hexdigest()
            sources.append({"target_trading_date": session.isoformat(), "run_date": session.isoformat(),
                "published_at": dt.datetime.now(dt.timezone.utc).isoformat(), "source_mode": "historical_reconstruction",
                "rules_version": "power_gap_volume_watch_v1", "source_run_id": None, "source_fingerprint": fingerprint,
                "replay_from_date": start.isoformat(), "replay_to_date": end.isoformat(), "hits": candidates})
        print(f"Reconstructed {min(offset + 200, len(symbols))}/{len(symbols)} tickers", flush=True)
    return sources, {"universe_size": len(symbols), "sessions": [day.isoformat() for day in sessions],
                     "coverage_issue_observations": data_issues}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Replay reconstructed Power Gap events in a separate daily gap-retest paper book.")
    parser.add_argument("--from-date", type=dt.date.fromisoformat, default=START)
    parser.add_argument("--to-date", type=dt.date.fromisoformat, default=DEFAULT_END)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--dry-run", action="store_true", help="Evaluate each reconstructed session without saving book rows (default).")
    modes.add_argument("--apply", action="store_true", help="Persist sessions in the isolated replay book.")
    args = parser.parse_args(argv)
    if args.from_date != START or args.to_date < args.from_date:
        parser.error(f"This replay book is fixed to start on {START.isoformat()}; --to-date must be on or after --from-date.")
    web_config = load_webapp_config()
    if not web_config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    app_config = load_app_config()
    repo = MartinForwardTestRepository(database_url=web_config.database_url, read_only=not args.apply)
    if args.apply and repo.latest_session_date(BOOK_ID) is not None:
        parser.error(f"Replay book {BOOK_ID} already has saved sessions; refusing to overwrite it.")
    sources, coverage = reconstruct_sources(args.from_date, args.to_date, database_url=web_config.database_url, config=app_config)
    if args.apply:
        preview_service = MartinGapRetestService(MartinForwardTestRepository(database_url=web_config.database_url, read_only=True),
                                                 database_url=web_config.database_url)
        for source in sources:
            preview = preview_service.run_session(as_of_date=dt.date.fromisoformat(source["target_trading_date"]),
                source_record=source, dry_run=True, strategy_id=BOOK_ID, historical_replay=True)
            if preview.get("status") != "dry_run_complete":
                parser.error(f"Replay preflight failed for {source['target_trading_date']}: {preview.get('status')}")
    service = MartinGapRetestService(repo, database_url=web_config.database_url)
    results = []
    for source in sources:
        result = service.run_session(as_of_date=dt.date.fromisoformat(source["target_trading_date"]),
            source_record=source, dry_run=not args.apply, strategy_id=BOOK_ID, historical_replay=True)
        results.append({"date": source["target_trading_date"], "gap_events": len(source["hits"]),
                        "candidate_count": result.get("candidate_count"), "status": result.get("status"),
                        "skipped": result.get("skipped", False), "reason": result.get("reason")})
        if result.get("status") == "missing_prerequisite":
            print(json.dumps({"ready": False, "coverage": coverage, "sessions": results}, indent=2))
            return 2
    print(json.dumps({"ready": True, "strategy_id": BOOK_ID, "mode": "apply" if args.apply else "dry_run",
        "coverage": coverage, "sessions": results,
        "note": "Reconstructed universe and stored OHLCV; no contemporaneous scanner snapshots exist before Oct 23."}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
