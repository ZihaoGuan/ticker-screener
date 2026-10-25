#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured
from src.artifact_paths import build_screener_artifact_paths
from src.config import load_app_config
from src.market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from src.martin_bar_watchlist_screen import STRATEGY_ID, _has_recent_sessions, _valid_ohlcv, build_rank_context, evaluate_martin_bar_watchlist
from src.webapp.config import load_webapp_config


def _require_completed_xnys_session(day: dt.date) -> list[dt.date]:
    import exchange_calendars as xcals
    import pandas as pd

    calendar = xcals.get_calendar("XNYS", start=(day - dt.timedelta(days=365)).isoformat(), end=day.isoformat())
    if not calendar.is_session(day.isoformat()):
        raise ValueError(f"{day} is not an XNYS session.")
    close = calendar.session_close(day.isoformat())
    if close > pd.Timestamp.now(tz="UTC"):
        raise ValueError(f"{day} has not reached the XNYS regular-market close.")
    sessions = calendar.sessions_in_range((day - dt.timedelta(days=365)).isoformat(), day.isoformat())
    if len(sessions) < 120:
        raise ValueError(f"Could not resolve 120 XNYS sessions through {day}.")
    return [session.date() for session in sessions[-120:]]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the completed-daily-bar Martin watchlist v1 scan.")
    parser.add_argument("--as-of-date", help="Use this stored session (defaults to latest stored session).")
    args = parser.parse_args()
    from src.market_data_access import _connect

    database_url = resolve_database_url()
    connection = _connect(database_url)
    if connection is None:
        raise RuntimeError("Martin bar watchlist requires configured Postgres daily_bars; no fallback source is used.")
    with connection:
        with connection.cursor() as cursor:
            if args.as_of_date:
                as_of_date = dt.date.fromisoformat(args.as_of_date)
            else:
                cursor.execute("SELECT MAX(trade_date) FROM daily_bars")
                row = cursor.fetchone()
                as_of_date = row[0] if row else None
            if as_of_date is None:
                raise RuntimeError("daily_bars has no completed session.")
            expected_sessions = _require_completed_xnys_session(as_of_date)
            cursor.execute("SELECT DISTINCT ticker FROM daily_bars WHERE trade_date <= %s ORDER BY ticker", (as_of_date,))
            stored_tickers = [str(row[0]).upper() for row in cursor.fetchall()]

    known_etf_exclusions = sorted(set(stored_tickers) & {"SPY", "QQQ"})
    tickers = [ticker for ticker in stored_tickers if ticker not in {"SPY", "QQQ"}]

    app_config = load_app_config()
    if app_config.benchmark_ticker.upper() != "SPY":
        raise RuntimeError("martin_bar_watchlist_v1 requires SPY as its fixed benchmark.")
    web_config = load_webapp_config()
    frames = load_many_ticker_windows([*tickers, app_config.benchmark_ticker], as_of_date, 320, database_url=database_url)
    benchmark = frames.get(app_config.benchmark_ticker.upper())
    if benchmark is None or not _valid_ohlcv(benchmark) or not _has_recent_sessions(benchmark, expected_sessions):
        raise RuntimeError(f"SPY benchmark has incomplete or invalid XNYS history through {as_of_date}.")
    rank_context = build_rank_context({ticker: frames.get(ticker) for ticker in tickers if frames.get(ticker) is not None}, as_of_date, expected_sessions)
    metadata = load_ticker_metadata_map(tickers, database_url=database_url)
    hits: list[dict[str, object]] = []
    blocked: list[dict[str, str]] = []
    for ticker in tickers:
        bars = frames.get(ticker)
        if bars is None or as_of_date not in {index.date() for index in bars.index}:
            blocked.append({"ticker": ticker, "error": "missing_current_session_bar"})
            continue
        if len(bars) < 120:
            blocked.append({"ticker": ticker, "error": "insufficient_120_session_baseline"})
            continue
        if not _valid_ohlcv(bars):
            blocked.append({"ticker": ticker, "error": "invalid_or_duplicate_ohlcv"})
            continue
        if not _has_recent_sessions(bars, expected_sessions):
            blocked.append({"ticker": ticker, "error": "missing_required_market_session"})
            continue
        hit = evaluate_martin_bar_watchlist(bars, benchmark, rank_context, ticker=ticker,
                                             as_of_date=as_of_date, metadata=metadata.get(ticker),
                                             expected_sessions=expected_sessions)
        if hit:
            hits.append(hit)
    hits.sort(key=lambda row: (-len(row["discovery_categories"]),
                               -float(row.get("stock_spy_relative_return_30d_pct") if row.get("stock_spy_relative_return_30d_pct") is not None else -1e9),
                               -float(row.get("adr14_pct") or 0), -float(row.get("adv20_dollars") or 0),
                               str(row["ticker"])))
    for rank, hit in enumerate(hits, 1):
        hit["rank"] = rank
    paths = build_screener_artifact_paths(web_config.artifacts_dir, strategy_id=STRATEGY_ID, date_label=as_of_date.isoformat())
    paths.raw_results_path.parent.mkdir(parents=True, exist_ok=True)
    raw = {"strategy_id": STRATEGY_ID, "as_of_date": as_of_date.isoformat(), "hits": hits,
           "failed_tickers": blocked, "universe_size": len(tickers), "rules_version": STRATEGY_ID,
           "known_etf_exclusions": known_etf_exclusions}
    fingerprint = hashlib.sha256()
    for ticker in sorted(frames):
        frame = frames[ticker]
        for index, row in frame.iterrows():
            record = [ticker, index.date().isoformat(), *[None if item != item else float(item) for item in row]]
            fingerprint.update(json.dumps(record, separators=(",", ":")).encode())
            fingerprint.update(b"\n")
    input_fingerprint = fingerprint.hexdigest()
    for hit in hits:
        hit["input_fingerprint"] = input_fingerprint
        hit["code_version"] = os.getenv("TICKER_SCREENER_CODE_VERSION", "development")
    paths.raw_results_path.write_text(json.dumps(raw, indent=2, default=str), encoding="utf-8")
    paths.watchlist_path.write_text(json.dumps(hits, indent=2, default=str), encoding="utf-8")
    summary = {"strategy_id": STRATEGY_ID, "date_label": as_of_date.isoformat(), "as_of_date": as_of_date.isoformat(),
               "source": "stored-daily-bars", "total_tickers": len(tickers), "passed_tickers": len(hits),
               "failed_tickers": len(blocked), "rules_version": STRATEGY_ID,
               "eligible_rank_universe": len(rank_context), "input_fingerprint": input_fingerprint,
               "code_version": os.getenv("TICKER_SCREENER_CODE_VERSION", "development"),
               "decision_cutoff": "completed_session_close", "known_etf_exclusions": known_etf_exclusions,
               "classification_uncertainty": "SPY and QQQ were excluded by known symbol identity; other ETF/common-stock classification is unavailable in stored metadata",
               "raw_results_file": str(paths.raw_results_path),
               "watchlist_file": str(paths.watchlist_path)}
    paths.summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote run summary to {paths.summary_path}")
    persisted = persist_screen_run_artifacts_if_configured(args=args, summary_path=paths.summary_path,
                                                            option_overrides={"market_data_source": "database-first"})
    if persisted is not None:
        print(f"Persisted screen run id={persisted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
