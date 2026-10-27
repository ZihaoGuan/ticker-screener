#!/usr/bin/env python3
"""Freeze regular-session 30m bars for saved RS Leader candidate symbols."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_martin_intraday_execution import (digest, fetch_safely,
    fetch_with_exchange_fallback, load_ticker_exchange, normalize_exchange, session_starts)
from scripts.replay_martin_bar_retest_range import load_range_sources, xnys_sessions
from src.webapp.services.rs_leader_30m_pivot import candidates_from_saved_run, evaluate_pivot_sequence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--client-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args(argv)
    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    if not args.book_id.startswith("rs_leader_30m_pivot_replay_"):
        parser.error("--book-id must start with rs_leader_30m_pivot_replay_")
    out = Path(args.output_dir)
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        parser.error("--output-dir must be empty")
    out.mkdir(parents=True, exist_ok=True)
    from src.webapp.config import load_webapp_config
    from src.webapp.repositories.history_repository import HistoryRepository
    from src.webapp.services.screener_history_service import ScreenerHistoryService
    from src.market_data_access import load_many_ticker_windows
    import psycopg
    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    sessions = xnys_sessions(args.from_date, args.to_date)
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=True))
    sources = load_range_sources(history, sessions)
    records = []
    with psycopg.connect(config.database_url,
            options="-c default_transaction_read_only=on -c statement_timeout=60000") as connection:
        prior_watch: dict[str, dict] = {}
        for day in sessions:
            source = sources[day]
            current_source_tickers = sorted({str(hit.get("ticker") or "").strip().upper() for hit in source.get("hits", [])
                if str(hit.get("ticker") or "").strip() and set(hit.get("discovery_categories") or []).intersection(
                    {"leader_30_session", "potent_mover", "gap_mover"})})
            current_frames = load_many_ticker_windows(current_source_tickers, day, 260, database_url=config.database_url) if current_source_tickers else {}
            candidates = candidates_from_saved_run(source, current_frames, day)
            # Entry on D uses a setup discovered on the preceding session D-1.
            tickers = sorted(prior_watch)
            for ticker in tickers:
                exchange = load_ticker_exchange(connection, ticker)
                requested = f"{normalize_exchange(exchange)}:{ticker}"
                data = fetch_with_exchange_fallback(args.client_dir, ticker, exchange, day.isoformat(), "30",
                    fetcher=fetch_safely)
                base = {"ticker": ticker, "date": day.isoformat(), "requested_symbol": requested,
                    "resolved_symbol": (data or {}).get("resolved_symbol"),
                    "source_run_id": prior_watch[ticker].get("source_run_id"),
                    "source_fingerprint": prior_watch[ticker].get("source_fingerprint"),
                    "session_source_run_id": source.get("source_run_id"),
                    "timeframe": "30m", "session": "regular", "adjustment": "splits"}
                if not data or data.get("error_code"):
                    records.append({**base, "classification": (data or {}).get("error_code") or "provider_unavailable",
                        "provider_error_code": (data or {}).get("error_code") or "provider_unavailable",
                        "candle_file": None, "candle_fingerprint": None,
                        "exchange_lookup": (data or {}).get("exchange_lookup")})
                    continue
                expected = session_starts(day.isoformat(), 30)
                candles = data.get("candles") or []
                by_time = {int(row["time"]): row for row in candles if "time" in row}
                if expected and set(by_time) == set(expected) and len(by_time) == len(candles):
                    outcome = evaluate_pivot_sequence([by_time[stamp] for stamp in expected])
                    if outcome.get("status") == "ambiguous":
                        fine = fetch_with_exchange_fallback(args.client_dir, ticker, exchange, day.isoformat(), "1",
                            fetcher=fetch_safely)
                        if fine is not None and not fine.get("error_code"):
                            data["one_minute"] = fine
                filename = f"{ticker}_{day.isoformat()}.json"
                (out / filename).write_text(json.dumps(data, sort_keys=True, indent=2, default=str) + "\n")
                records.append({**base, "classification": "frozen", "exchange_lookup": data.get("exchange_lookup"),
                    "one_minute_status": "frozen" if "one_minute" in data else "not_required_or_unavailable",
                    "candle_file": filename, "candle_fingerprint": digest(data)})
            prior_watch = {row["ticker"]: row for row in candidates if row["status"] == "pending"}
    manifest = {"schema": "rs_leader_30m_pivot_audit_v1", "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "read_only": True, "book_id": args.book_id, "from_date": args.from_date.isoformat(),
        "to_date": args.to_date.isoformat(), "provider": "TradingView", "timeframe": "30m",
        "session": "regular", "adjustment": "splits", "records": records}
    manifest["fingerprint"] = digest(manifest)
    (out / "audit.json").write_text(json.dumps(manifest, sort_keys=True, indent=2, default=str) + "\n")
    print(json.dumps({"audit_file": str(out / "audit.json"), "records": len(records),
                      "frozen": sum(row["classification"] == "frozen" for row in records),
                      "fingerprint": manifest["fingerprint"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
