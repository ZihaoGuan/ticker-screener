from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import sys

from src.webapp.services.insider_fetcher import (
    DEFAULT_USER_AGENT,
    fetch_insider_trades_window,
    normalize_ticker,
    parse_date,
    write_insider_window_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch recent SEC Form 4 insider trades and write artifact JSON.")
    parser.add_argument("--tickers", required=True, help="Comma-separated tickers, e.g. NVDA,AAPL,MSFT")
    parser.add_argument("--lookback-days", type=int, default=14, help="Include filings on/after as-of-date minus this many days.")
    parser.add_argument("--min-gross-amount", type=float, default=0.0, help="Optional gross amount filter.")
    parser.add_argument("--as-of-date", default="", help="Anchor date in YYYY-MM-DD. Default: today.")
    parser.add_argument(
        "--output",
        default="artifacts/raw/insider/insider_trades_latest.json",
        help="Output artifact path.",
    )
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="SEC-friendly user agent.")
    args = parser.parse_args()

    tickers = [normalize_ticker(raw) for raw in args.tickers.split(",") if normalize_ticker(raw)]
    if not tickers:
        print("No valid tickers provided.", file=sys.stderr)
        return 1

    as_of_date = parse_date(args.as_of_date) if args.as_of_date else dt.date.today()
    payload = fetch_insider_trades_window(
        tickers=tickers,
        as_of_date=as_of_date,
        lookback_days=max(1, args.lookback_days),
        min_gross_amount=max(0.0, args.min_gross_amount),
        user_agent=args.user_agent,
    )

    output_path = Path(args.output)
    caches = {}
    for ticker in tickers:
        cache_key = f"{ticker}|{as_of_date.isoformat()}|{max(1, args.lookback_days)}"
        caches[cache_key] = {
            "ticker": ticker,
            "requested_tickers": [ticker],
            "as_of_date": as_of_date.isoformat(),
            "lookback_days": max(1, args.lookback_days),
            "refreshed_at": payload.get("generated_at"),
            "entries": [entry for entry in payload.get("entries", []) if str(entry.get("ticker") or "").strip().upper() == ticker],
        }
    wrapped = {
        "generated_at": payload.get("generated_at"),
        "source": payload.get("source"),
        "caches": caches,
    }
    write_insider_window_artifact(payload=wrapped, output_path=output_path)
    print(f"Wrote {len(payload.get('entries', []))} insider trade entries to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
