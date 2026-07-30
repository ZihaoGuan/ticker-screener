#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ssga_holdings import refresh_ssga_holdings_cache
from src.webapp.config import load_webapp_config
from src.webapp.services.sector_leaderboard_service import DEFAULT_SECTOR_ETFS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily SSGA ETF holdings and write the sector ETF holdings cache.")
    parser.add_argument("--tickers", nargs="+", help="Optional ETF tickers to refresh. Defaults to the sector leaderboard catalog.")
    parser.add_argument("--output-dir", type=Path, help="Optional artifacts directory override.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP timeout per SSGA holdings file.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configured = load_webapp_config()
    artifacts_dir = args.output_dir or configured.artifacts_dir
    tickers = [item.strip().upper() for item in (args.tickers or [item.ticker for item in DEFAULT_SECTOR_ETFS]) if item.strip()]
    payload = refresh_ssga_holdings_cache(
        etf_tickers=tickers,
        artifacts_dir=artifacts_dir,
        timeout_seconds=max(1.0, float(args.timeout_seconds)),
    )
    print(f"Fetched holdings for {payload['etf_count']} / {len(tickers)} ETFs.")
    if payload.get("errors"):
        print(f"Failed ETFs: {', '.join(sorted(payload['errors']))}")
    print(f"Unique holding tickers: {len(payload.get('holding_tickers') or [])}")
    print(f"Wrote run summary to {payload['output_file']}")
    print(json.dumps({"output_file": payload["output_file"], "dated_output_file": payload["dated_output_file"]}, sort_keys=True))
    return 1 if payload.get("errors") and not payload.get("results") else 0


if __name__ == "__main__":
    raise SystemExit(main())
