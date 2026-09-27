#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.momentum_etf_holdings import MOMENTUM_ETFS, refresh_momentum_etf_holdings_cache
from src.webapp.config import load_webapp_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch issuer-published holdings for the momentum ETF portfolio board.")
    parser.add_argument("--tickers", nargs="+", help="Optional ETF tickers to refresh.")
    parser.add_argument("--output-dir", type=Path, help="Optional artifacts directory override.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    requested = {item.strip().upper() for item in args.tickers or [] if item.strip()}
    etfs = tuple(item for item in MOMENTUM_ETFS if not requested or item.ticker in requested)
    payload = refresh_momentum_etf_holdings_cache(
        artifacts_dir=args.output_dir or load_webapp_config().artifacts_dir,
        etfs=etfs,
        timeout_seconds=max(1.0, float(args.timeout_seconds)),
    )
    print(f"Fetched holdings for {payload['etf_count']} / {len(etfs)} ETFs.")
    if payload["errors"]:
        print(f"Failed ETFs: {', '.join(sorted(payload['errors']))}")
    print(json.dumps({"output_file": payload["output_file"], "updated_cache": payload["updated_cache"]}, sort_keys=True))
    return 1 if payload["errors"] and not payload["results"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
