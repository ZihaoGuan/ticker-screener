#!/usr/bin/env python3
"""Preview or promote TradingView leveraged/inverse fund exclusions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.tradingview_fund_exclusions import fetch_classified_funds, refresh


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Atomically promote the fresh list (default is dry-run).")
    args = parser.parse_args()
    if args.apply:
        print(json.dumps(refresh(), indent=2))
    else:
        rows = fetch_classified_funds()
        print(json.dumps({"dry_run": True, "count": len(rows), "tickers": [r["ticker"] for r in rows]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
