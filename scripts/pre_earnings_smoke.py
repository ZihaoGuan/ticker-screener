#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "vendor" / "cookstock" / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "vendor" / "cookstock" / "src"))

from src.config import load_app_config
from src.universe import load_universe
import cookStock


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a pre-earnings smoke test against a limited universe sample."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of universe tickers to include in the smoke test.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    config = load_app_config()
    started = time.time()

    cookStock.algoParas.REQUEST_TIMEOUT_SECONDS = 10
    cookStock.algoParas.PRE_EARNINGS_USE_MARKET_MEMORY = False
    cookStock.algoParas.TICKER_TIMEOUT_SECONDS = 12
    cookStock.algoParas.PRE_EARNINGS_RETRY_TIMEOUT_SECONDS = 24
    cookStock.algoParas.PARALLEL_ENABLED = False

    print("loading universe...", flush=True)
    universe = load_universe(config, limit=args.limit)
    print(f"universe_loaded={len(universe)} elapsed={time.time()-started:.2f}s", flush=True)

    tickers = [item.symbol for item in universe]
    sector_by_ticker = {item.symbol: item.sector for item in universe}

    batch = cookStock.batch_process(
        tickers,
        "pre_earnings_smoke",
        sector_by_ticker=sector_by_ticker,
        benchmark_ticker=config.benchmark_ticker,
        earnings_event_by_ticker={},
    )
    batch.batch_pipeline_pre_earnings()
    print(
        f"smoke_done elapsed={time.time()-started:.2f}s result_file={batch.result_file}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
