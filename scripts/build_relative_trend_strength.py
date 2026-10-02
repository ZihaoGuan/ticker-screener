#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config
from src.market_data_access import load_many_ticker_windows, resolve_database_url
from src.relative_trend_strength import RTS_HISTORY_DAYS, build_relative_trend_strength_snapshot
from src.universe import load_universe
from src.webapp.config import load_webapp_config
from src.webapp.repositories.relative_trend_strength_repository import RelativeTrendStrengthRepository


_SECTOR_ETFS = {
    "basic materials": "XLB", "materials": "XLB", "communication services": "XLC", "communications": "XLC",
    "consumer discretionary": "XLY", "consumer cyclical": "XLY", "consumer staples": "XLP",
    "energy": "XLE", "financial": "XLF", "finance": "XLF", "health care": "XLV", "healthcare": "XLV",
    "industrials": "XLI", "real estate": "XLRE", "technology": "XLK", "information technology": "XLK",
    "utilities": "XLU",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily Relative Trend Strength snapshots.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--as-of-date")
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_app_config(args.config)
    web_config = load_webapp_config()
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.date.today()
    universe = load_universe(config, limit=args.limit)
    symbols = [item.symbol.upper() for item in universe]
    required = sorted({*symbols, config.benchmark_ticker.upper(), *_SECTOR_ETFS.values()})
    frames = load_many_ticker_windows(required, as_of_date, RTS_HISTORY_DAYS + 10, database_url=resolve_database_url(web_config.database_url))
    benchmark = frames.get(config.benchmark_ticker.upper())
    rows: list[dict[str, object]] = []
    failures = 0
    for item in universe:
        sector = str(item.sector or "").strip()
        sector_etf = _SECTOR_ETFS.get(sector.lower())
        snapshot = build_relative_trend_strength_snapshot(
            frames.get(item.symbol.upper()), benchmark, ticker=item.symbol, sector=item.sector,
            sector_etf=sector_etf, sector_frame=frames.get(sector_etf) if sector_etf else None, as_of_date=as_of_date,
        )
        if snapshot is None:
            failures += 1
            continue
        rows.append(snapshot.to_dict())
    persisted = RelativeTrendStrengthRepository(database_url=web_config.database_url).upsert_snapshots(rows)
    status = {"status": "success", "as_of_date": as_of_date.isoformat(), "total_tickers": len(universe), "persisted": persisted, "failed": failures}
    status_path = web_config.artifacts_dir / "status" / "build_relative_trend_strength.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
    print(json.dumps(status))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
