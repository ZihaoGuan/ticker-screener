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

from src.market_data_access import load_many_ticker_windows_for_range, resolve_database_url
from src.rts_validation import build_rts_validation_report
from src.webapp.config import load_webapp_config
from src.webapp.repositories.relative_trend_strength_repository import RelativeTrendStrengthRepository


def parse_args() -> argparse.Namespace:
    today = dt.date.today()
    parser = argparse.ArgumentParser(description="Validate persisted RTS snapshots against forward returns.")
    parser.add_argument("--start-date", default=(today - dt.timedelta(days=465)).isoformat())
    parser.add_argument("--end-date", default=(today - dt.timedelta(days=100)).isoformat())
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    if end_date < start_date:
        raise ValueError("end-date must not be before start-date")
    config = load_webapp_config()
    database_url = resolve_database_url(config.database_url)
    repository = RelativeTrendStrengthRepository(database_url=database_url)
    snapshots = repository.load_snapshots_for_validation(start_date=start_date, end_date=end_date)
    tickers = sorted({str(row.get("ticker") or "").upper() for row in snapshots} | {"SPY"})
    price_end = min(dt.date.today(), end_date + dt.timedelta(days=120))
    frames = load_many_ticker_windows_for_range(
        tickers,
        start_date,
        price_end,
        220,
        database_url=database_url,
    )
    report = build_rts_validation_report(snapshots, frames)
    report.update({"start_date": start_date.isoformat(), "end_date": end_date.isoformat()})
    output_dir = config.artifacts_dir / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)
    dated_path = output_dir / f"rts_validation_{start_date}_{end_date}.json"
    latest_path = output_dir / "rts_validation_latest.json"
    payload = json.dumps(report, indent=2)
    dated_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    status_path = config.artifacts_dir / "status" / "rts_validation.json"
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps({"status": "success", "report_file": str(dated_path), **{key: report[key] for key in ("start_date", "end_date", "snapshot_count", "observation_count", "skipped_count")}}, indent=2), encoding="utf-8")
    print(f"Wrote run summary to {dated_path}")
    print(json.dumps({"snapshot_count": report["snapshot_count"], "observation_count": report["observation_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
