#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.overlap_backtest_service import OverlapBacktestService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run overlap backtest v1.")
    parser.add_argument("--strategy-ids-json", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--entry-signal-threshold", type=int, default=4)
    parser.add_argument("--hold-periods-json", default="[5, 10]")
    parser.add_argument("--job-run-id", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_webapp_config()
    service = OverlapBacktestService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    payload = service.run_backtest(
        start_date=dt.date.fromisoformat(args.start_date),
        end_date=dt.date.fromisoformat(args.end_date),
        strategy_ids=[str(item).strip() for item in json.loads(args.strategy_ids_json) if str(item).strip()],
        entry_signal_threshold=int(args.entry_signal_threshold),
        hold_periods=[int(item) for item in json.loads(args.hold_periods_json)],
        job_run_id=args.job_run_id,
    )
    artifact_path = str(payload.get("artifact_path") or "")
    if artifact_path:
        print(f"Wrote run summary to {artifact_path}")
    print(
        f"Backtest completed | trades={int(payload.get('summary', {}).get('trade_count') or 0)} "
        f"| threshold={int(payload.get('parameters', {}).get('entry_signal_threshold') or 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
