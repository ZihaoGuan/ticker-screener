#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.backtest_service import BacktestService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run persisted screener backtest.")
    parser.add_argument("--entry-rule-json", required=True)
    parser.add_argument("--date-range-json", required=True)
    parser.add_argument("--exit-rules-json", default="[]")
    parser.add_argument("--position-rules-json", default="{}")
    parser.add_argument("--signal-cache-policy", default="reuse_then_fill")
    parser.add_argument("--market-data-mode", default="database_only")
    parser.add_argument("--job-run-id", type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = {
        "entry_rule": json.loads(args.entry_rule_json),
        "date_range": json.loads(args.date_range_json),
        "exit_rules": json.loads(args.exit_rules_json),
        "position_rules": json.loads(args.position_rules_json),
        "signal_cache_policy": args.signal_cache_policy,
        "market_data_mode": args.market_data_mode,
    }
    config = load_webapp_config()
    service = BacktestService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    result = service.run_backtest(payload, job_run_id=args.job_run_id)
    print(f"Wrote backtest JSON to {result['json_report_path']}")
    print(f"Wrote backtest HTML to {result['html_report_path']}")
    summary_path = Path(result["json_report_path"])
    print(f"Wrote run summary to {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
