#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch-fill persisted screener history cache.")
    parser.add_argument("--strategy-ids-json", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--market-data-source", default="database-first")
    parser.add_argument("--overwrite-policy", default="skip_existing")
    parser.add_argument("--scope-json", default="{}")
    parser.add_argument("--job-run-id", type=int)
    return parser.parse_args()


def iter_dates(start_date: dt.date, end_date: dt.date):
    cursor = start_date
    while cursor <= end_date:
        yield cursor
        cursor += dt.timedelta(days=1)


def main() -> int:
    args = parse_args()
    strategy_ids = [str(item).strip() for item in json.loads(args.strategy_ids_json) if str(item).strip()]
    scope = dict(json.loads(args.scope_json))
    if not strategy_ids:
        raise ValueError("strategy ids required")
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    config = load_webapp_config()
    run_service = RunService(project_root=PROJECT_ROOT, database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    history_service = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    actions = run_service._actions  # type: ignore[attr-defined]
    dates = list(iter_dates(start_date, end_date))
    total = len(strategy_ids) * len(dates)
    completed = 0
    persisted_runs: list[dict[str, Any]] = []

    for strategy_id in strategy_ids:
        action = actions.get(strategy_id)
        if action is None:
            raise ValueError(f"Unknown screener strategy: {strategy_id}")
        for target_date in dates:
            existing = history_service.list_runs(
                strategy_id=strategy_id,
                start_date=target_date,
                end_date=target_date,
                include_deleted=False,
                limit=5,
            )
            if existing and args.overwrite_policy == "skip_existing":
                completed += 1
                print(f"[{completed}/{total}] processing {strategy_id} {target_date.isoformat()} | passed={len(persisted_runs)}")
                continue

            command = [sys.executable, action.script_path, "--date-label", target_date.isoformat(), "--as-of-date", target_date.isoformat()]
            command.extend(action.extra_args)
            tickers = scope.get("tickers")
            if isinstance(tickers, list) and tickers:
                command.append("--tickers")
                command.extend([str(item).strip().upper() for item in tickers if str(item).strip()])
            if scope.get("limit") not in (None, ""):
                command.extend(["--limit", str(scope["limit"])])
            process_env = os.environ.copy()
            process_env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = args.market_data_source
            if config.database_url:
                process_env["TICKER_SCREENER_DATABASE_URL"] = config.database_url
            result = subprocess.run(
                command,
                cwd=str(PROJECT_ROOT),
                env=process_env,
                capture_output=True,
                text=True,
            )
            if result.stdout:
                print(result.stdout.rstrip())
            if result.stderr:
                print(result.stderr.rstrip())
            if result.returncode != 0:
                raise RuntimeError(f"Historical screener failed for {strategy_id} {target_date.isoformat()}")

            summary_path = _extract_summary_path(result.stdout)
            if not summary_path:
                raise RuntimeError("Unable to find run summary path from screener output.")
            summary_payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            raw_path = str(summary_payload.get("raw_results_file") or "").strip()
            if not raw_path:
                raise RuntimeError("Historical screener summary missing raw_results_file.")
            raw_payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
            run_id = history_service.persist_screen_run(
                strategy_id=strategy_id,
                options={**scope, "as_of_date": target_date.isoformat(), "date_label": target_date.isoformat(), "market_data_source": args.market_data_source},
                summary_payload=summary_payload,
                raw_payload=raw_payload,
                job_run_id=args.job_run_id,
            )
            if run_id is not None:
                persisted_runs.append({"strategy_id": strategy_id, "run_date": target_date.isoformat(), "screen_run_id": run_id})
            completed += 1
            print(f"[{completed}/{total}] processing {strategy_id} {target_date.isoformat()} | passed={len(persisted_runs)}")

    summary = {
        "strategy_ids": strategy_ids,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "persisted_runs": persisted_runs,
    }
    output_path = config.artifacts_dir / "raw" / f"screener_history_batch_{start_date.isoformat()}_{end_date.isoformat()}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote run summary to {output_path}")
    return 0


def _extract_summary_path(stdout: str) -> str:
    for line in (stdout or "").splitlines():
        if line.startswith("Wrote run summary to "):
            return line.removeprefix("Wrote run summary to ").strip()
    return ""


if __name__ == "__main__":
    raise SystemExit(main())
