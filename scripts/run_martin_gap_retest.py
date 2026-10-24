#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.repositories.history_repository import HistoryRepository
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_gap_retest_service import MartinGapRetestService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.martin_daily_gap_retest import REPLAY_V4_STRATEGY_ID

SOURCE_ID = "power_gap_volume_watch"
STRATEGY_ID = REPLAY_V4_STRATEGY_ID


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def _load_source(history: ScreenerHistoryService, day: dt.date) -> dict | None:
    records = history.list_runs(strategy_id=SOURCE_ID, start_date=day, end_date=day, limit=100)
    records = sorted(records, key=lambda item: (str(item.get("created_at") or item.get("run_date") or ""), int(item.get("id") or 0)), reverse=True)
    if not records:
        return None
    run = history.get_run(int(records[0]["id"]), include_hits=False)
    if not run:
        return None
    if str(run.get("run_date") or "")[:10] != day.isoformat():
        return None
    hits = []
    total = int(run.get("hit_count") or 0)
    offset = 0
    while offset < total:
        page = history.get_run(int(run["id"]), include_hits=True, hit_limit=5000, hit_offset=offset) or {}
        batch = page.get("hits") or page.get("screen_run_hits") or []
        if not batch:
            raise RuntimeError(f"Power Gap source run {run.get('id')} returned only {offset} of {total} persisted hits.")
        hits.extend(batch)
        offset += len(batch)
    summary = run.get("result_summary_json") if isinstance(run.get("result_summary_json"), dict) else {}
    fingerprint = run.get("source_fingerprint") or summary.get("source_fingerprint") or hashlib.sha256(
        f"{run.get('id')}:{run.get('created_at')}:{run.get('config_hash')}:{total}".encode()).hexdigest()[:16]
    return {**run, "source_run_id": run.get("id"), "published_at": run.get("created_at") or run.get("published_at"),
            "source_fingerprint": fingerprint, "target_trading_date": str(run.get("run_date") or day.isoformat())[:10], "hits": hits}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate one Martin Daily Gap Retest session.")
    parser.add_argument("--as-of-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--job-run-id", type=int)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Write a report without book writes (default).")
    mode.add_argument("--apply", action="store_true", help="Persist the evaluated paper-book session.")
    args = parser.parse_args()

    config = load_webapp_config()
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
                                    repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=not args.apply))
    source = _load_source(history, args.as_of_date)
    repo = MartinForwardTestRepository(database_url=config.database_url, read_only=not args.apply)
    service = MartinGapRetestService(repo, database_url=config.database_url)
    result = service.run_session(as_of_date=args.as_of_date, source_record=source, dry_run=not args.apply,
                                 strategy_id=STRATEGY_ID)
    result.update({"execution_mode": "applied" if args.apply else "dry_run", "source_run_id": (source or {}).get("source_run_id", result.get("source_run_id")),
                   "source_published_at": (source or {}).get("published_at"), "source_fingerprint": (source or {}).get("source_fingerprint")})

    root = config.artifacts_dir / "experiments" / STRATEGY_ID / args.as_of_date.strftime("%Y-%m-%d") / ("applied" if args.apply else "dry-run")
    raw_path, watch_path, summary_path = root / "raw_results.json", root / "watchlist.json", root / "run_summary.json"
    _atomic_json(raw_path, result)
    _atomic_json(watch_path, {"strategy_id": STRATEGY_ID, "as_of_date": args.as_of_date.isoformat(), "candidates": result.get("candidates", [])})
    _atomic_json(summary_path, {"strategy_id": STRATEGY_ID, "as_of_date": args.as_of_date.isoformat(), "execution_mode": result["execution_mode"],
                                "source_run_id": result["source_run_id"], "summary": result.get("summary", result),
                                "raw_results_file": str(raw_path), "watchlist_file": str(watch_path),
                                "job_run_id": args.job_run_id or os.getenv("JOB_RUN_ID")})
    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watch_path}")
    print(f"Wrote run summary to {summary_path}")
    job_run_id = args.job_run_id or os.getenv("JOB_RUN_ID")
    if job_run_id:
        print(f"job_run_id={job_run_id}")
    print(json.dumps(result, sort_keys=True, default=str))
    return 2 if result.get("status") == "missing_prerequisite" else 0


if __name__ == "__main__":
    raise SystemExit(main())
