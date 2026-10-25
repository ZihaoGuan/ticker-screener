#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.repositories.history_repository import HistoryRepository
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.martin_bar_watchlist_screen import STRATEGY_ID as SOURCE_ID
from src.martin_bar_retest import STRATEGY_ID


def _load_source(history: ScreenerHistoryService, day: dt.date) -> dict | None:
    runs = sorted(history.list_runs(strategy_id=SOURCE_ID, start_date=day, end_date=day, limit=100),
                   key=lambda r: (str(r.get("created_at") or ""), int(r.get("id") or 0)), reverse=True)
    if not runs: return None
    run = history.get_run(int(runs[0]["id"]), include_hits=False)
    if (not run or run.get("strategy_id") != SOURCE_ID
            or str(run.get("run_date") or "")[:10] != day.isoformat()): return None
    hits, total, offset = [], int(run.get("hit_count") or 0), 0
    while offset < total:
        page = history.get_run(int(run["id"]), include_hits=True, hit_limit=5000, hit_offset=offset) or {}
        batch = page.get("hits") or page.get("screen_run_hits") or []
        if not batch: raise RuntimeError(f"Source run {run['id']} returned only {offset} of {total} hits")
        hits.extend(batch); offset += len(batch)
    normalized_hits = []
    for row in hits:
        if row.get("passed") is False:
            continue
        payload = {**(row.get("metrics_json") or {}), **(row.get("hit_payload_json") or {}),
                   **{k: row.get(k) for k in ("ticker", "rank", "signal_date")}}
        if payload.get("rules_version") not in (None, SOURCE_ID):
            raise RuntimeError(f"Source run {run['id']} contains a hit from a different rules version.")
        normalized_hits.append(payload)
    source_identity = {key: run.get(key) for key in ("id", "strategy_id", "run_date", "created_at", "config_hash", "scope_hash", "hit_count")}
    fp = hashlib.sha256(json.dumps({"run": source_identity, "hits": normalized_hits}, sort_keys=True,
                                   separators=(",", ":"), default=str).encode()).hexdigest()
    return {**run, "source_run_id": run["id"], "published_at": run.get("created_at"),
            "source_fingerprint": fp, "target_trading_date": day.isoformat(), "hits": normalized_hits,
            "rules_version": SOURCE_ID,
            "source_mode": "persisted_screen_run_retrospective"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate persisted Martin Bar Watchlist hits against completed daily bars.")
    parser.add_argument("--as-of-date", required=True, type=dt.date.fromisoformat)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Evaluate without persistence (default).")
    mode.add_argument("--apply", action="store_true", help="Persist this daily paper watch decision.")
    args = parser.parse_args()
    config = load_webapp_config()
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=not args.apply))
    source = _load_source(history, args.as_of_date)
    repo = MartinForwardTestRepository(database_url=config.database_url, read_only=not args.apply)
    result = MartinBarRetestService(repo, database_url=config.database_url).run_session(
        as_of_date=args.as_of_date, source_record=source, dry_run=not args.apply)
    print(json.dumps(result, sort_keys=True, default=str))
    return 2 if result.get("status") in {"missing_source", "blocked_source"} else 0


if __name__ == "__main__": raise SystemExit(main())
