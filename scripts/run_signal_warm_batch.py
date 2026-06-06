#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.overlap_backtest_service import OverlapBacktestService
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Warm screener cache and daily overlap summaries.")
    parser.add_argument("--strategy-ids-json", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--market-data-source", default="database-first")
    parser.add_argument("--overwrite-policy", default="skip_existing")
    parser.add_argument("--scope-json", default="{}")
    parser.add_argument("--candidate-threshold", type=int, default=4)
    parser.add_argument("--max-parallel", type=int, default=5)
    parser.add_argument("--job-run-id", type=int)
    return parser.parse_args()


def iter_dates(start_date: dt.date, end_date: dt.date):
    cursor = start_date
    while cursor <= end_date:
        yield cursor
        cursor += dt.timedelta(days=1)


def _finished_at_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def main() -> int:
    args = parse_args()
    strategy_ids = [str(item).strip() for item in json.loads(args.strategy_ids_json) if str(item).strip()]
    if not strategy_ids:
        raise ValueError("strategy ids required")
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    scope = dict(json.loads(args.scope_json))
    config = load_webapp_config()
    run_service = RunService(project_root=PROJECT_ROOT, database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    history_service = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    overlap_service = OverlapBacktestService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    actions = run_service._actions  # type: ignore[attr-defined]
    total = len(strategy_ids) * sum(1 for _ in iter_dates(start_date, end_date))
    completed = 0
    overlap_outputs: list[dict[str, Any]] = []

    for target_date in iter_dates(start_date, end_date):
        with ThreadPoolExecutor(max_workers=max(1, min(20, int(args.max_parallel)))) as executor:
            futures = [
                executor.submit(
                    _run_single_strategy,
                    strategy_id=strategy_id,
                    target_date=target_date,
                    scope=scope,
                    market_data_source=args.market_data_source,
                    overwrite_policy=args.overwrite_policy,
                    config=config,
                    actions=actions,
                    run_service=run_service,
                    history_service=history_service,
                    parent_job_run_id=args.job_run_id,
                )
                for strategy_id in strategy_ids
            ]
            failures: list[str] = []
            for future in as_completed(futures):
                result = future.result()
                completed += 1
                if result["status"] != "success":
                    failures.append(str(result["message"]))
                print(f"[{completed}/{total}] processing {result['strategy_id']} {target_date.isoformat()} | passed={int(result.get('success_count') or 0)}")
            if failures:
                raise RuntimeError(failures[0])

        overlap_payload = overlap_service.build_overlap_for_date(
            run_date=target_date,
            strategy_ids=strategy_ids,
            market_data_mode=args.market_data_source,
            candidate_threshold=args.candidate_threshold,
            source_job_run_id=args.job_run_id,
        )
        overlap_outputs.append(
            {
                "date": target_date.isoformat(),
                "overlap_run_id": overlap_payload.get("overlap_run_id"),
                "candidate_count": overlap_payload.get("candidate_count"),
                "artifact_path": overlap_payload.get("artifact_path"),
            }
        )
        print(f"Built overlap summary for {target_date.isoformat()} | candidates={int(overlap_payload.get('candidate_count') or 0)}")

    output = {
        "strategy_ids": strategy_ids,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "candidate_threshold": int(args.candidate_threshold),
        "overlap_outputs": overlap_outputs,
    }
    output_path = config.artifacts_dir / "raw" / f"signal_warm_batch_{start_date.isoformat()}_{end_date.isoformat()}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Wrote run summary to {output_path}")
    return 0


def _extract_summary_path(stdout: str) -> str:
    for line in (stdout or "").splitlines():
        if line.startswith("Wrote run summary to "):
            return line.removeprefix("Wrote run summary to ").strip()
    return ""


def _run_streaming_subprocess(*, command: list[str], process_env: dict[str, str]) -> tuple[int, str, str]:
    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        env=process_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    collected_lines: list[str] = []
    summary_path = ""
    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip()
        print(line, flush=True)
        collected_lines.append(line)
        if len(collected_lines) > 400:
            collected_lines = collected_lines[-400:]
        if not summary_path and line.startswith("Wrote run summary to "):
            summary_path = line.removeprefix("Wrote run summary to ").strip()
    return_code = process.wait()
    combined = "\n".join(collected_lines)
    if not summary_path:
        summary_path = _extract_summary_path(combined)
    return return_code, combined, summary_path


def _run_single_strategy(
    *,
    strategy_id: str,
    target_date: dt.date,
    scope: dict[str, Any],
    market_data_source: str,
    overwrite_policy: str,
    config: Any,
    actions: dict[str, Any],
    run_service: RunService,
    history_service: ScreenerHistoryService,
    parent_job_run_id: int | None,
) -> dict[str, Any]:
    action = actions.get(strategy_id)
    if action is None:
        raise ValueError(f"Unknown screener strategy: {strategy_id}")
    existing = history_service.list_runs(
        strategy_id=strategy_id,
        start_date=target_date,
        end_date=target_date,
        include_deleted=False,
        limit=5,
    )
    child_request_payload = {
        "strategy_id": strategy_id,
        "run_date": target_date.isoformat(),
        "market_data_source": market_data_source,
        "overwrite_policy": overwrite_policy,
        "scope": scope,
    }
    child_job_run_id = run_service.history_repository.create_job_run(
        job_type="screen_run",
        job_name=f"{action.label} ({target_date.isoformat()})",
        status="running",
        trigger_source="batch",
        request_payload=child_request_payload,
        parent_job_run_id=parent_job_run_id,
    )
    if existing and overwrite_policy == "skip_existing":
        existing_run = existing[0]
        run_service.history_repository.update_job_run(
            child_job_run_id,
            status="success",
            result_payload={
                "strategy_id": strategy_id,
                "run_date": target_date.isoformat(),
                "screen_run_id": existing_run.get("id"),
                "success_count": int(existing_run.get("hit_count") or 0),
                "watchlist_file": str(existing_run.get("watchlist_artifact_path") or ""),
                "raw_results_file": str(existing_run.get("raw_artifact_path") or ""),
                "message": "Skipped existing cached run.",
                "skipped": True,
            },
            artifact_path=str(existing_run.get("raw_artifact_path") or existing_run.get("watchlist_artifact_path") or "") or None,
            finished_at=_finished_at_iso(),
        )
        print(f"Skipped cached sub-job {strategy_id} {target_date.isoformat()} using existing screen run {existing_run.get('id')}.")
        return {"strategy_id": strategy_id, "status": "success", "success_count": int(existing_run.get("hit_count") or 0), "message": "skipped"}

    command = [sys.executable, "-u", action.script_path, "--date-label", target_date.isoformat(), "--as-of-date", target_date.isoformat()]
    command.extend(action.extra_args)
    tickers = scope.get("tickers")
    if isinstance(tickers, list) and tickers:
        command.append("--tickers")
        command.extend([str(item).strip().upper() for item in tickers if str(item).strip()])
    if scope.get("limit") not in (None, ""):
        command.extend(["--limit", str(scope["limit"])])
    for option_name, option_flag in (("filter_precedence", "--filter-precedence"),):
        value = scope.get(option_name)
        if value not in (None, ""):
            command.extend([option_flag, str(value)])
    for option_name, option_flag in (
        ("include_sectors", "--include-sectors"),
        ("exclude_sectors", "--exclude-sectors"),
        ("include_industries", "--include-industries"),
        ("exclude_industries", "--exclude-industries"),
        ("include_themes", "--include-themes"),
        ("exclude_themes", "--exclude-themes"),
    ):
        value = scope.get(option_name)
        if isinstance(value, list):
            command.extend([option_flag, *[str(item) for item in value if str(item).strip()]])
    child_request_payload["command"] = " ".join(command)
    process_env = os.environ.copy()
    process_env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = market_data_source
    process_env["PYTHONUNBUFFERED"] = "1"
    if config.database_url:
        process_env["TICKER_SCREENER_DATABASE_URL"] = config.database_url
    return_code, combined, summary_path = _run_streaming_subprocess(command=command, process_env=process_env)
    if return_code != 0:
        message = f"Warm screener failed for {strategy_id} {target_date.isoformat()}"
        run_service.history_repository.update_job_run(
            child_job_run_id,
            status="failed",
            result_payload={
                "strategy_id": strategy_id,
                "run_date": target_date.isoformat(),
                "success_count": 0,
                "log_tail": combined[-8000:],
                "message": message,
            },
            finished_at=_finished_at_iso(),
        )
        return {"strategy_id": strategy_id, "status": "failed", "success_count": 0, "message": message}
    if not summary_path:
        raise RuntimeError("Unable to find run summary path from screener output.")
    summary_payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
    raw_path = str(summary_payload.get("raw_results_file") or "").strip()
    raw_payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    run_id = history_service.persist_screen_run(
        strategy_id=strategy_id,
        options={**scope, "as_of_date": target_date.isoformat(), "date_label": target_date.isoformat(), "market_data_source": market_data_source},
        summary_payload=summary_payload,
        raw_payload=raw_payload,
        job_run_id=child_job_run_id,
    )
    success_count = int(summary_payload.get("passed_tickers") or 0)
    run_service.history_repository.update_job_run(
        child_job_run_id,
        status="success",
        result_payload={
            "strategy_id": strategy_id,
            "run_date": target_date.isoformat(),
            "screen_run_id": run_id,
            "success_count": success_count,
            "summary_file": summary_path,
            "watchlist_file": str(summary_payload.get("watchlist_file") or ""),
            "raw_results_file": raw_path,
            "log_tail": combined[-8000:],
            "message": f"Persisted warmed screener result for {strategy_id} {target_date.isoformat()}",
            "skipped": False,
        },
        artifact_path=summary_path,
        finished_at=_finished_at_iso(),
    )
    return {"strategy_id": strategy_id, "status": "success", "success_count": success_count, "message": "ok"}


if __name__ == "__main__":
    raise SystemExit(main())
