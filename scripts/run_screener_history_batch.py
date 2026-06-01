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


def _finished_at_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


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
            child_request_payload = {
                "strategy_id": strategy_id,
                "run_date": target_date.isoformat(),
                "market_data_source": args.market_data_source,
                "overwrite_policy": args.overwrite_policy,
                "scope": scope,
            }
            if existing and args.overwrite_policy == "skip_existing":
                child_job_run_id = run_service.history_repository.create_job_run(
                    job_type="screen_run",
                    job_name=f"{action.label} ({target_date.isoformat()})",
                    status="running",
                    trigger_source="batch",
                    request_payload=child_request_payload,
                    parent_job_run_id=args.job_run_id,
                )
                existing_run = existing[0]
                run_service.history_repository.update_job_run(
                    child_job_run_id,
                    status="success",
                    result_payload={
                        "strategy_id": strategy_id,
                        "run_date": target_date.isoformat(),
                        "screen_run_id": existing_run.get("id"),
                        "success_count": int(existing_run.get("hit_count") or 0),
                        "summary_file": "",
                        "watchlist_file": str(existing_run.get("watchlist_artifact_path") or ""),
                        "raw_results_file": str(existing_run.get("raw_artifact_path") or ""),
                        "log_tail": "Skipped screener execution because matching cached run already exists.",
                        "message": "Skipped existing cached run.",
                        "skipped": True,
                    },
                    artifact_path=str(existing_run.get("raw_artifact_path") or existing_run.get("watchlist_artifact_path") or "") or None,
                    finished_at=_finished_at_iso(),
                )
                completed += 1
                print(f"Skipped cached sub-job {strategy_id} {target_date.isoformat()} using existing screen run {existing_run.get('id')}.")
                print(f"[{completed}/{total}] processing {strategy_id} {target_date.isoformat()} | passed={len(persisted_runs)}")
                continue

            command = [sys.executable, "-u", action.script_path, "--date-label", target_date.isoformat(), "--as-of-date", target_date.isoformat()]
            command.extend(action.extra_args)
            tickers = scope.get("tickers")
            if isinstance(tickers, list) and tickers:
                command.append("--tickers")
                command.extend([str(item).strip().upper() for item in tickers if str(item).strip()])
            if scope.get("limit") not in (None, ""):
                command.extend(["--limit", str(scope["limit"])])
            child_request_payload["command"] = " ".join(command)
            child_job_run_id = run_service.history_repository.create_job_run(
                job_type="screen_run",
                job_name=f"{action.label} ({target_date.isoformat()})",
                status="running",
                trigger_source="batch",
                request_payload=child_request_payload,
                parent_job_run_id=args.job_run_id,
            )
            process_env = os.environ.copy()
            process_env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = args.market_data_source
            process_env["PYTHONUNBUFFERED"] = "1"
            if config.database_url:
                process_env["TICKER_SCREENER_DATABASE_URL"] = config.database_url
            log_file = _child_log_path(
                artifacts_dir=config.artifacts_dir,
                parent_job_run_id=args.job_run_id,
                strategy_id=strategy_id,
                run_date=target_date,
            )
            _initialize_child_log(log_file)
            _update_child_job(
                run_service=run_service,
                child_job_run_id=child_job_run_id,
                status="running",
                strategy_id=strategy_id,
                run_date=target_date.isoformat(),
                success_count=0,
                log_tail="Starting screener sub-job...",
                log_file=str(log_file),
                message=f"Running screener for {strategy_id} {target_date.isoformat()}",
                skipped=False,
            )
            return_code, log_tail, summary_path = _stream_screener_sub_job(
                command=command,
                process_env=process_env,
                log_file=log_file,
                run_service=run_service,
                child_job_run_id=child_job_run_id,
                strategy_id=strategy_id,
                run_date=target_date.isoformat(),
            )
            if return_code != 0:
                run_service.history_repository.update_job_run(
                    child_job_run_id,
                    status="failed",
                    result_payload={
                        "strategy_id": strategy_id,
                        "run_date": target_date.isoformat(),
                        "success_count": 0,
                        "log_tail": log_tail,
                        "log_file": str(log_file),
                        "message": f"Historical screener failed for {strategy_id} {target_date.isoformat()}",
                    },
                    artifact_path=str(log_file),
                    finished_at=_finished_at_iso(),
                )
                print(f"Sub-job failed {strategy_id} {target_date.isoformat()} | see child log tail in webapp.")
                raise RuntimeError(f"Historical screener failed for {strategy_id} {target_date.isoformat()}")

            if not summary_path:
                run_service.history_repository.update_job_run(
                    child_job_run_id,
                    status="failed",
                    result_payload={
                        "strategy_id": strategy_id,
                        "run_date": target_date.isoformat(),
                        "success_count": 0,
                        "log_tail": log_tail,
                        "log_file": str(log_file),
                        "message": "Unable to find run summary path from screener output.",
                    },
                    artifact_path=str(log_file),
                    finished_at=_finished_at_iso(),
                )
                raise RuntimeError("Unable to find run summary path from screener output.")
            summary_payload = json.loads(Path(summary_path).read_text(encoding="utf-8"))
            raw_path = str(summary_payload.get("raw_results_file") or "").strip()
            if not raw_path:
                run_service.history_repository.update_job_run(
                    child_job_run_id,
                    status="failed",
                    result_payload={
                        "strategy_id": strategy_id,
                        "run_date": target_date.isoformat(),
                        "success_count": 0,
                        "summary_file": summary_path,
                        "log_tail": log_tail,
                        "log_file": str(log_file),
                        "message": "Historical screener summary missing raw_results_file.",
                    },
                    artifact_path=summary_path,
                    finished_at=_finished_at_iso(),
                )
                raise RuntimeError("Historical screener summary missing raw_results_file.")
            raw_payload = json.loads(Path(raw_path).read_text(encoding="utf-8"))
            run_id = history_service.persist_screen_run(
                strategy_id=strategy_id,
                options={**scope, "as_of_date": target_date.isoformat(), "date_label": target_date.isoformat(), "market_data_source": args.market_data_source},
                summary_payload=summary_payload,
                raw_payload=raw_payload,
                job_run_id=child_job_run_id,
            )
            if run_id is not None:
                persisted_runs.append({"strategy_id": strategy_id, "run_date": target_date.isoformat(), "screen_run_id": run_id})
            run_service.history_repository.update_job_run(
                child_job_run_id,
                status="success",
                result_payload={
                    "strategy_id": strategy_id,
                    "run_date": target_date.isoformat(),
                    "screen_run_id": run_id,
                    "success_count": int(summary_payload.get("passed_tickers") or 0),
                    "summary_file": summary_path,
                    "watchlist_file": str(summary_payload.get("watchlist_file") or ""),
                    "raw_results_file": raw_path,
                    "log_tail": log_tail,
                    "log_file": str(log_file),
                    "message": f"Persisted cached screener result for {strategy_id} {target_date.isoformat()}",
                    "skipped": False,
                },
                artifact_path=summary_path,
                finished_at=_finished_at_iso(),
            )
            completed += 1
            print(
                f"Completed sub-job {strategy_id} {target_date.isoformat()} | hits={int(summary_payload.get('passed_tickers') or 0)}"
            )
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


def _stream_screener_sub_job(
    *,
    command: list[str],
    process_env: dict[str, str],
    log_file: Path,
    run_service: RunService,
    child_job_run_id: int | None,
    strategy_id: str,
    run_date: str,
) -> tuple[int, str, str]:
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
    line_counter = 0
    summary_path = ""
    assert process.stdout is not None
    with log_file.open("a", encoding="utf-8") as handle:
        for raw_line in process.stdout:
            line = raw_line.rstrip()
            collected_lines.append(line)
            if len(collected_lines) > 160:
                collected_lines = collected_lines[-160:]
            handle.write(raw_line)
            handle.flush()
            print(line, flush=True)
            line_counter += 1
            if not summary_path and line.startswith("Wrote run summary to "):
                summary_path = line.removeprefix("Wrote run summary to ").strip()
            if line_counter == 1 or line_counter % 5 == 0:
                _update_child_job(
                    run_service=run_service,
                    child_job_run_id=child_job_run_id,
                    status="running",
                    strategy_id=strategy_id,
                    run_date=run_date,
                    success_count=_extract_success_count(collected_lines),
                    log_tail="\n".join(collected_lines[-120:]),
                    log_file=str(log_file),
                    message=f"Running screener for {strategy_id} {run_date}",
                    skipped=False,
                )
    return_code = process.wait()
    log_tail = "\n".join(collected_lines[-120:])
    if not summary_path:
        summary_path = _extract_summary_path(log_tail)
    return return_code, log_tail, summary_path


def _extract_summary_path(stdout: str) -> str:
    for line in (stdout or "").splitlines():
        if line.startswith("Wrote run summary to "):
            return line.removeprefix("Wrote run summary to ").strip()
    return ""


def _child_log_path(
    *,
    artifacts_dir: Path,
    parent_job_run_id: int | None,
    strategy_id: str,
    run_date: dt.date,
) -> Path:
    output_dir = artifacts_dir / "raw" / "batch_subjob_logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    parent_prefix = str(parent_job_run_id) if parent_job_run_id is not None else "batch"
    return output_dir / f"{parent_prefix}_{strategy_id}_{run_date.isoformat()}.log"


def _initialize_child_log(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text("", encoding="utf-8")


def _extract_success_count(lines: list[str]) -> int:
    for line in reversed(lines):
        if "passed=" not in line:
            continue
        try:
            return int(line.split("passed=", 1)[1].split()[0])
        except (TypeError, ValueError):
            continue
    return 0


def _update_child_job(
    *,
    run_service: RunService,
    child_job_run_id: int | None,
    status: str,
    strategy_id: str,
    run_date: str,
    success_count: int,
    log_tail: str,
    log_file: str,
    message: str,
    skipped: bool,
) -> None:
    run_service.history_repository.update_job_run(
        child_job_run_id,
        status=status,
        result_payload={
            "strategy_id": strategy_id,
            "run_date": run_date,
            "success_count": success_count,
            "log_tail": log_tail,
            "log_file": log_file,
            "message": message,
            "skipped": skipped,
        },
    )


if __name__ == "__main__":
    raise SystemExit(main())
