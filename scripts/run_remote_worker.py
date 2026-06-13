#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.run_service import RunService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll queued remote jobs from Postgres and execute them on this worker.")
    parser.add_argument("--worker-name", default=socket.gethostname())
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=5.0)
    parser.add_argument("--once", action="store_true", help="Claim and run at most one job, then exit.")
    return parser.parse_args()


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _build_worker_env(run_service: RunService, options: dict[str, Any]) -> dict[str, str]:
    env = os.environ.copy()
    if options.get("market_data_source"):
        env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = str(options["market_data_source"])
    if run_service.database_url:
        env["TICKER_SCREENER_DATABASE_URL"] = run_service.database_url
    return env


def _publish_state(
    run_service: RunService,
    *,
    job_run_id: int,
    state: dict[str, Any],
    status: str | None = None,
    finished_at: str | None = None,
) -> None:
    artifact_path = str(state.get("summary_file") or state.get("watchlist_file") or "")
    run_service.history_repository.patch_job_run_result(
        job_run_id,
        result_payload_patch=state,
        status=status,
        artifact_path=artifact_path or None,
        finished_at=finished_at,
    )


def _run_claimed_job(run_service: RunService, row: dict[str, Any], *, worker_name: str, heartbeat_seconds: float) -> int:
    request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
    options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
    action_id = str(request_payload.get("action_id") or "")
    if not action_id:
        run_service.history_repository.update_job_run(
            int(row["id"]),
            status="failed",
            result_payload={"message": "Missing action_id in request payload.", "worker_name": worker_name},
            finished_at=_now_iso(),
        )
        return 1

    command = run_service.build_command(action_id, options, normalized=True)
    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        env=_build_worker_env(run_service, options),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    job_run_id = int(row["id"])
    state: dict[str, Any] = {
        "worker_name": worker_name,
        "execution_mode": "remote",
        "command": " ".join(command),
        "log_tail": "Starting...\n",
        "progress_label": "Starting…",
        "progress_current": None,
        "progress_total": None,
        "progress_percent": None,
        "success_count": 0,
        "cancel_requested": False,
        "message": f"Running on worker {worker_name}.",
    }
    _publish_state(run_service, job_run_id=job_run_id, state=state, status="running")
    run_service.history_repository.heartbeat_remote_worker(
        worker_name=worker_name,
        status="running",
        current_job_run_id=job_run_id,
        metadata={"action_id": action_id},
    )

    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    log_lines: list[str] = []
    artifact_state: dict[str, Any] = {"summary_file": "", "watchlist_file": "", "raw_results_file": "", "backtest_run_id": None}
    last_heartbeat = time.monotonic()
    cancel_requested = False

    try:
        while True:
            if run_service.history_repository.is_remote_job_cancel_requested(job_run_id):
                cancel_requested = True
                state["cancel_requested"] = True
                state["message"] = f"Cancellation requested on worker {worker_name}."
                run_service._terminate_process(process)

            events = selector.select(timeout=1.0)
            if events:
                line = process.stdout.readline()
                if line:
                    normalized_line = line.rstrip()
                    log_lines.append(normalized_line)
                    log_lines = log_lines[-80:]
                    progress = run_service._extract_progress(log_lines)
                    temp_job = dict(artifact_state)
                    run_service._update_artifacts(temp_job, normalized_line)
                    artifact_state.update(temp_job)
                    state.update(
                        {
                            "log_tail": "\n".join(log_lines),
                            "progress_current": progress["current"],
                            "progress_total": progress["total"],
                            "progress_percent": progress["percent"],
                            "progress_label": progress["label"] or state.get("progress_label"),
                            "success_count": int(progress["success_count"] or state.get("success_count") or 0),
                            "summary_file": str(artifact_state.get("summary_file") or ""),
                            "watchlist_file": str(artifact_state.get("watchlist_file") or ""),
                            "raw_results_file": str(artifact_state.get("raw_results_file") or ""),
                            "backtest_run_id": artifact_state.get("backtest_run_id"),
                        }
                    )
                    _publish_state(run_service, job_run_id=job_run_id, state=state)

            return_code = process.poll()
            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_seconds:
                state["worker_heartbeat_at"] = _now_iso()
                _publish_state(run_service, job_run_id=job_run_id, state=state)
                run_service.history_repository.heartbeat_remote_worker(
                    worker_name=worker_name,
                    status="running",
                    current_job_run_id=job_run_id,
                    metadata={"action_id": action_id},
                )
                last_heartbeat = now
            if return_code is not None:
                break
    finally:
        selector.unregister(process.stdout)
        selector.close()

    finished_at = _now_iso()
    final_status = "cancelled" if cancel_requested else ("success" if process.returncode == 0 else "failed")
    if final_status == "success":
        state["progress_percent"] = 100
        state["progress_label"] = "Completed"
        state["message"] = f"Completed on worker {worker_name}."
    elif final_status == "cancelled":
        state["progress_label"] = "Cancelled"
    else:
        state["message"] = f"Failed on worker {worker_name}."
    state["return_code"] = process.returncode
    _publish_state(run_service, job_run_id=job_run_id, state=state, status=final_status, finished_at=finished_at)
    run_service.history_repository.heartbeat_remote_worker(
        worker_name=worker_name,
        status="idle",
        current_job_run_id=None,
        metadata={"last_completed_job_run_id": job_run_id, "last_status": final_status},
    )
    return int(process.returncode or 0)


def main() -> int:
    args = parse_args()
    database_url = load_webapp_config().database_url
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Set TICKER_SCREENER_DATABASE_URL for the worker.")
    run_service = RunService(project_root=PROJECT_ROOT, database_url=database_url)
    exit_code = 0

    while True:
        run_service.history_repository.heartbeat_remote_worker(
            worker_name=str(args.worker_name).strip() or socket.gethostname(),
            status="idle",
            current_job_run_id=None,
        )
        claimed = run_service.history_repository.claim_remote_job_run(worker_name=str(args.worker_name).strip() or socket.gethostname())
        if claimed is None:
            if args.once:
                return exit_code
            time.sleep(max(1.0, float(args.poll_seconds)))
            continue
        exit_code = _run_claimed_job(
            run_service,
            claimed,
            worker_name=str(args.worker_name).strip() or socket.gethostname(),
            heartbeat_seconds=max(1.0, float(args.heartbeat_seconds)),
        )
        if args.once:
            return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
