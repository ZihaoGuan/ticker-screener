#!/usr/bin/env sh
set -eu

usage() {
  echo "Usage: $0 <job_id> <job_label> -- <command ...>" >&2
  exit 2
}

if [ "$#" -lt 4 ]; then
  usage
fi

JOB_ID=$1
shift
JOB_LABEL=$1
shift

if [ "$1" != "--" ]; then
  usage
fi
shift

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
STATUS_DIR=${TICKER_SCREENER_STATUS_DIR:-"$PROJECT_ROOT/artifacts/status"}
LOG_DIR="$STATUS_DIR/logs"
START_STAMP=$(date -u +"%Y%m%dT%H%M%SZ")
STARTED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
STATUS_FILE="$STATUS_DIR/${JOB_ID}.json"
LOG_FILE="$LOG_DIR/${JOB_ID}-${START_STAMP}.log"
ARTIFACT_FILE=${TICKER_SCREENER_STATUS_ARTIFACT:-}

mkdir -p "$STATUS_DIR" "$LOG_DIR"

write_status() {
  STATUS_VALUE=$1
  FINISHED_AT_VALUE=$2
  EXIT_CODE_VALUE=$3
  MESSAGE_VALUE=$4
  export JOB_ID JOB_LABEL STATUS_FILE LOG_FILE ARTIFACT_FILE STARTED_AT STATUS_VALUE FINISHED_AT_VALUE EXIT_CODE_VALUE MESSAGE_VALUE
  python3 - <<'PY'
import json
import os
from pathlib import Path

payload = {
    "job_id": os.environ["JOB_ID"],
    "job_label": os.environ["JOB_LABEL"],
    "status": os.environ["STATUS_VALUE"],
    "last_started_at": os.environ["STARTED_AT"],
    "last_finished_at": os.environ["FINISHED_AT_VALUE"] or None,
    "exit_code": int(os.environ["EXIT_CODE_VALUE"]) if os.environ["EXIT_CODE_VALUE"] else None,
    "log_file": os.environ["LOG_FILE"],
    "artifact_file": os.environ["ARTIFACT_FILE"] or None,
    "message": os.environ["MESSAGE_VALUE"],
}
status_path = Path(os.environ["STATUS_FILE"])
tmp_path = status_path.with_suffix(".tmp")
tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
tmp_path.replace(status_path)
PY
}

write_status "running" "" "" "Job started."

if "$@" >"$LOG_FILE" 2>&1; then
  EXIT_CODE=0
  STATUS_VALUE="success"
  MESSAGE_VALUE="Job completed successfully."
else
  EXIT_CODE=$?
  STATUS_VALUE="failed"
  MESSAGE_VALUE="Job failed with exit code ${EXIT_CODE}."
fi

FINISHED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
write_status "$STATUS_VALUE" "$FINISHED_AT" "$EXIT_CODE" "$MESSAGE_VALUE"

exit "$EXIT_CODE"
