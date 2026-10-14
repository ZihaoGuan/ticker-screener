#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <immutable-image-tag>" >&2
  exit 2
fi

image_tag="$1"
if ! [[ "$image_tag" =~ ^[0-9a-f]{7,64}$ ]]; then
  echo "Worker image tag must be a Git SHA (7-64 lowercase hexadecimal characters)." >&2
  exit 2
fi

if docker compose version >/dev/null 2>&1; then
  compose() { docker compose "$@"; }
else
  compose() { docker-compose "$@"; }
fi

if ! docker image inspect "ticker-screener:${image_tag}" >/dev/null 2>&1; then
  echo "Image ticker-screener:${image_tag} is not available on this host." >&2
  exit 1
fi

db_psql() {
  local query="${1//$'\n'/ }"
  compose exec -T db sh -lc \
    "psql -v ON_ERROR_STOP=1 -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atqc \"$query\""
}

# A version-bound queued job can only drain on a healthy worker with the same
# image. Retire it after a grace period when that worker release no longer exists.
orphaned_jobs="$(db_psql "
  WITH healthy_versions AS (
    SELECT DISTINCT metadata_json->>'code_version' AS code_version
    FROM remote_workers
    WHERE last_heartbeat_at >= NOW() - INTERVAL '60 seconds'
  ), orphaned_jobs AS (
    SELECT job.id
    FROM job_runs AS job
    WHERE job.status = 'queued'
      AND COALESCE(job.request_payload->>'execution_mode', 'local') = 'remote'
      AND COALESCE(job.request_payload->>'code_version', '') <> ''
      AND job.request_payload->>'code_version' <> '${image_tag}'
      AND job.created_at < NOW() - INTERVAL '10 minutes'
      AND NOT EXISTS (
        SELECT 1
        FROM healthy_versions AS worker
        WHERE worker.code_version = job.request_payload->>'code_version'
      )
    FOR UPDATE SKIP LOCKED
  ), cancelled AS (
    UPDATE job_runs AS job
    SET status = 'cancelled',
        finished_at = NOW(),
        result_payload = COALESCE(job.result_payload, '{}'::jsonb) ||
          jsonb_build_object(
            'cancel_requested', true,
            'cancel_reason', 'no_compatible_healthy_worker',
            'progress_label', 'Cancelled: no compatible worker',
            'message', 'Cancelled during worker release after 10 minutes without a healthy worker for code version ' || COALESCE(job.request_payload->>'code_version', ''),
            'target_code_version', '${image_tag}'
          )
    FROM orphaned_jobs
    WHERE job.id = orphaned_jobs.id
    RETURNING job.id
  )
  SELECT COUNT(*) FROM cancelled;
")"
orphaned_jobs="${orphaned_jobs//[[:space:]]/}"
if [ "${orphaned_jobs:-0}" != "0" ]; then
  echo "Cancelled ${orphaned_jobs} orphaned previous-version remote job(s)."
fi

# Running work always drains. A queued previous-version job only drains when a
# healthy worker with that version remains; target-version and legacy jobs can
# be claimed by the replacement workers.
active_jobs="$(db_psql "
  WITH healthy_versions AS (
    SELECT DISTINCT metadata_json->>'code_version' AS code_version
    FROM remote_workers
    WHERE last_heartbeat_at >= NOW() - INTERVAL '60 seconds'
  )
  SELECT COUNT(*)
  FROM job_runs AS job
  WHERE COALESCE(job.request_payload->>'execution_mode', 'local') = 'remote'
    AND (
      job.status = 'running'
      OR (
        job.status = 'queued'
        AND COALESCE(job.request_payload->>'code_version', '') <> ''
        AND job.request_payload->>'code_version' <> '${image_tag}'
        AND (
          job.created_at >= NOW() - INTERVAL '10 minutes'
          OR EXISTS (
            SELECT 1
            FROM healthy_versions AS worker
            WHERE worker.code_version = job.request_payload->>'code_version'
          )
        )
      )
    );
")"
active_jobs="${active_jobs//[[:space:]]/}"
if [ "${active_jobs:-0}" != "0" ]; then
  echo "Worker drain is incomplete: ${active_jobs} running or drainable previous-version remote job(s) remain." >&2
  exit 1
fi

echo "Worker drain complete; replacing only the worker with ticker-screener:${image_tag}."
worker_container_ids="$(compose ps -q worker worker_parallel worker_parallel_2 worker_parallel_3 || true)"
if [ -n "$worker_container_ids" ]; then
  # docker-compose v1 can fail during --force-recreate when the image metadata
  # omits ContainerConfig. Removing the already-drained workers first avoids
  # that compatibility path while leaving every other service untouched.
  while IFS= read -r worker_container_id; do
    [ -n "$worker_container_id" ] && docker rm -f "$worker_container_id"
  done <<EOF
$worker_container_ids
EOF
fi
TICKER_SCREENER_IMAGE_TAG="$image_tag" compose up -d --no-deps \
  worker worker_parallel worker_parallel_2 worker_parallel_3

for _ in $(seq 1 30); do
  healthy_workers="$(db_psql "
    SELECT COUNT(*)
    FROM remote_workers
    WHERE worker_name IN ('primary-worker', 'parallel-worker', 'parallel-worker-2', 'parallel-worker-3')
      AND last_heartbeat_at >= NOW() - INTERVAL '60 seconds'
      AND metadata_json->>'code_version' = '${image_tag}';
  ")"
  healthy_workers="${healthy_workers//[[:space:]]/}"
  if [ "${healthy_workers:-0}" = "4" ]; then
    echo "All four workers are healthy on ticker-screener:${image_tag}."
    exit 0
  fi
  sleep 2
done

echo "Workers started but did not report healthy on ticker-screener:${image_tag}." >&2
exit 1
