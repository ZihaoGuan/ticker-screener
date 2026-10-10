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

# The old worker must finish its running work and consume any jobs assigned to
# its image before it is replaced. Jobs for this image are deliberately allowed
# to remain queued: the new worker will claim them immediately after startup.
active_jobs="$(
  compose exec -T db sh -lc \
    "psql -v ON_ERROR_STOP=1 -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atqc \"SELECT COUNT(*) FROM job_runs WHERE COALESCE(request_payload->>'execution_mode', 'local') = 'remote' AND (status = 'running' OR (status = 'queued' AND COALESCE(request_payload->>'code_version', '') <> '${image_tag}'));\""
)"
active_jobs="${active_jobs//[[:space:]]/}"
if [ "${active_jobs:-0}" != "0" ]; then
  echo "Worker drain is incomplete: ${active_jobs} running or previous-version remote job(s) remain." >&2
  exit 1
fi

echo "Worker drain complete; replacing only the worker with ticker-screener:${image_tag}."
worker_container_ids="$(compose ps -q worker worker_parallel || true)"
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
TICKER_SCREENER_IMAGE_TAG="$image_tag" compose up -d --no-deps worker worker_parallel
