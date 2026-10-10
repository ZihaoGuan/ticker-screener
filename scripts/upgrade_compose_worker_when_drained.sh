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

script_dir="$(cd "$(dirname "$0")" && pwd)"
deploy_dir="${TICKER_SCREENER_DEPLOY_DIR:-${script_dir}/../deploy}"
status_dir="${TICKER_SCREENER_WORKER_UPGRADE_STATUS_DIR:-${script_dir}/../artifacts/status}"
upgrade_script="${TICKER_SCREENER_WORKER_UPGRADE_SCRIPT:-${script_dir}/upgrade_compose_worker.sh}"
poll_seconds="${TICKER_SCREENER_WORKER_UPGRADE_POLL_SECONDS:-15}"
mkdir -p "$status_dir"

if docker compose version >/dev/null 2>&1; then
  compose() { docker compose "$@"; }
else
  compose() { docker-compose "$@"; }
fi

exec 9>"${status_dir}/compose-worker-upgrade.lock"
flock 9
cd "$deploy_dir"

while true; do
  web_container_id="$(compose ps -q web || true)"
  web_image=""
  if [ -n "$web_container_id" ]; then
    web_image="$(docker inspect -f '{{.Config.Image}}' "$web_container_id" 2>/dev/null || true)"
  fi
  if [ "$web_image" != "ticker-screener:${image_tag}" ]; then
    echo "Worker release ${image_tag} superseded by web image ${web_image:-missing}; exiting."
    exit 0
  fi

  if "$upgrade_script" "$image_tag"; then
    echo "Worker release ${image_tag} completed after drain."
    exit 0
  fi

  echo "Worker release ${image_tag} is waiting for previous-version jobs to drain."
  sleep "$poll_seconds"
done
