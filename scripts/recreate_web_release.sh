#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [immutable-image-tag]" >&2
  exit 2
fi

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image_tag="${1:-$(git -C "$project_root" rev-parse --short=12 HEAD)}"

if ! [[ "$image_tag" =~ ^[0-9a-f]{7,64}$ ]]; then
  echo "Web image tag must be a Git SHA (7-64 lowercase hexadecimal characters)." >&2
  exit 2
fi

if ! docker image inspect "ticker-screener:${image_tag}" >/dev/null 2>&1; then
  echo "Image ticker-screener:${image_tag} is not available on this host." >&2
  exit 1
fi

cd "$project_root/deploy"

if docker compose version >/dev/null 2>&1; then
  compose() { docker compose "$@"; }
else
  compose() { docker-compose "$@"; }
fi

# docker-compose v1 can fail with KeyError: ContainerConfig when asked to
# force-recreate an image built by a newer Docker version. Removing only the
# web service beforehand avoids that code path and retains bind-mounted config
# and artifacts. Workers and Postgres are intentionally untouched.
web_container_ids="$(compose ps -q web || true)"
if [ -n "$web_container_ids" ]; then
  while IFS= read -r container_id; do
    [ -n "$container_id" ] && docker rm -f "$container_id"
  done <<EOF
$web_container_ids
EOF
fi

TICKER_SCREENER_IMAGE_TAG="$image_tag" compose up -d --no-deps web
compose ps web
