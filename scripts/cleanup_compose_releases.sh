#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [immutable-image-tag]" >&2
  exit 2
fi

image_tag="${1:-}"
if [ -n "$image_tag" ] && ! [[ "$image_tag" =~ ^[0-9a-f]{7,64}$ ]]; then
  echo "Worker image tag must be a Git SHA (7-64 lowercase hexadecimal characters)." >&2
  exit 2
fi

script_dir="$(cd "$(dirname "$0")" && pwd)"
project_root="$(cd "${script_dir}/.." && pwd)"
compose_project="$(basename "${project_root}/deploy")"

echo "Disk usage before ticker-screener release cleanup:"
df -h "$project_root"
docker system df

# Only remove exited Compose containers that are both owned by this project and
# use an application image. Database, Caddy, volumes, and other projects stay untouched.
for service in web worker worker_parallel worker_parallel_2 worker_parallel_3; do
  while IFS= read -r container_id; do
    [ -n "$container_id" ] || continue
    container_image="$(docker inspect -f '{{.Config.Image}}' "$container_id" 2>/dev/null || true)"
    case "$container_image" in
      ticker-screener:*) docker rm "$container_id" >/dev/null ;;
    esac
  done < <(docker ps -aq \
    --filter "label=com.docker.compose.project=${compose_project}" \
    --filter "label=com.docker.compose.service=${service}" \
    --filter "status=exited")
done

protected_image_ids=$'\n'
has_image_id() {
  case "$protected_image_ids" in
    *$'\n'"$1"$'\n'*) return 0 ;;
    *) return 1 ;;
  esac
}
protect_image_id() {
  has_image_id "$1" || protected_image_ids+="$1"$'\n'
}

while IFS= read -r container_id; do
  [ -n "$container_id" ] || continue
  container_image_id="$(docker inspect -f '{{.Image}}' "$container_id" 2>/dev/null || true)"
  [ -n "$container_image_id" ] && protect_image_id "$container_image_id"
done < <(docker ps -aq)

if [ -n "$image_tag" ]; then
  target_image_id="$(docker image inspect -f '{{.Id}}' "ticker-screener:${image_tag}" 2>/dev/null || true)"
  [ -n "$target_image_id" ] && protect_image_id "$target_image_id"
fi

# docker image ls is newest first. Retaining the newest two distinct images
# gives the active release and one rollback release even when no container references them.
retained_images=0
newest_image_ids=$'\n'
while IFS=' ' read -r image_ref image_id; do
  [ -n "$image_ref" ] || continue
  [ "$image_ref" = "ticker-screener:<none>" ] && continue
  case "$newest_image_ids" in
    *$'\n'"$image_id"$'\n'*)
      continue
      ;;
  esac
  if [ "$retained_images" -lt 2 ]; then
    protect_image_id "$image_id"
    newest_image_ids+="$image_id"$'\n'
    retained_images=$((retained_images + 1))
  fi
done < <(docker image ls --no-trunc --format '{{.Repository}}:{{.Tag}} {{.ID}}' ticker-screener)

while IFS=' ' read -r image_ref image_id; do
  [ -n "$image_ref" ] || continue
  [ "$image_ref" = "ticker-screener:<none>" ] && continue
  if ! has_image_id "$image_id"; then
    docker image rm "$image_ref" >/dev/null || true
  fi
done < <(docker image ls --no-trunc --format '{{.Repository}}:{{.Tag}} {{.ID}}' ticker-screener)

docker image prune -f >/dev/null
docker builder prune -f --filter 'until=24h' >/dev/null || true

echo "Disk usage after ticker-screener release cleanup:"
df -h "$project_root"
docker system df
