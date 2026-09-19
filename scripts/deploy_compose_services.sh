#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <compose command and arguments>" >&2
  exit 2
fi

if docker compose version >/dev/null 2>&1; then
  exec docker compose "$@"
fi

deploy_log=$(mktemp)
trap 'rm -f "$deploy_log"' EXIT

if docker-compose "$@" 2>&1 | tee "$deploy_log"; then
  exit 0
fi

if ! grep -Fq "KeyError: 'ContainerConfig'" "$deploy_log"; then
  exit 1
fi

echo "Legacy Docker Compose hit its ContainerConfig recreate bug; removing only the web and caddy containers before retrying."
docker-compose rm -sf web caddy
docker-compose "$@"
