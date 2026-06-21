#!/usr/bin/env sh
set -eu

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose "$@"
  else
    docker-compose "$@"
  fi
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
DEPLOY_DIR="$PROJECT_ROOT/deploy"

cd "$DEPLOY_DIR"

DB_CONTAINER_ID=$(compose ps -q db || true)
if [ -z "$DB_CONTAINER_ID" ] || [ "$(docker inspect -f '{{.State.Running}}' "$DB_CONTAINER_ID" 2>/dev/null || echo false)" != "true" ]; then
  echo "Skipping active remote job check because db container is not currently running."
  exit 0
fi

POSTGRES_USER_IN_CONTAINER=$(compose exec -T db printenv POSTGRES_USER | tr -d '\r')
POSTGRES_DB_IN_CONTAINER=$(compose exec -T db printenv POSTGRES_DB | tr -d '\r')
ACTIVE_REMOTE_COUNT=$(compose exec -T db psql -U "$POSTGRES_USER_IN_CONTAINER" -d "$POSTGRES_DB_IN_CONTAINER" -Atqc "SELECT COUNT(*) FROM job_runs WHERE status IN ('queued', 'running') AND COALESCE(request_payload->>'execution_mode', 'local') = 'remote';")
ACTIVE_REMOTE_COUNT=${ACTIVE_REMOTE_COUNT:-0}

if [ "$ACTIVE_REMOTE_COUNT" = "0" ]; then
  exit 0
fi

case "${ALLOW_ACTIVE_REMOTE_JOBS:-false}" in
  true|TRUE|1|yes|YES)
    echo "Proceeding despite ${ACTIVE_REMOTE_COUNT} active remote job(s) because allow_active_remote_jobs=${ALLOW_ACTIVE_REMOTE_JOBS}."
    exit 0
    ;;
esac

echo "Blocked deploy: found ${ACTIVE_REMOTE_COUNT} active remote job(s)."
echo "Active remote jobs:"
compose exec -T db psql -U "$POSTGRES_USER_IN_CONTAINER" -d "$POSTGRES_DB_IN_CONTAINER" -Atqc "SELECT id || ' | ' || job_name || ' | status=' || status || ' | target_worker=' || COALESCE(request_payload->>'target_worker', '') FROM job_runs WHERE status IN ('queued', 'running') AND COALESCE(request_payload->>'execution_mode', 'local') = 'remote' ORDER BY created_at ASC LIMIT 20;"
echo "Wait for remote jobs to finish, or rerun manual deploy with allow_active_remote_jobs=true if you accept the risk."
exit 1
