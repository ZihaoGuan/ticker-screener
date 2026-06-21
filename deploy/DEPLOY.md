# Oracle instance deployment

This deployment path is intentionally simple:

- Docker Compose
- Postgres
- FastAPI
- Caddy
- no Ansible

It is aimed at a single Oracle instance where one person is operating the system.

## 1. Prepare the server

On a fresh Ubuntu host:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin git
sudo systemctl enable docker
sudo systemctl start docker
```

If you prefer Oracle Linux, use the equivalent `dnf` packages.

## 2. Clone the repo

Pick a home directory for the app. One reasonable layout is:

```bash
sudo mkdir -p /opt/ticker-screener
sudo chown -R $USER:$USER /opt/ticker-screener
cd /opt/ticker-screener
git clone <your-repo-url> app
cd app
```

The repository already contains the deploy files under:

- [deploy/docker-compose.yml](/Users/Zihao.Guan/Personal/ticker-screener/deploy/docker-compose.yml)
- [deploy/Caddyfile](/Users/Zihao.Guan/Personal/ticker-screener/deploy/Caddyfile)
- [deploy/.env.example](/Users/Zihao.Guan/Personal/ticker-screener/deploy/.env.example)

## 3. Prepare writable directories

```bash
mkdir -p data/postgres
mkdir -p artifacts/raw artifacts/watchlists artifacts/output
```

## 4. Create `.env`

```bash
cd deploy
cp .env.example .env
```

Edit `.env` and set:

- `APP_DOMAIN`
- `APP_FQDN`
- `REPORTS_FQDN`
- `POSTGRES_PASSWORD`
- `TICKER_SCREENER_DATABASE_URL`
- `WEBAPP_BASE_URL`
- `WEBAPP_AUTH_SECRET_KEY`
- `WEBAPP_AUTH_BOOTSTRAP_ADMIN_EMAILS`
- `WEBAPP_GOOGLE_CLIENT_ID`
- `WEBAPP_GOOGLE_CLIENT_SECRET`
- `WEBAPP_GOOGLE_REDIRECT_URI`

If you keep the default service names, the example database URL is already correct.

For auth, also configure as needed:

- `WEBAPP_GOOGLE_HOSTED_DOMAIN`
- `WEBAPP_AUTH_SESSION_COOKIE_NAME`
- `WEBAPP_AUTH_OAUTH_STATE_COOKIE_NAME`
- `WEBAPP_AUTH_SESSION_TTL_HOURS`
- `WEBAPP_AUTH_COOKIE_SECURE`
- `WEBAPP_AUTH_COOKIE_SAMESITE`

## 5. Start the database

```bash
docker-compose up -d db
docker-compose ps
```

Wait until the `db` service is healthy.

## 6. Load the Postgres schema

The app schema lives in:

- [sql/postgres_app_schema.sql](/Users/Zihao.Guan/Personal/ticker-screener/sql/postgres_app_schema.sql)

Load it with:

```bash
docker-compose exec -T db psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" < ../sql/postgres_app_schema.sql
```

## 7. Build the React frontend

The app domain serves the React build from `frontend/dist`.

Build it before bringing up the full stack:

```bash
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp/npm-home \
  -e npm_config_cache=/tmp/npm-cache \
  -v "/opt/ticker-screener/app:/app" \
  -w /app/frontend \
  node:20 \
  sh -c "mkdir -p /tmp/npm-home /tmp/npm-cache && npm install && npm run build"
```

## 8. Start the web stack

```bash
docker-compose up -d
docker-compose ps
docker-compose logs -f web
```

The web service installs Python dependencies at container start and then launches:

```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

Because the `web` container entrypoint runs `pip install -r requirements.txt -r requirements-web.txt` on every container start, `docker-compose exec -T web python ...` jobs reuse an environment that already has the repo Python requirements installed. If requirements change, restart or recreate the `web` container before running scheduled jobs.

## 9. Smoke checks

Check health:

```bash
curl -k https://app.your-domain.com/healthz
```

Open in browser:

- `https://app.your-domain.com/`
- `https://reports.your-domain.com/`

Auth smoke checks:

- open `https://app.your-domain.com/login`
- click `Sign In With Google` using one bootstrap admin email
- confirm `/api/auth/me` shows `admin`
- confirm anonymous browser session can still open watchlists and backtests
- confirm anonymous browser session gets `401` for `POST /api/runs/rs`

## 10. Run jobs manually

Until the `/runs` page launches real jobs, use `docker compose exec`:

```bash
docker-compose exec web python scripts/run_rs_screen.py --limit 25
docker-compose exec web python scripts/run_vcp_screen.py --limit 25
docker-compose exec web python scripts/run_cup_handle_screen.py --limit 25
docker-compose exec -T web python scripts/run_earnings_weekly_criteria_screen.py --reference-date 2026-06-06
docker-compose exec web python scripts/build_overlap_backtest_report.py --start-date 2024-01-01 --end-date 2026-05-01
```

Rendered and raw outputs stay under:

- `artifacts/raw`
- `artifacts/watchlists`
- `artifacts/output`

The `reports.<domain>` site in the default Caddyfile serves `artifacts/output` directly.

## 11. Optional cron on the host

For a simple single-instance setup, host cron is enough. Example:

```bash
crontab -e
```

Example entries:

```cron
0 6 * * 2-6 cd /opt/ticker-screener/app/deploy && /opt/ticker-screener/app/scripts/run_with_status.sh rs_daily "Daily RS Screen" -- docker-compose exec -T web python scripts/run_rs_screen.py
10 6 * * 2-6 cd /opt/ticker-screener/app/deploy && /opt/ticker-screener/app/scripts/run_with_status.sh vcp "VCP Screen" -- docker-compose exec -T web python scripts/run_vcp_screen.py
20 6 * * 2-6 cd /opt/ticker-screener/app/deploy && /opt/ticker-screener/app/scripts/run_with_status.sh cup_handle "Cup Handle Screen" -- docker-compose exec -T web python scripts/run_cup_handle_screen.py
35 6 * * 2-6 cd /opt/ticker-screener/app/deploy && TICKER_SCREENER_STATUS_ARTIFACT=/opt/ticker-screener/app/artifacts/output/daily_overlap_$(date +\%F).html /opt/ticker-screener/app/scripts/run_with_status.sh overlap_daily "Daily Overlap Summary" -- docker-compose exec -T web python scripts/build_daily_overlap_summary.py --date-label $(date +\%F)
0 8 * * 6 cd /opt/ticker-screener/app/deploy && TICKER_SCREENER_STATUS_ARTIFACT=/opt/ticker-screener/app/artifacts/watchlists/weekly_rs_new_high_$(date +\%F).json /opt/ticker-screener/app/scripts/run_with_status.sh weekly_rs "Weekly RS Watchlist" -- docker-compose exec -T web python scripts/run_weekly_rs_screen.py
15 8 * * 6 cd /opt/ticker-screener/app/deploy && /opt/ticker-screener/app/scripts/run_with_status.sh earnings_weekly_criteria "Earnings Weekly Criteria" -- docker-compose exec -T web python scripts/run_earnings_weekly_criteria_screen.py --reference-date $(date +\%F)
```

The wrapper script writes status JSON files under `artifacts/status/`. The Admin page reads those files and shows the latest status, timestamps, exit code, log path, and optional artifact path for each tracked cron job.

If you want to manage screener schedules from the Admin page instead of editing every cron entry by hand, use one fixed host cron that runs the scheduler every 5 minutes:

```cron
*/5 * * * * cd /opt/ticker-screener/app/deploy && /opt/ticker-screener/app/scripts/run_scheduled_jobs.py
```

The Admin page writes app-owned schedule config to `config/scheduled_jobs.json`. The scheduler script reads that file, matches cron expressions in the configured timezone, and launches the corresponding screener command through Docker Compose with the same status-file tracking used above.

`Run Earnings Weekly Criteria` is available as a schedulable Admin action. Its scheduled run uses the scheduler day as the script reference date, equivalent to running:

```bash
docker-compose exec -T web python scripts/run_earnings_weekly_criteria_screen.py --reference-date $(date +%F)
```

## Notes

- This setup is intentionally not optimized for fast image builds yet.
- The web container installs dependencies on startup for simplicity.
- One-off jobs such as `docker-compose run --rm web python3 scripts/sync_postgres_market_data.py ...` now go through the same dependency-installing entrypoint as the normal web container.
- Once the stack settles, the next improvement is a dedicated `Dockerfile` for the web and worker environments.

## GitHub Actions

This repo now includes:

- [.github/workflows/ci.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/ci.yml)
- [.github/workflows/deploy.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/deploy.yml)
- [.github/workflows/deploy-worker.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/deploy-worker.yml)

The deploy workflow supports both:

- automatic deploy after `CI` succeeds on `main` or `master`
- manual deploy through `workflow_dispatch`

The manual deploy form also supports:

- `compose_command`: override the default service restart command
- `allow_active_remote_jobs`: emergency override for the remote-job safety check

Required GitHub repository secrets:

- `ORACLE_HOST`
- `ORACLE_USER`
- `ORACLE_SSH_KEY`
- `ORACLE_PORT`
- `ORACLE_APP_DIR`

Suggested values:

- `ORACLE_PORT`: `22`
- `ORACLE_APP_DIR`: `/opt/ticker-screener/app`

The deploy workflow logs into the Oracle instance, checks out the requested git ref, then runs:

```bash
BACKUP_DIR="$(mktemp -d)"
PRESERVE_PATHS="config/smallcap_exclude_tickers.txt config/manual_exclude_tickers.txt config/manual_include_tickers.txt"
for preserve_path in ${PRESERVE_PATHS}; do
  if [ -f "${preserve_path}" ]; then
    mkdir -p "${BACKUP_DIR}/$(dirname "${preserve_path}")"
    cp "${preserve_path}" "${BACKUP_DIR}/${preserve_path}"
  fi
done
RESTORE_PATHS="frontend/package-lock.json ${PRESERVE_PATHS}"
for restore_path in ${RESTORE_PATHS}; do
  if git ls-files --error-unmatch -- "${restore_path}" >/dev/null 2>&1; then
    git restore --source=HEAD --worktree --staged -- "${restore_path}" || true
  fi
done
git clean -fd -- frontend/package-lock.json frontend/dist frontend/tsconfig.app.tsbuildinfo frontend/tsconfig.node.tsbuildinfo frontend/vite.config.js frontend/vite.config.d.ts
git pull --ff-only origin "${GIT_REF}"
for preserve_path in ${PRESERVE_PATHS}; do
  if [ -f "${BACKUP_DIR}/${preserve_path}" ]; then
    cp "${BACKUP_DIR}/${preserve_path}" "${preserve_path}"
  fi
done
docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp/npm-home -e npm_config_cache=/tmp/npm-cache -v "${APP_DIR}:/app" -w /app/frontend node:20 sh -c "mkdir -p /tmp/npm-home /tmp/npm-cache && npm ci && npm run build"
cd deploy
docker-compose up -d --no-deps web caddy
```

That default deploy path intentionally avoids restarting Postgres, which reduces the chance of interrupting remote worker jobs that depend on the master server database.

Before the frontend rebuild and compose step, the workflow now checks `job_runs` for queued or running remote jobs while the current `db` container is available. That query runs inside the `db` container, so the workflow does not need to source `deploy/.env` on the host. If any active remote jobs are found, the deploy stops by default and prints the first few matching jobs.

If you truly need to force a manual deploy anyway, set `allow_active_remote_jobs=true` in the workflow UI and rerun it.

You can still override the compose command from the workflow UI when needed. For example, if you intentionally need a broader restart, you can pass:

```bash
up -d db web caddy
```

or force a full service recreate:

```bash
up -d --force-recreate db web caddy
```

### Remote worker deploy

Use the separate worker workflow when you want to update a slave server that only runs `scripts/run_remote_worker.py`.

Required GitHub repository secrets:

- `SLAVE_HOST`
- `SLAVE_USER`
- `SLAVE_SSH_KEY`
- `SLAVE_PORT`
- `SLAVE_APP_DIR`
- `SLAVE_WORKER_SERVICE` (optional; defaults to `ticker-remote-worker`)

Suggested values:

- `SLAVE_PORT`: `22`
- `SLAVE_APP_DIR`: `/opt/ticker-screener/app`
- `SLAVE_WORKER_SERVICE`: `ticker-remote-worker`

The worker workflow:

- checks out the requested git ref on the slave server
- preserves server-local config files plus `.worker.env`
- rebuilds the Python virtualenv
- runs `npm ci` under `frontend/` so Playwright probes stay in sync
- restarts the worker service with `systemctl`

Unlike the main web deploy workflow, the worker workflow does not run `docker-compose down` or restart the web stack.

The deploy script now preserves the server-local exclusion files (`config/smallcap_exclude_tickers.txt`, `config/manual_exclude_tickers.txt`, and `config/manual_include_tickers.txt`) across `git pull`. It temporarily restores the tracked repo versions so fast-forward deploys do not fail, then copies the server-local files back before rebuild. The same script still restores `frontend/package-lock.json` from `HEAD` and runs the narrow `git clean` for frontend build artefacts, which avoids `git pull` being blocked by a mutated lockfile while still not wiping unrelated local files. The frontend container also uses `npm ci` so the checked-in lockfile is respected instead of being rewritten during deploy.

## Domain mapping

The checked-in [Caddyfile](/Users/Zihao.Guan/Personal/ticker-screener/deploy/Caddyfile) now reads its public hostnames from environment variables so the repository can stay generic.

Typical production values:

- `APP_FQDN=app.your-domain.com`
- `REPORTS_FQDN=reports.your-domain.com`
