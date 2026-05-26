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
- `POSTGRES_PASSWORD`
- `TICKER_SCREENER_DATABASE_URL`

If you keep the default service names, the example database URL is already correct.

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

## 7. Start the web stack

```bash
docker-compose up -d
docker-compose ps
docker-compose logs -f web
```

The web service installs Python dependencies at container start and then launches:

```bash
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

## 8. Smoke checks

Check health:

```bash
curl http://localhost/healthz
```

Open in browser:

- `http://<server-ip>/`
- or your configured domain once DNS points at the instance

## 9. Run jobs manually

Until the `/runs` page launches real jobs, use `docker compose exec`:

```bash
docker-compose exec web python scripts/run_rs_screen.py --limit 25
docker-compose exec web python scripts/run_vcp_screen.py --limit 25
docker-compose exec web python scripts/run_cup_handle_screen.py --limit 25
docker-compose exec web python scripts/build_overlap_backtest_report.py --start-date 2024-01-01 --end-date 2026-05-01
```

Rendered and raw outputs stay under:

- `artifacts/raw`
- `artifacts/watchlists`
- `artifacts/output`

The `reports.<domain>` site in the default Caddyfile serves `artifacts/output` directly.

## 10. Optional cron on the host

For a simple single-instance setup, host cron is enough. Example:

```bash
crontab -e
```

Example entries:

```cron
0 6 * * 2-6 cd /opt/ticker-screener/app/deploy && docker-compose exec -T web python scripts/run_rs_screen.py
10 6 * * 2-6 cd /opt/ticker-screener/app/deploy && docker-compose exec -T web python scripts/run_vcp_screen.py
20 6 * * 2-6 cd /opt/ticker-screener/app/deploy && docker-compose exec -T web python scripts/run_cup_handle_screen.py
35 6 * * 2-6 cd /opt/ticker-screener/app/deploy && docker-compose exec -T web python scripts/build_daily_overlap_summary.py --date-label $(date +\%F)
```

## Notes

- This setup is intentionally not optimized for fast image builds yet.
- The web container installs dependencies on startup for simplicity.
- Once the stack settles, the next improvement is a dedicated `Dockerfile` for the web and worker environments.

## GitHub Actions

This repo now includes:

- [.github/workflows/ci.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/ci.yml)
- [.github/workflows/deploy.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/deploy.yml)

The deploy workflow is intentionally manual through `workflow_dispatch`.

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
cd deploy
docker-compose up -d --remove-orphans
```

You can override that compose command from the workflow UI when needed.
