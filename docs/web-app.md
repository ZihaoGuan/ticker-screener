# Web app MVP plan

This repo can grow into a single-process Python web app without throwing away the current script workflows.

## Recommended stack

- FastAPI for web routes and JSON APIs
- Jinja templates for the first UI pass
- Postgres for application state and market data
- Existing `artifacts/` output remains the report handoff surface

## Schema

The initial Postgres schema lives in:

- [sql/postgres_app_schema.sql](/Users/Zihao.Guan/Personal/ticker-screener/sql/postgres_app_schema.sql)

It covers two layers:

1. market data
   - `ticker_metadata`
   - `daily_bars`
   - `earnings_events`
2. app state
   - `job_runs`
   - `screen_runs`
   - `backtest_runs`
   - `report_artifacts`

## App shell

The first app shell lives in:

- [web/app.py](/Users/Zihao.Guan/Personal/ticker-screener/web/app.py)

Routes included:

- `/`
- `/runs`
- `/watchlists`
- `/watchlists/{stem}`
- `/backtests`
- `/admin/exclusions`
- `/healthz`

The current shell is intentionally thin:

- dashboard reads local artifact metadata
- watchlists load existing watchlist JSON files
- admin exclusions reuses the current exclusion-file loader
- backtests and runs pages are placeholders for job-launch UI

## Local dev

Install the optional web dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements-web.txt
```

Run the app:

```bash
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```

## Migration path

The intended next steps are:

1. move CLI business logic behind service-layer functions
2. add real Postgres repositories for `job_runs`, `screen_runs`, and `backtest_runs`
3. let `/runs` submit jobs instead of only showing commands
4. link rendered reports and raw artifacts directly from the watchlist detail pages
