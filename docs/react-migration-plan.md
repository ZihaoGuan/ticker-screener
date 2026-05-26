# React Migration Plan

## Goal

Prepare the current FastAPI + Jinja web app to migrate toward a React frontend that follows the Stitch dashboard design language in:

- `/Users/Zihao.Guan/Downloads/stitch_ticker_screener_web_dashboard`

This is a preparation phase, not a full cutover. The intent is to:

1. establish the React application shell
2. mirror the target information architecture
3. define the backend API surface needed for migration
4. keep the existing FastAPI app usable during transition

## Design references used

The migration scaffold was aligned to the following design references:

- dashboard
- screener runs
- watchlists
- overlap summary
- admin exclusions
- backtests

Shared visual guidance came from:

- `/Users/Zihao.Guan/Downloads/stitch_ticker_screener_web_dashboard/precision_utility/DESIGN.md`

## What was added

A new React frontend scaffold now lives under:

- [frontend/package.json](/Users/Zihao.Guan/Personal/ticker-screener/frontend/package.json)
- [frontend/src/App.tsx](/Users/Zihao.Guan/Personal/ticker-screener/frontend/src/App.tsx)
- [frontend/src/styles.css](/Users/Zihao.Guan/Personal/ticker-screener/frontend/src/styles.css)

Initial React pages were created for:

- Dashboard
- Runs
- Watchlists
- Overlap
- Backtests
- Admin

The watchlists page includes a React-side `lightweight-charts` component and now reads chart data from FastAPI JSON endpoints.

## Migration strategy

## Phase 1: parallel frontend

Keep the current FastAPI/Jinja app as the stable operator surface.

Add React as a parallel frontend for iterative development:

- FastAPI still owns API and can keep legacy HTML pages during transition
- React runs separately during development
- React uses real API calls for dashboard, runs, watchlists, overlap, backtests, and admin

This is the current recommended mode.

## Phase 2: API-first backend

Promote the backend toward JSON endpoints for React consumption.

### Needed backend APIs

#### Dashboard

- `GET /api/dashboard`

#### Runs

- `GET /api/jobs`
- `GET /api/jobs/{job_id}`
- `POST /api/runs/{action_id}`

#### Watchlists

- `GET /api/watchlists`
- `GET /api/watchlists/{stem}`
- `GET /api/watchlists/{stem}/chart/{ticker}`

#### Overlap

- `GET /api/overlap/latest`
- `GET /api/overlap/{date_label}`

#### Backtests

- `GET /api/backtests`
- `POST /api/backtests/{template_id}`

#### Admin

- `GET /api/exclusions`

## Phase 3: route handoff

Once the React routes are feature-complete:

1. build the React app into static assets
2. serve the React build from Caddy
3. keep FastAPI focused on `/api/*` and `/healthz`
4. turn old Jinja pages into:
   - redirects
   - fallback admin/operator pages
   - or remove them after confidence is high

## Recommended technical architecture

## Frontend

- React
- TypeScript
- Vite
- React Router
- TradingView Lightweight Charts

## Backend

- FastAPI remains the API and orchestration layer
- existing Python service layer remains the business logic source

## Data

- short term: artifacts + live fetch fallback
- medium term: Postgres-backed market data and job state

## Mapping from current app to React

## Dashboard

Current:

- Jinja page from `web/templates/dashboard.html`

React target:

- strategy KPI cards
- recent job table
- recent watchlist panel
- persistent left navigation

## Runs

Current:

- server-rendered form + in-memory job cards

React target:

- denser run trigger toolbar
- jobs table
- console tail pane
- polling refresh

## Watchlists

Current:

- watchlist list page
- watchlist detail page with inline JS chart

React target:

- JSON file list pane
- ticker list pane
- richer symbol header
- React `lightweight-charts` component
- lower tabbed research panels

## Overlap

Current:

- output exists as artifacts
- no first-class app page yet

React target:

- top-level page
- summary cards
- candidate table
- drill-in path to chart/research

## Backtests

Current:

- placeholder template page

React target:

- backtest template launcher
- result list
- summary stats panels

## Admin

Current:

- read-only exclusions page

React target:

- exclusions grid
- future edit actions

## Current limitations

The migration is now partially wired:

1. React pages consume FastAPI JSON APIs
2. production deploy builds `frontend/` and serves `frontend/dist` through Caddy
3. FastAPI remains the backend for `/api/*` and `/healthz`

There are still open pieces:

1. local build validation has not been run in this environment
2. some legacy Jinja routes still exist as fallback surfaces
3. runs use in-memory job state rather than Postgres persistence

## Next recommended implementation order

1. validate the React build in CI
2. add deploy-time health checks for `https://app.<domain>/healthz`
3. persist run history into Postgres instead of process memory
4. decide whether to keep or retire legacy Jinja routes

## Production cutover recommendation

When ready, prefer:

1. build React into static assets
2. serve those assets from Caddy
3. keep FastAPI as the only backend/API process

Avoid introducing a second frontend server in production unless there is a strong reason.
