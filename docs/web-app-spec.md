# Ticker Screener Web App Spec

## Purpose

Build a single-user or small-team web app on top of the existing `ticker-screener` repo so users can:

- run screeners from the browser
- inspect recent watchlists without SSH
- review interactive charts in the app
- browse overlap and backtest outputs
- gradually move from artifact-driven workflows to database-backed workflows

This spec is intentionally grounded in the current repo shape:

- FastAPI app shell in `web/`
- service layer in `src/webapp/`
- existing CLI screeners in `scripts/`
- artifact outputs in `artifacts/`
- deploy target on a single Oracle Ubuntu instance

## Product Goals

### Primary goals

1. Replace routine SSH usage for common screening tasks.
2. Make daily screening results visible from a browser within minutes of a run.
3. Provide a strong single-ticker analysis experience with interactive charts.
4. Keep compatibility with existing CLI workflows and artifact outputs.
5. Leave room to migrate data reads from external live fetches to the local database.

### Non-goals for the first major version

1. Multi-tenant SaaS behavior.
2. Fine-grained user permissions.
3. Real-time streaming market data.
4. Full TradingView-style drawing tools.
5. Replacing every existing render/publish job on day one.

## Users

### Primary user

- the repo owner running daily and historical screeners

### Secondary users

- a small set of trusted collaborators reviewing watchlists and reports

## Core user journeys

### 1. Run a screener from the browser

User opens `/runs`, chooses a screener, optionally enters a universe limit, clicks run, and watches status move from `running` to `success` or `failed`.

### 2. Review the newest watchlists

User opens `/watchlists`, selects a watchlist, reviews entries, and clicks different tickers to inspect interactive charts.

### 3. Review overlap candidates

User opens a dedicated overlap area, sees the latest overlap summary, and drills into names that appear across multiple pipelines.

### 4. Review backtests

User opens `/backtests`, launches or opens an overlap backtest report, and reviews summary metrics and result files.

### 5. Inspect and manage the operating state

User opens dashboard/admin pages to understand:

- last successful runs
- failures
- exclusion lists
- artifact freshness
- data sync freshness

## Functional requirements

## A. Dashboard

### Required

1. Show recent screening activity.
2. Show recent watchlists.
3. Show key strategy cards:
   - RS
   - VCP
   - Cup Handle
   - Overlap
4. Show health indicators:
   - web app healthy
   - database configured
   - artifacts directory reachable

### Nice to have

1. Last successful run timestamp per strategy.
2. Hit counts for the latest run.
3. Quick links to latest watchlists and reports.

## B. Screener runs

### Required

1. Support browser-triggered runs for:
   - RS
   - VCP
   - Cup Handle
2. Show:
   - status
   - command
   - start time
   - finish time
   - return code
   - recent log tail
3. Prevent malformed input for numeric fields like `limit`.

### Next phase

1. Add browser-triggered runs for:
   - Weekly HTF Pullback
   - HTF 8W Runup
   - Gap Fill
   - PEG variants
   - Overlap summary
   - Overlap backtest

### Future

1. Replace in-memory job tracking with database-backed job tracking.
2. Support queued execution instead of only in-process background threads.

## C. Watchlists

### Required

1. List recent watchlist JSON artifacts.
2. Open a watchlist detail page by file stem.
3. Show up to a sensible capped number of entries on page load.
4. For each entry, show core fields such as:
   - ticker
   - setup label
   - summary
   - event date
   - trigger/entry/stop where present

### Interactive chart requirement

The watchlist detail page must support in-app interactive charts using TradingView Lightweight Charts.

Required chart capabilities:

1. Candlestick chart.
2. Volume histogram.
3. MA20 / MA50 / MA200 overlays.
4. Switching chart ticker by clicking watchlist rows.
5. Empty state when no chart data is available.

Next-phase chart capabilities:

1. Signal marker for screener event date.
2. Trigger marker.
3. Entry marker.
4. Stop marker.
5. Earnings marker where relevant.

### Important product choice

The web app does not need pre-rendered SVG charts for ticker detail viewing.

Implications:

1. Ubuntu instance should not need to persist SVG artifacts for normal web watchlist inspection.
2. Existing render jobs become optional for archival, publishing, or static sharing workflows.

## D. Overlap

### Required

1. Show the latest available overlap summary output.
2. Show the pipelines currently participating in overlap:
   - RS
   - Sean PEG
   - Legacy PEG
   - VCP
   - Cup Handle
   - Weekly HTF Pullback
   - HTF 8W Runup
   - Gap Fill
3. Show counts such as:
   - unique ticker count
   - overlap >= 2
   - overlap >= 3
4. Allow drill-in from overlap candidate to ticker chart.

### Data note

Of the current overlap-member screeners, only the PEG variants depend on earnings-oriented logic. The other overlap members are technical/price-volume driven.

## E. Backtests

### Required

1. A page listing backtest templates and/or recent backtest outputs.
2. A way to launch the overlap count backtest from the UI.
3. Links to generated report artifacts.

### Next phase

1. Persist backtest runs in database tables.
2. Show summary metrics in the UI:
   - date range
   - total signals
   - median return
   - win rate
   - best/worst period

## F. Admin

### Required

1. Show the exclusion list.
2. Show exclusion count.
3. Keep the view read-only for the first version if editing is not yet implemented.

### Next phase

1. Add add/remove exclusion operations in the browser.
2. Persist changes safely back to the repo-managed config surface.

## G. Data access

### Current state

1. Watchlists come from `artifacts/watchlists/*.json`.
2. Charts currently fetch market data live with `yfinance`.
3. Job status is in memory only.

### Target state

The app should evolve toward a database-backed model:

1. `daily_bars`
2. `ticker_metadata`
3. `earnings_events`
4. `job_runs`
5. `screen_runs`
6. `backtest_runs`
7. `report_artifacts`

### Data rules

1. Non-PEG technical screeners should run from `daily_bars + ticker_metadata`.
2. PEG variants should additionally use `earnings_events` when migrated fully off external live logic.
3. Interactive chart endpoints should prefer database reads over live `yfinance` fetches once historical data is backfilled.

## API requirements

## Existing routes

The app currently exposes:

- `/`
- `/runs`
- `/watchlists`
- `/watchlists/{stem}`
- `/watchlists/api/chart/{ticker}`
- `/backtests`
- `/admin/exclusions`
- `/healthz`

## Required new or expanded routes

### HTML routes

1. `/overlap`
2. `/overlap/{date_label}` or equivalent detail route
3. `/runs/{job_id}` optional dedicated job detail page

### JSON routes

1. `GET /api/jobs`
2. `GET /api/jobs/{job_id}`
3. `POST /api/runs/{action_id}`
4. `GET /api/watchlists/{stem}`
5. `GET /api/overlap/latest`
6. `GET /api/overlap/{date_label}`

## UI requirements

The long-term UI direction may move from the current Jinja templates to a React frontend. A migration scaffold and plan now live in:

- [docs/react-migration-plan.md](/Users/Zihao.Guan/Personal/ticker-screener/docs/react-migration-plan.md)
- [frontend/](/Users/Zihao.Guan/Personal/ticker-screener/frontend)

## Layout

1. Quiet, work-focused layout.
2. Left nav or top nav with stable sections:
   - Dashboard
   - Runs
   - Watchlists
   - Overlap
   - Backtests
   - Admin
3. Responsive design for laptop-first use.

## Watchlist detail layout

1. Chart should be the primary element on the page.
2. Entry table/list should sit below or beside the chart depending on width.
3. Clicking a ticker should update the chart without leaving the page.

## Runs page layout

1. Action buttons/forms at top.
2. Recent jobs below.
3. Clear status styling:
   - running
   - success
   - failed

## Non-functional requirements

## Reliability

1. Web UI should survive failed jobs without crashing.
2. Route handlers should return friendly empty states instead of raw tracebacks.
3. Browser-triggered runs should not block request threads.

## Deployability

1. The app must remain deployable on the current single Oracle Ubuntu instance.
2. It must work under the current Docker Compose deployment model.
3. It must remain compatible with CI-pass -> auto-deploy GitHub Actions flow.

## Observability

1. `/healthz` must remain cheap and stable.
2. Job failures should expose enough log tail for diagnosis.
3. Deploy workflow should eventually validate post-deploy health.

## Security

1. Do not expose secrets in templates or client-side JS.
2. Restrict browser-triggered commands to a curated allowlist of screeners/backtests.
3. Reject arbitrary shell execution from the UI.

## Performance

1. Watchlist detail should load quickly for ordinary watchlists.
2. Chart API should cap or paginate if symbol history becomes too heavy.
3. Recent lists should use reasonable limits to avoid loading all artifacts on each request.

## Storage and artifact policy

## Required

1. Keep JSON watchlists and raw outputs as first-class artifacts.
2. Keep rendered static outputs optional rather than mandatory for day-to-day UI operation.

## Optional

1. Static HTML and SVG render outputs may still be generated for:
   - archival
   - publishing
   - external sharing

## Explicit policy

The web app should not require storing SVG chart assets on the Ubuntu instance for routine usage.

## Phased implementation plan

## Phase 1: Shell and basic operations

Already mostly present:

1. FastAPI app shell
2. Dashboard
3. Watchlist browser
4. Interactive ticker chart
5. Manual run buttons for a subset of screeners

## Phase 2: Operational usefulness

1. Add overlap page.
2. Add job polling or auto-refresh.
3. Add persistent job tracking in Postgres.
4. Add browser-triggered overlap summary and overlap backtest.
5. Show links from runs to produced watchlists/reports.

## Phase 3: Database-first data layer

1. Backfill `daily_bars` from 2020 to present.
2. Move chart API to read from database first.
3. Move non-PEG screeners to database-backed reads.
4. Add `earnings_events` and migrate PEG toward a more self-owned data path.

## Phase 4: Research platform mode

1. Add backtest result visualizations.
2. Add richer ticker annotations.
3. Add overlap drill-down workflows.
4. Add artifact/report registry pages from `report_artifacts`.

## Acceptance criteria

The app can be considered to meet the intended first usable version when:

1. A user can log into the site and open it over HTTPS on the Oracle instance.
2. A user can run RS/VCP/Cup Handle from `/runs`.
3. A user can open a watchlist and inspect tickers with interactive charts.
4. A user can review the latest overlap output without SSH access.
5. The app works without requiring pre-generated SVG charts.
6. CI can pass and deploy can automatically roll the app to the server.

## Open decisions

1. Whether authentication is required for public access or only for private operation.
2. Whether overlap should be a dedicated top-level page or embedded into dashboard first.
3. Whether static render workflows remain on a schedule or become manual/archive-only.
4. When to replace live `yfinance` chart fetches with database-backed reads.
