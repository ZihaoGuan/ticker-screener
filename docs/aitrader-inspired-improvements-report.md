# `ticker-screener` Improvement Report

## Goal

Learn from `aitrader` and identify what `ticker-screener` can improve while:

- keeping the product practical for daily use
- continuing to support free data sources and free-ish implementations
- staying compatible with the current script-first workflow

## Executive summary

`ticker-screener` already has more product surface area than `aitrader`: more screeners, a real web app, more routes, more watchlist/chart experiences, and a large test suite. The main gap is not raw capability. The gap is operating model.

`aitrader` is better at:

- defining a canonical source of truth
- making data-budget tradeoffs explicit
- documenting "what path should I use?"
- packaging workflows instead of only features
- generating docs/catalogs from structured metadata

The biggest opportunity is to make `ticker-screener` feel like a coherent screening platform instead of a growing pile of powerful scripts.

## What `aitrader` does better

### 1. Clear canonical metadata

`aitrader` explicitly treats `skills-index.yaml` and `workflows/*.yaml` as canonical sources of truth, and generates docs from them.

Relevant references:

- `aitrader/README.md:53`
- `aitrader/README.md:65`
- `aitrader/README.md:116`
- `aitrader/workflows/README.md`

### 2. Clear "no paid API" path

`aitrader` has a dedicated "No API Key Starter Path" and clearly explains what works without paid providers.

Relevant references:

- `aitrader/README.md:53`
- `aitrader/README.md:63`

### 3. Workflow-first thinking

`aitrader` organizes the product around repeatable workflows with cadence, inputs, outputs, and decision gates instead of just individual tools.

Relevant references:

- `aitrader/workflows/README.md`

### 4. Human trust and explicit boundaries

`aitrader` is strong at saying what the system is and is not for. That makes the product easier to trust and easier to extend safely.

Relevant references:

- `aitrader/README.md:13`

## What `ticker-screener` already does well

- Strong breadth of screeners and strategy variants.
- Good migration path from scripts to web app.
- Real application structure already exists across `web/`, `frontend/`, and `src/webapp/`.
- Good artifact discipline and database migration direction.
- Plenty of tests across screening logic and app services.

This matters because the recommendation is not "rebuild it." The recommendation is "organize and simplify what already exists."

## Main issues in `ticker-screener`

### 1. No single canonical screener registry

There is useful metadata in code, but it is split across multiple places:

- runner/action definitions in `src/webapp/services/run_service.py`
- screener logic in `src/screener_catalog.py`
- artifact naming in `src/artifact_paths.py`
- docs in `README.md` and `docs/`
- UI assumptions in frontend pages

That makes adding or changing a screener more expensive than it should be.

Relevant references:

- `ticker-screener/src/webapp/services/run_service.py:31`
- `ticker-screener/src/webapp/services/run_service.py:53`
- `ticker-screener/src/screener_catalog.py`
- `ticker-screener/docs/web-app.md:75`

### 2. Provider strategy is powerful but inconsistent

The repo currently mixes multiple provider paths:

- `yfinance`
- Yahoo page scraping
- Yahoo Playwright probes
- Finviz scraping/APIs
- optional `FMP`
- optional `AInvest`
- optional `OpenBB`
- optional `AKShare`

That flexibility is useful, but it is not presented as an explicit operating model. Users and future maintainers have to infer the fallback graph.

Relevant references:

- `ticker-screener/README.md:202`
- `ticker-screener/README.md:203`
- `ticker-screener/README.md:204`
- `ticker-screener/README.md:205`
- `ticker-screener/README.md:259`
- `ticker-screener/docs/web-app-spec.md:236`

### 3. A few services have become "god objects"

Two files stand out:

- `src/webapp/services/watchlist_service.py` at about 3,037 lines
- `src/webapp/services/run_service.py` at about 2,786 lines

`watchlist_service.py` now owns too many responsibilities at once:

- chart payload assembly
- screen board config
- caching
- Yahoo scraping
- Playwright fallback
- GEX/chart enrichment
- insider data handling
- ETF/theme enrichment

Relevant references:

- `ticker-screener/src/webapp/services/watchlist_service.py:19`
- `ticker-screener/src/webapp/services/watchlist_service.py:20`
- `ticker-screener/src/webapp/services/watchlist_service.py:71`
- `ticker-screener/src/webapp/services/watchlist_service.py:88`
- `ticker-screener/src/webapp/services/watchlist_service.py:98`
- `ticker-screener/src/webapp/services/watchlist_service.py:2205`
- `ticker-screener/src/webapp/services/watchlist_service.py:2386`
- `ticker-screener/src/webapp/services/watchlist_service.py:2516`

`run_service.py` now owns too many responsibilities too:

- UI form metadata
- action registry
- local execution
- remote queue rules
- progress parsing
- artifact discovery

Relevant references:

- `ticker-screener/src/webapp/services/run_service.py:31`
- `ticker-screener/src/webapp/services/run_service.py:44`
- `ticker-screener/src/webapp/services/run_service.py:53`
- `ticker-screener/src/webapp/services/run_service.py:55`

### 4. The web app contract is still thinner than the product complexity

The frontend API client is extremely small, which is fine for an MVP, but the product has outgrown that level of abstraction.

Relevant references:

- `ticker-screener/frontend/src/lib/api.ts:1`
- `ticker-screener/frontend/src/lib/api.ts:11`

### 5. The product is feature-first, not workflow-first

The docs describe routes and pages well, but there is less emphasis on repeatable user journeys such as:

- daily leadership review
- earnings prep review
- overlap drilldown
- post-run validation
- data-freshness review

Relevant references:

- `ticker-screener/docs/web-app-spec.md`
- `ticker-screener/docs/web-app.md:3`

## Recommended improvements

## Priority 0: Establish structure before adding more features

### 1. Add a canonical screener registry

Create one metadata file such as:

- `config/screener_registry.yaml`

Each screener entry should define:

- `id`
- `label`
- `category`
- `timeframe`
- `cadence`
- `api_profile`
- `required_inputs`
- `optional_inputs`
- `providers`
- `artifacts`
- `script_entrypoint`
- `web_run_enabled`
- `scanner_board_enabled`
- `bias_group`
- `chart_support`
- `notes`

Suggested `api_profile` values:

- `free-db-first`
- `free-live`
- `free-scrape-fallback`
- `optional-premium`

Why this helps:

- one place to understand the system
- one place to drive docs
- one place to drive run forms
- one place to drive UI labels and capabilities
- one place to express provider and artifact contracts

This would be the closest `ticker-screener` equivalent to `aitrader`'s `skills-index.yaml`.

### 2. Create an explicit free-data operating model

Document and enforce a default free stack like this:

#### Default free stack

- Price/history: Postgres cache first, then `yfinance`
- Earnings dates/basic earnings context: Yahoo or `yfinance`
- Holders/basic stats: Yahoo scrape or Playwright fallback only when needed
- Insider activity: existing Finviz path
- Ratings/fundamental overlays: existing Finviz-based path where already implemented

#### Optional enhanced stack

- `FMP`
- `OpenBB`
- `AInvest`
- `AKShare`

The key change is not just documentation. The code should return source metadata with each payload:

- `source`
- `fallback_used`
- `fetched_at`
- `stale`
- `degraded`
- `warnings`

This makes the app much easier to trust when free providers are flaky.

### 3. Split `watchlist_service.py`

Suggested extraction:

- `chart_data_service.py`
- `chart_overlay_service.py`
- `scanner_board_service.py`
- `fundamentals_enrichment_service.py`
- `yahoo_probe_service.py`
- `watchlist_cache_service.py`
- `sector_momentum_service.py`

Rule of thumb:

- if a module talks to Yahoo, it should not also define scanner board cards
- if a module defines cache TTLs, it should not also own HTML/business presentation logic

This is the single highest code-quality improvement in the repo.

### 4. Split `run_service.py`

Suggested extraction:

- `run_action_catalog.py`
- `job_execution_service.py`
- `job_progress_parser.py`
- `job_artifact_locator.py`
- `remote_queue_service.py`
- `run_form_schema.py`

This will make browser-run support much easier to extend safely.

## Priority 1: Make the product feel guided

### 5. Add workflow manifests inspired by `aitrader`

Create a small `workflows/` directory for user journeys, for example:

- `daily-leadership-review.yaml`
- `earnings-prep-review.yaml`
- `overlap-drilldown.yaml`
- `weekend-watchlist-build.yaml`

Each workflow should define:

- estimated minutes
- recommended screeners
- required data freshness
- outputs to inspect
- manual review checklist
- stop/go decision points

This would help turn the app from "many buttons" into "repeatable routines."

### 6. Generate docs and UI labels from metadata

Once the registry exists:

- generate screener catalog docs
- generate API profile docs
- generate operator docs for free-only mode
- generate frontend labels/help text from the same source

This is another place where `aitrader` is ahead today.

### 7. Add run manifests for observability

Every screener run should write a small machine-readable manifest, for example:

- `manifest.json`

Fields:

- screener id
- run timestamp
- provider set used
- fallback count
- stale fields count
- ticker universe size
- pass count
- excluded count
- failed count
- output artifact paths

This would make debugging and support much easier.

## Priority 2: Improve the app contract

### 8. Expand the frontend API layer

Right now `frontend/src/lib/api.ts` is only a tiny `fetchJson` helper. That is too small for the number of app surfaces now present.

Suggested shape:

- `frontend/src/lib/api/client.ts`
- `frontend/src/lib/api/dashboard.ts`
- `frontend/src/lib/api/runs.ts`
- `frontend/src/lib/api/watchlists.ts`
- `frontend/src/lib/api/charts.ts`
- `frontend/src/lib/api/earnings.ts`

Return typed DTOs and normalize date/time/source fields in one place.

### 9. Surface provider quality in the UI

For chart and watchlist detail pages, show badges like:

- `DB`
- `Yahoo`
- `Finviz`
- `Fallback`
- `Stale`

This is especially important when free data sources are involved. Free is fine, but silent degradation is not.

### 10. Add a data-freshness dashboard

Add a simple operator-facing card set for:

- last `daily_bars` sync
- last successful screener per strategy
- last successful earnings refresh
- last successful insider refresh
- percent of latest watchlist enriched from cache vs live fallback

This fits the existing product direction very well.

## Suggested free-first implementation direction

If the goal is to keep using free sources as much as possible, I would standardize on this order:

1. Local Postgres cache for price/history and derived indicators.
2. `yfinance` for non-critical live fill-ins.
3. Yahoo HTML or Playwright fallback only for fields not available elsewhere.
4. Finviz scraping or existing Finviz adapters for ratings/insider workflows already built around that source.
5. Optional premium providers only as opt-in enhancements, not as hidden assumptions.

That gives you a stable story:

- fast when cache is warm
- cheap to operate
- resilient when one provider gets flaky
- understandable to users

## Practical roadmap

### Phase 1

- Add `config/screener_registry.yaml`
- Refactor code to read screener metadata from it
- Document `api_profile` and provider rules

### Phase 2

- Split `watchlist_service.py`
- Split `run_service.py`
- Add per-run `manifest.json`

### Phase 3

- Add `workflows/` manifests
- Generate catalog docs from metadata
- Add freshness/provider badges in the UI

### Phase 4

- Expand typed frontend API modules
- Add operator dashboard for data freshness and degraded mode

## Highest-value changes

If only a few things should happen next, I would do these first:

1. Add a canonical screener registry.
2. Split `watchlist_service.py`.
3. Split `run_service.py`.
4. Define and document a strict free-data provider order.
5. Add workflow manifests for the top 3 user journeys.

## Bottom line

`aitrader` is not better because it has more features. It is better at making a trading toolkit legible.

For `ticker-screener`, the next level is not "more screeners." The next level is:

- one source of truth
- one free-data operating model
- smaller services
- workflow-guided UX
- better visibility into freshness, fallback, and trust

That would make the web app easier to maintain, easier to trust, and much easier to keep growing without turning every new screener into more hidden complexity.
