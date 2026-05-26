# ticker-screener

Focused chart-screen workflows for `RS new high before price`, `VCP`, `Cup & Handle`, and `power earnings gap`.

The repo also keeps a project-level small-cap exclusion list at [smallcap_exclude_tickers.txt](/Users/Zihao.Guan/Personal/ticker-screener/config/smallcap_exclude_tickers.txt). All screener workflows exclude those tickers by default.
That file is refreshed by the weekly workflow in [refresh-smallcap-exclusion.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/refresh-smallcap-exclusion.yml), which pulls the latest list from `https://earnings.beavern.com/ics/smallcap.ics` and commits changes back to the repo.
You can also manually exclude tickers by editing [manual_exclude_tickers.txt](/Users/Zihao.Guan/Personal/ticker-screener/config/manual_exclude_tickers.txt). All screener workflows merge that file with the small-cap exclusion list.

This project keeps the responsibilities narrow:

- screen the US equity universe for `RS new high before price`
- screen the configured exchange universe for `VCP`
- screen the configured exchange universe for `Cup & Handle`
- screen the configured exchange universe for `power earnings gap`
- emit raw screen results plus a renderer-ready watchlist JSON
- render daily setup charts from that watchlist

The RS engine is vendored from `cookstock` so GitHub Actions can run without depending on a sibling local repository. The chart renderer is vendored from the `trade-master-signals` skill so the output format stays aligned with your current chart workflow.

## Project layout

- `src/`: config, universe loading, RS screening, and watchlist building
- `scripts/run_rs_screen.py`: produces raw results and watchlist JSON
- `scripts/run_weekly_rs_screen.py`: produces weekly RS raw results and watchlist JSON
- `scripts/run_weekly_htf_pullback_screen.py`: produces weekly RS + HTF 8-week pullback raw results and watchlist JSON
- `scripts/run_htf_runup_screen.py`: produces raw results and watchlist JSON for tickers up 100% or more inside the last 8 weeks
- `scripts/run_gap_fill_screen.py`: produces raw results and watchlist JSON for potential overhead gap-fill setups
- `scripts/run_pre_earnings_ma_stack_screen.py`: produces raw results and watchlist JSON for next-week earnings names with `MA20 > MA50 > MA200`
- `scripts/build_pre_earnings_calendar_index.py`: rewrites the pre-earnings MA stack report index with a next-week earnings calendar table linked to each chart
- `scripts/build_candidate_etf_table.py`: builds a local table mapping candidate tickers to sector-matched or theme-matched ETFs from the maintained ETF catalog
- `scripts/build_universe_etf_match_db.py`: builds a local SQLite database for full-universe ticker metadata plus ETF matches derived from the maintained ETF catalog and ticker theme overrides
- `scripts/filter_low_market_cap_hits.py`: checks passed tickers with `yfinance`, removes names under `$1B` market cap from current artifacts, and writes shared auto excludes to `config/auto_exclude_tickers/shared-low-cap.txt`
- `scripts/run_vcp_screen.py`: produces VCP raw results and watchlist JSON
- `scripts/run_peg_screen.py`: produces PEG raw results and watchlist JSON
- `scripts/run_cup_handle_screen.py`: produces Cup & Handle raw results and watchlist JSON
- `scripts/render_rs_watchlist.py`: renders charts from a watchlist JSON
- `scripts/render_sector_rotation_rrg.py`: renders an RRG-style sector or industry rotation map
- `vendor/cookstock/`: vendored RS engine dependencies
- `vendor/trade_master_signals/`: vendored chart renderer
- `artifacts/`: local outputs and CI handoff artifacts

## Local usage

Install dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements.txt
```

For Oracle NoSQL export or direct sync support, install the optional SDK dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements-oracle-nosql.txt
```

For the web app shell and Postgres-backed app layer, install the optional web dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements-web.txt
```

Seed or incrementally sync Postgres market data for screener use:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_postgres_market_data.py \
  --ensure-schema \
  --start-date 2020-01-01 \
  --end-date 2026-05-26
```

Run a smaller smoke sync:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_postgres_market_data.py \
  --ensure-schema \
  --tickers AAPL MSFT NVDA \
  --start-date 2024-01-01 \
  --end-date 2024-03-31
```

For the next-week earnings growth screener, install the extra provider dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements-earnings-growth.txt
```

Run the full RS screen:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_rs_screen.py
```

Run the weekly RS new-high screen:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_weekly_rs_screen.py

python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_weekly_htf_pullback_screen.py

python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_gap_fill_screen.py
```

Run a smaller smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_rs_screen.py --limit 25
```

Run a weekly RS smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_weekly_rs_screen.py --limit 25

python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_weekly_htf_pullback_screen.py --limit 25

python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_gap_fill_screen.py --limit 25
```

Run the legacy PEG screener across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py
```

Run the Sean-style post-earnings-gap workflow across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --strategy-profile sean-peg
```

Run the VCP screener across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_vcp_screen.py
```

Run the Cup & Handle screener across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_cup_handle_screen.py
```

Run a VCP smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_vcp_screen.py --limit 25
```

Run a Cup & Handle smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_cup_handle_screen.py --limit 25
```

Run a Sean PEG smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --strategy-profile sean-peg --limit 10
```

Run the PEG screener against only the next-week earnings watchlist:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --source earnings-watchlist
```

Run the next-week earnings growth screener:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_earnings_growth_screen.py
```

This workflow filters next-week earnings candidates for:

- at least 2 of the last 4 earnings reactions above 7%
- latest quarterly revenue YoY above 100%
- latest quarterly revenue above 50M
- latest EPS still negative but improving over 3 quarters
- institutional ownership above 10%
- `close > ma20 > ma50 > ma200`

Provider priority for this workflow is currently:

- `OpenBB` as the primary earnings history and calendar source when the package is available
- `AInvest` as the next earnings-history fallback when `AINVEST_API_KEY` is present
- `yfinance` as the default financials / institutional ownership source and final earnings fallback
- `AKShare` as an additional fallback for quarterly revenue history when `yfinance` comes up empty

Run the next-week earnings MA stack screener:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_pre_earnings_ma_stack_screen.py
```

This workflow filters next-week earnings candidates for:

- next-week earnings
- `MA20 > MA50 > MA200`
- shared post-screen market-cap filter above `$1B`

Build a SQLite ETF-match database for a small smoke subset:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/build_universe_etf_match_db.py \
  --tickers MRVL VSAT MASI SVM
```

Build the full-universe SQLite ETF-match database:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/build_universe_etf_match_db.py
```

The generated database defaults to:

- `data/universe_etf_matches.sqlite`

The SQLite schema lives in:

- `sql/universe_etf_match_schema.sql`

Render charts from the generated watchlist:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/render_rs_watchlist.py \
  --watchlist-file /Users/Zihao.Guan/Personal/ticker-screener/artifacts/watchlists/rs_new_high_before_price_YYYY-MM-DD.json
```

Enrich an existing watchlist or raw screen JSON with earnings date, recent beat/miss status, and next-two-weeks earnings context:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/enrich_with_earnings.py \
  --input-file /Users/Zihao.Guan/Personal/ticker-screener/artifacts/watchlists/rs_new_high_before_price_YYYY-MM-DD.json
```

The enricher supports either:

- a watchlist JSON array in `artifacts/watchlists/*.json`
- a raw screen payload with `hits[]` in `artifacts/raw/*.json`

Supported providers are `fmp`, `ainvest`, `yfinance`, and `auto`. The default provider is `yfinance`. `auto` prefers `AInvest` when `AINVEST_API_KEY` is present, then `FMP` when `FMP_API_KEY` is present, then `yfinance`.

Render the daily-refreshed market RRG report:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/render_sector_rotation_rrg.py \
  --output-dir /Users/Zihao.Guan/Personal/ticker-screener/artifacts/output/sector_rotation_rrg_YYYY-MM-DD \
  --benchmark SPY \
  --universe all
```

Export Oracle NoSQL-ready market data files for the configured universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01
```

Run an incremental Oracle NoSQL export for the last 10 days:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --incremental-days 10 \
  --end-date 2026-05-01
```

Run a small Oracle NoSQL export smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --tickers AAPL MSFT NVDA \
  --start-date 2024-01-01 \
  --end-date 2024-03-31
```

Apply exported rows directly to existing Oracle NoSQL tables:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01 \
  --apply \
  --endpoint https://<your-endpoint> \
  --compartment <your-compartment-ocid>
```

Create the Oracle NoSQL tables on a first seed run and then write rows:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01 \
  --apply \
  --create-tables \
  --endpoint https://<your-endpoint> \
  --compartment <your-compartment-ocid>
```

The Oracle NoSQL schema notes and table design rationale live in:

- [docs/oracle-nosql.md](/Users/Zihao.Guan/Personal/ticker-screener/docs/oracle-nosql.md)

Run the web app shell locally:

```bash
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```

The initial Postgres schema for the web app lives in:

- [sql/postgres_app_schema.sql](/Users/Zihao.Guan/Personal/ticker-screener/sql/postgres_app_schema.sql)

The web app MVP notes live in:

- [docs/web-app.md](/Users/Zihao.Guan/Personal/ticker-screener/docs/web-app.md)

The Docker Compose deployment files for a single Oracle instance live in:

- [deploy/docker-compose.yml](/Users/Zihao.Guan/Personal/ticker-screener/deploy/docker-compose.yml)
- [deploy/Caddyfile](/Users/Zihao.Guan/Personal/ticker-screener/deploy/Caddyfile)
- [deploy/DEPLOY.md](/Users/Zihao.Guan/Personal/ticker-screener/deploy/DEPLOY.md)

The checked-in deploy config is generic for public-repo use. Set your real hostnames in `deploy/.env` with:

- `APP_FQDN`
- `REPORTS_FQDN`

GitHub Actions now includes:

- [.github/workflows/ci.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/ci.yml)
- [.github/workflows/deploy.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/deploy.yml)

## Artifacts

The screen step writes:

- `artifacts/raw/rs_new_high_before_price_<date>.json`
- `artifacts/raw/run_summary_<date>.json`
- `artifacts/watchlists/rs_new_high_before_price_<date>.json`
- `artifacts/raw/weekly_rs_new_high_<date>.json`
- `artifacts/raw/weekly_rs_run_summary_<date>.json`
- `artifacts/watchlists/weekly_rs_new_high_<date>.json`
- `artifacts/raw/weekly_htf_pullback_<date>.json`
- `artifacts/raw/weekly_htf_pullback_run_summary_<date>.json`
- `artifacts/watchlists/weekly_htf_pullback_<date>.json`
- `artifacts/raw/vcp_<date>.json`
- `artifacts/raw/vcp_run_summary_<date>.json`
- `artifacts/watchlists/vcp_<date>.json`
- `artifacts/raw/peg_earnings_gap_<date>.json`
- `artifacts/raw/peg_run_summary_<date>.json`
- `artifacts/watchlists/peg_earnings_gap_<date>.json`
- `artifacts/raw/cup_handle_<date>.json`
- `artifacts/raw/cup_handle_run_summary_<date>.json`
- `artifacts/watchlists/cup_handle_<date>.json`

The render step writes:

- `artifacts/output/<watchlist-stem>/charts/*.svg`
- `artifacts/output/<watchlist-stem>/index.html`
- `artifacts/output/<watchlist-stem>/run_summary.json`
- optional `watchlist_page_<N>.svg`

## GitHub Actions

The workflow in [.github/workflows/rs-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/rs-screen-render.yml) runs two separate jobs:

1. `screen`
2. `render`

The `render` job downloads the watchlist artifact produced by `screen` and then renders charts from that exact file.

The weekly workflow in [.github/workflows/weekly-rs-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/weekly-rs-screen-render.yml) scans for weekly RS new highs and runs once every Saturday on the GitHub cron schedule.

The PEG workflow in [.github/workflows/peg-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/peg-screen-render.yml) follows the same pattern for the earnings-gap screener.

The legacy PEG workflow in [.github/workflows/legacy-peg-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/legacy-peg-screen-render.yml) runs daily in parallel with the Sean PEG workflow and always uses `--strategy-profile legacy`.

The VCP workflow in [.github/workflows/vcp-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/vcp-screen-render.yml) follows the same screen, render, publish, and notify pattern. It supports manual runs with an optional `limit` input and is scheduled once per trading day at `UTC 01:00`, which corresponds to New Zealand `1pm` during standard time and `2pm` during daylight saving because GitHub Actions schedules are UTC-based.

The Cup & Handle workflow in [.github/workflows/cup-handle-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/cup-handle-screen-render.yml) follows the same screen and render pattern for daily breakout candidates.

The overlap workflow in [.github/workflows/daily-overlap-summary.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/daily-overlap-summary.yml) summarizes overlap across daily `RS`, `Sean PEG`, `Legacy PEG`, `VCP`, `Cup and Handle`, `Weekly HTF 8W Pullback`, `8W 100% Runup`, and `Gap Fill`. It runs on its own schedule at `UTC 02:00`, which corresponds to New Zealand `2pm` during standard time and `3pm` during daylight saving because GitHub Actions schedules are UTC-based. It distinguishes missing upstream watchlist files from valid `0`-hit runs, writes JSON, text, and HTML summary artifacts, and follows the same R2 publish plus Discord notify pattern as the other first-class workflows.

The pre-earnings workflow in [.github/workflows/pre-earnings-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/pre-earnings-screen-render.yml) screens the next-week earnings watchlist, renders charts, and follows the same R2/Discord pattern as the RS and PEG workflows. It currently runs manually with optional `limit` and `reference_date` inputs.

The pre-earnings MA stack workflow in [.github/workflows/pre-earnings-ma-stack-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/pre-earnings-ma-stack-screen-render.yml) screens the next-week earnings watchlist for `MA20 > MA50 > MA200`, then applies the shared post-screen market-cap filter above `$1B`. It currently runs manually with optional `limit` and `reference_date` inputs.
It is now scheduled once each Saturday on the GitHub cron schedule and its rendered `index.html` includes a next-week earnings calendar table whose ticker links jump to the corresponding chart cards further down the page.

The earnings growth workflow in [.github/workflows/earnings-growth-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/earnings-growth-screen-render.yml) screens next-week earnings names for explosive growth plus post-earnings reaction behavior. It currently runs manually with optional `limit` and `reference_date` inputs. It now prefers `OpenBB` for the earnings calendar layer, falls back to `AInvest` when `AINVEST_API_KEY` is available, then falls back to `yfinance`, with `AKShare` as an extra fallback for quarterly revenue history.

The market rotation workflow in [.github/workflows/sector-rrg-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/sector-rrg-render.yml) now refreshes once per trading day and renders separate weekly RRG reports for `sector`, `industry`, and `theme` instead of forcing every ETF into one chart. It defaults to `--universe all`, splits the theme ETF set into multiple smaller batch charts, and publishes a top-level `index.html` that links to each sub-report. Its cron is `10 1 * * 2-6`, which corresponds to New Zealand `1:10pm` during standard time and `2:10pm` during daylight saving because GitHub Actions schedules are UTC-based.

### Optional Discord notifications

If you want GitHub Actions to post run summaries to Discord, add a repository secret named `DISCORD_WEBHOOK_URL`.

The workflow will then send a message with:

- run date
- screen job result
- render job result
- ticker count
- hit count
- failure count
- top hit tickers when available
- a link to the GitHub Actions run

Without that secret, the notification step is skipped and the rest of the workflow still runs normally.

### Optional Cloudflare R2 publishing

If you want GitHub Actions to upload generated artifacts to Cloudflare R2, add these repository secrets:

- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET`
- `R2_PUBLIC_BASE_URL` for public links in notifications

When those secrets are configured, the workflow uploads each successful run to:

- `rs-screen/<date>/raw/`
- `rs-screen/<date>/watchlists/`
- `rs-screen/<date>/rendered/`

It also maintains these bucket-root files:

- `index.html`
- `watchlist_index_manifest.json`

The root `index.html` acts as a landing page across published workflows and lets you filter runs by workflow, text, and minimum hit count.

The Discord notification will include the public `index.html` link when `R2_PUBLIC_BASE_URL` is set. Without the R2 secrets, the publish job is skipped and the rest of the workflow still runs normally.
