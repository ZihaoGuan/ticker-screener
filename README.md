# ticker-screener

Focused chart-screen workflows for `RS new high before price`, `VCP`, and `power earnings gap`.

This project keeps the responsibilities narrow:

- screen the US equity universe for `RS new high before price`
- screen the configured exchange universe for `VCP`
- screen the configured exchange universe for `power earnings gap`
- emit raw screen results plus a renderer-ready watchlist JSON
- render daily setup charts from that watchlist

The RS engine is vendored from `cookstock` so GitHub Actions can run without depending on a sibling local repository. The chart renderer is vendored from the `trade-master-signals` skill so the output format stays aligned with your current chart workflow.

## Project layout

- `src/`: config, universe loading, RS screening, and watchlist building
- `scripts/run_rs_screen.py`: produces raw results and watchlist JSON
- `scripts/run_vcp_screen.py`: produces VCP raw results and watchlist JSON
- `scripts/run_peg_screen.py`: produces PEG raw results and watchlist JSON
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

For the next-week earnings growth screener, install the extra provider dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements-earnings-growth.txt
```

Run the full RS screen:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_rs_screen.py
```

Run a smaller smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_rs_screen.py --limit 25
```

Run the PEG screener across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py
```

Run the VCP screener across the configured exchange universe:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_vcp_screen.py
```

Run a VCP smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_vcp_screen.py --limit 25
```

Run a PEG smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --limit 10
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

Render charts from the generated watchlist:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/render_rs_watchlist.py \
  --watchlist-file /Users/Zihao.Guan/Personal/ticker-screener/artifacts/watchlists/rs_new_high_before_price_YYYY-MM-DD.json
```

Enrich an existing watchlist or raw screen JSON with earnings date, recent beat/miss status, and next-two-weeks earnings context:

```bash
FMP_API_KEY=... python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/enrich_with_earnings.py \
  --input-file /Users/Zihao.Guan/Personal/ticker-screener/artifacts/watchlists/rs_new_high_before_price_YYYY-MM-DD.json
```

The enricher supports either:

- a watchlist JSON array in `artifacts/watchlists/*.json`
- a raw screen payload with `hits[]` in `artifacts/raw/*.json`

Supported providers are `fmp`, `ainvest`, `yfinance`, and `auto`. `auto` prefers `FMP` when `FMP_API_KEY` is present, then `AInvest`, then `yfinance`.

Render a weekly RRG-style rotation map:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/render_sector_rotation_rrg.py \
  --output-dir /Users/Zihao.Guan/Personal/ticker-screener/artifacts/output/sector_rotation_rrg_YYYY-MM-DD \
  --benchmark SPY \
  --universe industry
```

## Artifacts

The screen step writes:

- `artifacts/raw/rs_new_high_before_price_<date>.json`
- `artifacts/raw/run_summary_<date>.json`
- `artifacts/watchlists/rs_new_high_before_price_<date>.json`
- `artifacts/raw/vcp_<date>.json`
- `artifacts/raw/vcp_run_summary_<date>.json`
- `artifacts/watchlists/vcp_<date>.json`
- `artifacts/raw/peg_earnings_gap_<date>.json`
- `artifacts/raw/peg_run_summary_<date>.json`
- `artifacts/watchlists/peg_earnings_gap_<date>.json`

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

The PEG workflow in [.github/workflows/peg-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/peg-screen-render.yml) follows the same pattern for the earnings-gap screener.

The VCP workflow in [.github/workflows/vcp-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/vcp-screen-render.yml) follows the same screen, render, publish, and notify pattern. It supports manual runs with an optional `limit` input and is scheduled once per trading day at `UTC 00:00`, which corresponds to New Zealand noon during standard time and 1pm during daylight saving.

The pre-earnings workflow in [.github/workflows/pre-earnings-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/pre-earnings-screen-render.yml) screens the next-week earnings watchlist, renders charts, and follows the same R2/Discord pattern as the RS and PEG workflows. It currently runs manually with optional `limit` and `reference_date` inputs.

The earnings growth workflow in [.github/workflows/earnings-growth-screen-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/earnings-growth-screen-render.yml) screens next-week earnings names for explosive growth plus post-earnings reaction behavior. It currently runs manually with optional `limit` and `reference_date` inputs. It now prefers `OpenBB` for the earnings calendar layer, falls back to `AInvest` when `AINVEST_API_KEY` is available, then falls back to `yfinance`, with `AKShare` as an extra fallback for quarterly revenue history.

The sector rotation workflow in [.github/workflows/sector-rrg-render.yml](/Users/Zihao.Guan/Personal/ticker-screener/.github/workflows/sector-rrg-render.yml) renders an RRG-style rotation map for the configured ETF universe. Its cron is `0 0 * * 0`, which corresponds to Sunday noon in New Zealand winter and Sunday 1pm during New Zealand daylight saving because GitHub Actions schedules are UTC-based.

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
