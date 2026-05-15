# ticker-screener

Focused chart-screen workflows for `RS new high before price` and `power earnings gap`.

This project keeps the responsibilities narrow:

- screen the US equity universe for `RS new high before price`
- screen the configured exchange universe for `power earnings gap`
- emit raw screen results plus a renderer-ready watchlist JSON
- render daily setup charts from that watchlist

The RS engine is vendored from `cookstock` so GitHub Actions can run without depending on a sibling local repository. The chart renderer is vendored from the `trade-master-signals` skill so the output format stays aligned with your current chart workflow.

## Project layout

- `src/`: config, universe loading, RS screening, and watchlist building
- `scripts/run_rs_screen.py`: produces raw results and watchlist JSON
- `scripts/run_peg_screen.py`: produces PEG raw results and watchlist JSON
- `scripts/render_rs_watchlist.py`: renders charts from a watchlist JSON
- `vendor/cookstock/`: vendored RS engine dependencies
- `vendor/trade_master_signals/`: vendored chart renderer
- `artifacts/`: local outputs and CI handoff artifacts

## Local usage

Install dependencies:

```bash
python3 -m pip install -r /Users/Zihao.Guan/Personal/ticker-screener/requirements.txt
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

Run a PEG smoke test:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --limit 10
```

Run the PEG screener against only the next-week earnings watchlist:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/run_peg_screen.py --source earnings-watchlist
```

Render charts from the generated watchlist:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/render_rs_watchlist.py \
  --watchlist-file /Users/Zihao.Guan/Personal/ticker-screener/artifacts/watchlists/rs_new_high_before_price_YYYY-MM-DD.json
```

## Artifacts

The screen step writes:

- `artifacts/raw/rs_new_high_before_price_<date>.json`
- `artifacts/raw/run_summary_<date>.json`
- `artifacts/watchlists/rs_new_high_before_price_<date>.json`
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

The Discord notification will include the public `index.html` link when `R2_PUBLIC_BASE_URL` is set. Without the R2 secrets, the publish job is skipped and the rest of the workflow still runs normally.
