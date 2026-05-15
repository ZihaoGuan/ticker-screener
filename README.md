# ticker-screener

Focused RS screen and chart-render workflow for `RS new high before price`.

This project keeps the responsibilities narrow:

- screen the US equity universe for `RS new high before price`
- emit raw screen results plus a renderer-ready watchlist JSON
- render daily setup charts from that watchlist

The RS engine is vendored from `cookstock` so GitHub Actions can run without depending on a sibling local repository. The chart renderer is vendored from the `trade-master-signals` skill so the output format stays aligned with your current chart workflow.

## Project layout

- `src/`: config, universe loading, RS screening, and watchlist building
- `scripts/run_rs_screen.py`: produces raw results and watchlist JSON
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
