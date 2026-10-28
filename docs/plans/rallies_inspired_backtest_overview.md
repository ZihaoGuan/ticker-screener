# Backtest activity and performance overview

## Goal

Make `/backtests/martin-forward` and `/backtests/bar-retest` answer two questions together: how has this simulated book performed, and which saved trades produced that result? Use the Rallies screenshots as an information hierarchy reference, while retaining this app's dark theme, strategy controls, and simulation/provenance labels.

## Current surface

Both routes render `MartinForwardTestPage.tsx`. The page already has saved book metrics, account/benchmark history, positions, and events, but these are split across Today, Positions, Performance, and the audit drawer. The bar-retest route exists on current `origin/main`; the older local checkout does not contain it.

## Smallest complete change

1. Add a shared Overview view as the default on both routes. Keep the existing Today, Positions, and Performance views for full detail.
2. In Overview, show a compact strip of trustworthy saved metrics followed by the existing account-versus-benchmark chart when available. Reuse persisted metrics/history and preserve missing-data, preliminary-sample, retrospective, and simulated-result notices. Do not derive account equity from current positions or present unsupported Sharpe/alpha figures.
3. Add a readable activity log grouped by saved position: ticker, open/closed state, remaining shares, realized/unrealized result where available, then dated buy/trim/exit events from `book.events`. Show action direction separately from profit/loss color. Use only recognized execution event types; other event types stay in the existing audit view. Match events by `position_id` when present, and handle legacy records without it conservatively.
4. Keep data provenance visible for the execution books. Where daily marks or benchmark data are unavailable, show a plain empty state instead of a fabricated line.
5. Make the overview usable at narrow widths: metric cards wrap; each activity item keeps ticker/action/date/amount readable; buttons and details remain keyboard accessible.

## Verification

- Add a focused test for event-to-position grouping and the open/closed/unknown-data cases if nontrivial data mapping is introduced.
- Run the frontend build and relevant UI test/probe if available.
- Check both routes at desktop and mobile widths with representative saved and empty states; inspect once, fix observed defects, and confirm once.

## Boundaries

No trading-engine or database changes, new performance calculations, new dependencies, broker-order actions, or copy of the Rallies branding. Preserve existing route and query links and all existing detailed views.
