# Vela Charts integration review

Reviewed 2026-10-08 against Vela's official site, documentation, and public repositories, plus the current ticker-screener frontend source.

## Conclusion

Vela is worth prototyping as an **optional engine on the full `/charts` page**, but it should not replace every existing chart yet.

The important correction is that ticker-screener is not using TradingView Advanced Charts or the hosted TradingView widget. It uses TradingView's open-source `lightweight-charts` package (`4.2.3`) directly. Vela's own comparison says moving from Lightweight Charts is an integration rewrite, not a package swap; only the bar data carries over cleanly. Vela is also a larger “whole terminal,” with no published bundle size, while Lightweight Charts is deliberately small.[^vela-compare]

The strongest reason to add Vela is Pine support. The strongest reasons not to switch wholesale are:

1. PineTS and Vela-PineTS are AGPL-3.0. A closed-source commercial product needs LuxAlgo's commercial license; the free Vela core alone does **not** include a Pine runtime.[^pine-chart-license][^vela-pricing]
2. The current full chart contains a large amount of product-specific rendering and interaction logic that will need to be ported: separate price/RS/fib panes, many backend and client-computed overlays, custom gap and high-tight-flag primitives, markers, annotations, and synchronized range/crosshair behavior.
3. Vela is currently `0.8.3`, and its changelog includes behavior changes and explicitly marked breaking changes inside `0.x` minors. Pinning an exact version and running visual regression tests would be prudent.[^vela-package][^vela-changelog]

Recommended direction: keep `lightweight-charts` as the default and for all mini-card charts; add a feature-flagged **Vela** choice to the full chart only; decide the Pine license before shipping Pine to users.

## What Vela is

Vela is a framework-agnostic TypeScript charting library published as `@luxalgo/vela`. It exposes:

- `@luxalgo/vela`: imperative/headless chart core;
- `@luxalgo/vela/workspace`: single- or multi-chart terminal chrome;
- `@luxalgo/vela/ui`: UI components;
- `@luxalgo/vela/plugin`: extension SDK;
- optional provider packages for Binance, Coinbase, and Hyperliquid.[^vela-install][^vela-readme]

The default renderer is native WebGL2 with a Canvas 2D fallback. The package ships ESM, CJS, type declarations, and self-contained browser bundles. It is framework-agnostic rather than a React component; React integration is the same imperative lifecycle pattern already used in this repo: mount into a `ref` in an effect and call `destroy()` during cleanup.[^vela-install][^vela-api]

Two integration levels are available:

- **Headless `Vela`**: best fit for preserving ticker-screener's current panels, controls, and page layout.
- **`VelaWorkspace` with `layout: false`**: provides symbol/timeframe/style controls, indicators, drawings, status line, object tree, persistence, and keyboard behavior. This would duplicate or displace more of the current product UI.[^vela-workspace]

## Data responsibilities

Vela does not provide licensed US-equities market data. The software license covers software only. The bundled keyless providers are crypto-only; stocks, futures, and forex require the host's own licensed feed.[^vela-pricing]

That is not a blocker here. Vela accepts an offline OHLCV array:

```ts
new Vela(container, {
  data: bars,
  timeframe: "1D",
  theme: "dark",
});
```

Each bar is `{ time, open, high, low, close, volume? }`; `time` is the bar-open time in **epoch milliseconds**. Offline `data` makes no network request and is mutually exclusive with provider fetching.[^vela-quickstart][^vela-data]

Ticker-screener already has nearly the right data contract. [`CandlePoint`](../../frontend/src/lib/types.ts) carries string dates and OHLCV. [`ChartsPage`](../../frontend/src/pages/ChartsPage.tsx) merges the API's separate candle and volume arrays before rendering. A Vela adapter therefore needs only to validate/sort the bars and convert each date string to a UTC epoch-millisecond bar-open timestamp.

For `request.security()` in Pine, Vela routes higher/lower-timeframe and cross-symbol requests through Vela's data feed; the Pine engine does not fetch market data itself.[^vela-engines] Supplying only one offline array is enough for single-symbol/single-timeframe scripts, but Pine scripts requesting another timeframe or symbol will require a ticker-screener `DataProvider`/`MarketDataFeed` adapter backed by the existing API. This is an important second phase, not part of the minimal trial.

## Pine Script support

Vela core deliberately ships without a scripting engine. Pine requires all three packages:

```text
@luxalgo/vela
@luxalgo/vela-pinets
pinets
```

`PineEngine` runs on the main thread. `PineWorkerEngine` runs with the same semantics in a Web Worker and is the appropriate default for nontrivial scripts.[^vela-engines]

The documented integration accepts Pine v5/v6 source strings, turns `input.*` declarations into settings, renders plots/panes/drawings, exposes alerts and script state, and renders strategy fills. The runtime can also return plot values or a backtest ledger for host features.[^pine-chart][^vela-pine-page]

However, “can use Pine Script” should not be interpreted as perfect TradingView parity:

- The language coverage table says `while` and `for in` loops are missing, objects and methods are still in progress, and imports are planned.[^pine-language]
- The API coverage documentation explicitly distinguishes implemented/tested, implemented-but-needs-testing, not-implemented, and in-progress functions; scripts must be checked against those tables.[^pine-api]
- Strategy execution has documented divergences in liquidation pricing, cross-currency conversion, OCA enforcement, commission rounding, per-trade excursions, and some performance ratios.[^pine-strategy]
- The PineTS repository describes native Pine v6 execution as experimental.[^pinets-repo]

Therefore each script should get a parity fixture: identical source, identical bars, and expected final plots/markers/trades captured from TradingView where legally and operationally possible.

### Licensing and pricing

| Capability | License / price as published |
| --- | --- |
| Vela core | Apache-2.0, $0; commercial use allowed |
| Vela core attribution | Visible Vela attribution required on every chart page/screen unless a paid license removes it |
| PineTS + Vela-PineTS | AGPL-3.0 for an open-source stack |
| Developer license | USD 1,199 per developer/year; only for companies under USD 500k trailing-12-month gross revenue |
| Business license | USD 20,000/year, unlimited developers |
| Enterprise | Custom; required for specified regulated businesses, 100,000+ users, USD 25M+ revenue, or custom MSA/DPA/SLA/security-review needs |

Paid terms include the PineTS commercial license and remove Vela attribution. The pricing page says there is no engine-only commercial license.[^vela-pricing] The Vela `NOTICE` requires visible attribution on every chart-bearing page/screen in free builds, even if the built-in mark is disabled.[^vela-notice]

This is a product/legal decision before implementation, not something to defer until release. The exact intended deployment and company-revenue tier should be confirmed with the license owner; this note is not legal advice.

## Security and hosting

- Vela can be bundled and self-hosted. With ticker-screener's bars passed through `data`, charting requires no LuxAlgo API, key, iframe, or market-data egress.[^vela-install][^vela-faq]
- `PineWorkerEngine` uses an inline worker spawned from a `blob:` URL by default. If the site's Content Security Policy blocks `blob:`, Vela documents hosting the worker file at a same-origin URL and passing `workerUrl`.[^pine-chart]
- A worker prevents heavy Pine computation from blocking the chart UI, but the official docs do not present it as a security sandbox for arbitrary hostile scripts. User-pasted scripts should be treated as untrusted input, with script length/history limits, time or resource budgets, error boundaries, and no automatic publication/sharing until reviewed.
- Vela's security policy supports the latest published minor and asks vulnerability reports to be sent privately. PineTS/Vela-PineTS vulnerabilities are handled in their own repositories.[^vela-security]
- Persisted Vela workspace documents are explicitly treated as untrusted on restore, but plugin `ext` payloads are opaque and must be validated by the host.[^vela-workspace][^vela-plugin]

## Current ticker-screener chart surface

The frontend has two distinct chart workloads:

1. [`ScannerMiniChart`](../../frontend/src/components/ScannerMiniChart.tsx) is used repeatedly on Scanner Top Hits, Scanner Result, My Picks, and Sector Leaderboard. It draws candles/bars/line, volume, several moving averages, and RS markers. These dashboard grids benefit from the smaller renderer and should remain on Lightweight Charts initially.
2. [`PriceChart`](../../frontend/src/components/PriceChart.tsx) is the full analytical chart used on Charts and a Sector Leaderboard detail view. It creates three synchronized charts/panes and contains the concentrated custom chart logic. This is the only sensible initial Vela target.

The full chart currently owns or renders:

- price/volume and separate RS/fib surfaces;
- EMA 8/21, MA 50/200, weekly EMA 8, IPO VWAP, anchored VWAP, market-extension, Bollinger, flexible support/resistance, and channel series;
- gap-zone and high-tight-flag custom primitives;
- setup, RS, sell, Wyckoff, fib, and annotation markers/lines;
- visible-range and crosshair synchronization, plus external hover synchronization with surrounding page content.

Vela has public extension points for scripting engines, native indicators, renderer layers, drawings, and workspace events, so these features are port-able. They are not source-compatible with Lightweight Charts' `addLineSeries`, `setMarkers`, `createPriceLine`, or `attachPrimitive` calls. Vela's own documentation confirms the migration is a rewrite of the integration.[^vela-plugin][^vela-compare]

There is another design choice: Vela offers 70+ built-in indicators, but ticker-screener's backend currently returns authoritative overlay arrays and signal metadata. Recomputing standard studies inside Vela can simplify rendering, but silently replacing backend-calculated values risks divergence. Preserve the existing API values first; move a calculation only after parity tests establish identical semantics.

## Recommended implementation shape

### Phase 1 — low-risk proof on `/charts`

- Add a persisted chart-engine selector: `Current` / `Vela (preview)`.
- Dynamically import Vela only when selected, so dashboard and default-route bundle cost do not change.
- Implement a small `VelaPriceChart` React wrapper around headless `Vela`, not `VelaWorkspace`.
- Reuse the current `CandlePoint[]`; convert dates to epoch milliseconds and pass offline `data`.
- Start with candles, volume, theme, resize, fit/range, and cleanup only.
- Keep `PriceChart` untouched and make fallback automatic if Vela construction fails.

Success gate: candle/volume parity, responsive resize, keyboard/pointer behavior, no leaked WebGL contexts after route/ticker switching, and acceptable load/render cost on the deployed browser targets.

### Phase 2 — existing feature parity

Port the current chart in vertical slices:

1. moving-average/VWAP overlays;
2. RS pane and synchronized hover/range behavior;
3. signal markers and horizontal annotations;
4. gap/HTF/fib/flexible-SR/channel rendering through native indicators or renderer layers;
5. persistence and drawings if desired.

Do not make Vela the default until the selected full-chart configuration has parity. Do not migrate `ScannerMiniChart` yet; Vela explicitly positions itself as a terminal rather than a micro-chart and publishes no bundle-size target.[^vela-compare]

### Phase 3 — Pine pilot

- First settle AGPL versus paid commercial licensing.
- Use `PineWorkerEngine`, with a same-origin worker URL if CSP requires it.
- Start with one checked-in, read-only Pine indicator rather than arbitrary user code.
- Add source versioning, deterministic bar fixtures, compile/runtime diagnostics, and TradingView comparison snapshots.
- Add a ticker-screener data-feed adapter only when a chosen script actually needs `request.security()`.
- Consider a user editor only later. The free core accepts source strings, but Vela's built-in Pine editor is a Pro feature; otherwise ticker-screener must build and secure its own editor UI.[^vela-compare]

## Decision

**Proceed with an optional full-chart prototype, not a replacement.** Vela is strategically attractive because Pine can become a first-class, local execution layer over ticker-screener's own bars. The current chart data contract makes the candle proof easy. The real cost is porting existing specialized overlays and interactions, validating Pine parity, and choosing a compliant Pine license.

If the immediate goal is only a better chart, Vela core can be trialed free with attribution. If the goal is Pine Script in the deployed product, approve the licensing path first.

## Official sources

[^vela-readme]: [Vela official GitHub README](https://github.com/LuxAlgo/Vela#readme)
[^vela-package]: [Vela package metadata (`0.8.3` at review time)](https://github.com/LuxAlgo/Vela/blob/main/package.json)
[^vela-changelog]: [Vela changelog](https://github.com/LuxAlgo/Vela/blob/main/CHANGELOG.md)
[^vela-install]: [Vela installation and package entry points](https://docs.luxalgo.com/vela/user/installation)
[^vela-quickstart]: [Vela quickstart and offline OHLCV format](https://docs.luxalgo.com/vela/user/quickstart)
[^vela-data]: [Vela data providers](https://docs.luxalgo.com/vela/user/data-providers)
[^vela-api]: [Vela public API reference](https://docs.luxalgo.com/vela/user/api-reference)
[^vela-workspace]: [Vela workspace and persistence](https://docs.luxalgo.com/vela/user/workspace)
[^vela-plugin]: [Vela plugin SDK](https://docs.luxalgo.com/vela/contributing/plugin-sdk)
[^vela-engines]: [Vela scripting engines](https://docs.luxalgo.com/vela/user/scripting-engines)
[^vela-faq]: [Vela FAQ](https://docs.luxalgo.com/vela/user/faq)
[^vela-pine-page]: [Vela + PineTS product explanation](https://velacharts.dev/pine-script/)
[^pine-chart]: [Using PineTS within Vela charts](https://docs.luxalgo.com/developers/pinets/using-within-charts)
[^pine-chart-license]: [Pine chart integration license table and AGPL warning](https://docs.luxalgo.com/developers/pinets/using-within-charts#how-the-pieces-fit)
[^pine-language]: [PineTS language coverage](https://docs.luxalgo.com/developers/pinets/lang-coverage)
[^pine-api]: [PineTS API coverage](https://docs.luxalgo.com/developers/pinets/api-coverage)
[^pine-strategy]: [PineTS strategy known divergences](https://docs.luxalgo.com/developers/pinets/strategy#known-divergences)
[^pinets-repo]: [PineTS official GitHub repository](https://github.com/LuxAlgo/PineTS#readme)
[^vela-pricing]: [Vela commercial pricing and licensing FAQ](https://velacharts.dev/pricing/)
[^vela-notice]: [Vela attribution NOTICE](https://github.com/LuxAlgo/Vela/blob/main/NOTICE)
[^vela-security]: [Vela security policy](https://github.com/LuxAlgo/Vela/blob/main/SECURITY.md)
[^vela-compare]: [Vela's official comparison with TradingView Lightweight Charts and Advanced Charts](https://velacharts.dev/compare/)
