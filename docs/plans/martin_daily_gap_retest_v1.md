# Martin-inspired Daily Gap Retest v1 — developer handoff

Status: implementation-ready proposal; no strategy implementation or production changes authorised by this document alone.

Repository inspected: `792aa4e` on 24 October 2026. Recheck HEAD and working-tree changes before editing. This checkout contains unrelated work, including changes in `src/screener_catalog.py`, `src/webapp/services/run_service.py`, portfolio files, RunsPage and IDE files. Preserve them and stage only this feature's hunks.

## 1. Outcome and strategy boundary

Build an independent daily paper-trading book that follows technically qualified opening-gap events, waits for the first supported EMA pullback episode, and enters only after a completed daily reclaim. Reuse existing persistence, account reporting, benchmarks and operational job patterns.

Workflow: **Gap discovered → Watch → Support zone → Daily confirmation → Risk/capacity qualification → Next-session open → Manage → Exit**.

Martin's first-person [2024 lessons](https://tradingresourcehub.substack.com/p/martin-luk-283-usic-2024-key-lessons) describe scanning gap-ups and leaders, watching subsequent pullbacks, defining hard stops before entry, favouring high ADR and often using stops within half ADR. Those are the inspiration. All numerical setup, timing, sizing and execution rules below are our testable daily adaptation, not a claim to reproduce his discretionary trading.

Stable identifiers:

| Purpose | Value |
|---|---|
| Strategy/book ID | `martin_daily_gap_retest_v1` |
| Dashboard book key | `gap_retest` |
| Label | `Daily Gap Retest · experimental` |
| Entry policy | `gap_daily_reclaim_next_open_v1` |
| Stop policy | `gap_reclaim_candle_low_v1` |
| Management policy | `gap_trim10_3r_ema9_v1` |
| Discovery source | `power_gap_volume_watch`, rules `power_gap_volume_watch_v1` |

Do not add gap candidates to `FOCUS_SCANNERS`, change any existing book's results, enable rotation, add broker orders or introduce a new standalone backtest service. Use a small dedicated evaluator and book service; share stable helpers only where semantics match.

## 2. Prerequisites and data contract

Before enabling entries:

1. Verify the deployed lookup fix `a924b79` and reporting fix `792aa4e`. The details endpoint must load persisted hits without depending on worker-local files. Confirm actual running image versions, not only host Git.
2. Audit the coverage/invalid-OHLC problems observed in the first production scan. `60ebf97` already adds TradingView history refresh/preservation tooling; inspect its current coverage and use the existing repair path before proposing another downloader. Do not assume the initial failure counts still describe production.
3. Validate completed regular-session OHLCV for each required calculation window, with consistent price/volume adjustment provenance. Never repair invalid ranges by clamping open/high/low/close or silently removing sessions. Treat provider corrections as a new input revision.
4. New-candidate data failures block that ticker and produce a reason. Missing execution bars for an existing position or pending order prevent finalising the session and roll back book writes; do not silently ignore a potentially triggered stop.

Read the exact session's persisted **Power Gap scanner run directly** through `ScreenerHistoryService.get_run(..., include_hits=True)` or the existing repository, with an explicit adequate hit limit/pagination. Do not source the universe from a user-selected frontend preset or Guru Board: both can filter it. Use normalised persisted hit payloads.

Accept only hits with `power_gap_volume_context.active_signals` containing `gap`. HVE/HVY-only rows remain research candidates. Earnings confirmation is an informational cohort label, not an entry gate; missing catalyst data must not be treated as weak technical quality.

Keep these dates separate: source session, source publication time, original gap date, first observed date, pullback episode dates, confirmation date, order session, fill date and exit date.

Event identity: `(ticker, original_gap_date, discovery_rules_version)`. Preserve the original event values and source-run ID. Do not re-anchor a gap or rewrite previous fills when later OHLCV is corrected. Store a digest of the evaluated bars and immutable decision evidence with each session.

## 3. Frozen v1 parameters

These are experimental defaults. Persist their version/config hash. Changes require a new version/book rather than rewriting this book.

| Parameter | v1 value |
|---|---|
| Gap age eligible for setup | 1–60 XNYS sessions after the gap; no entry on the gap day |
| Setup support | Daily EMA9 or EMA21 |
| History | Require 260 completed sessions before entry; load at least 326 sessions to reconstruct a 60-session-old event with indicator warmup and the five-session slope comparison |
| EMA calculation | For each evaluated date d, use Pandas `ewm(span=n, adjust=False)` over the trailing 260 sessions ending at d. Calculate historical values using their own as-of windows; never reseed yesterday's EMA from today's window |
| Trend gate | Close > SMA50; EMA21 today > EMA21 five sessions earlier |
| Liquidity gate | Mean of `close × volume` over last 20 sessions ≥ $20 million |
| Price gate | Decision close ≥ $5 |
| ADR14 | `mean((high - low) / close × 100)` over last 14 sessions, matching existing readiness definition; require 14 positive finite ranges |
| Touch tolerance | `0.25 × decision_close × ADR14_pct / 100` dollars |
| Pullback episode duration | At most five trading sessions including first touch |
| Maximum entry-to-stop distance | 5%, inclusive |
| Account base / leverage | $100,000 fixed sizing base; cash only |
| Planned risk per trade | At most $500 |
| Maximum name allocation | $20,000 at the order's maximum entry |
| Slots | Eight open positions plus executable pending reservations |
| Aggregate committed stop risk | At most $2,500 across holdings and executable pending orders |
| Order participation | Planned notional ≤ 0.1% of ADV20 dollars |
| Slippage model | 10 bps adverse on buys, hard stops and next-open exits |
| Management | Sell 10% once at 3R; trail remainder via daily-close EMA9 exit |
| Re-entry | At most one filled trade per gap event in v1 |

No RS90 minimum. Rank with independently computed 20-session stock return minus SPY return; do not require current Finviz ratings. Higher ADR is a ranking preference and stop/ADR >0.5 is a caution, not a rejection. Missing ADR blocks entry because the setup tolerance and ranking cannot be evaluated.

AVWAP, prior pivots and gap references may be displayed as context. They are not extra mandatory gates or additional entry types in v1. A later version can test gap-anchored AVWAP separately after its adjustment/volume conventions are settled.

## 4. Exact setup and watch lifecycle

Evaluate through completed session T only; never use T+1 prices to select or confirm a candidate.

### Event admission and retention

- Admit the newest active gap event per ticker from the valid source run. First observation can be late; scan history from the gap through T to reconstruct whether an earlier pullback episode already occurred. Label first-observed versus first-event dates honestly.
- Reassess existing watches daily even when the ticker is absent from the next scanner output. Absence alone is not invalidation.
- A newer gap supersedes an unfilled older event. Keep an open position attached to its original event; track the new event as informational until that holding is closed.
- Retire an unfilled event at age 61, after a completed close strictly below its original gap-day low, after its first pullback episode expires, or after a trade for that event has filled. A wick below the gap low followed by a recovery is not event invalidation.
- A close below the original gap low retires only this book's event. It must not invalidate another strategy's later supported setup or force an existing position out independently of its saved stop/trail rules.

### First pullback episode

On each post-gap session t, inspect EMA9 and EMA21. A first touch starts an episode when:

1. `close[t-1] >= EMA[t-1]` for that support;
2. `low[t] < low[t-1]`, representing a pullback/undercut rather than a rising candle merely near its EMA;
3. the daily range intersects `[EMA[t] - tolerance[t], EMA[t] + tolerance[t]]`.

If both supports qualify on the first day, choose EMA9; persist the chosen period and do not switch during the episode. Subsequent sessions can confirm the same episode. If that first episode expired before initial observation, record `first_episode_already_expired` rather than retrospectively opening an order or relabelling a later touch as the first.

### Daily confirmation

Within the episode, including its first day, all must hold on T:

- The range intersects the chosen EMA's tolerance band again.
- `close[T] > EMA[T]` and `close[T] > open[T]`.
- The trend, price, liquidity and history gates in section 3 pass.
- The event has not retired.

Record the selected EMA, its value, touch tolerance, episode start, candle OHLCV and each gate result. Do not additionally require a prior-day-high breakout, RS90, rising SMA200 or a second scanner match. These would change this experiment.

Confirmation creates a new plan for that exact date; it is not a standing permission to buy indefinitely. A risk/capacity rejection can remain under observation for the remainder of the episode, but any later executable order requires a fresh confirming candle and fresh levels.

### Canonical states and reasons

Use daily candidate rows plus their versioned payloads to preserve the watch lifecycle; add a strategy-scoped repository query for the latest watch state per ticker. No new watch table is required initially. Keep the scalar status compatible with existing storage; add an explicit `gap_state` in payloads for precise UI labels.

| `gap_state` | Meaning |
|---|---|
| `watch` | Gap retained; no qualifying first touch yet |
| `support_zone` | First episode active; daily reclaim not confirmed |
| `confirmed` | Confirmed today; plan evaluated |
| `waiting_risk` | Confirmation exists but the structural plan exceeds the cap |
| `waiting_capacity` | Eligible today but not allocated an executable slot |
| `pending` | Executable order for the next session only |
| `open` | Position exists; no additional entry allowed |
| `closed` / `retired` | Position ended / event no longer eligible |
| `blocked_data` | Relevant data is unavailable or invalid |

At minimum expose: `awaiting_first_retest`, `awaiting_daily_reclaim`, `trend_not_qualified`, `liquidity_below_minimum`, `price_below_minimum`, `risk_exceeds_5pct`, `capacity_full`, `cash_or_risk_budget`, `gap_event_expired`, `gap_support_close_failed`, `first_episode_expired`, `first_episode_already_expired`, `event_already_traded`, `open_position_exists`, `source_missing_or_stale`, `history_missing_sessions`, `invalid_ohlcv`, `adjustment_basis_unknown`, `missing_adr`, `opened_at_or_below_stop`, `opened_above_max_entry`, and `signal_not_fresh`.

## 5. Plan, selection and sizing

For confirmation T:

```text
reference_entry = close[T]
initial_stop = low[T]                     # no ATR subtraction or arbitrary tightening
reference_risk_pct = 100 * (reference_entry - initial_stop) / reference_entry
max_entry = min(initial_stop / 0.95, reference_entry)
stop_to_adr = reference_risk_pct / ADR14_pct
```

`max_entry` deliberately allows no upward chase beyond the confirmation close. It can be below that close when the reference risk exceeds 5%. Such a plan remains `waiting_risk` in v1; do not silently turn it into a resting bargain-entry order. A later version can test that alternative separately.

Require finite positive levels with `initial_stop < reference_entry`, risk ≤5%, and known ADR. Expose a wide-stop caution above half ADR. Never apply the generic existing structural-plan function blindly: it currently supports only MA retest, EMA21 and SMA200 source IDs. This strategy needs its explicit reclaim-candle-low policy adapter and source identity.

Rank eligible confirmations deterministically by:

1. 20-session return relative to SPY, descending;
2. ADR14%, descending;
3. ADV20 dollars, descending;
4. ticker ascending.

Do not use optional/missing sector or RS metadata to disqualify a technically complete candidate. Persist ranking inputs for replay. Save all eligible candidates, including those that exceed capacity, but only allocated orders get `pending` status and reserve cash/risk/slots. `waiting_capacity` is an observation queue, not a multiday executable order.

Allocate sequentially against the book's state known at T. Existing positions, including scheduled next-open exits, still own their slots and cash until filled. Intraday exits tomorrow do not fund tomorrow's already selected opening orders. This conservative baseline can leave cash temporarily unused.

At maximum entry P, stop S and current free budgets:

```text
shares = floor(min(
  500 / (P - S),
  20000 / P,
  available_cash / P,
  available_committed_risk / (P - S),
  (0.001 * ADV20_dollars) / P
))
```

Reject zero-share orders. Reserve `shares × P` cash and `shares × (P-S)` planned stop risk. Store the fixed planned share count; do not resize upward using tomorrow's favourable open. Released cash/risk from trims can be allocated only at a later decision close. No pyramiding, rotation, margin or cross-book netting.

Eight is a capacity ceiling, not a promise to hold eight names. The aggregate risk and cash limits can leave fewer names allocated.

## 6. Execution and management: do not inherit incompatible fills

An order created after T is eligible only at the regular-session open of the next XNYS session U. Verify decision/source publication occurred before that open.

1. If `open[U] <= stop`, cancel.
2. Compute model buy fill `open[U] × 1.001`. If it exceeds saved `max_entry`, expire the order. Equality with maximum entry is allowed.
3. Otherwise fill the saved shares at that model price, rechecking cash and risk budgets. If budgets no longer support the fixed order, cancel; do not borrow or silently increase risk.
4. Never fill later merely because `low[U] <= max_entry`. The existing `order_outcome()` implements next-session limit-touch fills, so it cannot be reused unchanged.
5. Expire every unfilled order after U. Do not shift it to the next available data day if U's bar is missing.

This is an explicit paper opening-fill approximation with adverse slippage, not a guarantee of auction liquidity or live broker behaviour.

Precision: use stored six-decimal price precision. Floor maximum-entry prices to six decimals; round simulated fills/targets to six decimals with Decimal `ROUND_HALF_UP`. Calculate share counts and money from Decimal values, keep cash/P&L ledgers to at least six decimals, and round only for display. Do not round risk percentages before eligibility checks. Indicator calculations may remain floating point; persist their values and inputs.

Save `initial_r_per_share = actual_fill - original_stop`, initial shares and initial dollar risk. R and profit targets never change after a trim or stop update.

Management policy is frozen to the existing backend baseline, not the frontend's configurable presets:

- Hard stop active immediately, including entry day.
- Gap through stop: exit at opening price minus 10 bps; intraday touch: stop price minus 10 bps. Never fill a gap stop at the unavailable original stop price.
- At/above 3R, sell `floor(initial_shares × 0.10)` once at the 3R price minus 10 bps. If this is zero, record trim unavailable and retain the position.
- For a later session opening above the 3R target, model the trim at that opening price minus 10 bps. Entry-day trims are disabled, matching the current conservative backend baseline.
- A completed close strictly below EMA9 schedules exit of all remaining shares at the next session open minus 10 bps. Equality is not a breach; recovering tomorrow does not cancel the saved exit.
- The initial hard stop remains unchanged. No automatic breakeven stop, second trim, 6R target or trailing intraday EMA stop in v1.
- If stop and 3R are both touched and daily bars cannot establish order, process the stop first and suppress the trim. A pre-scheduled next-open exit takes place at the open; record a gap-through-stop diagnostic if relevant, without a duplicate sell.
- Closed positions remain in performance history. Scanner disappearance, event age 61 and later gap-reference breaches are not separate exit rules for an open position.

## 7. Session ordering, persistence and recovery

Implement one public orchestrator, conceptually:

```python
run_session(day, source_record, dry_run=True) -> SessionResult
evaluate_gap_retest(frame_as_of_day, original_event, prior_watch_state) -> CandidateDecision
resolve_gap_next_open(order, execution_bar) -> FillDecision
```

Pure evaluator and execution helpers take data as arguments; they must not fetch providers, mutate DB state or know frontend presets.

For every session:

1. Validate completed XNYS date and chronological continuity; bootstrap an empty book explicitly.
2. Acquire the existing strategy advisory transaction lock through `atomic_session(strategy_id)`. Do not also acquire a conflicting session lock on another connection. Ensure schema before entering the transaction.
3. Same strategy/date already applied → return `session_already_processed`, even if the scanner was rerun and changed fingerprint. A changed source is audit evidence, not permission to duplicate executions.
4. Load/validate execution bars for all existing positions and pending orders; process scheduled opening exits, old positions' opening hard stops, previously allocated next-open orders, then remaining intraday stops/trims and close marks.
5. Update trail signals; discover/reassess watch events through T; allocate fresh orders for U using closing state.
6. Persist candidate evidence, events, positions, cash/equity marks and summary atomically. Injected failure must leave no fills or watch transitions committed.

Reuse `martin_forward_runs`, candidates, positions and events with the new strategy ID. The latest per-ticker candidate payload stores watch state; retain event identity in order/position/event payloads. Query all prior fills for an event when checking `event_already_traded`, not just current open positions.

Current uniqueness is `(strategy_id, session_date, source_fingerprint)`, so DB uniqueness alone does not prevent processing one date twice with a different source. Enforce the date rule under the book lock. Do not alter existing books' rerun semantics.

Source failure is different from execution-data failure:

- Missing/late source with an empty book: do not create the first session or any orders; report missing prerequisite.
- Missing fresh source for an established book: settle existing orders/positions using valid execution bars, preserve watches, and create no new entries. Use the previous valid source ID as the persisted source reference and explicitly store `new_source_run_id: null`, `discovery_complete: false`, and the reason. Never imply that older discovery data was fresh. This is still an execution-complete session; do not replay its fills when discovery recovers.
- Missing required execution data: fail and roll back the book session. Restore the data and retry that session before advancing to a later date.

No force-reset or overwrite flag on the ordinary scheduled action. Historical recovery runs sessions sequentially under the same lock; a new historical experiment uses a distinct book identity and cannot mix with live-forward history.

Persist summary counts for source hits, active-gap events, watches, blocked data, confirmations, risk rejections, capacity waits, pending orders, fills, expiries, stops, trims and trail exits. Include source IDs, publication times, input digests, policy versions, parameters, execution/discovery completeness and provenance.

## 8. Files and concrete integration work

| File/seam | Work |
|---|---|
| New `src/martin_daily_gap_retest.py` | Constants/config, pure indicators, event/episode evaluator, plan and deterministic ranking. |
| New `src/webapp/services/martin_gap_retest_service.py` | Dedicated orchestration, next-open fill/management adapters; use existing repository. Do not copy the entire current service. |
| `src/webapp/repositories/martin_forward_test_repository.py` | Strategy-scoped latest-watch/source lookup, atomic session reads/writes, account registration, context and benchmark wiring. Prefer existing JSON fields; no schema migration unless required by tested invariants. |
| `src/webapp/services/martin_forward_test_service.py` | Reuse stable helpers or extract only the small helper needed by both engines. Existing global `FOCUS_SCANNERS`, fill model and management remain unchanged. |
| New `scripts/run_martin_gap_retest.py` | Manual/scheduled CLI, dry-run default, explicit persistence, standard status/artifact reporting and `job_run_id` linkage. |
| `src/webapp/services/run_service.py` | Hidden-from-normal-screeners manual action `run_martin_gap_retest`; expose date and dry-run/apply controls using existing argument conventions. |
| Existing schedule configuration/service | New weekday job after price reload and Power Gap source completion. It reads the source run directly, so Top Hits snapshot is not a prerequisite. Existing books do not depend on this new job. |
| `frontend/src/lib/types.ts` | New optional book plus typed watch/plan/evidence/management fields. |
| `frontend/src/pages/MartinForwardTestPage.tsx` | New book choice and URL retention, queue/watch reasons, positions, saved management rules, equity/drawdown/SPY/QQQ. Register account-like behaviour everywhere, not only the selector. |
| `src/webapp/services/watchlist_service.py` + Top Hits types/UI | Optional namespaced `gap_retest_context` containing this evaluator's latest stage and plan. Preserve `power_gap_volume_context` and existing plans. Add the field to compact list projection. Include the current-session book decision ID/version in optional snapshot freshness metadata so completion of the book job can refresh the preview. Context reads must not run new calculations or provider calls. |
| `web/routes/api.py` | Existing `/api/admin/martin-forward-test` remains the read endpoint; extend the returned book contract. No new route unless existing access controls cannot support it. |

Use the book's saved decision for dashboard plans. At Top Hits snapshot publication, call the same pure evaluator using a decision-state read and persist its preview; mark the preview date and book/config version. A preview is not a submitted order. No writes to the book from snapshot publication or HTTP reads. If no matching saved state/inputs exist, show `Preview unavailable` rather than fabricate a plan.

The book must never depend on a Top Hits rebuild, and Top Hits must never require a successful book run to publish other scanners. Its optional book fingerprint solves the race where a snapshot finishes before the book job. Prior-session manual runs on a weekend require an explicit rebuild for that completed session; do not falsify publication timestamps to bypass the weekday scheduled gate.

Do not repurpose existing `martin_daily_structural_v1` as if it supported this new source. Do not make the frontend's generic 10–15% or 33%/33% trim presets describe this book: display its exact saved 10%-at-3R policy. A sizing link must carry the exact displayed stop, max entry, stop/entry versions and decision date; it must not pick up a different policy from a previous page selection.

## 9. UI and performance acceptance

The existing forward route gains a separate experimental book. Explain the daily-only model, eight slots, $500 planned risk, no rotation and the 10%-at-3R/EMA9 management rule. Do not show broker-order wording.

For every candidate, expose original gap/date/age, episode dates, selected EMA, confirmation test, plan levels, stop basis/date, risk %, ADR, stop/ADR, maximum entry, ranking inputs, allocation decision and plain-language reasons. Separate data issues, setup waiting, risk rejection and capacity waiting.

For each position show actual entry, original stop/R, original/remaining shares, realised P&L, marked P&L, trim history and scheduled trend exit. Show bars and source dates so users can audit the sequence.

Reuse stored daily account marks and `_benchmark_comparison` for SPY/QQQ on identical account dates. Label price-return versus total-return conventions and existing mark/benchmark limitations. Report trade count, win rate, expectancy in R, profit factor, drawdown, exposure, fill rate and capacity-rejected opportunity counts. Group optional research summaries by earnings status, ADR band and gap age; do not make extra trading gates from those reports.

## 10. Required tests and fixtures

Use unittest and the existing frontend build/mock harness. Tests must exercise state across sessions, not only compare a formula to itself.

1. Gap-only admission; volume-only excluded; age 0/1/60/61; holidays/weekends use XNYS sessions.
2. Exact touch band boundaries, lower-low requirement, EMA9 tie preference, EMA21-only setup, same-day reclaim, five-session episode expiry and an earlier expired episode discovered late.
3. Bullish close above support qualifies; MA touch alone, red close, non-rising EMA21 and missing warmup do not. Future bars cannot alter yesterday's decision.
4. Event retained after scanner absence; newer event supersedes unfilled event; original position keeps its event; one fill per event even after exit; no duplicate open ticker.
5. Reclaim-low stop, 5% equality and just over, max entry, ADR >/<3 and >/<5 remaining preferences, half-ADR caution, missing ADR and no ATR fallback.
6. Next-session open fill with slippage; open below stop cancellation; open above cap expiration even when later low touches cap; no invented fill on missing execution day; Friday order executes Monday or next exchange session.
7. Fixed shares/reservations, eight slots, $20k cap, $500 trade/$2,500 book risk, cash/participation limits, fractional-share rejection and deterministic ties. Capacity waits require fresh qualification tomorrow.
8. Entry-day stop, gap-through-stop, stop-plus-target ambiguity, single 10% trim, zero-share trim, gap above trim target, immutable initial R and next-open EMA9 exit. No entry-day trim or second 6R target.
9. Same-date/different-source rerun cannot duplicate events; concurrent launches serialize; failed transaction rolls back all book state; missing execution day prevents advancing; source failure still permits established-book risk management.
10. Round-trip a new book through repository `context()`, account marks, the existing API and UI selector. Detail/list projections retain the plan; existing book outputs and Top Hits defaults stay unchanged. Completing a book run after snapshot publication changes its optional fingerprint; a missing/failed book does not block publication.
11. Standard artifact messages and `job_run_id` link are parsed by RunService. A successful technical scan with partial enrichment is distinguishable from an execution-book failure.
12. Historical evaluator uses only as-of OHLCV and permitted metadata. Current earnings/RS/universe data cannot silently leak into old decisions.

Concrete fixture: confirmation close $100, reclaim low $97 → 3% reference risk, max entry $100. Next open $99 → model fill $99.099, shares fixed from the $100 maximum, actual initial R $2.099. An open of $100 fails after buy slippage; an open of $101 followed by low $99 also expires. A $96 opening on a later holding day exits at $95.904, not $97. Test exact arithmetic with an explicit precision policy.

Test implementation dates should be fixed historic sessions and exchange calendars injected where appropriate; do not replace production XNYS logic with generic weekdays. Use a real-calendar integration check in an environment that has the dependency.

## 11. CLI, delivery order and rollout

Implement the following CLI contract:

```bash
# Read-only evaluation; creates a reviewable JSON report, no book writes.
../venv/bin/python scripts/run_martin_gap_retest.py --as-of-date 2026-10-23 --dry-run

# Explicitly persist one completed session after prerequisites have passed.
../venv/bin/python scripts/run_martin_gap_retest.py --as-of-date 2026-10-23 --apply
```

Require exactly one execution mode; default to dry-run when omitted. Print canonical raw/watch/candidate-report/summary paths in the same recognised format as existing runners. Reports use configured shared artifact storage and atomic file replacement. Persist DB rows only under `--apply`. Reports must identify dry-run versus applied and source provenance.

Deliver in reviewable steps:

1. **Data/read contract and fixtures:** verify current fixes/coverage; freeze candidate/evidence schema and source lookup. Resolve unresolved adjustment provenance before claiming usable historical results.
2. **Pure evaluator:** implement sections 3–5; run boundary/lifecycle tests and inspect saved plans on a bounded sample. No trades yet.
3. **Paper engine/repository:** implement sections 6–7, dry-run/apply CLI, account/benchmark registration and transactional integration tests.
4. **Dashboard/Top Hits preview:** wire saved evidence and exact management/sizing, then build and run mocked user-flow checks.
5. **Operational rollout:** deploy, verify web and worker revisions, run dry-run for the latest completed session, inspect candidate and blocked-data reasons, apply once, then enable the weekday schedule. Respect the user's current deployment-wait instruction when performing that later work.

Useful validation commands after the implementation creates these tests:

```bash
../venv/bin/python -m unittest tests.test_martin_daily_gap_retest tests.test_martin_gap_retest_service tests.test_martin_gap_retest_reporting
../venv/bin/python -m unittest tests.test_history_repository tests.test_watchlist_repository tests.test_power_gap_volume_watch tests.test_power_gap_volume_watch_screen
../venv/bin/python scripts/run_martin_gap_retest.py --help
cd frontend
npm run build
```

Run the relevant existing forward-test, sizing, benchmark and snapshot tests as well. Record known baseline failures separately; a passing build or mocked browser check is not live-data proof.

Historical replay is a second rollout step using the same evaluator/execution policy, with dry-run preflight and a separate replay book. Persisted snapshots created later cannot be passed off as contemporaneous evidence. Reconstructed scanner events from adjusted daily history are retrospective research; label universe survivorship, revisions, catalyst timing and actual data coverage. No Finviz requirement should be introduced for the technical baseline. Do not replay into or overwrite existing live books.

Definition of done: one reproducible source run flows into explainable saved watches, fresh confirmations, bounded next-open paper orders, correct exits and account/benchmark marks; reruns are safe; existing books are unchanged; API/UI show the saved evidence; production reports expose accurate persistence/status. There is no requirement that the latest session produce a buy.

Not included: intraday triggers, day-one gap-and-go, EMA50/SMA200 or AVWAP entry variants, automatic rotation, pyramiding, re-entry for the same gap, 3R/6R one-third scaling, additional market-regime filters, a profitability claim, or brokerage integration.
