# Power Gap & Volume Watch v1

## Objective and scope

Add one daily screener and a dedicated Top Hits preset to discover substantial gaps and recent record-volume events, retain them for later setup review, and explain their earnings context. Reuse stored daily OHLCV and the event enrichment committed in `33eb72f`.

Source: TXInvestor's supplied “Power Earnings Gap (PEG) & HVE” Pine v6 script, supplied at `/Users/Zihao.Guan/.codex/attachments/4b436245-71d3-430c-b37c-2ad4e55c0584/Pasted text.txt`.

This is an explicitly adapted strategy, not a claim of exact TradingView parity: earnings qualification and earnings-based resets are removed. Preserve source attribution and applicable license notices when deriving code from the script. Existing PEG/HVE screeners, Top Hits defaults, saved presets, stop policies and forward-test books retain their behavior.

## 1. Define and test the detector

Stable scanner ID: `power_gap_volume_watch`. Rules version: `power_gap_volume_watch_v1`.

Evaluate completed regular-session daily bars through the requested exchange session. Reject malformed OHLCV, mixed adjustment bases and duplicate dates. A missing required observation is an explicit coverage issue, not a fabricated match or a successful empty result.

Coverage problems affect only the relevant ticker/component. Distinguish a genuinely recent listing from unexpectedly missing history; neither should abort other tickers' valid results.

| Component | v1 definition |
| --- | --- |
| Opening gap | `(open / previous_close - 1) * 100 >= 10` |
| Relative volume | Event volume / 20-session SMA of volume >= 3; the SMA includes the event bar, as in the source |
| Event trend | Event-day close strictly above its 200-session SMA |
| Close strength | `(close - low) / (high - low) * 100 >= 50`; zero-range bars fail this condition |
| New gap event | All four price/volume tests pass; retain the supplied `bar_index - last_trigger_index > 1` duplicate suppression |
| Active gap watch | Most recent triggered gap is 0–60 trading sessions old; expires at age 61 |
| Earnings | Informational enrichment only; no EPS beat, report-presence gate or earnings-based watch reset |
| Recent HVY | Location of the maximum volume in the trailing 252-bar window, with its event within 30 calendar days |
| Recent HVE | Preserve the source's daily running-record plus prior-week 250-week volume guard; its recorded event must be within 30 calendar days |

Gap, HVE and HVY are independent signals. A row passes if any is active; never require their intersection. A nonqualifying later bar does not erase an earlier gap event. A later triggered gap replaces the gap reference. Preserve event dates and values at the event, separately from current price.

The HVE guard compares daily volume with weekly totals; do not silently replace it with the existing conventional HVE detector. Implement and document the weekly-series alignment against saved Pine reference cases, using only data available at each evaluation date. If historical versus live Pine higher-timeframe mapping differs, document that difference rather than advertise exact parity.

“Ever” means available input history, not the company's lifetime. Persist history start/end and coverage. Label incomplete HVY history as incomplete rather than claim a full year's record. Gap detection requires 200 valid warmup sessions; volume-only discovery need not inherit that requirement. Do not invent an IPO exception to the SMA200 rule.

## 2. Persist distinct event and current context

Store a namespaced `power_gap_volume_context` on each hit:

- `rules_version`, `as_of_date`, active signal kinds and source/history coverage.
- `gap_event`: original date, age in trading sessions, opening gap, volume ratio/baseline, close strength, OHLC levels and event SMA200.
- `volume_events`: separate HVE/HVY dates, values, ages in calendar days, algorithms and coverage.
- `current_context`: latest close, distance from event levels, whether the gap-day low has since been breached, and existing extension/setup evidence when available.
- `earnings_context`: enrichment for the original gap date, with status, source, retrieval time and retrospective-evidence label.

Keep scanner publication date, historical event date and current setup-confirmation date separate. A 40-session-old gap is still an active historical event, not a fresh gap or current entry trigger.

The gap-day low is a reference level, not an automatically valid stop for every later entry. A breached event reference remains visible as context; it must not silently invalidate an unrelated, later supported setup on the same ticker.

## 3. Enrich shortlisted gaps, not the whole universe

After technical detection, call `build_earnings_event_contexts` only for distinct `(ticker, gap_event_date)` pairs. Supply the requested as-of cutoff and retain per-row identity. Do not match an old gap against today's latest or next earnings report.

Use the existing configured Yahoo/FMP/AInvest provider and positive event cache. Surface confirmed earnings, nearby earnings with uncertain timing, known mismatched reaction sessions, unknown catalysts, and lookup failures distinctly. No event or failed enrichment does not remove a technical hit.

Do network enrichment during the scanner job, once, with bounded requests. Persist the result in its artifacts; HTTP reads and five-minute Top Hits rebuilds must not repeat provider lookups. Volume-only rows without a gap should not borrow an unrelated gap's earnings explanation.

No TradingView account dependency in v1. A saved TradingView earnings-export script can be evaluated separately later. Historical enrichment remains retrospective unless actual contemporaneous availability is recorded.

## 4. Wire the scanner into the existing application

Use the established detector, watchlist builder and runner pattern with database-first daily history. Do not run an independent external price download for every ticker. Load bounded batches and slice each ticker's history to the as-of date. Keep existing universe/exclusion behavior and distinguish provider/data failures from no matches.

Required integration points:

- New detector module, watchlist builder and `scripts/run_power_gap_volume_watch.py`.
- `src/screener_catalog.py`: daily-bar inputs, appropriate warmup/history and evaluator.
- `src/artifact_paths.py`: canonical artifacts and persisted strategy identity.
- `src/webapp/services/run_service.py`: manual/scheduled action and supported date/config fields.
- `src/webapp/repositories/watchlist_repository.py`: strategy/group mapping; do not let results fall into `other`.
- `src/overlap_summary.py`: scanner registration and coherent discovery categorization; a bearish record-volume event must not be presented as bullish confirmation merely because volume is exceptional.
- `src/webapp/services/watchlist_service.py`: scanner definition and lossless propagation/merge of the namespaced context into table and guru/readiness snapshot rows.
- Frontend response types and scanner descriptions.

One ticker matching multiple events contributes one scanner match, with multiple signal tags—not three independent overlap points. Preserve fields when it also matches other scanners. Source-specific context must not overwrite another scanner's trade plan or earnings-risk fields.

## 5. Add the Top Hits preset and explain the context

Add built-in preset **Power Gap & Volume Watch**, based on empty filters rather than the Leadership + Trend default:

- One scanner group containing `power_gap_volume_watch`; no inherited RS90 or leadership-scanner requirement.
- Existing global universe/exclusion constraints remain in force; optional user filters stay explicit.
- Default to list view showing active power gaps. Offer volume-only signals as an explicit signal-type filter.
- Sort by gap-event recency, then event volume ratio, then available RS and ticker; define deterministic ordering for volume-only rows.
- Reuse existing sector grouping, charts, My Pick and readiness navigation.

Compact event summary example: `Gap +12.4% · 4.1× volume · 8 sessions ago`, plus `Earnings confirmed` or an explicit unknown/failure label. Keep HVE/HVY ages and history limitations accessible in details.

Useful filters: new gap today, active gap watch, recent HVE, recent HVY, earnings-context status and event age. Integrate filters/sorts with server pagination and saved preset state wherever the current route performs those operations. Avoid a separate route or new score in this change.

### Readiness boundary

Discovery status and entry readiness are separate. Event-only candidates belong in **Leader / Waiting** with an explanation such as `Gap detected; awaiting a supported daily entry setup`. They must not default to Support Zone or Plan Ready solely because the new scanner matched.

When another supported setup supplies confirmation and a complete plan, retain the existing stop-policy, risk, extension, earnings-risk and sizing checks. Display the new event context alongside that plan. Do not manufacture a trigger or stop from the gap candle just to populate a card.

This may require a narrow backend/frontend context-resolution correction for event-only rows. Test that it does not change the stages of existing non-gap candidates.

## 6. Schedule and snapshot freshness

Run the new scanner daily after successful market-data reload and early enough to finish before the intended post-close Top Hits publication. Use dependency completion, not only wall-clock ordering. Preserve the separately configured weekly schedules.

Register the new source with Top Hits freshness/fingerprint handling so a completed scanner run appears after automatic snapshot publication, without a manual Refresh Board click. Do not accidentally make every existing forward-test book depend on this optional research source. Define optional-source handling explicitly: unavailable/stale new results are labelled and excluded from current matches, while the prior coherent snapshot is retained when required inputs are incomplete.

An earnings-provider failure is partial enrichment, not failed technical discovery. Publish valid technical hits with explicit context status and accurate run counts.

## 7. Acceptance checks and rollout

1. Detector fixtures cover threshold equality, SMA200 strict comparison, inclusive volume baseline, zero-range candles, adjacent-bar suppression and age 60/61 expiry.
2. Verify HVE/HVY independently, weekly alignment, weekend/holiday calendar ages, partial history, tied maxima and no future-bar leakage. Keep reference examples from the supplied script and document intentional earnings-related deviations.
3. An earnings failure leaves the hit present. Event enrichment uses the gap date, including an old gap, repeated ticker/different event dates, Friday AMC and exchange holidays.
4. Persisted artifacts reload under the correct scanner group. Table and readiness rows retain all event context. Three signal tags still count as one scanner.
5. The new preset finds a gap-only ticker without requiring RS90 or another leadership scanner. Existing defaults and saved presets remain unchanged.
6. Event-only rows wait for a setup; rows with existing supported plans retain consistent readiness, management targets and Size Position inputs under each stop policy.
7. A completed job updates snapshot freshness and automatic publication; enrichment is not refetched by API reads or five-minute rebuilds.
8. Run focused backend tests, frontend build and a mocked UI check, then a bounded live scan and event-enrichment sample after deployment. Keep these evidence boundaries explicit.

Deliver in three reviewable changes: detector/job and artifacts; event-context/snapshot integration; Top Hits preset/display. Push/deploy the existing earnings-enrichment commit as part of the agreed rollout, not as an unannounced production action. Preserve unrelated local work.

User-required implementation model: delegate to a **GPT-6 Luna subagent with medium reasoning**, followed by independent parent review and verification. This document is a plan; implementation and delivery follow the user's subsequent instruction.

Do not add the scanner to `FOCUS_SCANNERS` or any existing Martin book in this change. After discovery quality is validated, propose a separately versioned experiment for admitting gap candidates. No broker integration, intraday triggers, alerts, automatic buys or historical performance promises.
