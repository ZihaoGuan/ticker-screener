# Martin range replay: reconstructed inputs v1

Status: implementation handoff, 25 October 2026. This plan covers the existing Account, Sector, Top 5, RS Expanded, and Rotation policies. Gap Retest already has a separate dated input loader.

## Outcome and evidence boundary

Replay every NYSE session from **25 September through 23 October 2026** into a new isolated book using one version of the current Martin policy and historical inputs. Do not insert a backdated `scanner_top_hits_snapshot` or claim that reconstructed rows were published at the time. Identify them as `martin_reconstructed_inputs_v1`, with the source code/config version and dated evidence saved in the replay run.

Production audit: 10 of 21 sessions have Top Hits snapshots accepted by `choose_snapshot` in `src/webapp/services/martin_rs_replay.py`. Five sessions have no snapshot; six have one whose `screen_runs.created_at` precedes that session's close. The latest contiguous saved-snapshot range is 15–23 October. Reconstruct **all 21 sessions** for this experiment; use the 10 accepted snapshots for calibration only, so one replay book never mixes input policies.

The existing `scripts/replay_martin_range.py` is the session runner. Preserve its default saved-snapshot mode, sequential session processing, empty-book guard, dry-run rollback, and all-or-nothing apply transaction. The current runner and related service changes are present as uncommitted local work; preserve unrelated working-tree changes and stage only intended files when implementation is later committed.

## 1. Build a source manifest from existing definitions

Derive the dependency set in code from `FOCUS_SCANNERS`, `_GURU_AVAILABLE_SCANNER_IDS`, `STRIKE_ZONE_SCANNER_IDS`, the three supported plan scanner IDs, and `rs_phase`, `weinstein_stage_analysis`, `rmv_tightness`. The current union has 47 distinct IDs: 40 configured board scanners and seven additional watchlist IDs. Do not copy a fixed list of 47 names into another maintained catalog. Persist a manifest/version digest with the reconstruction.

Use the seven Focus scanners across the as-of universe to determine candidate membership. Then gather every Guru and strike-zone source for those candidates. Some source scores or ranks are cross-sectional: calculate those over their required historical universe before projecting the result onto Focus tickers. Scanner absence has three states: an executed empty result, a confirmed source that did not yet exist, and an unavailable/failed source. Never turn unavailable into an empty result.

The 17 Guru and strike-zone context sources contribute only when they were published or can be recalculated. An optional source that demonstrably did not exist by `d` contributes no signal, with that absence recorded. A missing required Focus source can change the candidate universe; a missing plan source can change order eligibility. Both require recalculation or a blocker. Do not require every one of the 47 source IDs to have a run on every date.

Relevant code: `src/webapp/services/martin_forward_test_service.py`, `src/webapp/services/watchlist_service.py`, `src/webapp/services/strike_zone.py`, and `src/screener_catalog.py`. Reuse their evaluators and pure row/plan functions. Add one narrow reconstruction module rather than a second general screener framework.

## 2. Enforce the historical data boundary

For session `d`, read only bars through `d`; load enough prior NYSE sessions for the longest selected evaluator and existing Martin 260-session qualification. Use the market calendar, not dates that happen to appear in `daily_bars`. Validate OHLCV and adjustment basis with the existing replay checks. Define the historical universe from symbols observable by `d`, including names now inactive; current `ticker_metadata.is_active` must not silently remove past candidates.

For saved scanner runs or watchlists, require `run_date <= d` and proof that their data were available before the next session open. Keep source run ID, signal date, creation/completion time, code/config identity, and whether the result is saved or recalculated. A scanner whose original run is missing may be recalculated only if its existing evaluator can run deterministically from the permitted historical inputs. If its evaluator needs unavailable external data, report the blocker. A scanner added after `d` is a retrospective current-policy calculation, not a contemporaneous observation.

The replay row must also obtain as-of close, market cap, daily RS rating, sector and any available sector momentum, earnings risk, position action, Weinstein stage, RMV, pullback quality, ADR14, structural stop/plan, strike-zone state, readiness, and final Guru sort inputs. Audit availability before implementing each lookup. In particular, `_attach_latest_market_snapshots()` uses `date.today()` and `_attach_latest_rating_snapshots()` reads latest ratings; the reconstructed path must not call these unchanged. An earnings date learned only later is not contemporaneous earnings evidence. Preserve the current Martin policy's treatment of unknown earnings and sector momentum while recording that evidence as unknown; block only when a required gate or ordering input cannot be evaluated. Missing evidence that could change the Focus universe or top-N order blocks the session.

Read-only production calibration found no `sector_momentum` object on any Focus row in the valid 20 and 23 October snapshots (94 and 103 rows respectively). Earnings days are also absent on some Focus rows. Therefore neither field may be made a universal blocker in this reconstructed version; compare its handling with the saved policy on the ten accepted dates.

## 3. Reconstruct a snapshot-compatible Martin input

For each session, produce the `target_trading_date` and `guru_board.rows` shape consumed by `MartinForwardTestService`, but tag it as reconstructed and keep its provenance separate from `screen_runs`. Build candidate union, merge source evidence, calculate plans and readiness, then apply `_guru_row_sort_key` using as-of values. Do not reuse a live Top Hits payload or relabel a current scanner result as historical.

Return a concise per-session report: source coverage, recalculated sources, unavailable sources, candidate count, blocked candidates, rank uncertainty, and data cutoffs. Every Focus candidate must carry the signals, plan inputs, rating source, and bar revision used for its decision. A session with no candidates is still a valid input if the Focus scans completed and the source coverage is established.

## 4. Connect the existing range runner

Add `--input-source saved|reconstructed` to `scripts/replay_martin_range.py`, defaulting to `saved`. The new loader is for the five Top Hits policies only; Gap Retest keeps its existing loader. Preflight all sessions before any book write. Fail with dated reasons if a session's candidate universe or rank cannot be certified. Persist `input_source`, manifest/version digest, data cutoff, and source fingerprint in the isolated book's run metadata. Preserve the existing `martin_range_` book ID rule, dry-run default, and transaction rollback on partial execution.

Example after implementation, with the database configured:

```bash
../venv/bin/python scripts/replay_martin_range.py \
  --policy rs_expanded \
  --from-date 2026-09-25 --to-date 2026-10-23 \
  --input-source reconstructed \
  --book-id martin_range_rs_expanded_20260925_20261023_v1
```

`--apply` is a separate run into a **fresh** book ID after reviewing the dry-run report. Reuse `policy_id` and `book_id` separation; do not modify scheduled book IDs or published Top Hits snapshots.

## 5. Prove the reconstruction and report limits

On each of the 10 accepted snapshot dates, compare saved and reconstructed Focus membership, eligibility/Avoid reasons, entry/stop/max-entry, readiness, `strike_score`, ADR, scanner count, Guru rank, selected orders, and fills. Report differences with their source IDs. Resolve unexplained differences that alter candidate eligibility or orders; explicitly label any difference caused by applying today's versioned policy retrospectively.

Focused checks must cover: future-bar and future-rating rejection; unavailable versus executed-empty scanner results; current-inactive historical ticker inclusion; source rank requiring a full universe; missing earnings/ratings blocking; zero-candidate sessions; order priority; and rollback without writes. Run the existing Martin runner/service checks plus these focused cases. Finish with a 21-session read-only preflight and dry-run in an environment with production-equivalent data. If that environment is unavailable, report the exact blockers and leave application for the deployment step.

Acceptance: one versioned reconstructed feed covers every NYSE session or reports an explicit dated blocker; no later data can enter a decision; saved snapshots remain unchanged; the runner either completes all sessions into a fresh book or writes none; and the calibration report explains every order-changing difference.

Implementation risk: historical ratings, earnings publication times, or non-price scanner inputs may not be recoverable. Report those gaps rather than treating them as negative signals or promising an exact historical Top Hits reproduction.
