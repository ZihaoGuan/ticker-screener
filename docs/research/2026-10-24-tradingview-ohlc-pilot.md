# TradingView historical OHLCV refresh — 24 October 2026

The user selected TradingView historical candles as the trusted replacement source and authorized expanding the A/AA/AADX pilot to the scan universe. Unavailable symbols are skipped without a Yahoo fallback. This work replaces existing Yahoo-labelled OHLCV, not just range-invalid rows.

## Pilot evidence

Read-only production export at 2026-10-24T07:56:31Z confirmed all ten audit defects were unchanged. The initial comparison covered 267 stored sessions from 2026-06-18 through 2026-10-23 and 801 TradingView candles across split, unadjusted, and dividend settings.

- Invalid stored bars: A 4, AA 3, AADX 3. All corresponding TradingView candles passed strict validation.
- All 267 opens/closes matched within 0.00002; 19 neighbouring valid stored sessions also matched. This tolerance describes comparison only; range validation uses no tolerance.
- Split and unadjusted TradingView OHLC were identical in this window. Dividend adjustment changed 75 A bars and 36 AA bars. AADX had no adjustment differences.
- 58 rows had OHLC differences greater than one cent, including 49 rows whose stored ranges were valid. Volume differed on 262/267 sessions.
- Requested identities were `NYSE:A`, `NYSE:AA`, `NYSE:AADX`. TradingView returned `pro_name` matching these identities, NYSE listing metadata, USD, America/New_York, regular session, and resolved feed identities `BATS:A`, `BATS:AA`, `BATS:AADX` (Cboe One). The actual resolved feed is recorded for every ticker; these are not represented as entitled NYSE feeds.
- Daily timestamps were 09:30 New York time, with 89 expected sessions and no missing, duplicate, holiday, nonfinite, nonpositive, or negative-volume bars in each series.

The subsequent full-history pilot fetched 1,712 sessions each for A and AA, starting 2020-01-02, and 100 for AADX, starting 2026-06-03. It applied 3,503 Yahoo replacements (1,705 + 1,705 + 93). Other providers' rows were preserved. Post-apply database fingerprints matched backups' after-images; scanner range validation passed all three full series; coverage and corporate-action values were unchanged.

Local initial evidence is under `output/ohlc-repair-pilot/2026-10-24/`: `live_rows.json`, nine TradingView response files, `comparison.csv`, `comparison_summary.json`, `candidate_manifest.json`, `neighbours.json`, and the rerunnable offline `compare.py`. The initial comparison remains a pre-repair evidence snapshot.

## Cause and limits

Evidence favours stale or incomplete ingestion-time ranges/volumes over wrong dates or a uniform adjustment mismatch. Opens and closes align with TradingView, while the inconsistent extreme and volumes differ. This is an inference; the original ingestion responses are unavailable, so upstream revision versus ingestion-time assembly is not conclusively separated.

In the original 33,327-row audit, 32,751 violations involved the open lying outside high/low. 30,020 rows had update timestamps in the 21:00 UTC hour. Date clusters therefore warrant examination of the daily ingestion snapshot, not simply price precision.

Historical writer gaps are demonstrated in Git: full-history source validation was introduced by `cfef906` on 14 October (NZ time); `f2ec2e7` removed reload numeric/range validation later that day; `f1cee35` restored per-provider/replacement validation on 17 October. These gaps explain how malformed rows could persist, but do not establish which upstream response produced each row.

Yahoo was queried only as a diagnostic cross-check before the user's source preference was clarified. No Yahoo response is a repair candidate or repair fallback.

## Universe refresh command

`scripts/refresh_tradingview_history.py` uses the production configured exchange universe, active database metadata, all configured manual/automatic/special-security exclusions, and suffix filters. All exclusion text lists win even if a manual include overlaps. The live scope audit found 3,439 eligible tickers and approximately 4.75 million Yahoo-labelled rows. Production-only lists, including `no-price-3d.txt`, are read in production.

The command requests regular-session daily split-adjusted candles from each ticker's earliest existing Yahoo date through a completed XNYS session, with a 20,000-candle cap. It replaces only returned dates already carrying a Yahoo source (`yfinance`, `yfinance+nasdaq`, `yfinance_historical_repair`, `yahoo`, `yahoo_finance`). It does not insert or delete price dates, synthesize extrema, or replace other providers' rows. TradingView fractional historical volume is rounded to the nearest whole share (half up) for the existing integer database column; the exact raw value remains in the provider cache and the outcome counts affected candles. OHLC validation remains strict. Missing provider dates remain stored and are counted. Partial coverage with material close-basis differences is skipped to avoid mixing adjustment regimes.

Each ticker has a frozen full-row baseline, compressed provider cache, requested/resolved identity, retrieval timestamp, before/after backup, and outcome. Apply rechecks active status, exclusions and every baseline field under locks. It verifies exact persisted candles, unchanged coverage, unchanged untargeted rows and unchanged `adj_close`, dividends and split factors before commit. Provider/validation failures are isolated per ticker. Four bounded workers are used for the production run.

```sh
python scripts/refresh_tradingview_history.py \
  --client-dir /path/to/built/TradingView-API \
  --end-date 2026-10-23 \
  --artifacts-dir /durable/path/to/run
# Add --apply after reviewing the dry-run; repeat the same arguments to resume.
# Add --tickers A AA AADX for a pilot. Exclusions still apply to explicit tickers.
# Use --rollback instead of --apply with the same scope to restore this run.
```

Rollback refuses to overwrite any later database change and is idempotent. Use a new artifact directory for a new refresh or after a deliberate rollback. Existing outcome files are resume checkpoints, including skips. Credentials, if needed, come only from `TRADINGVIEW_SESSION` and `TRADINGVIEW_SIGNATURE`; anonymous access was used for this run.

Durable production artifacts are `/artifacts/tradingview-history/2026-10-24/`: `pilot/`, `universe/`, `universe.log`, `universe-process.json`, and the isolated `runtime/`. The client was built from a temporary copy of the local community TradingView API, leaving that checkout untouched. No dependency installation was performed in the application image.

## Writer preservation and verification

The existing daily-bar upsert, per-ticker replacement, and daily replacement now preserve TradingView-labelled rows from non-TradingView writers. This retains existing coverage-loss checks. Writer counts use actual affected rows, so protected rows are not falsely reported as persisted by Yahoo. The two guarded files were backed up under `runtime/pre-guard-files/` and installed in the running web and four normal worker containers before pilot apply. Container image tags did not change during this installation; a future container recreation requires delivery of these source changes.

59 focused unit tests pass. A separate integration probe used session-local temporary tables shadowing `daily_bars` and `ticker_metadata`; it verified partial apply, exact backups, coverage/corporate-action preservation, rollback, rollback idempotency, refusal after subsequent writes, exclusion rechecks, and all three Yahoo-overwrite guards. Permanent market tables were not changed by that probe.

Original screen runs, Top Hits publications, portfolio/Martin-book history and historical performance snapshots are not overwritten by the refresh. The completed universe pass and final verification are recorded below.


## Completed universe pass

Final read-only verification finished at **2026-10-24T08:58:44Z** (21:58:44 Pacific/Auckland). All 3,439 scoped ticker images matched their exact saved database images, including empty histories. No baseline was missing; none of the applied tickers was excluded or inactive at final verification.

| Outcome | Count |
| --- | ---: |
| Eligible universe | 3,439 tickers |
| Refreshed, including the pilot | 3,195 tickers |
| Yahoo bars replaced with TradingView | 4,478,609 |
| Stored rows before and after | 4,788,883 → 4,788,883 |
| Yahoo-labelled rows remaining | 270,584 |
| Invalid OHLC ranges in this universe | 14,849 → 1,651 |
| Complete before/after backups with unchanged coverage and corporate actions | 3,195 |
| Database-image mismatches | 0 |

This replaced **94.3% of the eligible universe's original Yahoo-labelled bars**. The handoff's 33,327 invalid rows covered all stored tickers; the 14,849 baseline here is only the eligible universe. They are different populations.

The bulk process exited 0 after checking 3,439/3,439 tickers. Its own ledger records 3,192 applied tickers and 4,475,106 writes, plus the previously verified pilot's 3 tickers and 3,503 writes. The pilot is counted once above.

| Remaining ticker outcome, excluding the already applied pilot | Tickers |
| --- | ---: |
| Non-stock instruments (bonds, rights, warrants, etc.) | 126 |
| Partial TradingView history with material adjustment/identity-basis differences | 95 |
| TradingView symbol unavailable: `NYSE:ACA` | 1 |
| No Yahoo-labelled rows to replace | 22 |

No Yahoo fallback was used. Missing or ambiguous data did not cause deletion or synthetic price repairs. The 95 partial-basis cases need individual history/identity investigation before a full-series rebase; this pass leaves them unchanged.

The 1,651 remaining inconsistent ranges are fully classified:

| Reason | Rows |
| --- | ---: |
| Skipped non-stock instruments | 1,450 |
| Skipped partial-history/basis differences | 184 |
| Unavailable ACA symbol | 10 |
| Provider did not return these dates (BCG, SSBI) | 2 |
| Nasdaq-labelled rows preserved outside Yahoo replacement scope | 5 |

**No TradingView-labelled rows have an inconsistent range.** Original `adj_close`, dividends and split factors remain unchanged. Other providers' row counts remain unchanged. The latest screening-feature table had no rows for 20–23 October, so there was no current feature snapshot to invalidate. Historical screen runs, Top Hits publications and portfolio/performance history were not rebuilt or rewritten.

Local review artifacts:

- `output/ohlc-repair-pilot/2026-10-24/universe_apply_summary.json`
- `output/ohlc-repair-pilot/2026-10-24/universe_verification.json`
- `output/ohlc-repair-pilot/2026-10-24/skipped_tickers.csv`
- `output/ohlc-repair-pilot/2026-10-24/remaining_invalid_ohlc.csv`
- `output/ohlc-repair-pilot/2026-10-24/remaining_invalid_ohlc_summary.json`

The original TradingView checkout and eight unrelated modified files were preserved; those eight files still match their preservation checksums. The refresh tools, writer guards and focused tests accompany this report. At verification time, the writer guards were installed in the running containers; **a permanent image rollout was still required** so a later container recreation would retain them. No frontend build was needed for these Python/Node data tools.
