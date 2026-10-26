# Martin Bar Retest execution v2

V2 is an isolated execution book. V1 remains the replay default. Freeze regular-session TradingView bars for every ticker/session needed by the replay, then pass the directory to the v2 replay:

```sh
python scripts/audit_martin_intraday_execution.py \
  --book-id martin_range_bar_retest_freeze \
  --from-date 2026-10-01 --to-date 2026-10-23 \
  --client-dir /path/to/tradingview-client \
  --output-dir /path/to/frozen-bars \
  --freeze-tickers AAPL,MSFT,NVDA --one-minute

python scripts/replay_martin_bar_retest_range.py \
  --from-date 2026-10-01 --to-date 2026-10-23 \
  --book-id martin_range_bar_retest_v2_trial \
  --execution-version v2 --intraday-dir /path/to/frozen-bars
```

Use `--apply` to save the replay; without it, the replay rolls back. The freeze command writes `<TICKER>_<DATE>.json` bundles with 15-minute bars, feed metadata, query settings, retrieval time, and fingerprints in `audit.json`. `--one-minute` includes 1-minute sessions for ambiguity refinement. Freeze all symbols and dates where orders can execute. Missing or invalid intraday evidence uses the existing conservative daily settlement, records the evidence failure on the event, and marks the session partial. Tampered bundle or manifest hashes abort the replay.

For a scheduled v2 continuation, the continuation job fetches the current pending-order tickers after `require_completed_session`, writes immutable bundles under `<artifacts_dir>/martin_intraday_execution_v2/<book>/<date>_<attempt>/`, then applies the session. It uses `TRADINGVIEW_CLIENT_DIR` when set, otherwise `/opt/tradingview/node_modules/@mathieuc/tradingview`. Provider failures are stored in the manifest and use the marked daily fallback. To reuse a reviewed bundle for replay/operator continuation, pass `--intraday-dir <bundle-dir>` or provide that path in the RunService continuation action. Continuation reads the saved book version, so v1 books keep v1 handling.
