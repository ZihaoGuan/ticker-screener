# Martin Bar Retest execution v3

V3 is a new isolated execution policy. V1 remains the replay default, and saved v2 books retain their original opening-fill agreement rule. V3 keeps v2's frozen regular-session 15-minute execution, one-minute refinement, and one-third 3R trim management.

The v3 change applies to a complete, valid regular-session BATS alternate feed only when its high/low stays inside the stored daily high/low envelope. Those lower-timeframe bars determine limit touch and stop order even when their opening price differs from the stored daily opening price. The fill evidence records the mismatch and identifies the feed used. Incomplete, invalid, or out-of-range data uses the marked daily fallback. One-minute same-bar ambiguity handling remains conservative when one-minute evidence cannot determine the sequence.

TradingView symbol lookup is attempted only after `SYMBOL_ERROR`. The fetcher searches US stocks for an exact ticker and retries only when one unique supported US listing is returned. It records the requested symbol, resolved listing, and initial error code in the frozen data and audit manifest. Ambiguous results, other instrument types, and non-US listings do not trigger a retry.

Freeze data using the v3 audit policy, then pass the resulting directory to a v3 replay:

```sh
python scripts/audit_martin_intraday_execution.py \
  --book-id martin_range_bar_retest_freeze \
  --from-date 2026-10-01 --to-date 2026-10-23 \
  --client-dir /path/to/tradingview-client \
  --output-dir /path/to/frozen-bars \
  --freeze-tickers AAPL,MSFT,NVDA --one-minute --execution-version v3

python scripts/replay_martin_bar_retest_range.py \
  --from-date 2026-10-01 --to-date 2026-10-23 \
  --book-id martin_range_bar_retest_v3_trial \
  --execution-version v3 --intraday-dir /path/to/frozen-bars
```

Use `--apply` to save the replay; without it, the replay rolls back. Continuation reads the version stored on the book and applies the same v3 policy automatically.
