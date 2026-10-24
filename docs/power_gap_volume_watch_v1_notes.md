# Power Gap & Volume Watch v1 implementation notes

The detector is an adaptation of TXInvestor's supplied Pine v6 “Power Earnings
Gap (PEG) & HVE” script (MPL-2.0). It removes the earnings qualification and
earnings-based reset; earnings are optional retrospective context.

The source's HVE test compares daily volume with the largest of the preceding
250 weekly volume bars. This implementation groups daily volume into ISO
calendar weeks and compares an event only with the last 250 weekly totals whose
weeks ended before that event's week. This avoids using the unfinished current
week or future data. TradingView's historical versus live higher-timeframe
mapping may differ, so this is not asserted as exact Pine parity. `weeklyMaxVol`
is recomputed from the bounded input history; HVE “ever” therefore means only
the supplied history. HVY completeness is only claimed when the caller supplies
an expected-session calendar that confirms 252 observations.

Age is counted in observed trading bars for the gap (0–60 inclusive) and
calendar days for HVE/HVY (0–30 inclusive). Tied HVY maxima select the latest
bar, matching Pine's `highestbars` offset-zero behavior. OHLCV validation,
duplicate dates, adjustment-basis consistency, requested as-of slicing, and
optional expected-session coverage checks are explicit. Provider/event
enrichment is limited to distinct gap ticker/date pairs; callers should invoke
it once during a scanner job and persist the returned context.

## Running and viewing results

Use the **Run Power Gap & Volume Watch** action, or run:

```bash
../venv/bin/python scripts/run_power_gap_volume_watch.py --as-of-date 2026-10-23
```

The runner requires the existing PostgreSQL connection and `exchange_calendars`.
It reads stored OHLCV in batches of 200 names, resolves the latest completed
XNYS session on or before the requested date, and writes raw results, a watchlist
and a run summary using the standard artifact paths. Errors are reported per
ticker; optional earnings failures leave technical hits present. Missing older
sessions can disable HVE without suppressing a gap or HVY whose recent window
is complete. A short supplied history is not evidence of a recent IPO.

In Top Hits, select **Power Gap & Volume Watch**. It starts with active gaps,
without an RS90 or additional leadership-scanner requirement. Signal, earnings,
new-gap and maximum-age filters are saved with custom presets. Event details
explain coverage and retrospective earnings evidence. Gap-only candidates are
discovery candidates awaiting a supported daily setup, not automatic entries.

For daily operation, create a weekday schedule after the market-data reload in
America/New_York. The existing schedule service adds the required reload
dependency from the scanner's daily-bar catalog inputs. The new source is
optional for Top Hits and existing Martin books; its completion participates in
the existing snapshot fingerprint and automatic refresh. Stale source matches
are excluded. No production schedule is created by this code change.
