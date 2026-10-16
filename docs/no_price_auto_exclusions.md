# No-price automatic exclusions

`scripts/reload_postgres_market_data_date.py` records a ticker as missing only
when Massive has no usable bar and both structured Yahoo passes fail to provide
a bar dated for the requested session. A ticker is added after three distinct,
consecutive US market sessions. Streaks advance only after replacement rows
pass validation and the atomic Postgres replacement commits. Failed Yahoo
chunks and a broad missing-data wave suppress streak updates for that reload.

The exclusion registry is
`config/auto_exclude_tickers/no-price-3d.txt`. Its dated reason lines remain
readable by the shared exclusion loader, so reloads and screeners apply the
same policy. Durable streak and probe dates live at
`$TICKER_SCREENER_ARTIFACTS_DIR/status/no-price-3d-state.json` (defaulting to
the repository's `artifacts/status` directory). A file lock serializes updates
on the shared filesystem and each file replacement is atomic. Production
containers already share `/app/config` and `/artifacts`. Multiple servers can
share this state when those paths use the same filesystem and honor POSIX file
locks; independent per-host mounts would need a centralized store.

Reloads probe at most ten auto-excluded tickers that have not been probed for
five market sessions. Valid dated data removes the ticker from the automatic
registry and clears its missing streak. When more than ten exclusions are due,
the probe selection rotates between batches. This recovery depends on daily
reload jobs continuing to run; it does not launch a separate job. Session
counting handles regular US market holidays and weekends, but not exceptional
one-off closures.

Manual inclusion continues to override exclusion in `load_excluded_tickers`.
Five-character symbols ending in `R` are filtered structurally as rights,
while other symbol lengths retain the prior behavior.
