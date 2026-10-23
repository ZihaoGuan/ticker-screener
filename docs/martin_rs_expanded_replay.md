# RS Expanded historical replay

Install the project runtime requirements first (`requirements.txt` and
`requirements-web.txt`). The command requires PostgreSQL and an already-initialized
application schema. It never initializes or alters the schema.

From the app directory, check inputs without writing:

```bash
python scripts/replay_martin_rs_expanded.py --from-date 2026-10-19 --to-date 2026-10-22 --dry-run
```

The default is also dry-run. This validates inputs, not simulated fill counts or
performance. Exit code 2 means missing/invalid inputs or an existing book; inspect
the JSON report. If ready, explicitly save:

```bash
python scripts/replay_martin_rs_expanded.py --from-date 2026-10-19 --to-date 2026-10-22 --apply
```

## Boundaries

- **RS Expanded must be empty**, including runs, positions and events. This first
  version does not merge, resume, delete or back-insert into existing history.
  A second apply refuses an already-populated book rather than duplicating trades.
- The complete range preflights before saving. Apply holds the same advisory book
  lock as scheduled execution and commits the entire range atomically. An error
  or incomplete session rolls back the range. Deploy this version to every daily
  worker before applying; old workers do not honor the new lock.
- The [XNYS exchange calendar](https://github.com/gerrymanoim/exchange_calendars)
  supplies real session dates, closes and following opens. Every requested session
  must be completed; missing SPY bars are not treated as exchange holidays.
- Only stored Top Hits `guru_board` rows are eligible. Both the database creation
  timestamp and payload `generated_at` must be after that session's close and
  before the next session's open. An overwritten snapshot with an old creation
  timestamp and newer payload is rejected. The latest qualifying publication wins.
- The original universe, ratings and Avoid decisions are retained. Structural v1
  is reconstructed on a copy using bars no later than each session. Original Top
  Hits history is never rewritten. Missing historical snapshots cannot be replaced
  by rebuilding today's Finviz/ratings board with an old date label.
  If saved ADR14 is missing, only the structural variant derives it from the 14
  historical completed bars; its evidence records that source. No rating is derived.
- Preflight conservatively requires valid OHLCV for each previously seen focus
  candidate's 35-session window on every subsequent replay session (even if it
  would not actually be held), plus the 205-session emerging-tier evidence and
  enough MA-retest evidence. Missing inputs block saving; they are not silently
  skipped. Inspect the date/ticker errors rather than loosening them silently.
- The replay starts flat. October 19 selections can first fill October 20. An
  October 19 fill would require an October 16 selection outside this range.
- Existing account sizing, one-session order expiry, stops, trims, EMA9 exits and
  costs remain unchanged. No broker orders are placed. Results appear in the
  existing RS Expanded dashboard with a historical-replay notice. The daily job
  skips dates already covered by replay and continues on later sessions normally.
- This is **not a fully point-in-time market-data backtest**: stored OHLCV may
  have subsequent split adjustments or corrections, and the original saved board
  itself may have imperfect data provenance. Replay metadata records source run,
  source publication, reconstruction version and this limitation in saved runs,
  candidates and positions. No historical fundamentals or ratings are invented.

No replay is automatically executed by deployment. Run dry-run against the intended
database before requesting apply. Keep database backups through normal operations.
