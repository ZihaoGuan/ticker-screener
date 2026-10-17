# Daily screening feature snapshot

The `build_daily_screening_features` Run action builds one validated date in
`ticker_daily_screening_features` from Postgres `daily_bars`. It honors the
shared ticker exclusion set. The table is added by `sql/postgres_app_schema.sql`;
apply that schema before the first build. A build must cover at least 90% of
the eligible active universe. The date replacement and row-count check share
one transaction, so an error restores the previous date.

To schedule the pilot in Admin, create an enabled weekday job for the action
`build_daily_screening_features` immediately after the existing Postgres daily
price reload. Add the price reload job ID to its `depends_on_job_ids`. Add the
feature job ID to `depends_on_job_ids` for the `one_year_winners`,
`trend_template`, and `weinstein_stage2_early` jobs. Use the existing app-local
schedule IDs shown in Admin; do not edit or replace a server-local
`config/scheduled_jobs.json` from a code deployment. The feature job should run
after the price reload and before the three pilot screens on the same market
date.

The catalog batch runner remains deferred until parity has been demonstrated
with production feature snapshots.
