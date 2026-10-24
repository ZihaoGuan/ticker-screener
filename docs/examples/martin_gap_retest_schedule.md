# Martin Daily Gap Retest schedule example

`martin_gap_retest_schedule.json` is a disabled example, not an enabled production job. After deployment and prerequisite review, add it to `config/scheduled_jobs.json` and enable it. The scheduler resolves and adds same-timezone, same-day dependencies on an enabled price reload and `power_gap_volume_watch`; it will reject an enabled retest schedule if either provider is missing, disabled, or on non-overlapping days. The apply option is explicit because scheduled runs persist the paper-book session.
