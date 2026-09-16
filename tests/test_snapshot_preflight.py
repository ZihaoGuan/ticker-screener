from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest

from src.webapp.services.snapshot_preflight import check_required_scheduled_jobs


class SnapshotPreflightTests(unittest.TestCase):
    def test_requires_all_jobs_to_succeed_on_the_current_new_york_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            status_dir = Path(temp_dir)
            (status_dir / "scanner_a.json").write_text(
                json.dumps({"status": "success", "last_finished_at": "2026-09-16T23:10:00Z"}), encoding="utf-8"
            )
            (status_dir / "ratings.json").write_text(
                json.dumps({"status": "failed", "last_finished_at": "2026-09-16T23:15:00Z"}), encoding="utf-8"
            )
            result = check_required_scheduled_jobs(
                status_dir=status_dir,
                required_job_ids=["scanner_a", "ratings"],
                now=dt.datetime(2026, 9, 17, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(result.target_date, "2026-09-16")
        self.assertFalse(result.ready)
        self.assertEqual(result.pending_job_ids, ("ratings",))

    def test_missing_or_prior_day_status_defers_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            status_dir = Path(temp_dir)
            (status_dir / "scanner_a.json").write_text(
                json.dumps({"status": "success", "last_finished_at": "2026-09-15T23:10:00Z"}), encoding="utf-8"
            )
            result = check_required_scheduled_jobs(
                status_dir=status_dir,
                required_job_ids=["scanner_a", "missing"],
                now=dt.datetime(2026, 9, 17, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(result.pending_job_ids, ("scanner_a", "missing"))
