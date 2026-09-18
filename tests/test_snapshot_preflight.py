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

    def test_daily_scanner_batch_resolves_only_enabled_screen_runs_at_daily_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "config").mkdir()
            (root / "config" / "scheduled_jobs.json").write_text(
                json.dumps(
                    {"jobs": [
                        {"job_id": "rs", "action_id": "rs", "cron_expr": "0 18 * * 1-5", "cron_tz": "America/New_York", "enabled": True},
                        {"job_id": "ratings", "action_id": "build_technical_ratings", "cron_expr": "0 18 * * 1-5", "cron_tz": "America/New_York", "enabled": True},
                        {"job_id": "disabled", "action_id": "vcp", "cron_expr": "0 18 * * 1-5", "cron_tz": "America/New_York", "enabled": False},
                    ]}
                ),
                encoding="utf-8",
            )
            status_dir = root / "artifacts" / "status"
            status_dir.mkdir(parents=True)
            (status_dir / "rs.json").write_text(
                json.dumps({"status": "success", "last_finished_at": "2026-09-16T23:10:00Z"}), encoding="utf-8"
            )
            result = check_required_scheduled_jobs(
                status_dir=status_dir,
                required_job_ids=[],
                required_job_groups=["daily_scanner_batch"],
                project_root=root,
                now=dt.datetime(2026, 9, 17, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(result.required_job_ids, ("rs",))
        self.assertTrue(result.ready)
