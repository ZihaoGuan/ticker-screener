from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest

from scripts.run_scheduled_jobs import _resolve_template_value
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService


class ScheduledJobServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)
        self.run_service = RunService(project_root=self.project_root)
        self.service = ScheduledJobService(project_root=self.project_root, run_service=self.run_service)

    def test_upsert_job_persists_options(self) -> None:
        job = self.service.upsert_job(
            job_id="earnings_this_plus_next",
            job_label="Earnings Criteria Rolling",
            action_id="earnings_weekly_criteria",
            cron_expr="15 8 * * 6",
            cron_tz="Pacific/Auckland",
            enabled=True,
            options={"reference_date": "{{local_date_plus_7}}", "limit": 25},
        )

        self.assertEqual(job["options"]["reference_date"], "{{local_date_plus_7}}")
        jobs = self.service.list_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["options"]["limit"], 25)

    def test_available_actions_include_reload_postgres_market_data_date(self) -> None:
        actions = {item["id"] for item in self.service.get_context()["available_actions"]}

        self.assertIn("reload_postgres_market_data_date", actions)
        self.assertNotIn("sync_postgres_market_data", actions)

    def test_template_resolution_expands_date_tokens(self) -> None:
        local_now = dt.datetime(2026, 6, 6, 8, 15)

        resolved = _resolve_template_value(
            {
                "reference_date": "{{local_date}}",
                "secondary_reference_date": "{{local_date_plus_7}}",
                "nested": ["{{local_date_plus_14}}"],
            },
            local_now=local_now,
        )

        self.assertEqual(
            resolved,
            {
                "reference_date": "2026-06-06",
                "secondary_reference_date": "2026-06-13",
                "nested": ["2026-06-20"],
            },
        )


if __name__ == "__main__":
    unittest.main()
