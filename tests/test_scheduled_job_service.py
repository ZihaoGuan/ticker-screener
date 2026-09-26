from __future__ import annotations

import datetime as dt
import json
import os
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

    def test_available_actions_include_market_data_sync_actions(self) -> None:
        actions = {item["id"] for item in self.service.get_context()["available_actions"]}

        self.assertIn("sync_postgres_market_data", actions)
        self.assertIn("reload_postgres_market_data_date", actions)
        self.assertIn("refresh_split_adjusted_history", actions)
        self.assertIn("venu_scanner", actions)
        self.assertIn("fundamental_quality", actions)
        self.assertNotIn("gamma_squeeze", actions)
        self.assertNotIn("liquid_growth", actions)

    def test_available_actions_include_grouping_metadata(self) -> None:
        actions = {item["id"]: item for item in self.service.get_context()["available_actions"]}

        self.assertEqual(actions["rs"]["bias_group"], "bullish")
        self.assertEqual(actions["rs"]["bullish_subgroup"], "leaders")

    def test_completed_log_durations_are_exposed_for_jobs_and_actions(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"jobs":[{"job_id":"daily_rs","job_label":"Daily RS","action_id":"rs","cron_expr":"0 18 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{}}]}\n',
            encoding="utf-8",
        )
        log_dir = self.project_root / "artifacts" / "status" / "logs"
        log_dir.mkdir(parents=True)
        for stamp, seconds in (("20260918T220000Z", 120), ("20260919T220000Z", 180), ("20260920T220000Z", 240)):
            log_path = log_dir / f"daily_rs-{stamp}.log"
            log_path.write_text("completed\n", encoding="utf-8")
            started_at = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.UTC)
            os.utime(log_path, (started_at.timestamp() + seconds, started_at.timestamp() + seconds))

        context = self.service.get_context()

        self.assertEqual(context["jobs"][0]["estimated_duration_seconds"], 180)
        self.assertEqual(context["jobs"][0]["estimate_sample_count"], 3)
        action = next(item for item in context["available_actions"] if item["id"] == "rs")
        self.assertEqual(action["estimated_duration_seconds"], 180)
        self.assertEqual(action["estimate_sample_count"], 3)

    def test_active_log_is_not_used_for_duration_estimate(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"jobs":[{"job_id":"daily_rs","job_label":"Daily RS","action_id":"rs","cron_expr":"0 18 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{}}]}\n',
            encoding="utf-8",
        )
        log_dir = self.project_root / "artifacts" / "status" / "logs"
        log_dir.mkdir(parents=True)
        log_path = log_dir / "daily_rs-20260920T220000Z.log"
        log_path.write_text("still running\n", encoding="utf-8")
        status_dir = self.project_root / "artifacts" / "status"
        (status_dir / "daily_rs.json").write_text(json.dumps({"status": "running", "log_file": str(log_path)}), encoding="utf-8")

        self.assertIsNone(self.service.list_jobs()[0]["estimated_duration_seconds"])

    def test_action_activity_keeps_manual_and_scheduled_states_separate(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"jobs":[{"job_id":"daily_rs","job_label":"Daily RS","action_id":"rs","cron_expr":"0 18 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{}}]}\n',
            encoding="utf-8",
        )
        status_dir = self.project_root / "artifacts" / "status"
        status_dir.mkdir(parents=True)
        (status_dir / "daily_rs.json").write_text(
            json.dumps(
                {
                    "status": "success",
                    "last_started_at": "2026-09-20T22:00:00Z",
                    "last_finished_at": "2026-09-20T22:02:00Z",
                    "success_count": 4,
                }
            ),
            encoding="utf-8",
        )

        activity = self.service.get_action_activity(
            run_jobs=[
                {
                    "job_id": "manual-rs",
                    "action_id": "rs",
                    "label": "Run RS",
                    "status": "running",
                    "started_at": "2026-09-21T00:00:00Z",
                    "finished_at": "",
                    "trigger_source": "manual",
                }
            ]
        )

        rs = next(item for item in activity if item["action_id"] == "rs")
        self.assertEqual(rs["adhoc_current"]["job_id"], "manual-rs")
        self.assertEqual(rs["scheduled_last"]["status"], "success")
        self.assertEqual(rs["scheduled_last"]["success_count"], 4)

    def test_stale_scheduled_running_status_is_exposed_as_interrupted(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"jobs":[{"job_id":"daily_rs","job_label":"Daily RS","action_id":"rs","cron_expr":"0 18 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{}}]}\n',
            encoding="utf-8",
        )
        status_dir = self.project_root / "artifacts" / "status"
        status_dir.mkdir(parents=True)
        (status_dir / "daily_rs.json").write_text(
            json.dumps({"status": "running", "last_started_at": "2020-01-01T00:00:00Z"}),
            encoding="utf-8",
        )

        activity = self.service.get_action_activity(run_jobs=[])

        self.assertEqual(activity[0]["scheduled_last"]["status"], "interrupted")

    def test_get_context_loads_even_with_stale_removed_action_in_jobs_file(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            """
{
  "jobs": [
    {
      "job_id": "old_hve",
      "job_label": "Old HVE",
      "action_id": "hve",
      "cron_expr": "0 9 * * 1",
      "cron_tz": "America/New_York",
      "enabled": true,
      "options": {}
    }
  ]
}
""".strip()
            + "\n",
            encoding="utf-8",
        )

        context = self.service.get_context()

        self.assertEqual(context["jobs"][0]["job_id"], "old_hve")
        self.assertEqual(context["jobs"][0]["action_id"], "hve")
        self.assertIn("sync_postgres_market_data", {item["id"] for item in context["available_actions"]})

    def test_upsert_reload_postgres_market_data_date_accepts_local_date_template(self) -> None:
        job = self.service.upsert_job(
            job_id="reload_one_day",
            job_label="Reload One Day",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 16 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={"trade_date": "{{local_date}}", "chunk_size": 25},
        )

        self.assertEqual(job["options"]["trade_date"], "{{local_date}}")

    def test_upsert_chart_fundamentals_cache_accepts_local_date_template(self) -> None:
        job = self.service.upsert_job(
            job_id="chart_fund_cache",
            job_label="Chart Fundamentals Cache",
            action_id="sync_chart_fundamentals_cache",
            cron_expr="0 7 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={"as_of_date": "{{local_date}}", "fundamental_limit": 200},
        )

        self.assertEqual(job["options"]["as_of_date"], "{{local_date}}")

    def test_upsert_job_allows_clearing_existing_options(self) -> None:
        self.service.upsert_job(
            job_id="rs_daily",
            job_label="RS Daily",
            action_id="rs",
            cron_expr="30 16 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={"market_data_source": "database-first"},
        )

        job = self.service.upsert_job(
            job_id="rs_daily",
            job_label="RS Daily",
            action_id="rs",
            cron_expr="30 16 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={},
        )

        self.assertEqual(job["options"], {})
        self.assertEqual(self.service.list_jobs()[0]["options"], {})

    def test_snapshot_schedule_preserves_required_job_ids_and_passes_them_to_the_script(self) -> None:
        job = self.service.upsert_job(
            job_id="top_hits_snapshot",
            job_label="Build Top Hits Snapshot",
            action_id="build_scanner_top_hits_snapshot",
            cron_expr="20 20 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={"required_job_ids": "daily_rs, technical_ratings", "required_job_groups": "daily_scanner_batch", "skip_if_current": True},
        )

        command = self.run_service.build_command(job["action_id"], job["options"])

        self.assertEqual(job["options"]["required_job_ids"], ["daily_rs", "technical_ratings"])
        self.assertEqual(job["options"]["required_job_groups"], ["daily_scanner_batch"])
        self.assertEqual(command[-7:], ["--required-job-id", "daily_rs", "--required-job-id", "technical_ratings", "--required-job-group", "daily_scanner_batch", "--skip-if-current"])

    def test_template_resolution_expands_date_tokens(self) -> None:
        local_now = dt.datetime(2026, 6, 6, 8, 15)

        resolved = _resolve_template_value(
            {
                "prior_reference_date": "{{local_date_minus_7}}",
                "reference_date": "{{local_date}}",
                "secondary_reference_date": "{{local_date_plus_7}}",
                "nested": ["{{local_date_minus_14}}", "{{local_date_plus_14}}"],
            },
            local_now=local_now,
        )

        self.assertEqual(
            resolved,
            {
                "prior_reference_date": "2026-05-30",
                "reference_date": "2026-06-06",
                "secondary_reference_date": "2026-06-13",
                "nested": ["2026-05-23", "2026-06-20"],
            },
        )


if __name__ == "__main__":
    unittest.main()
