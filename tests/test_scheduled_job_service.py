from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

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

    def test_status_files_use_run_service_artifacts_directory(self) -> None:
        artifacts_dir = self.project_root / "persistent-artifacts"
        service = ScheduledJobService(
            project_root=self.project_root,
            run_service=RunService(project_root=self.project_root, artifacts_dir=artifacts_dir),
        )

        service._write_status(
            job_id="reload_prices",
            job_label="Reload prices",
            message="Queued.",
            artifact_file="",
        )

        self.assertTrue((artifacts_dir / "status" / "reload_prices.json").exists())
        self.assertFalse((self.project_root / "artifacts" / "status" / "reload_prices.json").exists())

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

    def test_list_jobs_normalizes_queue_policy_and_upsert_preserves_it(self) -> None:
        config_path = self.project_root / "config" / "scheduled_jobs.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            '{"jobs":[{"job_id":"reload_prices","job_label":"Reload prices","action_id":"reload_postgres_market_data_date","cron_expr":"30 17 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{}},{"job_id":"daily_rs","job_label":"Daily RS","action_id":"rs","cron_expr":"0 18 * * 1-5","cron_tz":"America/New_York","enabled":true,"options":{},"priority":75,"depends_on_job_ids":["reload_prices","reload_prices"],"worker_pool":"general"}]}\n',
            encoding="utf-8",
        )

        job = next(item for item in self.service.list_jobs() if item["job_id"] == "daily_rs")
        self.assertEqual(job["priority"], 75)
        self.assertEqual(job["depends_on_job_ids"], ["reload_prices"])
        self.assertEqual(job["worker_pool"], "general")

        saved = self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS updated",
            action_id="rs",
            cron_expr="5 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={},
        )
        self.assertEqual(saved["priority"], 75)
        self.assertEqual(saved["depends_on_job_ids"], ["reload_prices"])
        self.assertEqual(saved["worker_pool"], "general")

    def test_upsert_job_validates_and_persists_queue_policy(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        job = self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS",
            action_id="rs",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            priority=90,
            depends_on_job_ids=["reload_prices", "reload_prices"],
            worker_pool="market-data",
        )

        self.assertEqual(job["priority"], 90)
        self.assertEqual(job["depends_on_job_ids"], ["reload_prices"])
        self.assertEqual(job["worker_pool"], "market-data")

    def test_upsert_job_rejects_invalid_queue_policy(self) -> None:
        kwargs = {
            "job_id": "daily_rs",
            "job_label": "Daily RS",
            "action_id": "rs",
            "cron_expr": "0 18 * * 1-5",
            "cron_tz": "America/New_York",
            "enabled": True,
        }

        with self.assertRaisesRegex(ValueError, "priority"):
            self.service.upsert_job(**kwargs, priority=101)
        with self.assertRaisesRegex(ValueError, "worker_pool"):
            self.service.upsert_job(**kwargs, worker_pool="")
        with self.assertRaisesRegex(ValueError, "cannot depend on itself"):
            self.service.upsert_job(**kwargs, depends_on_job_ids=["daily_rs"])
        with self.assertRaisesRegex(ValueError, "unknown schedule"):
            self.service.upsert_job(**kwargs, depends_on_job_ids=["reload_prices"])

    def test_upsert_adds_price_reload_dependency_for_daily_bar_screener(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        job = self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS",
            action_id="rs",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        self.assertEqual(job["depends_on_job_ids"], ["reload_prices"])

    def test_upsert_adds_price_reload_dependency_for_database_first_option(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        job = self.service.upsert_job(
            job_id="canslim",
            job_label="CAN SLIM",
            action_id="canslim",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            options={"market_data_source": "database-first"},
        )

        self.assertEqual(job["depends_on_job_ids"], ["reload_prices"])

    def test_upsert_keeps_finviz_schedule_independent(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        job = self.service.upsert_job(
            job_id="finviz_target_price",
            job_label="Finviz Target Price",
            action_id="finviz_target_price_50",
            cron_expr="0 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        self.assertEqual(job["depends_on_job_ids"], [])

    def test_feature_build_schedule_depends_on_price_reload(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        job = self.service.upsert_job(
            job_id="daily_screening_features",
            job_label="Build daily screening features",
            action_id="build_daily_screening_features",
            cron_expr="40 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        self.assertEqual(job["depends_on_job_ids"], ["reload_prices"])

    def test_gap_retest_schedule_requires_and_depends_on_price_and_gap_sources(self) -> None:
        self.service.upsert_job(job_id="reload_prices", job_label="Reload prices", action_id="reload_postgres_market_data_date",
                                cron_expr="30 17 * * 1-5", cron_tz="America/New_York", enabled=True)
        with self.assertRaisesRegex(ValueError, "enabled Power Gap"):
            self.service.upsert_job(job_id="gap_retest", job_label="Gap Retest", action_id="run_martin_gap_retest",
                                    cron_expr="35 20 * * 1-5", cron_tz="America/New_York", enabled=True,
                                    options={"as_of_date": "{{local_date}}", "apply": True})
        self.service.upsert_job(job_id="power_gap", job_label="Power Gap", action_id="power_gap_volume_watch",
                                cron_expr="0 19 * * 1-5", cron_tz="America/New_York", enabled=True)
        job = self.service.upsert_job(job_id="gap_retest", job_label="Gap Retest", action_id="run_martin_gap_retest",
                                      cron_expr="35 20 * * 1-5", cron_tz="America/New_York", enabled=False,
                                      options={"as_of_date": "{{local_date}}", "apply": True})
        self.assertEqual(job["depends_on_job_ids"], ["reload_prices", "power_gap"])

    def test_upsert_rejects_price_dependent_schedule_without_enabled_reload(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires an enabled Reload Postgres Market Data Date schedule"):
            self.service.upsert_job(
                job_id="daily_rs",
                job_label="Daily RS",
                action_id="rs",
                cron_expr="0 18 * * 1-5",
                cron_tz="America/New_York",
                enabled=True,
            )

    def test_upsert_rejects_price_dependent_schedule_when_reload_never_runs_that_day(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )

        with self.assertRaisesRegex(ValueError, "runs on the same day"):
            self.service.upsert_job(
                job_id="weekly_rs",
                job_label="Weekly RS",
                action_id="rs",
                cron_expr="0 18 * * 6",
                cron_tz="America/New_York",
                enabled=True,
            )

    def test_enqueue_job_adds_price_reload_dependency_for_legacy_schedule(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        self.run_service.launch = MagicMock(return_value="remote-123")  # type: ignore[method-assign]

        self.service.enqueue_job(
            job={
                "job_id": "daily_rs",
                "job_label": "Daily RS",
                "action_id": "rs",
                "cron_expr": "0 18 * * 1-5",
                "cron_tz": "America/New_York",
                "enabled": True,
                "options": {},
                "priority": 50,
                "depends_on_job_ids": [],
                "worker_pool": "any",
            },
            local_now=dt.datetime(2026, 10, 13, 18, 0, tzinfo=ZoneInfo("America/New_York")),
        )

        policy = self.run_service.launch.call_args.kwargs["queue_policy"]
        self.assertEqual(policy["dependency_job_ids"], ["reload_prices"])

    def test_upsert_job_rejects_disabled_provider_and_dependency_cycle(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=False,
        )
        with self.assertRaisesRegex(ValueError, "disabled schedule"):
            self.service.upsert_job(
                job_id="daily_rs",
                job_label="Daily RS",
                action_id="rs",
                cron_expr="0 18 * * 1-5",
                cron_tz="America/New_York",
                enabled=True,
                depends_on_job_ids=["reload_prices"],
            )

    def test_run_now_preserves_schedule_dependency_identity(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            worker_pool="market-data",
        )
        self.run_service.launch = MagicMock(return_value="remote-123")  # type: ignore[method-assign]

        result = self.service.run_now(job_id="reload_prices")

        self.assertEqual(result["job_id"], "remote-123")
        options = self.run_service.launch.call_args.kwargs["options"]
        policy = self.run_service.launch.call_args.kwargs["queue_policy"]
        self.assertEqual(options["scheduled_job_id"], "reload_prices")
        self.assertRegex(policy["dependency_key"], r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(policy["worker_pool"], "market-data")

    def test_best_effort_top_hits_snapshot_skips_without_idle_worker(self) -> None:
        self.run_service.history_repository = MagicMock()
        self.run_service.history_repository.has_active_remote_job_run.return_value = False
        self.run_service.history_repository.list_remote_workers.return_value = [
            {
                "status": "running",
                "current_job_run_id": 42,
                "is_healthy": True,
                "metadata_json": {"code_version": self.run_service.code_version, "worker_pools": ["any"]},
            }
        ]

        reason = self.service.best_effort_skip_reason(
            {
                "action_id": "build_scanner_top_hits_snapshot",
                "options": {"execution_mode": "remote"},
                "worker_pool": "any",
            }
        )

        self.assertEqual(reason, "no compatible worker is idle")

    def test_best_effort_top_hits_snapshot_skips_when_snapshot_is_active(self) -> None:
        self.run_service.history_repository = MagicMock()
        self.run_service.history_repository.has_active_remote_job_run.return_value = True

        reason = self.service.best_effort_skip_reason(
            {
                "action_id": "build_scanner_top_hits_snapshot",
                "options": {"execution_mode": "remote"},
                "worker_pool": "any",
            }
        )

        self.assertEqual(reason, "another Top Hits snapshot is already queued or running")

    def test_best_effort_top_hits_snapshot_runs_with_idle_compatible_worker(self) -> None:
        self.run_service.history_repository = MagicMock()
        self.run_service.history_repository.has_active_remote_job_run.return_value = False
        self.run_service.history_repository.list_remote_workers.return_value = [
            {
                "status": "idle",
                "current_job_run_id": None,
                "is_healthy": True,
                "metadata_json": {"code_version": self.run_service.code_version, "worker_pools": ["general"]},
            }
        ]

        reason = self.service.best_effort_skip_reason(
            {
                "action_id": "build_scanner_top_hits_snapshot",
                "options": {"execution_mode": "remote"},
                "worker_pool": "general",
            }
        )

        self.assertEqual(reason, "")

    def test_upsert_job_rejects_dependency_cycle(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS",
            action_id="rs",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            depends_on_job_ids=["reload_prices"],
        )
        with self.assertRaisesRegex(ValueError, "cycle"):
            self.service.upsert_job(
                job_id="reload_prices",
                job_label="Reload prices",
                action_id="reload_postgres_market_data_date",
                cron_expr="30 17 * * 1-5",
                cron_tz="America/New_York",
                enabled=True,
                depends_on_job_ids=["daily_rs"],
            )

    def test_context_uses_latest_remote_dependency_attempt_for_queue_state(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS",
            action_id="rs",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            depends_on_job_ids=["reload_prices"],
        )
        key = dt.datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        self.run_service.history_repository = MagicMock()
        self.run_service.history_repository.list_scheduled_job_runs.return_value = [
            {
                "id": 22,
                "status": "queued",
                "created_at": "2026-10-13T21:00:00Z",
                "request_payload": {"dependency_key": key, "options": {"scheduled_job_id": "daily_rs"}},
            },
            {
                "id": 21,
                "status": "failed",
                "created_at": "2026-10-13T20:00:00Z",
                "request_payload": {"dependency_key": key, "options": {"scheduled_job_id": "reload_prices"}},
            },
        ]
        self.run_service.history_repository.list_remote_workers.return_value = []

        context = self.service.get_context()
        daily_rs = next(item for item in context["jobs"] if item["job_id"] == "daily_rs")

        self.assertEqual(daily_rs["queue_state"]["state"], "blocked")
        self.assertEqual(daily_rs["queue_state"]["blocking_job_id"], "reload_prices")
        self.assertEqual(daily_rs["queue_state"]["blocking_job_run_id"], 21)
        self.assertEqual(daily_rs["queue_state"]["queued_at"], "2026-10-13T21:00:00Z")
        self.assertIsNone(daily_rs["queue_state"]["worker_started_at"])

    def test_context_reports_actual_worker_start_from_result_payload(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        key = dt.datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        self.run_service.history_repository = MagicMock()
        self.run_service.history_repository.list_scheduled_job_runs.return_value = [
            {
                "id": 24,
                "status": "running",
                "created_at": "2026-10-13T20:00:00Z",
                "result_payload": {"started_at": "2026-10-13T20:02:00Z"},
                "request_payload": {"dependency_key": key, "options": {"scheduled_job_id": "reload_prices"}},
            }
        ]
        self.run_service.history_repository.list_remote_workers.return_value = []

        context = self.service.get_context()
        reload_prices = next(item for item in context["jobs"] if item["job_id"] == "reload_prices")

        self.assertEqual(reload_prices["queue_state"]["state"], "running")
        self.assertEqual(reload_prices["queue_state"]["job_run_id"], 24)
        self.assertEqual(reload_prices["queue_state"]["queued_at"], "2026-10-13T20:00:00Z")
        self.assertEqual(reload_prices["queue_state"]["worker_started_at"], "2026-10-13T20:02:00Z")

    def test_delete_job_rejects_referenced_provider_and_cross_timezone_dependency(self) -> None:
        self.service.upsert_job(
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="30 17 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
        self.service.upsert_job(
            job_id="daily_rs",
            job_label="Daily RS",
            action_id="rs",
            cron_expr="0 18 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
            depends_on_job_ids=["reload_prices"],
        )
        with self.assertRaisesRegex(ValueError, "Cannot delete"):
            self.service.delete_job(job_id="reload_prices")
        with self.assertRaisesRegex(ValueError, "same timezone"):
            self.service.upsert_job(
                job_id="daily_rs",
                job_label="Daily RS",
                action_id="rs",
                cron_expr="0 18 * * 1-5",
                cron_tz="Pacific/Auckland",
                enabled=True,
                depends_on_job_ids=["reload_prices"],
            )
    def test_available_actions_include_market_data_sync_actions(self) -> None:
        actions = {item["id"] for item in self.service.get_context()["available_actions"]}

        self.assertIn("sync_postgres_market_data", actions)
        self.assertIn("continue_martin_bar_retest", actions)
        self.assertIn("reload_postgres_market_data_date", actions)
        self.assertIn("build_daily_screening_features", actions)
        self.assertIn("refresh_split_adjusted_history", actions)
        self.assertIn("venu_scanner", actions)
        self.assertIn("fundamental_quality", actions)
        self.assertIn("refresh_tradingview_fund_exclusions", actions)
        self.assertNotIn("gamma_squeeze", actions)
        self.assertNotIn("liquid_growth", actions)

    def test_daily_screening_feature_action_accepts_trade_date(self) -> None:
        command = self.run_service.build_command(
            "build_daily_screening_features",
            {"trade_date": "2026-10-16"},
        )
        self.assertEqual(command[-2:], ["--trade-date", "2026-10-16"])

    def test_available_actions_include_grouping_metadata(self) -> None:
        actions = {item["id"]: item for item in self.service.get_context()["available_actions"]}

        self.assertEqual(actions["rs"]["bias_group"], "bullish")
        self.assertEqual(actions["rs"]["bullish_subgroup"], "leaders")
        self.assertIn("github", actions["rs"]["execution_modes"])
        self.assertNotIn("github", actions["reload_postgres_market_data_date"]["execution_modes"])
        self.assertTrue(actions["rs"]["requires_daily_bars"])
        self.assertFalse(actions["sync_postgres_market_data"]["requires_daily_bars"])

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
            job_id="reload_prices",
            job_label="Reload prices",
            action_id="reload_postgres_market_data_date",
            cron_expr="0 16 * * 1-5",
            cron_tz="America/New_York",
            enabled=True,
        )
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
            options={"required_job_ids": "daily_rs, technical_ratings", "required_job_groups": "daily_scanner_batch", "skip_if_current": True, "allow_incremental_refresh": True},
        )

        command = self.run_service.build_command(job["action_id"], job["options"])

        self.assertEqual(job["options"]["required_job_ids"], ["daily_rs", "technical_ratings"])
        self.assertEqual(job["options"]["required_job_groups"], ["daily_scanner_batch"])
        self.assertEqual(command[-8:], ["--required-job-id", "daily_rs", "--required-job-id", "technical_ratings", "--required-job-group", "daily_scanner_batch", "--skip-if-current", "--allow-incremental-refresh"])

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
