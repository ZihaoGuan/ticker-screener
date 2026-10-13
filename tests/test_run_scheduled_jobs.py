from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import scripts.run_scheduled_jobs as module


class RunScheduledJobsTests(unittest.TestCase):
    def test_configure_artifact_paths_uses_persistent_artifacts_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifacts_dir = Path(temp_dir) / "persistent-artifacts"
            original_paths = (module.ARTIFACTS_DIR, module.STATUS_DIR, module.STATE_FILE)
            try:
                module._configure_artifact_paths(artifacts_dir)

                self.assertEqual(module.ARTIFACTS_DIR, artifacts_dir)
                self.assertEqual(module.STATUS_DIR, artifacts_dir / "status")
                self.assertEqual(module.STATE_FILE, artifacts_dir / "status" / "scheduler-state.json")
            finally:
                module.ARTIFACTS_DIR, module.STATUS_DIR, module.STATE_FILE = original_paths

    def test_main_queues_due_job_without_running_web_container_command(self) -> None:
        queued: list[tuple[str, dict[str, object], str, dict[str, object]]] = []
        saved_states: list[dict[str, str]] = []

        class _RunService:
            _actions = {"rs": SimpleNamespace(action_id="rs")}

            def __init__(self, **_: object) -> None:
                pass

            def recover_remote_jobs(self) -> dict[str, int]:
                return {"requeued": 0, "local_fallback_started": 0}

            def launch(self, action_id: str, *, options: dict[str, object], trigger_source: str, queue_policy: dict[str, object]) -> str:
                queued.append((action_id, options, trigger_source, queue_policy))
                return "remote-123"

        class _ScheduleService:
            def __init__(self, *, run_service: _RunService, **_: object) -> None:
                self.run_service = run_service

            def get_max_parallel_jobs(self) -> int:
                return 4

            def list_jobs(self) -> list[dict[str, object]]:
                return [
                    {
                        "job_id": "daily_rs",
                        "job_label": "Daily RS",
                        "action_id": "rs",
                        "cron_expr": "* * * * *",
                        "cron_tz": "America/New_York",
                        "enabled": True,
                        "options": {"limit": 25},
                        "priority": 75,
                        "depends_on_job_ids": ["reload_prices"],
                        "worker_pool": "general",
                    }
                ]

            def enqueue_job(self, *, job: dict[str, object], local_now, trigger_source: str = "scheduler") -> str:
                return self.run_service.launch(
                    str(job["action_id"]),
                    options={"limit": 25, "execution_mode": "remote", "scheduled_job_id": "daily_rs"},
                    trigger_source=trigger_source,
                    queue_policy={"queue_priority": 75, "dependency_job_ids": ["reload_prices"], "dependency_key": local_now.date().isoformat(), "dependency_expires_at": local_now.isoformat(), "worker_pool": "general", "parallel_limit": 4},
                )

        with (
            patch.object(
                module,
                "load_webapp_config",
                return_value=SimpleNamespace(
                    database_url="postgresql://queue-test",
                    artifacts_dir=module.PROJECT_ROOT / "artifacts",
                ),
            ),
            patch.object(module, "RunService", _RunService),
            patch.object(module, "ScheduledJobService", _ScheduleService),
            patch.object(module, "_load_state", return_value={}),
            patch.object(module, "_save_state", side_effect=lambda state: saved_states.append(dict(state))),
            patch.object(module, "_write_scheduler_status"),
        ):
            self.assertEqual(module.main(), 0)

        self.assertEqual(queued[0][0], "rs")
        self.assertEqual(queued[0][2], "scheduler")
        self.assertEqual(queued[0][1]["execution_mode"], "remote")
        self.assertEqual(queued[0][1]["scheduled_job_id"], "daily_rs")
        self.assertEqual(queued[0][3]["queue_priority"], 75)
        self.assertRegex(str(queued[0][3]["dependency_key"]), r"^\d{4}-\d{2}-\d{2}$")
        self.assertEqual(queued[0][3]["dependency_job_ids"], ["reload_prices"])
        self.assertEqual(queued[0][3]["worker_pool"], "general")
        self.assertEqual(queued[0][3]["parallel_limit"], 4)
        self.assertEqual(len(saved_states), 1)

    def test_main_drops_best_effort_snapshot_when_no_worker_is_idle(self) -> None:
        saved_states: list[dict[str, str]] = []

        class _RunService:
            _actions = {"build_scanner_top_hits_snapshot": SimpleNamespace(action_id="build_scanner_top_hits_snapshot")}

            def __init__(self, **_: object) -> None:
                pass

            def recover_remote_jobs(self) -> dict[str, int]:
                return {"requeued": 0, "local_fallback_started": 0}

        class _ScheduleService:
            def __init__(self, **_: object) -> None:
                pass

            def list_jobs(self) -> list[dict[str, object]]:
                return [
                    {
                        "job_id": "build_scanner_top_hits_snapshot",
                        "action_id": "build_scanner_top_hits_snapshot",
                        "cron_expr": "* * * * *",
                        "cron_tz": "America/New_York",
                        "enabled": True,
                    }
                ]

            def best_effort_skip_reason(self, _job: dict[str, object]) -> str:
                return "no compatible worker is idle"

            def enqueue_job(self, **_: object) -> str:
                raise AssertionError("a best-effort snapshot must not be queued without an idle worker")

        with (
            patch.object(
                module,
                "load_webapp_config",
                return_value=SimpleNamespace(database_url="postgresql://queue-test", artifacts_dir=module.PROJECT_ROOT / "artifacts"),
            ),
            patch.object(module, "RunService", _RunService),
            patch.object(module, "ScheduledJobService", _ScheduleService),
            patch.object(module, "_load_state", return_value={}),
            patch.object(module, "_save_state", side_effect=lambda state: saved_states.append(dict(state))),
        ):
            self.assertEqual(module.main(), 0)

        self.assertEqual(len(saved_states), 1)
        self.assertIn("build_scanner_top_hits_snapshot@", saved_states[0]["build_scanner_top_hits_snapshot"])

    def test_main_records_invalid_scheduled_dependency_without_stopping_scheduler(self) -> None:
        saved_states: list[dict[str, str]] = []
        statuses: list[dict[str, object]] = []

        class _RunService:
            _actions = {"rs": SimpleNamespace(action_id="rs")}

            def __init__(self, **_: object) -> None:
                pass

            def recover_remote_jobs(self) -> dict[str, int]:
                return {"requeued": 0, "local_fallback_started": 0}

        class _ScheduleService:
            def __init__(self, **_: object) -> None:
                pass

            def list_jobs(self) -> list[dict[str, object]]:
                return [{"job_id": "daily_rs", "job_label": "Daily RS", "action_id": "rs", "cron_expr": "* * * * *", "cron_tz": "America/New_York", "enabled": True}]

            def enqueue_job(self, **_: object) -> str:
                raise ValueError("This schedule requires an enabled Reload Postgres Market Data Date schedule in the same timezone.")

        with (
            patch.object(module, "load_webapp_config", return_value=SimpleNamespace(database_url="postgresql://queue-test", artifacts_dir=module.PROJECT_ROOT / "artifacts")),
            patch.object(module, "RunService", _RunService),
            patch.object(module, "ScheduledJobService", _ScheduleService),
            patch.object(module, "_load_state", return_value={}),
            patch.object(module, "_save_state", side_effect=lambda state: saved_states.append(dict(state))),
            patch.object(module, "_write_scheduler_status", side_effect=lambda **kwargs: statuses.append(kwargs)),
        ):
            self.assertEqual(module.main(), 0)

        self.assertEqual(len(saved_states), 1)
        self.assertEqual(statuses[0]["status"], "failed")
        self.assertIn("requires an enabled", str(statuses[0]["message"]))

    def test_main_preserves_scheduled_github_execution_mode(self) -> None:
        queued: list[dict[str, object]] = []

        class _RunService:
            _actions = {"finviz": SimpleNamespace(action_id="finviz")}

            def __init__(self, **_: object) -> None:
                pass

            def recover_remote_jobs(self) -> dict[str, int]:
                return {"requeued": 0, "local_fallback_started": 0}

            def launch(self, _: str, *, options: dict[str, object], trigger_source: str, queue_policy: dict[str, object]) -> str:
                queued.append(options)
                return "remote-456"

        class _ScheduleService:
            def __init__(self, *, run_service: _RunService, **_: object) -> None:
                self.run_service = run_service

            def list_jobs(self) -> list[dict[str, object]]:
                return [
                    {
                        "job_id": "finviz_external",
                        "job_label": "Finviz external",
                        "action_id": "finviz",
                        "cron_expr": "* * * * *",
                        "cron_tz": "America/New_York",
                        "enabled": True,
                        "options": {"execution_mode": "github"},
                    }
                ]

            def enqueue_job(self, *, job: dict[str, object], local_now, trigger_source: str = "scheduler") -> str:
                return self.run_service.launch(
                    str(job["action_id"]),
                    options={"execution_mode": "github", "scheduled_job_id": "finviz_external"},
                    trigger_source=trigger_source,
                    queue_policy={"queue_priority": 50, "dependency_job_ids": [], "dependency_key": local_now.date().isoformat(), "worker_pool": "any", "parallel_limit": 5},
                )

        with (
            patch.object(
                module,
                "load_webapp_config",
                return_value=SimpleNamespace(
                    database_url="postgresql://queue-test",
                    artifacts_dir=module.PROJECT_ROOT / "artifacts",
                ),
            ),
            patch.object(module, "RunService", _RunService),
            patch.object(module, "ScheduledJobService", _ScheduleService),
            patch.object(module, "_load_state", return_value={}),
            patch.object(module, "_save_state"),
            patch.object(module, "_write_scheduler_status"),
        ):
            self.assertEqual(module.main(), 0)

        self.assertEqual(queued[0]["execution_mode"], "github")

    def test_sync_scheduler_persistence_marks_snapshot_waiting_without_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "snapshot.log"
                log_path.write_text("SNAPSHOT_WAITING: Top Hits retained previous snapshot; waiting for ratings.\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "top_hits.json"
                status_path.write_text(
                    json.dumps({"job_id": "top_hits", "status": "success", "log_file": str(log_path)}), encoding="utf-8"
                )

                module._sync_scheduler_persistence_from_status("top_hits")
                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertEqual(payload["status"], "waiting")
        self.assertIn("prior completed snapshot remains active", payload["persistence_message"])

    def test_sync_scheduler_persistence_marks_current_snapshot_as_successful_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "snapshot.log"
                log_path.write_text("SNAPSHOT_CURRENT: Top Hits already has a completed snapshot.\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "top_hits.json"
                status_path.write_text(
                    json.dumps({"job_id": "top_hits", "status": "success", "log_file": str(log_path)}), encoding="utf-8"
                )

                module._sync_scheduler_persistence_from_status("top_hits")
                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertEqual(payload["status"], "success")
        self.assertIn("no rebuild was needed", payload["persistence_message"])
    def test_sync_scheduler_persistence_from_status_marks_success_when_log_contains_screen_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "job.log"
                log_path.write_text("hello\nPersisted screen run id=321\nbye\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "weekly_rs_close.json"
                status_path.write_text(
                    json.dumps(
                        {
                            "job_id": "weekly_rs_close",
                            "job_label": "Weekly RS Close",
                            "status": "success",
                            "log_file": str(log_path),
                        }
                    ),
                    encoding="utf-8",
                )

                module._sync_scheduler_persistence_from_status("weekly_rs_close")

                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertTrue(bool(payload["persisted_to_db"]))
        self.assertEqual(payload["screen_run_id"], 321)
        self.assertEqual(payload["persistence_message"], "Persisted screen run id=321.")

    def test_sync_scheduler_persistence_from_status_marks_failure_when_log_lacks_screen_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            original_status_dir = module.STATUS_DIR
            try:
                module.STATUS_DIR = Path(temp_dir)
                log_path = module.STATUS_DIR / "job.log"
                log_path.write_text("hello\njob done\n", encoding="utf-8")
                status_path = module.STATUS_DIR / "weekly_rs_close.json"
                status_path.write_text(
                    json.dumps(
                        {
                            "job_id": "weekly_rs_close",
                            "job_label": "Weekly RS Close",
                            "status": "success",
                            "log_file": str(log_path),
                        }
                    ),
                    encoding="utf-8",
                )

                module._sync_scheduler_persistence_from_status("weekly_rs_close")

                payload = json.loads(status_path.read_text(encoding="utf-8"))
            finally:
                module.STATUS_DIR = original_status_dir

        self.assertFalse(bool(payload["persisted_to_db"]))
        self.assertIsNone(payload["screen_run_id"])
        self.assertEqual(payload["persistence_message"], "No persisted screen run id found in job log.")


if __name__ == "__main__":
    unittest.main()
