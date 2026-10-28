from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import scripts.run_remote_worker as module


class _RunService:
    def __init__(self, artifacts_dir: Path) -> None:
        self.artifacts_dir = artifacts_dir
        self.code_version = "test-image"


class _HistoryRepository:
    def __init__(self) -> None:
        self.patches: list[dict[str, object]] = []
        self.heartbeats: list[dict[str, object]] = []

    def patch_job_run_result(self, _job_run_id: int, **kwargs: object) -> None:
        self.patches.append(dict(kwargs))

    def is_remote_job_cancel_requested(self, _job_run_id: int) -> bool:
        return False

    def heartbeat_remote_worker(self, **kwargs: object) -> None:
        self.heartbeats.append(dict(kwargs))


class _FailingJobRunService(_RunService):
    database_url = ""

    def __init__(self, artifacts_dir: Path) -> None:
        super().__init__(artifacts_dir)
        self.history_repository = _HistoryRepository()
        self.completed_job: dict[str, object] = {}

    def build_command(self, _action_id: str, _options: dict[str, object], *, normalized: bool) -> list[str]:
        assert normalized
        return [
            sys.executable,
            "-c",
            "import sys; [print(f'line-{i}') for i in range(100)]; print('FINAL_EXCEPTION'); sys.exit(1)",
        ]

    def _extract_progress(self, _lines: list[str]) -> dict[str, object]:
        return {"current": None, "total": None, "percent": None, "label": "", "success_count": 0}

    def _update_artifacts(self, _job: dict[str, object], _line: str) -> None:
        return None

    def _load_summary_metadata(self, _job: dict[str, object]) -> None:
        return None

    def finalize_completed_job(self, job: dict[str, object]) -> None:
        self.completed_job = dict(job)


class _UnknownActionRunService(_FailingJobRunService):
    def build_command(self, action_id: str, _options: dict[str, object], *, normalized: bool) -> list[str]:
        assert normalized
        raise ValueError(f"Unknown run action: {action_id}")


class RunRemoteWorkerTests(unittest.TestCase):
    def test_replay_jsonl_progress_is_exposed_as_job_progress(self) -> None:
        fields = module._replay_progress_fields(json.dumps({
            "event": "session_complete", "session_index": 4, "session_total": 10, "date": "2026-10-23",
            "counts": {"fills": 2},
        }))
        self.assertEqual(fields["progress_current"], 4)
        self.assertEqual(fields["progress_total"], 10)
        self.assertEqual(fields["progress_percent"], 40)
        self.assertIn("2026-10-23", fields["progress_label"])
        result = {"ready": True, "saved": True, "mode": "apply", "book_id": "demo",
                  "from_date": "2026-10-01", "to_date": "2026-10-23",
                  "sessions": [{"date": "2026-10-23", "execution_actions": {"filled": 2},
                                "book_performance": {"account_equity": 101.0}}]}
        self.assertEqual(module._replay_progress_fields(json.dumps(result))["replay_result"], result)
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "replay.log"
            log_path.write_text('{"event":"replay_committed"}\n' + json.dumps(result, indent=2) + "\n")
            compact = module._read_replay_result(log_path)
            self.assertEqual(compact["session_count"], 1)
            self.assertEqual(compact["aggregate_action_counts"], {"filled": 2})
            self.assertEqual(compact["final_book_performance"], {"account_equity": 101.0})
            self.assertNotIn("sessions", compact)

    def test_failed_split_job_keeps_partial_persistence_in_job_run_payload(self) -> None:
        from src.split_refresh_reporting import RESULT_PREFIX
        summary = {"version": 1, "ticker_results": [
            {"ticker": "GOOD", "status": "synced", "rows_persisted": 320},
            {"ticker": "BAD", "status": "failed_coverage_loss", "rows_persisted": 0},
        ]}

        class SplitRunService(_FailingJobRunService):
            def build_command(self, *_args, **_kwargs):
                return [sys.executable, "-c", f"import sys; print({(RESULT_PREFIX + json.dumps(summary))!r}); sys.exit(1)"]

        with tempfile.TemporaryDirectory() as temp_dir:
            service = SplitRunService(Path(temp_dir))
            code = module._run_claimed_job(service, {
                "id": 861, "job_name": "Split refresh", "trigger_source": "scheduled",
                "request_payload": {"action_id": "refresh_split_adjusted_history", "options": {
                    "scheduled_job_id": "refresh_split_adjusted_history"}},
            }, worker_name="test-worker", worker_pools=("general",), heartbeat_seconds=5.0)
            payload = service.history_repository.patches[-1]["result_payload_patch"]
        self.assertEqual(code, 1)
        self.assertEqual(payload["persistence_state"], "partial")
        self.assertTrue(payload["persisted_to_db"])
        self.assertEqual(payload["split_refresh_summary"]["rows_persisted"], 320)

    def test_main_passes_configured_artifacts_directory_to_run_service(self) -> None:
        captured: dict[str, object] = {}

        class _Repository:
            def heartbeat_remote_worker(self, **_: object) -> None:
                return None

            def claim_remote_job_run(self, **_: object) -> None:
                return None

        class _ConfiguredRunService:
            code_version = "test-image"

            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)
                self.history_repository = _Repository()

        with tempfile.TemporaryDirectory() as temp_dir:
            artifacts_dir = Path(temp_dir) / "persistent-artifacts"
            with (
                patch.object(
                    module,
                    "load_webapp_config",
                    return_value=SimpleNamespace(database_url="postgresql://worker-test", artifacts_dir=artifacts_dir),
                ),
                patch.object(module, "RunService", _ConfiguredRunService),
                patch.object(
                    module,
                    "parse_args",
                    return_value=SimpleNamespace(worker_name="test-worker", pools="any", poll_seconds=1, heartbeat_seconds=1, once=True),
                ),
            ):
                self.assertEqual(module.main(), 0)

        self.assertEqual(captured["artifacts_dir"], artifacts_dir)

    def test_parse_worker_pools_defaults_to_any_and_normalizes_values(self) -> None:
        self.assertEqual(module._parse_worker_pools(""), ("any",))
        self.assertEqual(module._parse_worker_pools("general, Finviz, general"), ("finviz", "general"))
        self.assertEqual(module._parse_worker_pools("general,any"), ("any",))

    def test_unknown_action_marks_claimed_job_failed_without_crashing_worker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _UnknownActionRunService(Path(temp_dir))

            exit_code = module._run_claimed_job(
                service,  # type: ignore[arg-type]
                {
                    "id": 860,
                    "job_name": "Run EMA 9/20 Buy Targets",
                    "trigger_source": "manual",
                    "request_payload": {"action_id": "ema9_20_crossover_targets", "options": {}},
                },
                worker_name="test-worker",
                worker_pools=("finviz",),
                heartbeat_seconds=5.0,
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(service.history_repository.patches[-1]["status"], "failed")
        self.assertIsNotNone(service.history_repository.patches[-1]["finished_at"])
        payload = service.history_repository.patches[-1]["result_payload_patch"]
        self.assertIn("Unknown run action", str(payload))
        self.assertEqual(service.history_repository.heartbeats[-1]["status"], "idle")

    def test_run_claimed_job_drains_output_after_short_process_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _FailingJobRunService(Path(temp_dir))

            exit_code = module._run_claimed_job(
                service,  # type: ignore[arg-type]
                {
                    "id": 809,
                    "job_name": "Run Finviz Target Price +50%",
                    "trigger_source": "manual",
                    "request_payload": {"action_id": "finviz_target_price_50", "options": {}},
                },
                worker_name="test-worker",
                worker_pools=("finviz",),
                heartbeat_seconds=5.0,
            )

            log_text = Path(str(service.completed_job["log_file"])).read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertIn("FINAL_EXCEPTION", log_text)
        self.assertIn("FINAL_EXCEPTION", str(service.completed_job["log_tail"]))
        self.assertEqual(service.history_repository.heartbeats[-1]["metadata"]["worker_pools"], ["finviz"])

    def test_write_scheduled_status_keeps_worker_log_and_persisted_screen_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = _RunService(Path(temp_dir))
            state = {
                "started_at": "2026-10-10T00:00:00+00:00",
                "return_code": 0,
                "log_file": "/artifacts/status/logs/weekly-rs.log",
                "watchlist_file": "/artifacts/watchlists/weekly-rs.json",
                "message": "Completed on worker primary-worker.",
                "screen_run_id": 321,
            }

            module._write_scheduled_status(
                service,  # type: ignore[arg-type]
                options={"scheduled_job_id": "weekly_rs", "scheduled_job_label": "Weekly RS"},
                state=state,
                status="success",
                finished_at="2026-10-10T00:01:00+00:00",
            )

            payload = json.loads((Path(temp_dir) / "status" / "weekly_rs.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["log_file"], state["log_file"])
        self.assertTrue(payload["persisted_to_db"])
        self.assertEqual(payload["screen_run_id"], 321)
