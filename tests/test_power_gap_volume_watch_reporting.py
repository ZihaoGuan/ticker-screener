from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from scripts import run_power_gap_volume_watch as runner
from src.webapp.services.run_service import RunService


class PowerGapVolumeWatchReportingTests(unittest.TestCase):
    def test_runner_artifacts_flow_through_persistence_helper_and_job_parser(self) -> None:
        hit = {
            "ticker": "ABC",
            "signals": ["gap"],
            "power_gap_volume_context": {
                "gap_event": {"date": "2026-10-23", "age_trading_sessions": 0, "gap_pct": 8.0, "volume_ratio": 2.5},
                "earnings_context": {"status": "catalyst_unknown"},
            },
        }
        result = {
            "strategy_id": "power_gap_volume_watch",
            "rules_version": "power_gap_volume_watch_v1",
            "run_date": "2026-10-23",
            "total_tickers": 12,
            "passed_tickers": 1,
            "failed_tickers": [],
            "hits": [hit],
            "results": [hit],
        }
        with tempfile.TemporaryDirectory() as tmp:
            artifacts_dir = Path(tmp) / "durable-artifacts"
            webapp_config = SimpleNamespace(database_url="postgres://test", artifacts_dir=artifacts_dir)
            history = MagicMock()
            history.is_configured.return_value = True
            history.persist_screen_run.return_value = 7099
            config = SimpleNamespace()

            with (
                patch("sys.argv", ["run_power_gap_volume_watch.py", "--tickers", "ABC", "--date-label", "2026-10-23"]),
                patch.object(runner, "load_app_config", return_value=config),
                patch.object(runner, "load_webapp_config", return_value=webapp_config),
                patch.object(runner, "load_excluded_tickers", return_value=set()),
                patch.object(runner, "filter_symbols", side_effect=lambda tickers, _excluded: tickers),
                patch.object(runner, "run_power_gap_volume_watch", return_value=result),
                patch("scripts._screen_run_persistence.load_webapp_config", return_value=webapp_config),
                patch("scripts._screen_run_persistence.ScreenerHistoryService", return_value=history),
            ):
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(runner.main(), 0)
                failed_output = io.StringIO()
                with patch.object(runner, "persist_screen_run_artifacts_if_configured", side_effect=RuntimeError("database unavailable")):
                    with contextlib.redirect_stdout(failed_output), self.assertRaisesRegex(RuntimeError, "database unavailable"):
                        runner.main()
                disabled_output = io.StringIO()
                with patch.object(runner, "persist_screen_run_artifacts_if_configured", return_value=None):
                    with contextlib.redirect_stdout(disabled_output):
                        self.assertEqual(runner.main(), 0)

            paths = runner.build_screener_artifact_paths(
                artifacts_dir, strategy_id="power_gap_volume_watch", date_label="2026-10-23"
            )
            self.assertTrue(paths.raw_results_path.is_file())
            self.assertTrue(paths.watchlist_path.is_file())
            self.assertTrue(paths.summary_path.is_file())
            for artifact_path in (paths.raw_results_path, paths.watchlist_path, paths.summary_path):
                self.assertIn(str(artifact_path), failed_output.getvalue())
            self.assertNotIn("Persisted screen run id=", failed_output.getvalue())
            self.assertNotIn("Persisted screen run id=", disabled_output.getvalue())
            self.assertIn(str(paths.summary_path), disabled_output.getvalue())
            summary = json.loads(paths.summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["source"], "manual-tickers")
            history.persist_screen_run.assert_called_once()
            self.assertEqual(history.persist_screen_run.call_args.kwargs["options"]["market_data_source"], "database-first")

            service = RunService(project_root=Path(tmp))
            job = {"success_count": 0}
            for line in output.getvalue().splitlines():
                service._update_artifacts(job, line)
            service._load_summary_metadata(job)
            self.assertEqual(job["success_count"], 1)
            self.assertEqual(job["summary_file"], str(paths.summary_path))
            self.assertEqual(job["watchlist_file"], str(paths.watchlist_path))
            self.assertEqual(job["raw_results_file"], str(paths.raw_results_path))
            raw = json.loads(paths.raw_results_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["hits"][0]["power_gap_volume_context"], hit["power_gap_volume_context"])

            service.history_repository.update_job_run = MagicMock()
            service.history_repository.patch_job_run_result = MagicMock()
            service.screener_history_service.persist_screen_run = MagicMock(return_value=7100)
            job.update(
                {
                    "job_run_id": 5771,
                    "action_id": "power_gap_volume_watch",
                    "label": "Run Power Gap & Volume Watch",
                    "status": "success",
                    "finished_at": "2026-10-24T00:00:00+00:00",
                    "return_code": 0,
                    "options": {"market_data_source": "internet"},
                }
            )
            service.finalize_completed_job(job)
            service.screener_history_service.persist_screen_run.assert_called_once()
            finalized = service.screener_history_service.persist_screen_run.call_args.kwargs
            self.assertEqual(finalized["job_run_id"], 5771)
            self.assertEqual(finalized["options"]["market_data_source"], "database-first")
            self.assertEqual(finalized["raw_payload"]["hits"][0]["power_gap_volume_context"], hit["power_gap_volume_context"])
            expected_watchlist_item = runner.build_power_gap_volume_watchlist([hit])[0]
            for key in ("setup_label", "summary", "master_note", "event_date", "event_label"):
                self.assertEqual(finalized["raw_payload"]["hits"][0][key], expected_watchlist_item[key])
            self.assertEqual(job["screen_run_id"], 7100)
            service.history_repository.patch_job_run_result.assert_called_with(
                5771, result_payload_patch={"screen_run_id": 7100}
            )


if __name__ == "__main__":
    unittest.main()
