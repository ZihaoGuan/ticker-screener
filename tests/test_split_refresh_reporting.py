from __future__ import annotations

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.split_refresh_reporting import RESULT_PREFIX, persistence_fields, read_persistence_fields
from scripts import refresh_split_adjusted_history as refresh
from scripts import run_scheduled_jobs as scheduler
from scripts import run_remote_worker as worker


class SplitRefreshReportingTests(unittest.TestCase):
    def setUp(self):
        self.results = [
            {"ticker": "GOOD", "status": "synced_partial_window", "rows_persisted": 320},
            {"ticker": "BAD", "status": "failed_coverage_loss", "rows_persisted": 0,
             "history_retained": True, "reason": "Existing dates missing in download."},
        ]

    def write_log(self, path):
        path.write_text(RESULT_PREFIX + json.dumps({"version": 1, "ticker_results": self.results}) + "\n")

    def test_partial_price_persistence_survives_failed_job_and_scheduler_sync(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = root / "current.log"
            self.write_log(log)
            worker._write_scheduled_status(SimpleNamespace(artifacts_dir=root),
                options={"scheduled_job_id": "refresh_split_adjusted_history"},
                state={"log_file": str(log), "return_code": 1}, status="failed")
            status_file = root / "status/refresh_split_adjusted_history.json"
            before = json.loads(status_file.read_text())
            with patch.object(scheduler, "STATUS_DIR", root / "status"):
                scheduler._sync_scheduler_persistence_from_status("refresh_split_adjusted_history")
            after = json.loads(status_file.read_text())
        self.assertEqual(before, after)
        self.assertEqual(after["status"], "failed")
        self.assertEqual(after["persistence_state"], "partial")
        self.assertTrue(after["persisted_to_db"])
        self.assertEqual(after["split_refresh_summary"]["ticker_results"], self.results)

    def test_states_distinguish_no_work_no_writes_unknown_and_complete(self):
        for results, state, persisted in [
            ([], "not_applicable", None),
            ([{"status": "ignored_excluded", "rows_persisted": 0}], "not_applicable", None),
            ([self.results[1]], "none", False),
            ([{"status": "failed_unconfirmed", "rows_persisted": None}], "unknown", None),
            ([self.results[0]], "complete", True),
            ([self.results[0], {"status": "failed_unconfirmed", "rows_persisted": None}], "partial", True),
        ]:
            with self.subTest(state=state):
                fields = persistence_fields(results)
                self.assertEqual(fields["persistence_state"], state)
                self.assertEqual(fields["persisted_to_db"], persisted)

    def test_missing_or_invalid_current_log_has_unknown_persistence(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "log"
            self.assertEqual(read_persistence_fields(str(path))["persistence_state"], "unknown")
            for value in ('garbage', RESULT_PREFIX + '{bad', RESULT_PREFIX + '{"version":2,"ticker_results":[]}',
                          RESULT_PREFIX + '{"version":1,"ticker_results":[{"status":"synced","rows_persisted":-1}]}'):
                path.write_text(value)
                self.assertEqual(read_persistence_fields(str(path))["persistence_state"], "unknown")

    def test_child_uses_unique_manifest_and_missing_manifest_does_not_claim_retained_history(self):
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            return SimpleNamespace(returncode=1)
        with tempfile.TemporaryDirectory() as tmp, patch.object(refresh.subprocess, "run", side_effect=run):
            first = refresh._refresh_ticker("GOOD", "2020-01-01", "2026-10-23", Path(tmp) / "state.json")
            refresh._refresh_ticker("GOOD", "2020-01-01", "2026-10-23", Path(tmp) / "state.json")
        self.assertNotEqual(commands[0][-1], commands[1][-1])
        self.assertIsNone(first["rows_persisted"])
        self.assertIsNone(first["history_retained"])

    def test_nonzero_child_exit_preserves_confirmed_commits(self):
        def run(command, **kwargs):
            manifest = Path(command[-1])
            manifest.parent.mkdir(parents=True, exist_ok=True)
            manifest.write_text(json.dumps({"ticker_results": [self.results[0]]}))
            return SimpleNamespace(returncode=1)
        with tempfile.TemporaryDirectory() as tmp, patch.object(refresh.subprocess, "run", side_effect=run):
            result = refresh._refresh_ticker("GOOD", "2020-01-01", "2026-10-23", Path(tmp) / "state.json")
        self.assertEqual(result["rows_persisted"], 320)
        self.assertEqual(result["status"], "failed_child_exit")


if __name__ == "__main__":
    unittest.main()
