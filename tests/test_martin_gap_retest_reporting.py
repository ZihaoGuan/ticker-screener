from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from scripts.run_martin_gap_retest import _atomic_json, _load_source
from src.webapp.repositories.martin_forward_test_repository import (
    GAP_RETEST_STRATEGY_ID,
    MartinForwardTestRepository,
)
from src.webapp.services.run_service import RunService


class GapRetestReportingTests(unittest.TestCase):
    def test_source_load_uses_exact_date_run_and_all_persisted_hits(self):
        day = dt.date(2026, 10, 23)
        history = Mock()
        history.list_runs.return_value = [{"id": 42, "run_date": day.isoformat(), "config_hash": "fp"}]
        history.get_run.side_effect = [
            {"id": 42, "run_date": day.isoformat(), "created_at": "2026-10-23T20:45:00Z", "config_hash": "cfg", "hit_count": 1,
             "result_summary_json": {"source_fingerprint": "fp"}},
            {"id": 42, "hits": [{"ticker": "ABC"}]},
        ]

        source = _load_source(history, day)

        history.list_runs.assert_called_once_with(strategy_id="power_gap_volume_watch", start_date=day, end_date=day, limit=100)
        self.assertEqual(history.get_run.call_args_list[0].args, (42,))
        self.assertEqual(history.get_run.call_args_list[0].kwargs, {"include_hits": False})
        self.assertEqual(history.get_run.call_args_list[1].kwargs, {"include_hits": True, "hit_limit": 5000, "hit_offset": 0})
        self.assertEqual(source["source_run_id"], 42)
        self.assertEqual(source["source_fingerprint"], "fp")
        self.assertEqual(source["target_trading_date"], day.isoformat())

    def test_report_write_is_atomic_json(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "reports" / "summary.json"
            _atomic_json(path, {"execution_mode": "dry_run", "counts": {"pending": 0}})
            self.assertEqual(json.loads(path.read_text()), {"execution_mode": "dry_run", "counts": {"pending": 0}})

    def test_hidden_run_action_defaults_to_dry_run_and_only_apply_adds_flag(self):
        service = RunService(project_root=Path(__file__).resolve().parents[1])
        self.assertNotIn("--apply", service.build_command("run_martin_gap_retest", {"as_of_date": "2026-10-23"}))
        command = service.build_command("run_martin_gap_retest", {"as_of_date": "2026-10-23", "apply": True, "job_run_id": 91})
        self.assertIn("--apply", command)
        self.assertEqual(command[command.index("--job-run-id") + 1], "91")

    def test_event_fill_lookup_uses_nested_event_identity_for_all_history(self):
        repo = MartinForwardTestRepository(database_url="")
        identity = {"ticker": "ABC", "original_gap_date": "2026-10-20", "discovery_rules_version": "power_gap_volume_watch_v1"}
        calls = []

        def rows(sql, params=()):
            calls.append((sql, params))
            return [{"id": 7}] if "martin_forward_events" in sql else []

        repo._rows = rows
        self.assertTrue(repo.has_gap_event_fill(GAP_RETEST_STRATEGY_ID, identity))
        self.assertEqual(json.loads(calls[0][1][1]), {"event_identity": identity})
        self.assertIn("event_type='order_filled'", calls[1][0])


if __name__ == "__main__":
    unittest.main()
