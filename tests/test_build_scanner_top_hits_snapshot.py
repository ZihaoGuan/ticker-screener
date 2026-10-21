from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import scripts.build_scanner_top_hits_snapshot as module


class BuildScannerTopHitsSnapshotTests(unittest.TestCase):
    def test_cli_uses_shared_manual_readiness_gate_and_reports_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = SimpleNamespace(artifacts_dir=Path(temporary), database_url="postgresql://example", market_data_source="database-first")
            outcome = {"status": "waiting", "target_date": "2026-10-20", "message": "Waiting for 2026-10-20 prerequisites: scanner_a"}
            with patch.object(module, "load_webapp_config", return_value=config), \
                 patch.object(module, "WatchlistService") as factory, \
                 patch.object(module, "refresh_scanner_top_hits", return_value=outcome) as refresh, \
                 patch("builtins.print") as output:
                self.assertEqual(module.main(["--required-job-id", "scanner_a", "--skip-if-current", "--allow-incremental-refresh"]), 0)
            self.assertEqual(refresh.call_args.args, (factory.return_value,))
            self.assertEqual(refresh.call_args.kwargs["options"], {"required_job_ids": ["scanner_a"], "skip_if_current": True})
            output.assert_called_once_with("SNAPSHOT_WAITING: Waiting for 2026-10-20 prerequisites: scanner_a")

    def test_cli_defaults_do_not_replace_configured_prerequisites(self):
        with tempfile.TemporaryDirectory() as temporary:
            config = SimpleNamespace(artifacts_dir=Path(temporary), database_url="", market_data_source="database-first")
            with patch.object(module, "load_webapp_config", return_value=config), \
                 patch.object(module, "WatchlistService"), \
                 patch.object(module, "refresh_scanner_top_hits", return_value={"status":"current", "message":"Unchanged for 2026-10-20"}) as refresh:
                self.assertEqual(module.main([]), 0)
            self.assertEqual(refresh.call_args.kwargs["options"], {})
