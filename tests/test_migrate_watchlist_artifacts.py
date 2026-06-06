from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.migrate_watchlist_artifacts_to_date_folders import migrate_artifacts


class MigrateWatchlistArtifactsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        (self.artifacts_dir / "raw").mkdir(parents=True, exist_ok=True)
        (self.artifacts_dir / "watchlists").mkdir(parents=True, exist_ok=True)

    def _write_legacy_rs(self, date_label: str = "2026-05-24") -> None:
        raw_path = self.artifacts_dir / "raw" / f"rs_new_high_before_price_{date_label}.json"
        watchlist_path = self.artifacts_dir / "watchlists" / f"rs_new_high_before_price_{date_label}.json"
        summary_path = self.artifacts_dir / "raw" / f"run_summary_{date_label}.json"
        raw_path.write_text(json.dumps({"hits": [{"ticker": "NVDA"}]}), encoding="utf-8")
        watchlist_path.write_text(json.dumps([{"ticker": "NVDA"}]), encoding="utf-8")
        summary_path.write_text(
            json.dumps(
                {
                    "date_label": date_label,
                    "raw_results_file": str(raw_path),
                    "watchlist_file": str(watchlist_path),
                    "passed_tickers": 1,
                }
            ),
            encoding="utf-8",
        )

    def test_dry_run_leaves_legacy_files_untouched(self) -> None:
        self._write_legacy_rs()

        payload = migrate_artifacts(artifacts_dir=self.artifacts_dir, dry_run=True)

        self.assertEqual(payload["migrated_runs"], 1)
        self.assertTrue((self.artifacts_dir / "raw" / "run_summary_2026-05-24.json").exists())
        self.assertFalse((self.artifacts_dir / "screeners" / "2026-05-24" / "rs" / "watchlist.json").exists())

    def test_real_migration_moves_files_and_rewrites_summary(self) -> None:
        self._write_legacy_rs()

        payload = migrate_artifacts(artifacts_dir=self.artifacts_dir)
        target_root = self.artifacts_dir / "screeners" / "2026-05-24" / "rs"
        summary_payload = json.loads((target_root / "run_summary.json").read_text(encoding="utf-8"))

        self.assertEqual(payload["migrated_runs"], 1)
        self.assertEqual(summary_payload["strategy_id"], "rs")
        self.assertEqual(summary_payload["watchlist_file"], str(target_root / "watchlist.json"))
        self.assertFalse((self.artifacts_dir / "raw" / "run_summary_2026-05-24.json").exists())
        self.assertFalse((self.artifacts_dir / "watchlists" / "rs_new_high_before_price_2026-05-24.json").exists())

    def test_rerun_is_idempotent(self) -> None:
        self._write_legacy_rs()
        migrate_artifacts(artifacts_dir=self.artifacts_dir)

        payload = migrate_artifacts(artifacts_dir=self.artifacts_dir)

        self.assertEqual(payload["migrated_runs"], 0)

    def test_ambiguous_summary_is_skipped(self) -> None:
        summary_path = self.artifacts_dir / "raw" / "mystery_summary.json"
        summary_path.write_text(json.dumps({"passed_tickers": 1}), encoding="utf-8")

        payload = migrate_artifacts(artifacts_dir=self.artifacts_dir)

        self.assertEqual(payload["migrated_runs"], 0)
        self.assertEqual(len(payload["skipped"]), 1)

    def test_db_path_rewrite_only_runs_when_database_url_present(self) -> None:
        self._write_legacy_rs()

        with patch("scripts.migrate_watchlist_artifacts_to_date_folders.HistoryRepository.rewrite_screen_run_artifact_paths", return_value=2) as rewrite:
            payload = migrate_artifacts(
                artifacts_dir=self.artifacts_dir,
                database_url="postgres://example",
                copy_mode=True,
            )

        rewrite.assert_called_once()
        self.assertEqual(payload["db_rows_updated"], 2)


if __name__ == "__main__":
    unittest.main()
