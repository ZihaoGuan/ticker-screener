from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts.run_external_screener import _artifact_paths


class RunExternalScreenerTests(unittest.TestCase):
    def test_artifact_paths_requires_standard_screening_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw = root / "raw.json"
            watchlist = root / "watchlist.json"
            summary = root / "summary.json"
            raw.write_text("{}", encoding="utf-8")
            watchlist.write_text("{}", encoding="utf-8")
            summary.write_text(
                json.dumps({"raw_results_file": str(raw), "watchlist_file": str(watchlist)}),
                encoding="utf-8",
            )

            self.assertEqual(
                _artifact_paths(summary),
                {"raw": str(raw), "watchlist": str(watchlist), "summary": str(summary)},
            )

    def test_artifact_paths_rejects_missing_watchlist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            summary = root / "summary.json"
            summary.write_text(json.dumps({"raw_results_file": str(root / "raw.json")}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "required artifacts"):
                _artifact_paths(summary)
