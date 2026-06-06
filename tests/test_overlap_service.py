from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.overlap_summary import discover_supported_dates, resolve_pipeline_path


class OverlapSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        self.watchlist_dir = self.artifacts_dir / "watchlists"
        self.watchlist_dir.mkdir(parents=True, exist_ok=True)

    def test_discover_supported_dates_reads_dated_layout(self) -> None:
        root = self.artifacts_dir / "screeners" / "2026-05-24" / "rs"
        root.mkdir(parents=True, exist_ok=True)
        (root / "watchlist.json").write_text(json.dumps([{"ticker": "AAPL"}]), encoding="utf-8")

        dates = discover_supported_dates(self.watchlist_dir)

        self.assertEqual(dates, ["2026-05-24"])

    def test_resolve_pipeline_path_prefers_dated_layout(self) -> None:
        legacy_path = self.watchlist_dir / "rs_new_high_before_price_2026-05-24.json"
        legacy_path.write_text(json.dumps([{"ticker": "OLD"}]), encoding="utf-8")
        dated_root = self.artifacts_dir / "screeners" / "2026-05-24" / "rs"
        dated_root.mkdir(parents=True, exist_ok=True)
        dated_watchlist = dated_root / "watchlist.json"
        dated_watchlist.write_text(json.dumps([{"ticker": "NEW"}]), encoding="utf-8")

        path, resolution = resolve_pipeline_path(
            self.watchlist_dir,
            "2026-05-24",
            {"id": "rs", "label": "RS", "filename": "rs_new_high_before_price_{date}.json"},
        )

        self.assertEqual(path, dated_watchlist)
        self.assertEqual(resolution, "dated")


if __name__ == "__main__":
    unittest.main()
