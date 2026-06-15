from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.overlap_summary import build_overlap_payload, discover_supported_dates, resolve_pipeline_path


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

    def test_build_overlap_payload_includes_new_bullish_pipelines(self) -> None:
        date_label = "2026-05-24"
        weekly_rs_root = self.artifacts_dir / "screeners" / date_label / "weekly_rs"
        weekly_rs_root.mkdir(parents=True, exist_ok=True)
        (weekly_rs_root / "watchlist.json").write_text(json.dumps([{"ticker": "AAPL"}]), encoding="utf-8")
        base_root = self.artifacts_dir / "screeners" / date_label / "base_detection"
        base_root.mkdir(parents=True, exist_ok=True)
        (base_root / "watchlist.json").write_text(json.dumps([{"ticker": "MSFT"}]), encoding="utf-8")
        tight_root = self.artifacts_dir / "screeners" / date_label / "weekly_tight_close_breakout"
        tight_root.mkdir(parents=True, exist_ok=True)
        (tight_root / "watchlist.json").write_text(json.dumps([{"ticker": "NVDA"}]), encoding="utf-8")
        bb_root = self.artifacts_dir / "screeners" / date_label / "bb_squeeze"
        bb_root.mkdir(parents=True, exist_ok=True)
        (bb_root / "watchlist.json").write_text(json.dumps([{"ticker": "SHOP"}]), encoding="utf-8")
        rti_root = self.artifacts_dir / "screeners" / date_label / "rti"
        rti_root.mkdir(parents=True, exist_ok=True)
        (rti_root / "watchlist.json").write_text(json.dumps([{"ticker": "CRWD"}]), encoding="utf-8")
        sean_breakout_root = self.artifacts_dir / "screeners" / date_label / "sean_breakout"
        sean_breakout_root.mkdir(parents=True, exist_ok=True)
        (sean_breakout_root / "watchlist.json").write_text(json.dumps([{"ticker": "APP"}]), encoding="utf-8")
        vcs_root = self.artifacts_dir / "screeners" / date_label / "vcs_critical_tightness"
        vcs_root.mkdir(parents=True, exist_ok=True)
        (vcs_root / "watchlist.json").write_text(json.dumps([{"ticker": "PLTR"}]), encoding="utf-8")

        payload = build_overlap_payload(date_label, self.watchlist_dir)
        pipeline_ids = [str(item["id"]) for item in payload["pipeline_status"]]

        self.assertIn("weekly_rs", pipeline_ids)
        self.assertIn("base_detection", pipeline_ids)
        self.assertIn("weekly_tight_close_breakout", pipeline_ids)
        self.assertIn("bb_squeeze", pipeline_ids)
        self.assertIn("rti", pipeline_ids)
        self.assertIn("sean_breakout", pipeline_ids)
        self.assertIn("vcs_critical_tightness", pipeline_ids)
        self.assertEqual(payload["pipeline_counts"]["weekly_rs"], 1)
        self.assertEqual(payload["pipeline_counts"]["base_detection"], 1)
        self.assertEqual(payload["pipeline_counts"]["weekly_tight_close_breakout"], 1)
        self.assertEqual(payload["pipeline_counts"]["bb_squeeze"], 1)
        self.assertEqual(payload["pipeline_counts"]["rti"], 1)
        self.assertEqual(payload["pipeline_counts"]["sean_breakout"], 1)
        self.assertEqual(payload["pipeline_counts"]["vcs_critical_tightness"], 1)


if __name__ == "__main__":
    unittest.main()
