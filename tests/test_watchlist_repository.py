from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.webapp.repositories.watchlist_repository import WatchlistRepository


class WatchlistRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        self.repository = WatchlistRepository(self.artifacts_dir)

    def _write_new_watchlist(self, *, date_folder: str, strategy_id: str, date_label: str, tickers: list[str]) -> Path:
        root = self.artifacts_dir / "screeners" / date_folder / strategy_id
        root.mkdir(parents=True, exist_ok=True)
        (root / "watchlist.json").write_text(json.dumps([{"ticker": ticker} for ticker in tickers]), encoding="utf-8")
        (root / "run_summary.json").write_text(
            json.dumps(
                {
                    "strategy_id": strategy_id,
                    "date_label": date_label,
                    "watchlist_file": str(root / "watchlist.json"),
                    "raw_results_file": str(root / "raw_results.json"),
                }
            ),
            encoding="utf-8",
        )
        return root / "watchlist.json"

    def test_list_recent_watchlists_reads_new_layout(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-05-24",
            strategy_id="weekly_rs",
            date_label="dell-2026-05-24",
            tickers=["DELL"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stem"], "weekly_rs_new_high_dell-2026-05-24")
        self.assertEqual(rows[0]["group_key"], "weekly_rs")

    def test_load_watchlist_reads_new_layout_by_logical_stem(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-05-24",
            strategy_id="fearzone",
            date_label="2026-05-24",
            tickers=["NVDA", "CRWD"],
        )

        payload = self.repository.load_watchlist("fearzone_2026-05-24")

        self.assertEqual([item["ticker"] for item in payload], ["NVDA", "CRWD"])

    def test_new_layout_wins_over_legacy_duplicate_stem(self) -> None:
        watchlists_dir = self.artifacts_dir / "watchlists"
        watchlists_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = watchlists_dir / "fearzone_2026-05-24.json"
        legacy_path.write_text(json.dumps([{"ticker": "OLD"}]), encoding="utf-8")
        new_path = self._write_new_watchlist(
            date_folder="2026-05-24",
            strategy_id="fearzone",
            date_label="2026-05-24",
            tickers=["NEW"],
        )

        rows = self.repository.list_recent_watchlists()
        payload = self.repository.load_watchlist("fearzone_2026-05-24")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["path"], str(new_path))
        self.assertEqual(payload[0]["ticker"], "NEW")

    def test_group_key_supports_fearzone_zeiierman(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="fearzone_zeiierman",
            date_label="2026-06-06",
            tickers=["CAVA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "fearzone_zeiierman_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "fearzone_zeiierman")

    def test_group_key_supports_td9_bullish(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="td9_bullish",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "td9_bullish_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "td9")

    def test_group_key_supports_macd_golden_cross(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="macd_golden_cross",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "macd_golden_cross_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "macd")


if __name__ == "__main__":
    unittest.main()
