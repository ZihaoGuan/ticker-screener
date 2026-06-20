from __future__ import annotations

import json
import datetime as dt
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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

    def test_group_key_maps_legacy_htf_runup_stem_to_new_group(self) -> None:
        watchlists_dir = self.artifacts_dir / "watchlists"
        watchlists_dir.mkdir(parents=True, exist_ok=True)
        legacy_path = watchlists_dir / "htf_8w_runup_2026-06-06.json"
        legacy_path.write_text(json.dumps([{"ticker": "NVDA"}]), encoding="utf-8")

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "htf_8w_runup_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "eight_week_100_runup")

    def test_group_key_supports_rsi_ma_bb_bullish(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="rsi_ma_bb_bullish",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "rsi_ma_bb_bullish_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "rsi_ma_bb")

    def test_group_key_supports_base_detection(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="base_detection",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "base_detection_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "base_detection")

    def test_list_recent_watchlists_reads_db_screen_runs(self) -> None:
        repository = WatchlistRepository(self.artifacts_dir, database_url="postgres://example")
        with patch.object(repository.history_repository, "is_configured", return_value=True), patch.object(
            repository.history_repository,
            "list_screen_runs",
            return_value=[
                {
                    "id": 41,
                    "strategy_id": "rs",
                    "run_date": dt.date(2026, 6, 18),
                    "watchlist_artifact_path": str(self.artifacts_dir / "screeners" / "2026-06-18" / "rs" / "watchlist.json"),
                    "created_at": dt.datetime(2026, 6, 19, 1, 2, tzinfo=dt.timezone.utc),
                }
            ],
        ):
            rows = repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "rs_new_high_before_price_2026-06-18")
        self.assertEqual(rows[0]["layout"], "db")
        self.assertEqual(rows[0]["screen_run_id"], 41)

    def test_load_watchlist_reads_db_hits(self) -> None:
        repository = WatchlistRepository(self.artifacts_dir, database_url="postgres://example")
        with patch.object(repository.history_repository, "is_configured", return_value=True), patch.object(
            repository.history_repository,
            "find_screen_run_by_watchlist_stem",
            return_value={
                "id": 52,
                "hits": [
                    {"passed": True, "hit_payload_json": {"ticker": "NVDA", "summary": "Leader"}},
                    {"passed": False, "hit_payload_json": {"ticker": "FAIL"}},
                    {"passed": True, "hit_payload_json": {"ticker": "AAPL", "summary": "Also leader"}},
                ],
            },
        ):
            payload = repository.load_watchlist("rs_new_high_before_price_2026-06-18")

        self.assertEqual([item["ticker"] for item in payload], ["NVDA", "AAPL"])

    def test_group_key_supports_cup_detection(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="cup_detection",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "cup_detection_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "cup_detection")

    def test_group_key_supports_double_bottom_detection(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="double_bottom_detection",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "double_bottom_detection_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "double_bottom_detection")

    def test_group_key_supports_weekly_tight_close(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="weekly_tight_close",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "weekly_tight_close_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "weekly_tight_close")

    def test_group_key_supports_weekly_tight_close_breakout(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="weekly_tight_close_breakout",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "weekly_tight_close_breakout_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "weekly_tight_close_breakout")

    def test_group_key_supports_weinstein_stage2_early(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="weinstein_stage2_early",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "weinstein_stage2_early_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "weinstein_stage2_early")

    def test_group_key_supports_rti(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="rti",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "rti_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "rti")

    def test_group_key_supports_bb_squeeze(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="bb_squeeze",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "bb_squeeze_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "bb_squeeze")

    def test_group_key_supports_ema21_pullback_buy(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="ema21_pullback_buy",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "ema21_pullback_buy_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "ema21_pullback_buy")

    def test_group_key_supports_sma200_pullback_buy(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="sma200_pullback_buy",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "sma200_pullback_buy_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "sma200_pullback_buy")

    def test_group_key_supports_sepa_vcp(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="sepa_vcp",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "sepa_vcp_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "sepa_vcp")

    def test_group_key_supports_sean_breakout(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="sean_breakout",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "sean_breakout_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "sean_breakout")

    def test_group_key_supports_trend_template(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="trend_template",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "trend_template_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "trend_template")

    def test_group_key_supports_leif_high_tight_flag(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="leif_high_tight_flag",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "leif_high_tight_flag_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "leif_high_tight_flag")

    def test_group_key_supports_high_tight_flag_setup(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="high_tight_flag_setup",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "high_tight_flag_setup_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "high_tight_flag_setup")

    def test_group_key_supports_inside_dryup_v2(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="inside_dryup_v2",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "inside_dryup_v2_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "inside_dryup_v2")

    def test_group_key_supports_wyckoff_buy_signal(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="wyckoff_buy_signal",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "wyckoff_buy_signal_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "wyckoff_buy_signal")

    def test_group_key_supports_wyckoff_sell_signal(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="wyckoff_sell_signal",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "wyckoff_sell_signal_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "wyckoff_sell_signal")

    def test_group_key_supports_vcs_setup_stage(self) -> None:
        self._write_new_watchlist(
            date_folder="2026-06-06",
            strategy_id="vcs_setup_stage",
            date_label="2026-06-06",
            tickers=["NVDA"],
        )

        rows = self.repository.list_recent_watchlists()

        self.assertEqual(rows[0]["stem"], "vcs_setup_stage_2026-06-06")
        self.assertEqual(rows[0]["group_key"], "vcs")


if __name__ == "__main__":
    unittest.main()
