from __future__ import annotations

import datetime as dt
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.watchlist_service import WatchlistService, _clear_chart_payload_cache


class WatchlistServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        _clear_chart_payload_cache()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.addCleanup(_clear_chart_payload_cache)
        artifacts_dir = Path(self.temp_dir.name)
        watchlists_dir = artifacts_dir / "watchlists"
        watchlists_dir.mkdir(parents=True, exist_ok=True)
        (watchlists_dir / "weekly_htf_pullback_2026-05-31.json").write_text(
            '[{"ticker":"NVDA","company_name":"NVIDIA"}]',
            encoding="utf-8",
        )
        self.service = WatchlistService(artifacts_dir=artifacts_dir)

    def _write_watchlist(self, stem: str, *, tickers: list[str], modified_at: dt.datetime) -> None:
        path = Path(self.temp_dir.name) / "watchlists" / f"{stem}.json"
        payload = [{"ticker": ticker} for ticker in tickers]
        path.write_text(str(payload).replace("'", '"'), encoding="utf-8")
        timestamp = modified_at.timestamp()
        path.touch()
        import os

        os.utime(path, (timestamp, timestamp))

    def _long_price_frame(self) -> pd.DataFrame:
        index = pd.date_range(start="2024-01-02", periods=320, freq="B")
        close_values: list[float] = []
        for idx in range(len(index)):
            if idx < 315:
                close_values.append(80.0 + (idx * 0.35))
            else:
                close_values.extend([189.2, 189.8, 190.1, 189.9, 190.3])
                break
        return pd.DataFrame(
            {
                "Open": [value - 0.35 for value in close_values],
                "High": [value + 0.85 for value in close_values],
                "Low": [value - 0.95 for value in close_values],
                "Close": close_values,
                "Volume": [1_200_000.0 for _ in close_values],
            },
            index=index,
        )

    def test_get_watchlist_detail_fails_open_when_universe_load_errors(self) -> None:
        with patch("src.webapp.services.watchlist_service.load_universe", side_effect=RuntimeError("nasdaq offline")), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ):
            payload = self.service.get_watchlist_detail("weekly_htf_pullback_2026-05-31")

        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["ticker"], "NVDA")

    def test_get_watchlist_detail_includes_latest_canslim_score(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        dated_dir = Path(self.temp_dir.name) / "screeners" / "2026-06-22" / "canslim"
        dated_dir.mkdir(parents=True, exist_ok=True)
        (dated_dir / "watchlist.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "NVDA",
                        "score": 11,
                        "max_score": 14,
                        "rank": 1,
                        "letter_scores": {"C": 2, "A": 2, "N": 2, "S": 1, "L": 2, "I": 1, "M": 1},
                    }
                ]
            ),
            encoding="utf-8",
        )

        with patch("src.webapp.services.watchlist_service.load_universe", return_value=[]), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_rating_snapshots_for_tickers",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={},
        ):
            payload = service.get_watchlist_detail("weekly_htf_pullback_2026-05-31")

        self.assertEqual(payload["entries"][0]["canslim_score"], 11)
        self.assertEqual(payload["entries"][0]["canslim_max_score"], 14)
        self.assertEqual(payload["entries"][0]["canslim_rank"], 1)

    def test_get_watchlist_detail_normalizes_non_finite_numbers(self) -> None:
        path = Path(self.temp_dir.name) / "watchlists" / "fundamental_quality_2026-06-28.json"
        path.write_text(
            '[{"ticker":"PLTR","revenue_3y_cagr_pct":NaN,"diluted_eps_1y_growth_pct":231.09,"nested":{"gross_margin_pct":NaN}}]',
            encoding="utf-8",
        )

        payload = self.service.get_watchlist_detail("fundamental_quality_2026-06-28")

        self.assertIsNone(payload["entries"][0]["revenue_3y_cagr_pct"])
        self.assertEqual(payload["entries"][0]["diluted_eps_1y_growth_pct"], 231.09)
        self.assertIsNone((payload["entries"][0].get("nested") or {}).get("gross_margin_pct"))

    def test_list_recent_can_hide_deprecated_legacy_watchlists(self) -> None:
        self._write_watchlist(
            "fearzone_2026-06-18",
            tickers=["NVDA"],
            modified_at=dt.datetime(2026, 6, 19, 0, 0, tzinfo=dt.timezone.utc),
        )
        (Path(self.temp_dir.name) / "screeners" / "2026-06-18" / "rs").mkdir(parents=True, exist_ok=True)
        dated_path = Path(self.temp_dir.name) / "screeners" / "2026-06-18" / "rs" / "watchlist.json"
        dated_path.write_text('[{"ticker":"PLTR"}]', encoding="utf-8")

        all_rows = self.service.list_recent(include_deprecated=True)
        visible_rows = self.service.list_recent(include_deprecated=False)

        legacy = next(item for item in all_rows if item["stem"] == "fearzone_2026-06-18")
        self.assertTrue(legacy["is_deprecated"])
        self.assertTrue(any(item["stem"] == "fearzone_2026-06-18" for item in all_rows))
        self.assertFalse(any(item["stem"] == "fearzone_2026-06-18" for item in visible_rows))
        self.assertTrue(any(not item["is_deprecated"] for item in visible_rows))

    def test_get_watchlist_detail_blocks_deprecated_for_non_admin(self) -> None:
        self._write_watchlist(
            "fearzone_2026-06-18",
            tickers=["NVDA"],
            modified_at=dt.datetime(2026, 6, 19, 0, 0, tzinfo=dt.timezone.utc),
        )
        with self.assertRaisesRegex(ValueError, "Deprecated watchlist is admin-only"):
            self.service.get_watchlist_detail("fearzone_2026-06-18", allow_deprecated=False)

    def test_get_scanner_board_uses_previous_trading_day_before_new_york_cutoff(self) -> None:
        self._write_watchlist(
            "rs_new_high_before_price_2026-06-11",
            tickers=["CRWD", "PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 0, 45, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "daily_rs_new_high_2026-06-11",
            tickers=["AAPL", "NVDA", "PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 0, 46, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-11",
            tickers=["APP", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 0, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-12",
            tickers=["PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "fearzone_2026-06-11",
            tickers=["TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 1, 0, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "canslim_2026-06-11",
            tickers=["NVDA", "PLTR", "APP"],
            modified_at=dt.datetime(2026, 6, 12, 0, 52, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "cup_detection_2026-06-11",
            tickers=["U"],
            modified_at=dt.datetime(2026, 6, 12, 0, 55, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "eight_week_100_runup_2026-06-11",
            tickers=["PLTR", "APP"],
            modified_at=dt.datetime(2026, 6, 12, 0, 57, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "three_weeks_tight_2026-06-11",
            tickers=["NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 0, 58, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "td9_bullish_2026-06-11",
            tickers=["SHOP"],
            modified_at=dt.datetime(2026, 6, 12, 1, 5, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_2026-06-06",
            tickers=["MSFT", "META"],
            modified_at=dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_all_2026-06-06",
            tickers=["NVDA", "META", "MSFT"],
            modified_at=dt.datetime(2026, 6, 8, 0, 5, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 12, 20, 30, tzinfo=dt.timezone.utc)
            )

        self.assertEqual(payload["target_trading_date"], "2026-06-11")
        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["rs"]["stem"], "rs_new_high_before_price_2026-06-11")
        self.assertEqual(cards["rs"]["entry_count"], 2)
        self.assertEqual(cards["daily_rs_new_high"]["stem"], "daily_rs_new_high_2026-06-11")
        self.assertEqual(cards["daily_rs_new_high"]["entry_count"], 3)
        self.assertEqual(cards["canslim"]["stem"], "canslim_2026-06-11")
        self.assertEqual(cards["canslim"]["entry_count"], 3)
        self.assertEqual(cards["sean_gap_up"]["stem"], "sean_peg_earnings_gap_2026-06-11")
        self.assertEqual(cards["sean_gap_up"]["entry_count"], 2)
        self.assertEqual(cards["cup_detection"]["stem"], "cup_detection_2026-06-11")
        self.assertEqual(cards["cup_detection"]["entry_count"], 1)
        self.assertEqual(cards["eight_week_100_runup"]["stem"], "eight_week_100_runup_2026-06-11")
        self.assertEqual(cards["eight_week_100_runup"]["entry_count"], 2)
        self.assertEqual(cards["three_weeks_tight"]["stem"], "three_weeks_tight_2026-06-11")
        self.assertEqual(cards["three_weeks_tight"]["entry_count"], 1)
        self.assertEqual(cards["fearzone"]["stem"], "fearzone_2026-06-11")
        self.assertEqual(cards["td9_bullish"]["stem"], "td9_bullish_2026-06-11")
        self.assertEqual(cards["weekly_rs_new_high"]["stem"], "weekly_rs_new_high_all_2026-06-06")
        self.assertEqual(cards["weekly_rs_before_price"]["stem"], "weekly_rs_new_high_2026-06-06")

    def test_get_scanner_board_uses_same_day_after_new_york_cutoff(self) -> None:
        self._write_watchlist(
            "rs_new_high_before_price_2026-06-12",
            tickers=["PLTR", "CRWV"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "daily_rs_new_high_2026-06-12",
            tickers=["AAPL", "PLTR", "CRWV"],
            modified_at=dt.datetime(2026, 6, 12, 23, 36, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "sean_peg_earnings_gap_2026-06-12",
            tickers=["PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "fearzone_2026-06-12",
            tickers=["TSLA", "HOOD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "canslim_2026-06-12",
            tickers=["NVDA", "APP"],
            modified_at=dt.datetime(2026, 6, 12, 23, 39, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "canslim_v2_2026-06-12",
            tickers=["NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 39, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "vcs_critical_tightness_2026-06-12",
            tickers=["APP", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 42, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "cup_detection_2026-06-12",
            tickers=["AAPL", "MSFT"],
            modified_at=dt.datetime(2026, 6, 12, 23, 43, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "eight_week_100_runup_2026-06-12",
            tickers=["CRWD", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 44, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "three_weeks_tight_2026-06-12",
            tickers=["ANET", "PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 44, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "td9_bullish_2026-06-12",
            tickers=["SHOP"],
            modified_at=dt.datetime(2026, 6, 12, 23, 45, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_2026-06-06",
            tickers=["MSFT"],
            modified_at=dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "weekly_rs_new_high_all_2026-06-06",
            tickers=["MSFT", "NVDA"],
            modified_at=dt.datetime(2026, 6, 8, 0, 5, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        self.assertEqual(payload["target_trading_date"], "2026-06-12")
        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["rs"]["stem"], "rs_new_high_before_price_2026-06-12")
        self.assertEqual(cards["rs"]["preview_tickers"], ["PLTR", "CRWV"])
        self.assertEqual(cards["daily_rs_new_high"]["stem"], "daily_rs_new_high_2026-06-12")
        self.assertEqual(cards["daily_rs_new_high"]["preview_tickers"], ["AAPL", "PLTR", "CRWV"])
        self.assertEqual(cards["canslim"]["stem"], "canslim_2026-06-12")
        self.assertEqual(cards["canslim"]["preview_tickers"], ["NVDA", "APP"])
        self.assertEqual(cards["canslim_v2"]["stem"], "canslim_v2_2026-06-12")
        self.assertEqual(cards["canslim_v2"]["preview_tickers"], ["NVDA"])
        self.assertEqual(cards["sean_gap_up"]["stem"], "sean_peg_earnings_gap_2026-06-12")
        self.assertEqual(cards["fearzone"]["stem"], "fearzone_2026-06-12")
        self.assertEqual(cards["vcs_critical_tightness"]["stem"], "vcs_critical_tightness_2026-06-12")
        self.assertEqual(cards["vcs_critical_tightness"]["preview_tickers"], ["APP", "NVDA"])
        self.assertEqual(cards["cup_detection"]["stem"], "cup_detection_2026-06-12")
        self.assertEqual(cards["cup_detection"]["preview_tickers"], ["AAPL", "MSFT"])
        self.assertEqual(cards["eight_week_100_runup"]["stem"], "eight_week_100_runup_2026-06-12")
        self.assertEqual(cards["eight_week_100_runup"]["preview_tickers"], ["CRWD", "NVDA"])
        self.assertEqual(cards["three_weeks_tight"]["stem"], "three_weeks_tight_2026-06-12")
        self.assertEqual(cards["three_weeks_tight"]["preview_tickers"], ["ANET", "PLTR"])
        self.assertEqual(cards["td9_bullish"]["stem"], "td9_bullish_2026-06-12")
        self.assertEqual(cards["fearzone"]["preview_tickers"], ["TSLA", "HOOD"])
        self.assertEqual(cards["weekly_rs_new_high"]["stem"], "weekly_rs_new_high_all_2026-06-06")
        self.assertEqual(cards["weekly_rs_new_high"]["preview_tickers"], ["MSFT", "NVDA"])

    def test_force_scanner_board_refresh_bypasses_cutoff_for_admin_override(self) -> None:
        self._write_watchlist(
            "rs_new_high_before_price_2026-06-11",
            tickers=["CRWD"],
            modified_at=dt.datetime(2026, 6, 12, 0, 45, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "rs_new_high_before_price_2026-06-12",
            tickers=["PLTR", "CRWV"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()):
            default_payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 12, 20, 30, tzinfo=dt.timezone.utc)
            )
            refreshed_payload = self.service.force_scanner_board_refresh(
                now=dt.datetime(2026, 6, 12, 20, 30, tzinfo=dt.timezone.utc),
                requested_by="admin@example.com",
            )

        default_cards = {item["id"]: item for item in default_payload["cards"]}
        refreshed_cards = {item["id"]: item for item in refreshed_payload["cards"]}
        self.assertEqual(default_payload["target_trading_date"], "2026-06-11")
        self.assertEqual(default_cards["rs"]["stem"], "rs_new_high_before_price_2026-06-11")
        self.assertEqual(refreshed_payload["target_trading_date"], "2026-06-12")
        self.assertTrue(refreshed_payload["manual_override_active"])
        self.assertEqual(refreshed_payload["manual_override_target_date"], "2026-06-12")
        self.assertEqual(refreshed_cards["rs"]["stem"], "rs_new_high_before_price_2026-06-12")

    def test_get_scanner_top_hits_payload_aggregates_overlap_and_sector_momentum(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "rs_new_high_before_price_2026-06-12.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    },
                    {
                        "ticker": "CRWD",
                        "company_name": "CrowdStrike",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 421.0,
                        "daily_change_pct": 1.2,
                    },
                ]
            ),
            encoding="utf-8",
        )
        (watchlists_dir / "fearzone_2026-06-12.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    },
                    {
                        "ticker": "TSLA",
                        "company_name": "Tesla",
                        "sector": "Consumer Discretionary",
                        "industry": "Auto Manufacturers",
                        "current_price": 188.0,
                        "daily_change_pct": -1.8,
                    },
                ]
            ),
            encoding="utf-8",
        )
        (watchlists_dir / "wyckoff_sell_signal_2026-06-12.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    }
                ]
            ),
            encoding="utf-8",
        )
        self._write_watchlist(
            "weekly_rs_new_high_2026-06-06",
            tickers=["MSFT"],
            modified_at=dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc),
        )
        import os

        os.utime(
            watchlists_dir / "rs_new_high_before_price_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "fearzone_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "wyckoff_sell_signal_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 41, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )

        class _FakeRrgService:
            def get_universe_report(self, *args: object, **kwargs: object) -> dict[str, object]:
                return {
                    "series": [
                        {
                            "ticker": "XLK",
                            "label": "Information Technology",
                            "quadrant": "Leading",
                            "latest": {"x": 103.4, "y": 101.8, "date": "2026-06-12"},
                        }
                    ]
                }

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_rating_snapshots_for_tickers",
            return_value={
                "PLTR": {"overall_rating": 91.0, "current_rank": 7, "sector": "Information Technology", "industry": "Software"},
                "CRWD": {"overall_rating": 88.0, "current_rank": 19, "sector": "Information Technology", "industry": "Software"},
                "TSLA": {"overall_rating": 74.0, "current_rank": 58, "sector": "Consumer Discretionary", "industry": "Auto Manufacturers"},
            },
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={
                "PLTR": {"overall_rating": 95.0, "leadership_score": 97.0, "sector": "Information Technology", "industry": "Software"},
                "CRWD": {"overall_rating": 90.0, "leadership_score": 92.0, "sector": "Information Technology", "industry": "Software"},
                "TSLA": {"overall_rating": 68.0, "leadership_score": 71.0, "sector": "Consumer Discretionary", "industry": "Auto Manufacturers"},
            },
        ), patch.object(
            service,
            "_load_latest_stored_canslim_score_map",
            return_value={"PLTR": {"canslim_score": 11, "canslim_max_score": 14, "canslim_rank": 2}},
        ), patch.object(
            service,
            "_load_latest_stored_vcp_score_map",
            return_value={"PLTR": {"vcp_score": 84.5, "vcp_rating": "Strong VCP"}},
        ):
            payload = service.get_scanner_top_hits_payload(
                rrg_service=_FakeRrgService(),
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(payload["total_unique_tickers"], 3)
        self.assertEqual(payload["overlapping_ticker_count"], 1)
        self.assertEqual(payload["total_live_scanners"], 2)
        pltr = payload["rows"][0]
        self.assertEqual(pltr["ticker"], "PLTR")
        self.assertEqual(pltr["scanner_count"], 2)
        self.assertEqual(pltr["scanner_labels"], ["RS New High Before Price", "Fearzone"])
        self.assertEqual(pltr["day_close"], 132.45)
        self.assertEqual(pltr["change_pct"], 2.1)
        self.assertEqual(pltr["rs_rating"], 97.0)
        self.assertEqual(pltr["ta_rating"], 95.0)
        self.assertEqual(pltr["fa_rating"], 91.0)
        self.assertEqual(pltr["fa_current_rank"], 7)
        self.assertEqual(pltr["canslim_score"], 11)
        self.assertEqual(pltr["canslim_max_score"], 14)
        self.assertEqual(pltr["vcp_score"], 84.5)
        self.assertEqual(pltr["vcp_rating"], "Strong VCP")
        self.assertEqual(pltr["sector_momentum"]["quadrant"], "Leading")
        self.assertEqual(pltr["sector_momentum"]["etf_ticker"], "XLK")

    def test_get_scanner_top_hits_payload_falls_back_to_weekly_rs_when_daily_missing(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "weekly_rs_new_high_all_2026-06-06.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    },
                    {
                        "ticker": "MSFT",
                        "company_name": "Microsoft",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 421.0,
                        "daily_change_pct": 0.8,
                    },
                ]
            ),
            encoding="utf-8",
        )
        (watchlists_dir / "weekly_rs_new_high_2026-06-06.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    },
                    {
                        "ticker": "NVDA",
                        "company_name": "NVIDIA",
                        "sector": "Information Technology",
                        "industry": "Semiconductors",
                        "current_price": 155.0,
                        "daily_change_pct": 1.1,
                    },
                ]
            ),
            encoding="utf-8",
        )
        (watchlists_dir / "fearzone_2026-06-12.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "PLTR",
                        "company_name": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "current_price": 132.45,
                        "daily_change_pct": 2.1,
                    }
                ]
            ),
            encoding="utf-8",
        )
        import os

        os.utime(
            watchlists_dir / "weekly_rs_new_high_all_2026-06-06.json",
            (dt.datetime(2026, 6, 8, 0, 5, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "weekly_rs_new_high_2026-06-06.json",
            (dt.datetime(2026, 6, 8, 0, 0, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "fearzone_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )

        class _FakeRrgService:
            def get_universe_report(self, *args: object, **kwargs: object) -> dict[str, object]:
                return {"series": []}

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_rating_snapshots_for_tickers",
            return_value={},
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={},
        ):
            payload = service.get_scanner_top_hits_payload(
                rrg_service=_FakeRrgService(),
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(payload["total_unique_tickers"], 3)
        self.assertEqual(payload["overlapping_ticker_count"], 1)
        self.assertEqual(payload["total_live_scanners"], 3)
        pltr = payload["rows"][0]
        self.assertEqual(pltr["ticker"], "PLTR")
        self.assertEqual(pltr["scanner_count"], 3)
        self.assertEqual(
            pltr["scanner_labels"],
            ["Fearzone", "Weekly RS New High", "Weekly RS New High Before Price"],
        )

    def test_get_scanner_top_hits_payload_uses_cache_for_same_target_date(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "rs_new_high_before_price_2026-06-12.json").write_text(
            json.dumps([{"ticker": "PLTR", "company_name": "Palantir", "sector": "Information Technology"}]),
            encoding="utf-8",
        )
        (watchlists_dir / "fearzone_2026-06-12.json").write_text(
            json.dumps([{"ticker": "PLTR", "company_name": "Palantir", "sector": "Information Technology"}]),
            encoding="utf-8",
        )
        import os

        os.utime(
            watchlists_dir / "rs_new_high_before_price_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "fearzone_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )

        class _FakeRrgService:
            def __init__(self) -> None:
                self.calls = 0

            def get_universe_report(self, *args: object, **kwargs: object) -> dict[str, object]:
                self.calls += 1
                return {"series": []}

        fake_rrg = _FakeRrgService()
        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_rating_snapshots_for_tickers",
            return_value={},
        ), patch(
            "src.ratings.repository.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={},
        ):
            first_payload = service.get_scanner_top_hits_payload(
                rrg_service=fake_rrg,
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
            )
            second_payload = service.get_scanner_top_hits_payload(
                rrg_service=fake_rrg,
                now=dt.datetime(2026, 6, 13, 1, 1, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(fake_rrg.calls, 1)
        self.assertEqual(first_payload["rows"], second_payload["rows"])

    def test_get_scanner_top_hits_payload_only_enriches_overlapping_tickers(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "rs_new_high_before_price_2026-06-12.json").write_text(
            json.dumps(
                [
                    {"ticker": "PLTR", "company_name": "Palantir", "sector": "Information Technology"},
                    {"ticker": "CRWD", "company_name": "CrowdStrike", "sector": "Information Technology"},
                ]
            ),
            encoding="utf-8",
        )
        (watchlists_dir / "fearzone_2026-06-12.json").write_text(
            json.dumps(
                [
                    {"ticker": "PLTR", "company_name": "Palantir", "sector": "Information Technology"},
                    {"ticker": "TSLA", "company_name": "Tesla", "sector": "Consumer Discretionary"},
                ]
            ),
            encoding="utf-8",
        )
        import os

        os.utime(
            watchlists_dir / "rs_new_high_before_price_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )
        os.utime(
            watchlists_dir / "fearzone_2026-06-12.json",
            (dt.datetime(2026, 6, 12, 23, 40, tzinfo=dt.timezone.utc).timestamp(),) * 2,
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch.object(
            service,
            "_attach_latest_market_snapshots",
        ) as attach_market_mock, patch.object(
            service,
            "_attach_latest_rating_snapshots",
        ) as attach_ratings_mock, patch.object(
            service,
            "_load_sector_momentum_map",
            return_value={},
        ) as load_sector_mock:
            payload = service.get_scanner_top_hits_payload(
                rrg_service=object(),
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(payload["total_unique_tickers"], 3)
        self.assertEqual(payload["overlapping_ticker_count"], 1)
        self.assertEqual([row["ticker"] for row in payload["rows"]], ["PLTR"])
        attach_market_mock.assert_called_once()
        attach_ratings_mock.assert_called_once()
        load_sector_mock.assert_called_once()
        self.assertEqual(attach_market_mock.call_args.args[1], ["PLTR"])
        self.assertEqual(attach_ratings_mock.call_args.args[1], ["PLTR"])

    def test_get_scanner_top_hits_payload_prefers_persisted_snapshot(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        run_date = dt.date(2026, 6, 12)
        persisted_run = {
            "id": 901,
            "strategy_id": "scanner_top_hits_snapshot",
            "run_date": run_date,
            "result_summary_json": {
                "generated_at": "2026-06-13T00:40:00Z",
                "reference_now_new_york": "2026-06-12T20:40:00-04:00",
                "target_trading_date": "2026-06-12",
                "cutoff_time_label": "20:30 America/New_York",
                "latest_update_at": "2026-06-13T00:38:00Z",
                "latest_signal_date": "2026-06-12",
                "manual_override_active": False,
                "manual_override_target_date": "",
                "manual_override_requested_at": "",
                "total_live_scanners": 3,
                "total_unique_tickers": 5,
                "overlapping_ticker_count": 1,
            },
        }
        persisted_detail = {
            **persisted_run,
            "hits": [
                {
                    "ticker": "PLTR",
                    "hit_payload_json": {
                        "ticker": "PLTR",
                        "company": "Palantir",
                        "sector": "Information Technology",
                        "industry": "Software",
                        "scanner_count": 2,
                        "scanner_labels": ["RS New High Before Price", "Fearzone"],
                        "scanners": [],
                    },
                }
            ],
        }

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value=set()), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch.object(
            service.screener_history_service,
            "is_configured",
            return_value=True,
        ), patch.object(
            service.screener_history_service,
            "list_runs",
            return_value=[persisted_run],
        ) as list_runs_mock, patch.object(
            service.screener_history_service,
            "get_run",
            return_value=persisted_detail,
        ) as get_run_mock, patch.object(
            service,
            "_select_scanner_top_hit_live_cards",
        ) as live_cards_mock:
            payload = service.get_scanner_top_hits_payload(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
            )

        self.assertEqual(payload["rows"][0]["ticker"], "PLTR")
        self.assertEqual(payload["total_unique_tickers"], 5)
        list_runs_mock.assert_called_once()
        get_run_mock.assert_called_once_with(901, include_hits=True, hit_limit=5000)
        live_cards_mock.assert_not_called()

    def test_force_scanner_board_refresh_persists_top_hit_snapshot(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        with patch.object(
            service,
            "persist_scanner_top_hits_snapshot",
            return_value=None,
        ) as persist_mock:
            payload = service.force_scanner_board_refresh(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc),
                requested_by="admin@example.com",
            )

        self.assertEqual(payload["target_trading_date"], "2026-06-12")
        persist_mock.assert_called_once()

    def test_get_chart_payload_snaps_to_latest_available_trading_day(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [100.0, 102.0],
                "High": [103.0, 104.0],
                "Low": [99.0, 101.0],
                "Close": [102.0, 103.0],
                "Volume": [1_000_000, 1_200_000],
            },
            index=pd.to_datetime(["2026-05-28", "2026-05-29"]),
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"NVDA", "SPY"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 30))

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["requested_as_of_date"], "2026-05-30")
        self.assertEqual(payload["resolved_as_of_date"], "2026-05-29")
        self.assertEqual(payload["latest_available_date"], "2026-05-29")
        self.assertEqual(payload["candles"][-1]["time"], "2026-05-29")
        self.assertEqual(payload["data_source"], "internet")
        self.assertIn("market_extension", payload)
        self.assertIn("vcs", payload)

    def test_get_watchlist_detail_prefers_db_previous_scan_comparison(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        db_rows = [
            {
                "id": 101,
                "strategy_id": "rs",
                "run_date": dt.date(2026, 6, 18),
                "watchlist_artifact_path": str(Path(self.temp_dir.name) / "screeners" / "2026-06-18" / "rs" / "watchlist.json"),
                "created_at": dt.datetime(2026, 6, 19, 0, 15, tzinfo=dt.timezone.utc),
            },
            {
                "id": 100,
                "strategy_id": "rs",
                "run_date": dt.date(2026, 6, 17),
                "watchlist_artifact_path": str(Path(self.temp_dir.name) / "screeners" / "2026-06-17" / "rs" / "watchlist.json"),
                "created_at": dt.datetime(2026, 6, 18, 0, 15, tzinfo=dt.timezone.utc),
            },
        ]
        db_hits = {
            "rs_new_high_before_price_2026-06-18": {
                "id": 101,
                "hits": [
                    {"passed": True, "hit_payload_json": {"ticker": "NVDA"}},
                    {"passed": True, "hit_payload_json": {"ticker": "CRWD"}},
                ],
            },
            "rs_new_high_before_price_2026-06-17": {
                "id": 100,
                "hits": [
                    {"passed": True, "hit_payload_json": {"ticker": "NVDA"}},
                ],
            },
        }

        with patch.object(service.repository.history_repository, "is_configured", return_value=True), patch.object(
            service.repository.history_repository,
            "list_screen_runs",
            return_value=db_rows,
        ), patch.object(
            service.repository.history_repository,
            "find_screen_run_by_watchlist_stem",
            side_effect=lambda stem, include_hits=True: db_hits.get(stem),
        ), patch(
            "src.webapp.services.watchlist_service.load_universe",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ):
            payload = service.get_watchlist_detail("rs_new_high_before_price_2026-06-18")

        self.assertTrue(payload["has_previous_scan"])
        self.assertEqual(payload["previous_stem"], "rs_new_high_before_price_2026-06-17")
        self.assertEqual(payload["new_ticker_count"], 1)
        entry_map = {entry["ticker"]: entry for entry in payload["entries"]}
        self.assertFalse(bool(entry_map["NVDA"]["is_new"]))
        self.assertTrue(bool(entry_map["CRWD"]["is_new"]))

    def test_get_watchlist_detail_filters_excluded_tickers(self) -> None:
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "fearzone_2026-06-13.json").write_text(
            '[{"ticker":"NVDA"},{"ticker":"TSLA"}]',
            encoding="utf-8",
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ):
            payload = self.service.get_watchlist_detail("fearzone_2026-06-13")

        self.assertEqual(payload["entry_count"], 1)
        self.assertEqual(payload["entries"][0]["ticker"], "NVDA")

    def test_get_watchlist_detail_marks_tickers_new_vs_previous_scan(self) -> None:
        self._write_watchlist(
            "fearzone_2026-06-12",
            tickers=["NVDA", "AAPL"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "fearzone_2026-06-13",
            tickers=["NVDA", "MSFT"],
            modified_at=dt.datetime(2026, 6, 13, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_universe", return_value=[]), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ):
            payload = self.service.get_watchlist_detail("fearzone_2026-06-13")

        entries = {str(item["ticker"]): item for item in payload["entries"]}
        self.assertTrue(payload["has_previous_scan"])
        self.assertEqual(payload["previous_stem"], "fearzone_2026-06-12")
        self.assertEqual(payload["new_ticker_count"], 1)
        self.assertFalse(entries["NVDA"]["is_new"])
        self.assertTrue(entries["MSFT"]["is_new"])

    def test_get_watchlist_detail_leaves_new_false_without_previous_scan(self) -> None:
        self._write_watchlist(
            "fearzone_2026-06-13",
            tickers=["NVDA"],
            modified_at=dt.datetime(2026, 6, 13, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_universe", return_value=[]), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ), patch(
            "src.webapp.services.watchlist_service.load_excluded_tickers",
            return_value=set(),
        ):
            payload = self.service.get_watchlist_detail("fearzone_2026-06-13")

        self.assertFalse(payload["has_previous_scan"])
        self.assertEqual(payload["previous_stem"], "")
        self.assertEqual(payload["new_ticker_count"], 0)
        self.assertFalse(payload["entries"][0]["is_new"])

    def test_get_watchlist_detail_attaches_latest_db_volume_and_change(self) -> None:
        watchlists_dir = Path(self.temp_dir.name) / "watchlists"
        (watchlists_dir / "fearzone_2026-06-13.json").write_text(
            '[{"ticker":"NVDA"}]',
            encoding="utf-8",
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = pd.DataFrame(
            {
                "Open": [100.0, 103.0],
                "High": [104.0, 106.0],
                "Low": [99.0, 102.0],
                "Close": [102.0, 105.0],
                "Adj Close": [102.0, 105.0],
                "Volume": [1_200_000, 1_500_000],
            },
            index=pd.to_datetime(["2026-06-12", "2026-06-15"]),
        )

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows", return_value={"NVDA": frame.copy()}), patch(
            "src.webapp.services.watchlist_service.load_etf_catalog",
            return_value=[],
        ), patch(
            "src.webapp.services.watchlist_service.load_ticker_theme_overrides",
            return_value={},
        ):
            payload = service.get_watchlist_detail("fearzone_2026-06-13")

        entry = payload["entries"][0]
        self.assertEqual(entry["latest_trade_date"], "2026-06-15")
        self.assertEqual(entry["current_volume"], 1_500_000)
        self.assertAlmostEqual(entry["daily_change_pct"], ((105.0 / 102.0) - 1.0) * 100.0)

    def test_get_scanner_board_filters_excluded_tickers_from_counts_and_previews(self) -> None:
        self._write_watchlist(
            "sepa_vcp_2026-06-12",
            tickers=["NVDA", "TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["sepa_vcp"]["entry_count"], 1)
        self.assertEqual(cards["sepa_vcp"]["preview_tickers"], ["NVDA"])

    def test_get_scanner_board_includes_trend_template_card(self) -> None:
        self._write_watchlist(
            "trend_template_2026-06-12",
            tickers=["NVDA", "CRWD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["trend_template"]["available"])
        self.assertEqual(cards["trend_template"]["entry_count"], 2)
        self.assertEqual(cards["trend_template"]["preview_tickers"], ["NVDA", "CRWD"])

    def test_get_scanner_board_includes_venu_scanner_card(self) -> None:
        self._write_watchlist(
            "venu_scanner_2026-06-12",
            tickers=["PLTR", "APP"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["venu_scanner"]["available"])
        self.assertEqual(cards["venu_scanner"]["entry_count"], 2)
        self.assertEqual(cards["venu_scanner"]["preview_tickers"], ["PLTR", "APP"])

    def test_get_scanner_board_includes_bullish_finviz_pattern_cards(self) -> None:
        dated_dir = Path(self.temp_dir.name) / "screeners" / "2026-06-12" / "finviz_pattern_doublebottom"
        dated_dir.mkdir(parents=True, exist_ok=True)
        (dated_dir / "watchlist.json").write_text(
            json.dumps([{"ticker": "PLTR"}, {"ticker": "APP"}]),
            encoding="utf-8",
        )
        (dated_dir / "run_summary.json").write_text(
            json.dumps(
                {
                    "strategy_id": "finviz_pattern_doublebottom",
                    "date_label": "2026-06-12",
                    "watchlist_file": str(dated_dir / "watchlist.json"),
                    "raw_results_file": str(dated_dir / "raw_results.json"),
                }
            ),
            encoding="utf-8",
        )
        timestamp = dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc).timestamp()
        import os

        os.utime(dated_dir / "watchlist.json", (timestamp, timestamp))

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        expected_card_ids = {
            "finviz_pattern_horizontal",
            "finviz_pattern_horizontal2",
            "finviz_pattern_tlsupport",
            "finviz_pattern_tlsupport2",
            "finviz_pattern_wedgedown",
            "finviz_pattern_wedgedown2",
            "finviz_pattern_wedgeresistance",
            "finviz_pattern_wedgeresistance2",
            "finviz_pattern_channelup",
            "finviz_pattern_channelup2",
            "finviz_pattern_doublebottom",
            "finviz_pattern_multiplebottom",
            "finviz_pattern_headandshouldersinv",
        }

        self.assertTrue(expected_card_ids.issubset(cards.keys()))
        self.assertTrue(cards["finviz_pattern_doublebottom"]["available"])
        self.assertEqual(cards["finviz_pattern_doublebottom"]["entry_count"], 2)
        self.assertEqual(cards["finviz_pattern_doublebottom"]["preview_tickers"], ["PLTR", "APP"])

    def test_get_scanner_board_includes_fundamental_quality_card(self) -> None:
        self._write_watchlist(
            "fundamental_quality_2026-06-12",
            tickers=["PLTR", "LLY", "AGI"],
            modified_at=dt.datetime(2026, 6, 12, 23, 37, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["fundamental_quality"]["available"])
        self.assertEqual(cards["fundamental_quality"]["entry_count"], 3)
        self.assertEqual(cards["fundamental_quality"]["preview_tickers"], ["PLTR", "LLY", "AGI"])

    def test_get_scanner_board_excludes_gamma_squeeze_card(self) -> None:
        self._write_watchlist(
            "gamma_squeeze_2026-06-12",
            tickers=["NVDA", "TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertNotIn("gamma_squeeze", cards)

    def test_get_scanner_board_includes_range_tightness_index_card(self) -> None:
        self._write_watchlist(
            "rti_2026-06-12",
            tickers=["NVDA", "CRWD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 36, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["rti"]["label"], "Range Tightness Index")
        self.assertTrue(cards["rti"]["available"])
        self.assertEqual(cards["rti"]["entry_count"], 2)
        self.assertEqual(cards["rti"]["preview_tickers"], ["NVDA", "CRWD"])

    def test_get_scanner_board_includes_vcp_spec_card(self) -> None:
        self._write_watchlist(
            "vcp_spec_2026-06-12",
            tickers=["NVDA", "CRWD"],
            modified_at=dt.datetime(2026, 6, 12, 23, 36, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["vcp_spec"]["label"], "VCP Spec")
        self.assertTrue(cards["vcp_spec"]["available"])
        self.assertEqual(cards["vcp_spec"]["entry_count"], 2)
        self.assertEqual(cards["vcp_spec"]["preview_tickers"], ["NVDA", "CRWD"])

    def test_get_scanner_board_includes_double_bottom_card(self) -> None:
        self._write_watchlist(
            "double_bottom_detection_2026-06-12",
            tickers=["APP", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 37, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["double_bottom_detection"]["label"], "Double Bottom")
        self.assertTrue(cards["double_bottom_detection"]["available"])
        self.assertEqual(cards["double_bottom_detection"]["entry_count"], 2)
        self.assertEqual(cards["double_bottom_detection"]["preview_tickers"], ["APP", "NVDA"])

    def test_get_scanner_board_includes_ftd_successful_sweep_card(self) -> None:
        self._write_watchlist(
            "ftd_sweep_2026-06-12",
            tickers=["PLTR", "APP"],
            modified_at=dt.datetime(2026, 6, 12, 23, 38, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertEqual(cards["ftd_sweep"]["label"], "FTD Successful Sweep")
        self.assertTrue(cards["ftd_sweep"]["available"])
        self.assertEqual(cards["ftd_sweep"]["entry_count"], 2)
        self.assertEqual(cards["ftd_sweep"]["preview_tickers"], ["PLTR", "APP"])
        self.assertIn("Trend Template (TTP)", cards["trend_template"]["description"])

    def test_get_scanner_board_includes_sean_breakout_card(self) -> None:
        self._write_watchlist(
            "sean_breakout_2026-06-12",
            tickers=["APP", "CRDO"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["sean_breakout"]["available"])
        self.assertEqual(cards["sean_breakout"]["entry_count"], 2)
        self.assertEqual(cards["sean_breakout"]["preview_tickers"], ["APP", "CRDO"])

    def test_get_scanner_board_includes_ema21_pullback_buy_card(self) -> None:
        self._write_watchlist(
            "ema21_pullback_buy_2026-06-12",
            tickers=["NVDA", "AAPL"],
            modified_at=dt.datetime(2026, 6, 12, 23, 31, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["ema21_pullback_buy"]["available"])
        self.assertEqual(cards["ema21_pullback_buy"]["entry_count"], 2)
        self.assertEqual(cards["ema21_pullback_buy"]["preview_tickers"], ["NVDA", "AAPL"])

    def test_get_scanner_board_includes_sma200_pullback_buy_card(self) -> None:
        self._write_watchlist(
            "sma200_pullback_buy_2026-06-12",
            tickers=["MSFT", "AAPL"],
            modified_at=dt.datetime(2026, 6, 12, 23, 31, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["sma200_pullback_buy"]["available"])
        self.assertEqual(cards["sma200_pullback_buy"]["entry_count"], 2)
        self.assertEqual(cards["sma200_pullback_buy"]["preview_tickers"], ["MSFT", "AAPL"])

    def test_get_scanner_board_includes_weekly_tight_close_card(self) -> None:
        self._write_watchlist(
            "weekly_tight_close_2026-06-12",
            tickers=["NVDA", "PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 31, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["weekly_tight_close"]["available"])
        self.assertEqual(cards["weekly_tight_close"]["entry_count"], 2)
        self.assertEqual(cards["weekly_tight_close"]["preview_tickers"], ["NVDA", "PLTR"])
        self.assertEqual(cards["weekly_tight_close"]["timeframe"], "Weekly")

    def test_get_scanner_board_includes_weinstein_stage2_early_card(self) -> None:
        self._write_watchlist(
            "weinstein_stage2_early_2026-06-12",
            tickers=["TSM", "NVDA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 31, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["weinstein_stage2_early"]["available"])
        self.assertEqual(cards["weinstein_stage2_early"]["entry_count"], 2)
        self.assertEqual(cards["weinstein_stage2_early"]["preview_tickers"], ["TSM", "NVDA"])
        self.assertEqual(cards["weinstein_stage2_early"]["timeframe"], "Weekly")

    def test_get_scanner_board_includes_gap_fill_card(self) -> None:
        self._write_watchlist(
            "gap_fill_2026-06-12",
            tickers=["PLTR", "NET"],
            modified_at=dt.datetime(2026, 6, 12, 23, 32, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["gap_fill"]["available"])
        self.assertEqual(cards["gap_fill"]["entry_count"], 2)
        self.assertEqual(cards["gap_fill"]["preview_tickers"], ["PLTR", "NET"])

    def test_get_scanner_board_includes_macd_golden_cross_card(self) -> None:
        self._write_watchlist(
            "macd_golden_cross_2026-06-12",
            tickers=["NVDA", "AAPL"],
            modified_at=dt.datetime(2026, 6, 12, 23, 33, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["macd_golden_cross"]["available"])
        self.assertEqual(cards["macd_golden_cross"]["entry_count"], 2)
        self.assertEqual(cards["macd_golden_cross"]["preview_tickers"], ["NVDA", "AAPL"])
        self.assertEqual(cards["macd_golden_cross"]["timeframe"], "Daily")

    def test_get_scanner_board_includes_inside_dryup_v2_card(self) -> None:
        self._write_watchlist(
            "inside_dryup_v2_2026-06-12",
            tickers=["NVDA", "PLTR"],
            modified_at=dt.datetime(2026, 6, 12, 23, 34, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["inside_dryup_v2"]["available"])
        self.assertEqual(cards["inside_dryup_v2"]["entry_count"], 2)
        self.assertEqual(cards["inside_dryup_v2"]["preview_tickers"], ["NVDA", "PLTR"])

    def test_get_scanner_board_includes_wyckoff_cards(self) -> None:
        self._write_watchlist(
            "wyckoff_buy_signal_2026-06-12",
            tickers=["AAPL", "MSFT"],
            modified_at=dt.datetime(2026, 6, 12, 23, 35, tzinfo=dt.timezone.utc),
        )
        self._write_watchlist(
            "wyckoff_sell_signal_2026-06-12",
            tickers=["TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 36, tzinfo=dt.timezone.utc),
        )

        payload = self.service.get_scanner_board(
            now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
        )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertTrue(cards["wyckoff_buy_signal"]["available"])
        self.assertEqual(cards["wyckoff_buy_signal"]["preview_tickers"], ["AAPL", "MSFT"])
        self.assertNotIn("wyckoff_sell_signal", cards)

    def test_get_scanner_board_marks_card_unavailable_when_all_results_excluded(self) -> None:
        self._write_watchlist(
            "sepa_vcp_2026-06-12",
            tickers=["TSLA"],
            modified_at=dt.datetime(2026, 6, 12, 23, 30, tzinfo=dt.timezone.utc),
        )

        with patch("src.webapp.services.watchlist_service.load_excluded_tickers", return_value={"TSLA"}):
            payload = self.service.get_scanner_board(
                now=dt.datetime(2026, 6, 13, 1, 0, tzinfo=dt.timezone.utc)
            )

        cards = {item["id"]: item for item in payload["cards"]}
        self.assertFalse(cards["sepa_vcp"]["available"])
        self.assertEqual(cards["sepa_vcp"]["entry_count"], 0)

    def test_get_chart_payload_coerces_decimal_db_values(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [Decimal("90.0"), Decimal("100.0"), Decimal("102.0")],
                "High": [Decimal("91.0"), Decimal("103.0"), Decimal("104.0")],
                "Low": [Decimal("89.0"), Decimal("99.0"), Decimal("101.0")],
                "Close": [Decimal("90.5"), Decimal("102.0"), Decimal("103.0")],
                "Adj Close": [Decimal("90.5"), Decimal("102.0"), Decimal("103.0")],
                "Volume": [900000, 1000000, 1200000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"NVDA": frame.copy(), "SPY": frame.copy()}):
            payload = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(payload["data_source"], "database")
        self.assertEqual(payload["candles"][-1]["close"], 103.0)
        self.assertTrue(len(payload["ipo_vwap"]) > 0)
        self.assertEqual(payload["market_extension"]["config"]["label"], "10W SMA")

    def test_get_chart_payload_falls_back_to_internet_for_missing_benchmark(self) -> None:
        ticker_frame = pd.DataFrame(
            {
                "Open": [90.0, 100.0, 102.0],
                "High": [91.0, 103.0, 104.0],
                "Low": [89.0, 99.0, 101.0],
                "Close": [90.5, 102.0, 103.0],
                "Adj Close": [90.5, 102.0, 103.0],
                "Volume": [900_000, 1_000_000, 1_200_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        benchmark_frame = pd.DataFrame(
            {
                "Open": [490.0, 500.0, 505.0],
                "High": [491.0, 506.0, 507.0],
                "Low": [489.0, 498.0, 503.0],
                "Close": [490.5, 504.0, 506.0],
                "Adj Close": [490.5, 504.0, 506.0],
                "Volume": [1_900_000, 2_000_000, 2_100_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-05-28", "2026-05-29"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"NVDA": ticker_frame.copy()}), patch(
            "src.webapp.services.watchlist_service._download_history_frame",
            return_value=benchmark_frame.copy(),
        ):
            payload = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(payload["data_source"], "database+ticker/internet+benchmark")
        self.assertTrue(len(payload["rs_line"]) > 0)

    def test_get_chart_payload_falls_back_to_internet_for_shallow_db_ticker_history(self) -> None:
        shallow_ticker_frame = pd.DataFrame(
            {
                "Open": [20.0, 21.0],
                "High": [21.0, 22.0],
                "Low": [19.0, 20.0],
                "Close": [20.5, 21.5],
                "Adj Close": [20.5, 21.5],
                "Volume": [1_000_000, 1_100_000],
            },
            index=pd.to_datetime(["2026-06-04", "2026-06-05"]),
        )
        full_frame = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0],
                "High": [101.0, 102.0, 103.0],
                "Low": [99.0, 100.0, 101.0],
                "Close": [100.5, 101.5, 102.5],
                "Adj Close": [100.5, 101.5, 102.5],
                "Volume": [2_000_000, 2_100_000, 2_200_000],
            },
            index=pd.to_datetime(["2024-11-01", "2026-06-04", "2026-06-05"]),
        )
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"FCEL", "SPY"}:
                return full_frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.load_many_ticker_windows_for_range", return_value={"FCEL": shallow_ticker_frame.copy()}), patch(
            "src.webapp.services.watchlist_service.yf.download",
            side_effect=fake_download,
        ):
            payload = service.get_chart_payload("FCEL", as_of_date=dt.date(2026, 6, 5))

        self.assertEqual(payload["data_source"], "internet")
        self.assertEqual(payload["candles"][0]["time"], "2026-06-04")
        self.assertEqual(payload["candles"][-1]["time"], "2026-06-05")

    def test_get_chart_payload_includes_market_extension_overlay(self) -> None:
        index = pd.date_range(start="2026-01-05", periods=90, freq="B")
        close_values = [100.0 + (idx * 0.8) for idx in range(len(index) - 8)]
        close_values.extend([176.0, 181.0, 187.0, 194.0, 201.0, 208.0, 214.0, 210.0])
        frame = pd.DataFrame(
            {
                "Open": [value - 1.0 for value in close_values],
                "High": [value + 2.0 for value in close_values],
                "Low": [value - 2.0 for value in close_values],
                "Close": close_values,
                "Volume": [1_500_000 for _ in close_values],
            },
            index=index,
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"SPY", "QQQ"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("SPY", as_of_date=dt.date(2026, 5, 8))

        self.assertGreater(len(payload["market_extension"]["line"]), 0)
        self.assertIsNotNone(payload["market_extension"]["latest"])
        latest = payload["market_extension"]["latest"]
        assert latest is not None
        self.assertIn(latest["state"], {"warning", "extreme"})
        self.assertGreater(latest["extension_pct"], 11.0)

    def test_get_chart_overlays_payload_includes_mark_daily_extend_marker(self) -> None:
        index = pd.date_range(start="2026-01-05", periods=40, freq="B")
        close_values = [100.0 + (idx * 0.4) for idx in range(len(index) - 1)] + [118.0]
        high_values = [value + 0.8 for value in close_values[:-1]] + [128.0]
        low_values = [value - 0.8 for value in close_values[:-1]] + [117.0]
        frame = pd.DataFrame(
            {
                "Open": [value - 0.5 for value in close_values],
                "High": high_values,
                "Low": low_values,
                "Close": close_values,
                "Volume": [1_500_000 for _ in close_values],
            },
            index=index,
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"NVDA", "SPY"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_overlays_payload(
                "NVDA",
                as_of_date=dt.date(2026, 2, 27),
                include_setup_markers=True,
            )

        markers = [marker for marker in payload["setup_markers"] if marker["kind"] == "mark_daily_extend"]
        self.assertEqual(len(markers), 1)
        self.assertEqual(markers[0]["time"], "2026-02-27")
        self.assertEqual(markers[0]["label"], "Mark Extend")
        self.assertGreater(markers[0]["distance"], markers[0]["threshold"])

    def test_get_chart_payload_includes_sepa_dashboard_snapshot(self) -> None:
        ticker_frame = self._long_price_frame()
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100.0 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "High": [100.4 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Low": [99.6 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Close": [100.0 + (idx * 0.05) for idx in range(len(ticker_frame.index))],
                "Volume": [2_000_000.0 for _ in range(len(ticker_frame.index))],
            },
            index=ticker_frame.index,
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers == "NVDA":
                return ticker_frame.copy()
            if tickers == "SPY":
                return benchmark_frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_payload("NVDA", as_of_date=dt.date(2025, 3, 24))

        self.assertIn("sepa_dashboard", payload)
        self.assertIsNotNone(payload["sepa_dashboard"])
        dashboard = payload["sepa_dashboard"]
        assert dashboard is not None
        self.assertEqual(dashboard["tpr_status"], "PASSED")
        self.assertEqual(dashboard["buy_risk_status"], "Low Risk")
        self.assertEqual(dashboard["pressure_status"], "Buying")
        self.assertEqual(dashboard["recent_vcp_signal_date"], "2025-03-24")

    def test_get_chart_overlays_payload_includes_trend_template_snapshot(self) -> None:
        frame = pd.DataFrame(
            {
                "Open": [value - 1.0 for value in [100.0 + (idx * 0.45) for idx in range(300)] + [236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0]],
                "High": [value + 1.5 for value in [100.0 + (idx * 0.45) for idx in range(300)] + [236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0]],
                "Low": [value - 1.5 for value in [100.0 + (idx * 0.45) for idx in range(300)] + [236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0]],
                "Close": [100.0 + (idx * 0.45) for idx in range(300)] + [236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0],
                "Volume": [1_400_000.0 for _ in range(320)],
            },
            index=pd.date_range("2025-01-02", periods=320, freq="B"),
        )
        for start, end, delta in ((131, 139, 20.0), (287, 299, -60.0), (281, 300, -50.0)):
            frame.loc[frame.index[start:end], "Close"] += delta
            frame.loc[frame.index[start:end], "High"] += delta
            frame.loc[frame.index[start:end], "Low"] += delta

        def fake_download(*, tickers: str, **_: object):
            if tickers in {"NVDA", "SPY"}:
                return frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_overlays_payload("NVDA", as_of_date=dt.date(2026, 3, 25))

        self.assertIn("trend_template", payload)
        self.assertIsNotNone(payload["trend_template"])
        snapshot = payload["trend_template"]
        assert snapshot is not None
        self.assertTrue(snapshot["matched"])
        self.assertEqual(snapshot["criteria_passed"], snapshot["criteria_total"])
        self.assertGreater(snapshot["rs_rating"], 70.0)

    def test_get_chart_payload_uses_backend_cache_for_repeat_requests(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ) as load_patch:
            first = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))
            second = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        self.assertEqual(load_patch.call_count, 1)
        self.assertEqual(first["candles"], second["candles"])
        self.assertEqual(first["data_source"], "database")

    def test_get_chart_payload_does_not_cache_empty_payloads(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={},
        ) as load_patch:
            first = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))
            second = service.get_chart_payload("NVDA", as_of_date=dt.date(2026, 5, 29))

        self.assertEqual(load_patch.call_count, 2)
        self.assertEqual(first["candles"], [])
        self.assertEqual(second["candles"], [])

    def test_get_chart_payload_skips_setup_markers_by_default(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_ftd_sweep_markers",
        ) as markers_patch:
            payload = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        markers_patch.assert_not_called()
        self.assertEqual(payload["setup_markers"], [])

    def test_get_chart_gex_payload_uses_existing_report_renderer_and_cache(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name))
        report = {
            "symbol": "NVDA",
            "as_of": "2026-06-24T20:15:00+00:00",
            "underlying_price": 155.25,
            "net_gex": 1_250_000_000.0,
            "gamma_flip": 152.0,
            "call_gex_total": 2_100_000_000.0,
            "put_gex_total": -850_000_000.0,
            "call_wall": 160.0,
            "put_wall": 150.0,
            "atm_pin_strike": 155.0,
            "put_call_oi_ratio": 0.91,
            "strike_count": 48,
            "next_expiry": "2026-06-26",
            "next_monthly_expiry": "2026-07-17",
            "summary": "NVDA all-expiry profile.",
            "methodology": "CBOE delayed chain with all listed expiries.",
            "source_url": "https://cdn.cboe.com/api/global/delayed_quotes/options/NVDA.json",
        }

        with patch(
            "src.webapp.services.watchlist_service.build_gamma_exposure_report",
            return_value=report,
        ) as report_patch, patch(
            "src.webapp.services.watchlist_service.render_gamma_exposure_report_svgs",
            return_value={"absolute": "<svg/>", "by_option_type": "<svg/>", "profile": "<svg/>"},
        ) as render_patch:
            first = service.get_chart_gex_payload("NVDA")
            second = service.get_chart_gex_payload("NVDA")

        report_patch.assert_called_once_with(symbol="NVDA", timeout_seconds=12)
        render_patch.assert_called_once_with(report)
        self.assertTrue(first["available"])
        self.assertEqual(first["gex_label"], "Positive Gamma")
        self.assertEqual(first["plots"]["profile"], "<svg/>")
        self.assertEqual(first, second)

    def test_get_chart_gex_payload_returns_unavailable_payload_when_report_fails(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name))

        with patch(
            "src.webapp.services.watchlist_service.build_gamma_exposure_report",
            side_effect=ValueError("No options rows available for expiry 2026-06-24."),
        ):
            payload = service.get_chart_gex_payload("XYZ")

        self.assertEqual(payload["ticker"], "XYZ")
        self.assertFalse(payload["available"])
        self.assertIn("No options rows available", payload["error"])
        self.assertIsNone(payload["plots"])

    def test_get_chart_payload_skips_heavy_overlay_computation(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_market_extension_overlay",
        ) as market_extension_patch, patch(
            "src.webapp.services.watchlist_service._compute_fearzone_panel",
        ) as fearzone_patch, patch(
            "src.webapp.services.watchlist_service.latest_vcs_snapshot",
        ) as vcs_patch, patch(
            "src.webapp.services.watchlist_service.build_sepa_dashboard_snapshot",
        ) as sepa_patch:
            payload = service.get_chart_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        market_extension_patch.assert_not_called()
        fearzone_patch.assert_not_called()
        vcs_patch.assert_not_called()
        sepa_patch.assert_not_called()
        self.assertEqual(payload["rs_line"], [])

    def test_get_chart_payload_includes_setup_markers_when_requested(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_ftd_sweep_markers",
            return_value=[{"time": "2025-03-24", "kind": "ftd_sweep_breakout", "label": "FTD Sweep"}],
        ) as markers_patch, patch(
            "src.webapp.services.watchlist_service.compute_wyckoff_markers",
            return_value=[{"time": "2025-03-20", "kind": "wyckoff_buy_signal", "label": "BUY"}],
        ) as wyckoff_patch:
            payload = service.get_chart_overlays_payload(
                "NVDA",
                period="6mo",
                as_of_date=dt.date(2025, 3, 24),
                include_setup_markers=True,
            )

        markers_patch.assert_called_once()
        wyckoff_patch.assert_called_once()
        self.assertEqual([marker["kind"] for marker in payload["setup_markers"]], ["ftd_sweep_breakout", "wyckoff_buy_signal"])

    def test_get_chart_overlays_payload_includes_danger_signals_snapshot(self) -> None:
        index = pd.date_range(start="2026-01-05", periods=80, freq="B")
        close_values = [100.0 + (idx * 0.9) for idx in range(76)] + [169.0, 164.0, 160.0, 157.2]
        open_values = [value - 0.8 for value in close_values[:-4]] + [169.4, 165.2, 161.8, 161.0]
        high_values = [value + 1.6 for value in close_values[:-4]] + [170.2, 165.6, 162.0, 162.0]
        low_values = [value - 1.6 for value in close_values[:-4]] + [168.0, 163.0, 159.0, 157.0]
        volume_values = [1_200_000.0 for _ in range(76)] + [1_500_000.0, 1_900_000.0, 2_300_000.0, 3_200_000.0]
        frame = pd.DataFrame(
            {
                "Open": open_values,
                "High": high_values,
                "Low": low_values,
                "Close": close_values,
                "Volume": volume_values,
            },
            index=index,
        )
        benchmark_frame = pd.DataFrame(
            {
                "Open": [100.0 + (idx * 0.35) for idx in range(len(index))],
                "High": [101.0 + (idx * 0.35) for idx in range(len(index))],
                "Low": [99.0 + (idx * 0.35) for idx in range(len(index))],
                "Close": [100.0 + (idx * 0.35) for idx in range(len(index))],
                "Volume": [2_000_000.0 for _ in range(len(index))],
            },
            index=index,
        )

        def fake_download(*, tickers: str, **_: object):
            if tickers == "NVDA":
                return frame.copy()
            if tickers == "SPY":
                return benchmark_frame.copy()
            return pd.DataFrame()

        with patch("src.webapp.services.watchlist_service.yf.download", side_effect=fake_download):
            payload = self.service.get_chart_overlays_payload("NVDA", as_of_date=dt.date(2026, 4, 24))

        snapshot = payload["danger_signals"]
        self.assertEqual(snapshot["as_of_date"], "2026-04-24")
        self.assertGreaterEqual(snapshot["active_count"], 6)
        labels = {item["label"] for item in snapshot["signals"]}
        self.assertIn("Price Closes Near Low on Heavy Volume", labels)
        self.assertIn("Price Closes Below Moving Average", labels)
        self.assertIn("Price Closes Below Swing Low", labels)
        self.assertIn("3 Consecutive Days of Lower Lows", labels)
        self.assertIn("Close Lower than 3 Previous Lows", labels)
        self.assertIn("RS Starts Curving Down", labels)

    def test_get_chart_overlays_payload_computes_heavy_overlays(self) -> None:
        service = WatchlistService(
            artifacts_dir=Path(self.temp_dir.name),
            database_url="postgres://example",
            market_data_source="database-first",
        )
        frame = self._long_price_frame()

        with patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows_for_range",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service._compute_market_extension_overlay",
            return_value={"config": {"timeframe": "weekly", "ma_type": "sma", "length": 10, "warning_pct": 11.0, "extreme_pct": 15.0, "label": "10W SMA"}, "line": [], "signals": [], "latest": None},
        ) as market_extension_patch, patch(
            "src.webapp.services.watchlist_service._compute_fearzone_panel",
            return_value={"rows": [], "signals": []},
        ) as fearzone_patch:
            payload = service.get_chart_overlays_payload("NVDA", period="6mo", as_of_date=dt.date(2025, 3, 24))

        market_extension_patch.assert_called_once()
        fearzone_patch.assert_called_once()
        self.assertIn("market_extension", payload)

    def test_get_chart_fundamentals_payload_includes_rating_bundle(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        frame = self._long_price_frame()
        with patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, None, None, {"earnings": {}, "holders": {}, "statistics": {}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_ticker_rating_bundle",
            return_value={
                "fundamentals_snapshot": {
                    "as_of_date": "2025-03-24",
                    "ticker": "NVDA",
                    "sector": "Technology",
                    "industry": "Semiconductors",
                    "parse_status": "ok",
                    "eps_qq_pct": 42.0,
                    "sales_qq_pct": 31.0,
                    "eps_this_y_pct": 33.0,
                    "eps_next_5y_pct": 24.0,
                    "roe_pct": 21.0,
                    "institutional_ownership_pct": 68.0,
                    "institutional_transactions_pct": 5.5,
                    "insider_ownership_pct": 2.2,
                    "insider_transactions_pct": 0.4,
                    "shares_float": 2.4e9,
                    "shares_outstanding": 2.5e9,
                },
                "rating_snapshot": {"overall_rating": 88.5, "rating_status": "ok"},
                "fundamental_rank": {"as_of_date": "2025-03-24", "current_rank": 42, "list_limit": 200},
                "rating_diagnostics": {"missing_metric_names": [], "insufficient_baseline_metrics": []},
            },
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"NVDA": {"technical_status": "ok", "leadership_score": 91.0}},
        ) as technical_mock, patch(
            "src.webapp.services.watchlist_service.load_finviz_insider_signal_map",
            return_value={
                "NVDA": {
                    "buy_count": 2,
                    "sell_count": 0,
                    "buy_amount": 1_250_000.0,
                    "sell_amount": 0.0,
                    "discretionary_sell_count": 0,
                    "discretionary_sell_amount": 0.0,
                    "net_amount_excl_10b5_1": 1_250_000.0,
                }
            },
        ), patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ):
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["fundamentals_snapshot"]["sector"], "Technology")
        self.assertEqual(payload["rating_snapshot"]["overall_rating"], 88.5)
        self.assertEqual(payload["fundamental_rank"]["current_rank"], 42)
        self.assertEqual(payload["rating_diagnostics"]["missing_metric_names"], [])
        self.assertEqual(payload["canslim_snapshot"]["ticker"], "NVDA")
        self.assertGreaterEqual(payload["canslim_snapshot"]["score"], 1)
        self.assertIn("S", payload["canslim_snapshot"]["letter_scores"])
        self.assertEqual(payload["canslim_snapshot"]["letter_scores"]["I"], 2)
        self.assertEqual(payload["canslim_snapshot"]["metrics"]["insider_buy_amount"], 1_250_000.0)
        technical_mock.assert_called_once_with(
            ["NVDA"],
            as_of_date=dt.date(2025, 3, 24),
            allow_older_as_of_date=True,
        )

    def test_get_chart_fundamentals_payload_prefers_stored_canslim_artifact(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        dated_dir = Path(self.temp_dir.name) / "screeners" / "2025-03-24" / "canslim"
        dated_dir.mkdir(parents=True, exist_ok=True)
        (dated_dir / "watchlist.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "NVDA",
                        "letter_scores": {"C": 2, "A": 2, "N": 2, "S": 1, "L": 2, "I": 2, "M": 2},
                    }
                ]
            ),
            encoding="utf-8",
        )
        (dated_dir / "raw_results.json").write_text(
            json.dumps(
                {
                    "hits": [
                        {
                            "ticker": "NVDA",
                            "as_of_date": "2025-03-24",
                            "score": 13,
                            "max_score": 14,
                            "rank": 1,
                            "letter_scores": {"C": 2, "A": 2, "N": 2, "S": 1, "L": 2, "I": 2, "M": 2},
                            "letter_passes": {"C": True, "A": True, "N": True, "S": True, "L": True, "I": True, "M": True},
                            "metrics": {"insider_buy_amount": 900000.0, "avg_volume_20d": 1500000.0},
                            "reasons": ["stored artifact reason"],
                            "leader_flags": ["leader"],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        frame = self._long_price_frame()
        with patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, None, None, {"earnings": {}, "holders": {}, "statistics": {}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_ticker_rating_bundle",
            return_value={
                "fundamentals_snapshot": {
                    "as_of_date": "2025-03-24",
                    "ticker": "NVDA",
                    "sector": "Technology",
                    "industry": "Semiconductors",
                    "parse_status": "ok",
                },
                "rating_snapshot": {"overall_rating": 88.5, "rating_status": "ok"},
                "fundamental_rank": {"as_of_date": "2025-03-24", "current_rank": 42, "list_limit": 200},
                "rating_diagnostics": {"missing_metric_names": [], "insufficient_baseline_metrics": []},
            },
        ), patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service.evaluate_canslim_ticker",
            side_effect=AssertionError("should not recompute canslim when stored artifact exists"),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            side_effect=AssertionError("should not load technical snapshot when stored artifact exists"),
        ):
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["canslim_snapshot"]["ticker"], "NVDA")
        self.assertEqual(payload["canslim_snapshot"]["letter_scores"]["S"], 1)
        self.assertEqual(payload["canslim_snapshot"]["metrics"]["insider_buy_amount"], 900000.0)
        self.assertEqual(payload["canslim_snapshot"]["reasons"], ["stored artifact reason"])

    def test_get_chart_fundamentals_payload_normalizes_stored_canslim_artifact(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        dated_dir = Path(self.temp_dir.name) / "screeners" / "2025-03-24" / "canslim"
        dated_dir.mkdir(parents=True, exist_ok=True)
        (dated_dir / "watchlist.json").write_text(
            json.dumps(
                [
                    {
                        "ticker": "NVDA",
                        "letter_scores": {"C": 2, "A": 2, "N": 2, "S": 1, "L": 2, "I": 2, "M": 0},
                    }
                ]
            ),
            encoding="utf-8",
        )
        (dated_dir / "raw_results.json").write_text(
            json.dumps(
                {
                    "hits": [
                        {
                            "ticker": "NVDA",
                            "as_of_date": "2025-03-24",
                            "score": 11,
                            "max_score": 14,
                            "rank": 2,
                            "letter_scores": {"C": 2, "A": 2, "N": 2, "S": 1, "L": 2, "I": 2, "M": 0},
                            "metrics": {"insider_buy_amount": 900000.0, "avg_volume_20d": 1500000.0},
                            "reasons": ["stored artifact reason"],
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        frame = self._long_price_frame()
        with patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, None, None, {"earnings": {}, "holders": {}, "statistics": {}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_ticker_rating_bundle",
            return_value={
                "fundamentals_snapshot": {
                    "as_of_date": "2025-03-24",
                    "ticker": "NVDA",
                    "sector": "Technology",
                    "industry": "Semiconductors",
                    "parse_status": "ok",
                },
                "rating_snapshot": {"overall_rating": 88.5, "rating_status": "ok"},
                "fundamental_rank": {"as_of_date": "2025-03-24", "current_rank": 42, "list_limit": 200},
                "rating_diagnostics": {"missing_metric_names": [], "insufficient_baseline_metrics": []},
            },
        ), patch(
            "src.webapp.services.watchlist_service.load_many_ticker_windows",
            return_value={"NVDA": frame.copy(), "SPY": frame.copy()},
        ), patch(
            "src.webapp.services.watchlist_service.evaluate_canslim_ticker",
            side_effect=AssertionError("should not recompute canslim when stored artifact exists"),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            side_effect=AssertionError("should not load technical snapshot when stored artifact exists"),
        ):
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["canslim_snapshot"]["ticker"], "NVDA")
        self.assertEqual(payload["canslim_snapshot"]["letter_scores"]["M"], 0)
        self.assertEqual(payload["canslim_snapshot"]["letter_passes"]["M"], False)
        self.assertEqual(payload["canslim_snapshot"]["leader_flags"], [])

    def test_get_top_ratings_payload_includes_latest_scanner_hit_count(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        dated_dir = Path(self.temp_dir.name) / "screeners" / "2026-06-22" / "canslim"
        dated_dir.mkdir(parents=True, exist_ok=True)
        (dated_dir / "watchlist.json").write_text(
            json.dumps(
                [
                    {"ticker": "PLTR", "score": 10, "max_score": 14, "rank": 2},
                    {"ticker": "NVDA", "score": 11, "max_score": 14, "rank": 1},
                ]
            ),
            encoding="utf-8",
        )
        with patch(
            "src.webapp.services.watchlist_service.RatingsRepository.list_top_rating_snapshots",
            return_value={
                "as_of_date": "2026-06-13",
                "previous_as_of_date": "2026-06-06",
                "rows": [
                    {"ticker": "PLTR", "as_of_date": "2026-06-13", "current_rank": 1, "previous_rank": 2, "rank_change": "up", "rank_delta": 1},
                    {"ticker": "NVDA", "as_of_date": "2026-06-13", "current_rank": 2, "previous_rank": 1, "rank_change": "down", "rank_delta": -1},
                ],
                "status_counts": {"ok": 2},
                "sector_options": ["Technology"],
            },
        ), patch.object(
            service,
            "_attach_top_rows_technical_indicator_ratings",
        ), patch.object(
            service,
            "_build_latest_scanner_hit_count_map",
            return_value={"PLTR": 3, "NVDA": 1},
        ):
            payload = service.get_top_ratings_payload()

        self.assertEqual(payload["rows"][0]["latest_scanner_hit_count"], 3)
        self.assertEqual(payload["rows"][1]["latest_scanner_hit_count"], 1)
        self.assertEqual(payload["rows"][0]["canslim_score"], 10)
        self.assertEqual(payload["rows"][1]["canslim_score"], 11)

    def test_get_chart_fundamentals_payload_uses_db_cache_when_complete(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        cached_entry = {
            "ticker": "NVDA",
            "as_of_date": "2026-06-15",
            "earnings_eps_history": [{"date": "2026-05-28", "eps_estimate": 0.9, "reported_eps": 1.1, "surprise_pct": 22.2}],
            "holders_float_held_by_institutions_pct": 79.25,
            "revenue_yoy_pct": 85.2,
            "earnings_yoy_pct": 210.6,
            "implied_move": {"strike": 100.0, "straddle_mid": 8.5, "dollar_move": 8.5, "percent_move": 7.8},
            "source_summary": {
                "diagnostics": {
                    "earnings": {"status": "ok", "attempts": [{"cache": True}]},
                    "holders": {"status": "ok", "attempts": [{"cache": True}]},
                    "statistics": {"status": "ok", "attempts": [{"cache": True}]},
                    "options": {"status": "ok", "attempts": [{"cache": True}]},
                }
            },
        }
        with patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_chart_fundamentals_cache_entry",
            return_value=cached_entry,
        ), patch.object(
            service,
            "_load_latest_stored_canslim_score_map",
            return_value={"NVDA": {"canslim_score": 12, "canslim_max_score": 14, "canslim_rank": 5}},
        ), patch.object(
            service,
            "_load_latest_stored_vcp_score_map",
            return_value={"NVDA": {"vcp_score": 86.5, "vcp_rating": "Strong VCP", "vcp_execution_state": "Pre-breakout", "vcp_pattern_type": "Textbook VCP", "vcp_signal_date": "2026-06-15"}},
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
        ) as scrape_patch, patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
        ) as implied_patch:
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["holders_float_held_by_institutions_pct"], 79.25)
        self.assertEqual(payload["implied_move"]["percent_move"], 7.8)
        self.assertEqual(payload["canslim_v2_score"], 12)
        self.assertEqual(payload["canslim_v2_max_score"], 14)
        self.assertEqual(payload["vcp_score"], 86.5)
        self.assertEqual(payload["vcp_rating"], "Strong VCP")
        self.assertEqual(payload["diagnostics"]["earnings"]["status"], "ok")
        scrape_patch.assert_not_called()
        implied_patch.assert_not_called()

    def test_get_chart_fundamentals_payload_persists_merged_cache_on_scrape(self) -> None:
        service = WatchlistService(artifacts_dir=Path(self.temp_dir.name), database_url="postgres://example")
        cached_entry = {
            "ticker": "NVDA",
            "as_of_date": "2026-06-08",
            "earnings_eps_history": [{"date": "2026-02-28", "eps_estimate": 0.7, "reported_eps": 0.8, "surprise_pct": 14.0}],
            "holders_float_held_by_institutions_pct": 71.5,
            "revenue_yoy_pct": None,
            "earnings_yoy_pct": 55.0,
            "implied_move": {"strike": 95.0, "straddle_mid": 6.0, "dollar_move": 6.0, "percent_move": 5.0},
            "source_summary": {},
        }
        with patch(
            "src.webapp.services.watchlist_service.RatingsRepository.load_latest_chart_fundamentals_cache_entry",
            return_value=cached_entry,
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_earnings_and_holders_playwright",
            return_value=([], None, 88.1, None, {"earnings": {"status": "error", "attempts": []}, "holders": {"status": "error", "attempts": []}, "statistics": {"status": "ok", "attempts": []}}),
        ), patch(
            "src.webapp.services.watchlist_service._load_yahoo_implied_move_playwright",
            return_value=(None, {"status": "error", "attempts": []}),
        ), patch(
            "src.webapp.services.watchlist_service.RatingsRepository.upsert_chart_fundamentals_cache_entry",
        ) as upsert_patch:
            payload = service.get_chart_fundamentals_payload("NVDA")

        self.assertEqual(payload["revenue_yoy_pct"], 88.1)
        self.assertEqual(payload["holders_float_held_by_institutions_pct"], 71.5)
        self.assertEqual(payload["earnings_eps_history"][0]["date"], "2026-02-28")
        self.assertEqual(payload["implied_move"]["percent_move"], 5.0)
        upsert_patch.assert_called_once()

    def test_get_chart_insider_payload_filters_recent_rows(self) -> None:
        insider_dir = Path(self.temp_dir.name) / "raw" / "insider"
        insider_dir.mkdir(parents=True, exist_ok=True)
        (insider_dir / "insider_trades_latest.json").write_text(
            """
            {
              "generated_at": "2026-06-12T00:00:00+00:00",
              "caches": {
                "NVDA|2026-05-31|14": {
                  "ticker": "NVDA",
                  "requested_tickers": ["NVDA"],
                  "as_of_date": "2026-05-31",
                  "lookback_days": 14,
                  "refreshed_at": "2026-06-12T00:00:00+00:00",
                  "entries": [
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-31",
                      "transaction_date": "2026-05-30",
                      "owner_name": "Jane Insider",
                      "position": "Officer, CEO",
                      "type": "BUY",
                      "shares": 1000,
                      "price": 10.25,
                      "gross_amount": 10250.0,
                      "net_amount": 10250.0,
                      "shares_owned_after": 15000,
                      "is_10b5_1": false,
                      "source_url": "https://www.sec.gov/Archives/example-buy.xml"
                    },
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-26",
                      "transaction_date": "2026-05-25",
                      "owner_name": "Jane Insider",
                      "position": "Officer, CEO",
                      "type": "SELL",
                      "shares": 500,
                      "price": 12.0,
                      "gross_amount": 6000.0,
                      "net_amount": -6000.0,
                      "shares_owned_after": 14500,
                      "is_10b5_1": true,
                      "source_url": "https://www.sec.gov/Archives/example-sell.xml"
                    },
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-01",
                      "transaction_date": "2026-05-01",
                      "owner_name": "Old Insider",
                      "position": "Director",
                      "type": "BUY",
                      "shares": 50,
                      "price": 1.0,
                      "gross_amount": 50.0,
                      "net_amount": 50.0,
                      "shares_owned_after": 15050,
                      "is_10b5_1": false,
                      "source_url": "https://www.sec.gov/Archives/example-old.xml"
                    }
                  ]
                }
              }
            }
            """,
            encoding="utf-8",
        )

        payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["resolved_as_of_date"], "2026-05-31")
        self.assertEqual(payload["window_start_date"], "2026-05-17")
        self.assertEqual(payload["summary"]["total_count"], 2)
        self.assertEqual(payload["summary"]["buy_count"], 1)
        self.assertEqual(payload["summary"]["sell_count"], 1)
        self.assertEqual(payload["summary"]["net_amount"], 4250.0)
        self.assertEqual(payload["cache_status"], "hit")
        self.assertEqual(payload["fetch_status"], "skipped")
        self.assertEqual(payload["entries"][0]["owner_name"], "Jane Insider")
        self.assertEqual(payload["entries"][1]["is_10b5_1"], True)

    def test_get_chart_insider_payload_fetches_on_cache_miss(self) -> None:
        fetched_payload = {
            "generated_at": "2026-06-02T12:00:00+00:00",
            "source": "sec_form4_submissions",
            "requested_tickers": ["NVDA"],
            "lookback_days": 14,
            "as_of_date": "2026-05-31",
            "entries": [
                {
                    "ticker": "NVDA",
                    "filing_date": "2026-05-31",
                    "transaction_date": "2026-05-30",
                    "owner_name": "Fresh Insider",
                    "position": "Director",
                    "type": "BUY",
                    "shares": 200,
                    "price": 20.0,
                    "gross_amount": 4000.0,
                    "net_amount": 4000.0,
                    "shares_owned_after": 1000,
                    "is_10b5_1": False,
                    "source_url": "https://www.sec.gov/Archives/fresh.xml",
                }
            ],
        }

        with patch("src.webapp.services.watchlist_service.fetch_insider_trades_window", return_value=fetched_payload):
            payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["cache_status"], "miss")
        self.assertEqual(payload["fetch_status"], "fetched")
        self.assertEqual(payload["entries"][0]["owner_name"], "Fresh Insider")
        saved = self.service.insider_repository.load_cache_window(ticker="NVDA", as_of_date="2026-05-31", lookback_days=14)
        self.assertIsNotNone(saved)
        self.assertEqual(saved["entries"][0]["owner_name"], "Fresh Insider")

    def test_get_chart_insider_payload_returns_stale_cache_when_refresh_fails(self) -> None:
        insider_dir = Path(self.temp_dir.name) / "raw" / "insider"
        insider_dir.mkdir(parents=True, exist_ok=True)
        (insider_dir / "insider_trades_latest.json").write_text(
            """
            {
              "generated_at": "2026-05-01T00:00:00+00:00",
              "caches": {
                "NVDA|2026-05-31|14": {
                  "ticker": "NVDA",
                  "requested_tickers": ["NVDA"],
                  "as_of_date": "2026-05-31",
                  "lookback_days": 14,
                  "refreshed_at": "2026-05-01T00:00:00+00:00",
                  "entries": [
                    {
                      "ticker": "NVDA",
                      "filing_date": "2026-05-31",
                      "transaction_date": "2026-05-30",
                      "owner_name": "Stale Insider",
                      "position": "Officer",
                      "type": "SELL",
                      "shares": 100,
                      "price": 15.0,
                      "gross_amount": 1500.0,
                      "net_amount": -1500.0,
                      "shares_owned_after": 900,
                      "is_10b5_1": true,
                      "source_url": "https://www.sec.gov/Archives/stale.xml"
                    }
                  ]
                }
              }
            }
            """,
            encoding="utf-8",
        )

        with patch("src.webapp.services.watchlist_service.fetch_insider_trades_window", side_effect=RuntimeError("sec down")):
            payload = self.service.get_chart_insider_payload("NVDA", as_of_date=dt.date(2026, 5, 31), lookback_days=14)

        self.assertEqual(payload["cache_status"], "stale")
        self.assertEqual(payload["fetch_status"], "failed")
        self.assertIn("sec down", payload["notice"])
        self.assertEqual(payload["entries"][0]["owner_name"], "Stale Insider")


if __name__ == "__main__":
    unittest.main()
