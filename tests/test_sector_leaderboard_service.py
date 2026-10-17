import datetime as dt
import json
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.webapp.services import sector_leaderboard_service as module
from src.webapp.services.sector_leaderboard_service import SectorEtf, SectorHolding, SectorLeaderboardService


def _frame(closes: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "Open": [value - 0.5 for value in closes],
            "High": [value + 1.0 for value in closes],
            "Low": [value - 1.0 for value in closes],
            "Close": closes,
            "Adj Close": closes,
            "Volume": [1_000_000 + index for index, _ in enumerate(closes)],
        },
        index=dates,
    )


class SectorLeaderboardServiceTest(unittest.TestCase):
    def setUp(self):
        module._clear_sector_leaderboard_cache()
        self.addCleanup(module._clear_sector_leaderboard_cache)

    def test_default_sector_catalog_excludes_xweb(self):
        self.assertNotIn("XWEB", {item.ticker for item in module.DEFAULT_SECTOR_ETFS})

    def test_stage_presentation_keeps_classifier_alias_and_maps_substages(self):
        hit = SimpleNamespace(stage_alias="Stage 2A", current_stage="Stage 2 - Advance", maturity="Early", signal_date="2026-07-30")
        self.assertEqual(module._stage_presentation(hit), {
            "code": "2A", "label": "Stage 2A · Early advance", "current_stage": "Stage 2 - Advance",
            "maturity": "Early", "signal_date": "2026-07-30",
        })
        self.assertIsNone(module._stage_presentation(None))

    def test_recent_darvas_breakout_window_and_missing_bars(self):
        frame = _frame([float(100 + index) for index in range(270)])
        wanted_date = frame.index[-6].date().isoformat()
        as_of_date = frame.index[-1].date()

        def one_hit(candidate, *, ticker):
            if candidate.index[-1].date().isoformat() == wanted_date:
                return SimpleNamespace(signal_date=wanted_date)
            return None

        with patch.object(module, "find_darvas_box_breakout_hit", one_hit):
            recent = module._find_recent_sector_breakout(frame, "XLC", as_of_date)
            missing = module._find_recent_sector_breakout(pd.DataFrame(), "XLC", as_of_date)
        expired_date = frame.index[-11].date().isoformat()

        def expired_hit(candidate, *, ticker):
            if candidate.index[-1].date().isoformat() == expired_date:
                return SimpleNamespace(signal_date=expired_date)
            return None

        with patch.object(module, "find_darvas_box_breakout_hit", expired_hit):
            expired = module._find_recent_sector_breakout(frame, "XLC", as_of_date)

        self.assertEqual(recent, {"signal_date": wanted_date, "sessions_ago": 5})
        self.assertIsNone(expired)
        self.assertIsNone(missing)

    def test_signal_fields_are_additive_and_null_when_unavailable(self):
        etfs = (
            SectorEtf("XLC", "Comm. Services", "Test", "https://example.test/xlc", ()),
            SectorEtf("XLY", "Consumer Disc.", "Test", "https://example.test/xly", ()),
            SectorEtf("XAR", "Aerospace", "Test", "https://example.test/xar", ()),
        )
        frames = {ticker: _frame([float(100 + index) for index in range(270)]) for ticker in ("XLC", "XLY", "XAR", "SPY")}
        rts_snapshot = SimpleNamespace(rts_score=72.5, rts_state="expanding", as_of_date="2026-07-30")
        stage_hit = SimpleNamespace(stage_alias="Stage 2B", current_stage="Stage 2 - Advance", maturity="Mature", signal_date="2026-07-24")

        with patch.object(module, "load_many_ticker_windows", lambda tickers, *args, **kwargs: {ticker: frames[ticker] for ticker in tickers if ticker in frames}), \
             patch.object(SectorLeaderboardService, "_load_technical_rating_map", return_value={}), \
             patch.object(module, "classify_weinstein_stage", side_effect=[stage_hit, None]), \
             patch.object(module, "build_relative_trend_strength_snapshot", side_effect=[rts_snapshot, None]), \
             patch.object(module, "_find_recent_sector_breakout", side_effect=[{"signal_date": "2026-07-29", "sessions_ago": 1}, None]):
            payload = SectorLeaderboardService(etfs=etfs).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(payload["core_sector_tickers"], list(module._CORE_SECTOR_TICKERS))
        by_ticker = {row["ticker"]: row for row in payload["rows"]}
        self.assertEqual(by_ticker["XLC"]["stage"]["code"], "2B")
        self.assertEqual(by_ticker["XLC"]["rts_score"], 72.5)
        self.assertEqual(by_ticker["XLC"]["rts_formula"], "rts-v1")
        self.assertEqual(by_ticker["XLC"]["darvas_breakout"]["sessions_ago"], 1)
        self.assertIsNone(by_ticker["XLY"]["stage"])
        self.assertIsNone(by_ticker["XLY"]["rts_score"])
        self.assertIsNone(by_ticker["XLY"]["darvas_breakout"])
        self.assertNotIn("stage", by_ticker["XAR"])

    def test_payload_is_cached_for_the_same_sector_catalog(self):
        service = SectorLeaderboardService(database_url="postgres://example", etfs=())
        with patch.object(module, "load_many_ticker_windows", return_value={}) as load_windows:
            first = service.get_payload(as_of_date=dt.date(2026, 7, 30))
            second = service.get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(load_windows.call_count, 2)
        self.assertEqual(first, second)
        self.assertIsNot(first, second)

    def test_ranks_rows_and_attaches_holding_direction(self):
        etfs = (
            SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("ONE", 10.0),)),
            SectorEtf("BBB", "Beta", "Test", "https://example.test/bbb", (SectorHolding("TWO", 5.0),)),
        )
        frames = {
            "AAA": _frame([100 + index for index in range(270)]),
            "BBB": _frame([100 + index * 0.5 for index in range(269)] + [80]),
            "SPY": _frame([100 for _ in range(270)]),
            "ONE": _frame([10, 11, 12]),
            "TWO": _frame([10, 9, 8]),
        }

        def fake_load_many_ticker_windows(tickers, as_of_date, trading_days_needed, *, database_url=None):
            return {ticker: frames[ticker] for ticker in tickers if ticker in frames}

        with patch.object(module, "load_many_ticker_windows", fake_load_many_ticker_windows), patch.object(
            SectorLeaderboardService,
            "_load_technical_rating_map",
            return_value={
                "ONE": {"daily_rs_rating": 91.0, "weekly_rs_rating": 87.0, "rs_rating_3m": 96.0, "rs_rating_6m": 93.0, "leadership_score": 94.0},
                "TWO": {"daily_rs_rating": 42.0, "weekly_rs_rating": 38.0, "rs_rating_3m": 45.0, "rs_rating_6m": 40.0, "leadership_score": 41.0},
            },
        ):
            payload = SectorLeaderboardService(database_url="postgres://example", etfs=etfs).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(payload["rows"][0]["ticker"], "AAA")
        self.assertEqual(payload["rows"][0]["price"], 369.0)
        self.assertEqual(payload["rows"][0]["day_change_pct"], 0.27)
        self.assertEqual(payload["rows"][0]["rs_vs_spy_1m_pct"], 6.03)
        self.assertEqual(payload["rows"][0]["rs_vs_spy_3m_pct"], 20.59)
        self.assertEqual(payload["rows"][0]["rs_momentum_score"], 100.0)
        self.assertEqual(payload["rows"][0]["rs_days_21d"], 21)
        self.assertEqual(payload["rows"][0]["rs_days_21d_pct"], 100.0)
        self.assertEqual(payload["rows"][0]["red_rs_days_21d"], 0)
        self.assertEqual(payload["rows"][0]["red_rs_days_21d_pct"], 0.0)
        self.assertEqual(payload["rows"][0]["rs_new_high_63d"], True)
        self.assertEqual(payload["rows"][0]["avg_volume_20d"], 1_000_260)
        self.assertEqual(payload["rows"][0]["relative_volume_20d"], 1.0)
        self.assertEqual(payload["rows"][0]["avg_dcr_21d"], 50.0)
        self.assertEqual(payload["rows"][0]["strong_close_days_21d"], 0)
        self.assertEqual(payload["rows"][0]["hv63"], True)
        self.assertEqual(payload["rows"][0]["volume_confirmation"], False)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["day_change_pct"], 9.09)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["avg_volume_20d"], 1_000_001)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["relative_volume_20d"], 1.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["rs_days_21d"], 2)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["rs_days_21d_pct"], 100.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["red_rs_days_21d"], 0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["red_rs_days_21d_pct"], 0.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["rs_new_high_63d"], True)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["avg_dcr_21d"], 50.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["strong_close_days_21d"], 0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["hv63"], True)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["volume_confirmation"], False)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["daily_rs_rating"], 91.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["weekly_rs_rating"], 87.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["rs_rating_3m"], 96.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["rs_rating_6m"], 93.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["leadership_score"], 94.0)
        self.assertEqual(payload["rows"][1]["ticker"], "BBB")
        self.assertEqual(payload["rows"][1]["top_holdings"][0]["day_change_pct"], -11.11)

    def test_keeps_empty_rows_when_database_has_no_bars(self):
        etfs = (SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("ONE", 10.0),)),)

        with patch.object(module, "load_many_ticker_windows", lambda *args, **kwargs: {}), patch.object(
            SectorLeaderboardService,
            "_load_technical_rating_map",
            return_value={},
        ):
            payload = SectorLeaderboardService(etfs=etfs).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(
            payload["rows"],
            [
                {
                    "ticker": "AAA",
                    "description": "Alpha",
                    "provider": "Test",
                    "source_url": "https://example.test/aaa",
                    "price": None,
                    "day_change_pct": None,
                    "week_change_pct": None,
                    "month_change_pct": None,
                    "year_change_pct": None,
                    "rs_vs_spy_1m_pct": None,
                    "rs_vs_spy_3m_pct": None,
                    "rs_momentum_score": None,
                    "rs_days_21d": None,
                    "rs_days_21d_pct": None,
                    "red_rs_days_21d": None,
                    "red_rs_days_21d_pct": None,
                    "rs_new_high_63d": None,
                    "atr_pct": None,
                    "volume": None,
                    "avg_volume_20d": None,
                    "relative_volume_20d": None,
                    "avg_dcr_21d": None,
                    "strong_close_days_21d": None,
                    "hv63": None,
                    "volume_confirmation": None,
                    "latest_date": None,
                    "top_holdings": [
                        {
                            "ticker": "ONE",
                            "name": "",
                            "weight": 10.0,
                            "shares_held": None,
                            "day_change_pct": None,
                            "avg_volume_20d": None,
                            "relative_volume_20d": None,
                            "rs_days_21d": None,
                            "rs_days_21d_pct": None,
                            "red_rs_days_21d": None,
                            "red_rs_days_21d_pct": None,
                            "rs_new_high_63d": None,
                            "avg_dcr_21d": None,
                            "strong_close_days_21d": None,
                            "hv63": None,
                            "volume_confirmation": None,
                            "daily_rs_rating": None,
                            "weekly_rs_rating": None,
                            "rs_rating_3m": None,
                            "rs_rating_6m": None,
                            "leadership_score": None,
                        }
                    ],
                }
            ],
        )
        self.assertEqual(payload["source"]["benchmark_ticker"], "SPY")

    def test_uses_cached_ssga_holdings_when_available(self):
        etfs = (SectorEtf("AAA", "Alpha", "Test", "https://example.test/aaa", (SectorHolding("OLD", 10.0),)),)

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "sector_etf_holdings"
            cache_dir.mkdir()
            (cache_dir / "latest.json").write_text(
                json.dumps(
                    {
                        "generated_at": "2026-07-30T12:00:00+00:00",
                        "results": {
                            "AAA": {
                                "holdings": [
                                    {
                                        "ticker": "NEW",
                                        "name": "New Holding",
                                        "weight": 17.5,
                                        "shares_held": 123.0,
                                    }
                                ]
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            with patch.object(module, "load_many_ticker_windows", lambda *args, **kwargs: {}), patch.object(
                SectorLeaderboardService,
                "_load_technical_rating_map",
                return_value={"NEW": {"daily_rs_rating": 88.0}},
            ):
                payload = SectorLeaderboardService(etfs=etfs, artifacts_dir=Path(tmpdir)).get_payload(as_of_date=dt.date(2026, 7, 30))

        self.assertEqual(payload["source"]["holdings_cache_generated_at"], "2026-07-30T12:00:00+00:00")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["ticker"], "NEW")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["name"], "New Holding")
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["shares_held"], 123.0)
        self.assertEqual(payload["rows"][0]["top_holdings"][0]["daily_rs_rating"], 88.0)


if __name__ == "__main__":
    unittest.main()
