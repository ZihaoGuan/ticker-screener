from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.config import AppConfig
from src.artifact_paths import build_screener_artifact_paths, strategy_id_from_legacy_stem
from src.qullamaggie_screen import evaluate_qullamaggie, evaluate_qullamaggie_ep, evaluate_qullamaggie_parabolic_short, find_qullamaggie_hit, run_qullamaggie_screen
from src.qullamaggie_watchlist_builder import build_qullamaggie_watchlist
from src.screener_catalog import build_screener_catalog
from src.universe import UniverseTicker
from src.webapp.services.run_service import RunService
from src.webapp.services.scheduled_job_service import ScheduledJobService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService


def _qullamaggie_frame(*, avg_volume: float = 1_500_000.0) -> pd.DataFrame:
    dates = pd.bdate_range("2025-06-02", periods=280)
    early = np.linspace(42.0, 54.0, 230)
    impulse = np.linspace(54.0, 100.0, 30)
    base = np.array([100.0, 104.0, 101.0, 106.0, 102.0, 108.0, 103.0, 107.0, 104.0, 109.0,
                     105.0, 108.0, 106.0, 109.0, 107.0, 109.5, 108.0, 110.0, 109.0, 109.4])
    close = np.concatenate([early, impulse, base])
    volume = np.full(len(close), avg_volume)
    volume[-10:] = avg_volume * 0.65
    return pd.DataFrame(
        {
            "Open": close * 0.995,
            "High": close * 1.025,
            "Low": close * 0.975,
            "Close": close,
            "Volume": volume,
        },
        index=dates,
    )


class QullamaggieScreenTest(unittest.TestCase):
    def test_candidate_passes_momentum_trend_liquidity_adr_and_tight_setup(self) -> None:
        snapshot = evaluate_qullamaggie(
            _qullamaggie_frame(),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot.matched)
        self.assertGreaterEqual(snapshot.recent_return_pct, 30.0)
        self.assertGreaterEqual(snapshot.avg_volume_20, 1_000_000.0)
        self.assertGreaterEqual(snapshot.adr_pct_20, 4.0)
        self.assertAlmostEqual(snapshot.adr_pct_20, (((_qullamaggie_frame()["High"] / _qullamaggie_frame()["Low"] - 1) * 100).tail(20).mean()))
        self.assertIn(snapshot.setup_state, {"forming", "ready", "triggered"})
        self.assertNotEqual(snapshot.setup_state, "triggered")
        self.assertGreaterEqual(snapshot.consolidation_sessions, 10)
        self.assertLessEqual(snapshot.consolidation_sessions, 40)
        self.assertGreaterEqual(snapshot.impulse_return_pct, 30.0)
        self.assertTrue(snapshot.higher_low)
        self.assertTrue(snapshot.range_contracting or snapshot.volume_contracting)
        self.assertTrue(snapshot.tight_flag or snapshot.pullback_near_fast_ma)

        hit = find_qullamaggie_hit(
            _qullamaggie_frame(),
            ticker=UniverseTicker(symbol="TEST", sector="Technology"),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
            signal_date=dt.date(2026, 6, 26),
        )
        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.ticker, "TEST")
        self.assertEqual(hit.criteria_passed, hit.criteria_total)
        row = build_qullamaggie_watchlist([hit])[0]
        self.assertEqual(row["trigger_price"], round(hit.pivot, 4))
        self.assertIn("Prior Impulse >= 30%", row["signal_badges"])
        self.assertIn("10-40 Session Base", row["signal_badges"])
        self.assertIn("Higher Low", row["signal_badges"])
        self.assertNotIn("3M/6M Return >= 30%", row["signal_badges"])
        if hit.setup_state != "triggered":
            self.assertIsNone(row["stop_price"])
            self.assertNotEqual(row["entry_price"], round(hit.current_price, 4))

    def test_ep_and_parabolic_daily_states_never_claim_intraday_confirmation(self) -> None:
        dates = pd.bdate_range("2025-01-01", periods=70)
        close = np.full(70, 20.0)
        open_price = close.copy()
        open_price[-1] = 22.2
        volume = np.full(70, 1_000_000.0)
        volume[-1] = 3_000_000.0
        ep = pd.DataFrame({"Open": open_price, "High": np.maximum(open_price, close) * 1.02, "Low": close * .98, "Close": close, "Volume": volume}, index=dates)
        candidate = evaluate_qullamaggie_ep(ep)
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["state"], "gap_candidate")
        self.assertNotIn("opening_confirmed", candidate.values())
        self.assertIsNone(evaluate_qullamaggie_ep(ep.assign(Open=close)))

        ramp = np.linspace(10.0, 30.0, 30)
        parab_volume = np.full(30, 2_000_000.0)
        parab_volume[-1] = 4_000_000.0
        parab = pd.DataFrame({"High": ramp * 1.03, "Low": ramp * .97, "Close": ramp, "Volume": parab_volume}, index=pd.bdate_range("2025-01-01", periods=30))
        short_candidate = evaluate_qullamaggie_parabolic_short(parab)
        self.assertIsNotNone(short_candidate)
        self.assertIn(short_candidate["state"], {"extended", "exhaustion_watch"})
        self.assertNotIn("triggered", short_candidate.values())
        self.assertEqual(short_candidate["consecutive_up_days_5"], 5)
        interrupted = parab.copy()
        interrupted.loc[interrupted.index[-3], "Close"] = float(interrupted["Close"].iloc[-2]) + 1.0
        self.assertIsNone(evaluate_qullamaggie_parabolic_short(interrupted))

    def test_smooth_advance_without_a_real_base_is_rejected(self) -> None:
        dates = pd.bdate_range("2025-01-01", periods=280)
        close = np.linspace(30.0, 110.0, len(dates))
        smooth = pd.DataFrame({"Open": close * .995, "High": close * 1.025, "Low": close * .975, "Close": close, "Volume": np.full(len(close), 1_500_000.0)}, index=dates)
        snapshot = evaluate_qullamaggie(smooth, market_cap=500_000_000.0, daily_rs_rating=92.0)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.criteria["post_impulse_base_10_40_sessions"])
        self.assertEqual(snapshot.consolidation_sessions, 0)

    def test_breakout_state_needs_latest_daily_close_to_cross_prior_base_pivot(self) -> None:
        bars = _qullamaggie_frame()
        bars.loc[bars.index[-1], ["Open", "High", "Low", "Close"]] = [111.0, 116.0, 110.0, 113.0]
        snapshot = evaluate_qullamaggie(bars, market_cap=500_000_000.0, daily_rs_rating=92.0)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.setup_state, "triggered")
        self.assertEqual(snapshot.breakout_day_low, 110.0)

    def test_candidate_fails_when_share_liquidity_is_below_one_million(self) -> None:
        snapshot = evaluate_qullamaggie(
            _qullamaggie_frame(avg_volume=900_000.0),
            market_cap=500_000_000.0,
            daily_rs_rating=92.0,
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["avg_volume_20_gt_1m"])

    def test_run_uses_database_prices_fundamentals_and_technical_rating(self) -> None:
        frame = _qullamaggie_frame()
        ticker = UniverseTicker(symbol="TEST", sector="Technology")
        with patch("src.qullamaggie_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.qullamaggie_screen.load_many_ticker_windows", return_value={"TEST": frame}
        ), patch(
            "src.qullamaggie_screen.load_ticker_metadata_map", return_value={"TEST": {"exchange": "NASDAQ"}}
        ), patch(
            "src.qullamaggie_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"TEST": {"market_cap": 500_000_000.0}},
        ), patch(
            "src.qullamaggie_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"TEST": {"daily_rs_rating": 92.0}},
        ), patch("src.qullamaggie_screen.load_configured_cookstock") as load_cookstock:
            result = run_qullamaggie_screen(AppConfig(), [ticker], as_of_date=frame.index[-1].date())

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "TEST")
        load_cookstock.assert_not_called()

    def test_is_available_for_ad_hoc_runs_and_admin_scheduling(self) -> None:
        catalog = build_screener_catalog(AppConfig())
        self.assertIn("qullamaggie", catalog)
        self.assertIn("qullamaggie_ep", catalog)
        self.assertIn("qullamaggie_parabolic_short", catalog)
        with tempfile.TemporaryDirectory() as directory:
            run_service = RunService(project_root=Path(directory))
            action_ids = {item["id"] for item in run_service.list_actions()}
            schedule_action_ids = {
                item["id"]
                for item in ScheduledJobService(
                    project_root=Path(directory),
                    run_service=run_service,
                ).get_context()["available_actions"]
            }

        self.assertIn("qullamaggie", action_ids)
        self.assertIn("qullamaggie", schedule_action_ids)
        self.assertIn("qullamaggie_ep", action_ids)
        self.assertIn("qullamaggie_parabolic_short", action_ids)
        self.assertIn("qullamaggie_ep", schedule_action_ids)
        self.assertIn("qullamaggie_parabolic_short", schedule_action_ids)
        self.assertEqual(build_screener_artifact_paths(Path("/tmp/artifacts"), strategy_id="qullamaggie", date_label="2026-10-19").logical_stem, "qullamaggie_2026-10-19")
        self.assertEqual(build_screener_artifact_paths(Path("/tmp/artifacts"), strategy_id="qullamaggie_ep", date_label="2026-10-19").logical_stem, "qullamaggie_ep_2026-10-19")
        self.assertEqual(strategy_id_from_legacy_stem("qullamaggie_2026-10-19"), "qullamaggie")
        self.assertEqual(strategy_id_from_legacy_stem("qullamaggie_ep_2026-10-19"), "qullamaggie_ep")
        self.assertEqual(strategy_id_from_legacy_stem("qullamaggie_parabolic_short_2026-10-19"), "qullamaggie_parabolic_short")

    def test_ad_hoc_service_returns_qullamaggie_hit(self) -> None:
        frame = _qullamaggie_frame()
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"TEST": frame, "SPY": frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"TEST": {"ticker": "TEST", "sector": "Technology", "exchange": "NASDAQ"}},
        ), patch(
            "src.screener_catalog.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers",
            return_value={"TEST": {"market_cap": 500_000_000.0}},
        ), patch(
            "src.screener_catalog.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value={"TEST": {"daily_rs_rating": 92.0}},
        ):
            payload = service.run(
                ticker="TEST",
                as_of_date=frame.index[-1].date(),
                screener_ids=["qullamaggie"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "qullamaggie")
        self.assertTrue(payload["screeners"][0]["passed"])


if __name__ == "__main__":
    unittest.main()
