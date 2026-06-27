from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.webapp.services.earnings_calendar_service import EarningsCalendarService


IMPLIED_MOVE_CRITERIA_KEY = "implied_move_ge_7_near_earnings"


class _FakeCookstock:
    def __init__(self, events: list[dict[str, object]]) -> None:
        self._events = events

    def fetch_earnings_calendar_watchlist(self, start_date: dt.date, end_date: dt.date):
        _ = (start_date, end_date)
        return list(self._events)


class EarningsCalendarServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        self.service = EarningsCalendarService(
            project_root=Path("/tmp/project"),
            artifacts_dir=self.artifacts_dir,
        )
        self.service._get_universe_index = lambda: {}  # type: ignore[method-assign]
        self.service.ratings_repository.load_latest_rating_snapshots_for_tickers = lambda tickers: {}  # type: ignore[method-assign]
        self.service.ratings_repository.load_latest_technical_rating_snapshots_for_tickers = lambda tickers: {}  # type: ignore[method-assign]
        self.service._load_criteria_meta_for_week = lambda week_start, week_end: {  # type: ignore[method-assign]
            "available": True,
            "strategy_id": "earnings_weekly_criteria",
            "run_id": 7,
            "run_date": "2026-06-01",
            "matched_tickers": ["AAA"],
            "ticker_details": {
                "AAA": {
                    "passed": True,
                    "criteria": {
                        "institutional_ownership_ge_10": True,
                        "bullish_ma_stack": True,
                        "revenue_yoy_ge_100": True,
                        "latest_eps_negative": True,
                        "eps_improving_last_4": True,
                        IMPLIED_MOVE_CRITERIA_KEY: True,
                    },
                    "matched_criteria": [],
                    "not_matched_criteria": [],
                    "pass_mode": "strict",
                    "error": "",
                    "implied_move_signal": {
                        "threshold_pct": 7.0,
                        "near_earnings": True,
                        "matched": True,
                        "percent_move": 8.4,
                        "status": "ok",
                    },
                }
            },
        }

    def test_next_week_calendar_uses_persisted_iv_criterion_and_badge(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2), week_offset=1)

        entry = payload["days"][1]["before_market"][0]
        self.assertEqual(entry["ticker"], "AAA")
        self.assertEqual(entry["implied_move_signal"]["percent_move"], 8.4)
        self.assertTrue(entry["criteria"]["criteria"][IMPLIED_MOVE_CRITERIA_KEY])
        self.assertTrue(entry["criteria"]["passed"])
        self.assertEqual(payload["criteria_filter"]["matched_count"], 1)

    def test_next_week_calendar_includes_rating_snapshots_when_available(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service.ratings_repository.load_latest_rating_snapshots_for_tickers = lambda tickers: {  # type: ignore[method-assign]
            "AAA": {
                "as_of_date": "2026-06-06",
                "overall_rating": 88.5,
                "rating_status": "ok",
                "rating_status_reason": "",
            }
        }
        self.service.ratings_repository.load_latest_technical_rating_snapshots_for_tickers = lambda tickers: {  # type: ignore[method-assign]
            "AAA": {
                "as_of_date": "2026-06-05",
                "overall_rating": 91.2,
                "rating_band": "A",
                "technical_status": "ok",
                "technical_status_reason": "",
                "flags": ["rs_leader"],
            }
        }

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2), week_offset=1)

        entry = payload["days"][1]["before_market"][0]
        self.assertEqual(entry["fundamental_rating"]["overall_rating"], 88.5)
        self.assertEqual(entry["fundamental_rating"]["rating_status"], "ok")
        self.assertEqual(entry["technical_rating"]["overall_rating"], 91.2)
        self.assertEqual(entry["technical_rating"]["rating_band"], "A")
        self.assertEqual(entry["technical_rating"]["technical_status"], "ok")

    def test_week_offset_zero_defaults_to_current_week_and_after_hours_maps_to_after_market(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 1),
                "summary": "AAA Example Corp. (After-hours) earnings",
            }
        ]

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 3), week_offset=0)

        self.assertEqual(payload["week_start"], "2026-05-31")
        self.assertEqual(payload["week_end"], "2026-06-06")
        self.assertEqual(payload["week_offset"], 0)
        entry = payload["days"][1]["after_market"][0]
        self.assertEqual(entry["ticker"], "AAA")
        self.assertEqual(entry["session"], "after_market")

    def test_only_criteria_filters_out_entry_when_persisted_iv_rule_fails(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service._load_criteria_meta_for_week = lambda week_start, week_end: {  # type: ignore[method-assign]
            "available": True,
            "strategy_id": "earnings_weekly_criteria",
            "run_id": 7,
            "run_date": "2026-06-01",
            "matched_tickers": [],
            "ticker_details": {
                "AAA": {
                    "passed": False,
                    "criteria": {
                        "institutional_ownership_ge_10": True,
                        "bullish_ma_stack": True,
                        "revenue_yoy_ge_100": True,
                        "latest_eps_negative": True,
                        "eps_improving_last_4": True,
                        IMPLIED_MOVE_CRITERIA_KEY: False,
                    },
                    "matched_criteria": [],
                    "not_matched_criteria": [IMPLIED_MOVE_CRITERIA_KEY],
                    "pass_mode": "strict",
                    "error": "criteria_not_met",
                    "implied_move_signal": {
                        "threshold_pct": 7.0,
                        "near_earnings": True,
                        "matched": False,
                        "percent_move": 5.2,
                        "status": "ok",
                    },
                }
            },
        }

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2), week_offset=1, only_criteria=True)

        self.assertEqual(payload["days"][1]["before_market"], [])
        self.assertEqual(payload["criteria_filter"]["matched_count"], 0)

    def test_week_specific_criteria_run_selected_instead_of_latest_only(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service._load_criteria_meta_for_week = EarningsCalendarService._load_criteria_meta_for_week.__get__(self.service, EarningsCalendarService)  # type: ignore[method-assign]
        self.service.history_service.is_configured = lambda: True  # type: ignore[method-assign]
        self.service.history_service.list_runs = lambda **kwargs: [  # type: ignore[method-assign]
            {"id": 11, "run_date": dt.date(2026, 6, 9)},
            {"id": 10, "run_date": dt.date(2026, 6, 2)},
        ]

        def fake_get_run(run_id: int, *, include_hits: bool = False, hit_limit: int = 200, hit_offset: int = 0):
            _ = (include_hits, hit_limit, hit_offset)
            if run_id == 11:
                return {
                    "id": 11,
                    "hits": [
                        {
                            "ticker": "BBB",
                            "passed": True,
                            "hit_payload_json": {
                                "ticker": "BBB",
                                "criteria": {IMPLIED_MOVE_CRITERIA_KEY: True},
                                "pass_mode": "loose",
                            },
                        },
                        {
                            "ticker": "CCC",
                            "passed": False,
                            "hit_payload_json": {
                                "ticker": "CCC",
                                "criteria": {IMPLIED_MOVE_CRITERIA_KEY: True},
                                "pass_mode": "loose",
                            },
                        }
                    ],
                }
            return {
                "id": 10,
                "hits": [
                    {
                        "ticker": "AAA",
                        "passed": True,
                        "hit_payload_json": {
                            "ticker": "AAA",
                            "earnings_date": "2026-06-08",
                            "criteria": {IMPLIED_MOVE_CRITERIA_KEY: True},
                            "pass_mode": "loose",
                        },
                    }
                ],
            }

        self.service.history_service.get_run = fake_get_run  # type: ignore[method-assign]

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 2), week_offset=1)

        entry = payload["days"][1]["before_market"][0]
        self.assertEqual(entry["ticker"], "AAA")
        self.assertTrue(entry["criteria"]["passed"])
        self.assertEqual(payload["criteria_filter"]["run_id"], 10)
        self.assertEqual(payload["criteria_filter"]["run_date"], "2026-06-02")

    def test_calendar_entry_includes_latest_analyzer_and_pead_payloads(self) -> None:
        fake_events = [
            {
                "ticker": "AAA",
                "event_date": dt.date(2026, 6, 8),
                "summary": "Before market open",
            }
        ]
        self.service._load_criteria_meta_for_week = lambda week_start, week_end: {  # type: ignore[method-assign]
            "available": False,
            "strategy_id": "earnings_weekly_criteria",
            "run_id": None,
            "run_date": None,
            "matched_tickers": [],
            "ticker_details": {},
        }
        self.service.history_service.is_configured = lambda: True  # type: ignore[method-assign]

        def fake_list_runs(*, strategy_id: str = "", **kwargs):
            _ = kwargs
            if strategy_id == "earnings_trade_analyzer":
                return [{"id": 21, "run_date": dt.date(2026, 6, 10)}]
            if strategy_id == "pead_screener":
                return [{"id": 31, "run_date": dt.date(2026, 6, 11)}]
            return []

        def fake_get_run(run_id: int, *, include_hits: bool = False, hit_limit: int = 200, hit_offset: int = 0):
            _ = (include_hits, hit_limit, hit_offset)
            if run_id == 21:
                return {
                    "id": 21,
                    "hits": [
                        {
                            "ticker": "AAA",
                            "passed": True,
                            "hit_payload_json": {
                                "ticker": "AAA",
                                "earnings_date": "2026-06-08",
                                "earnings_timing": "bmo",
                                "eligible_on": "2026-06-09",
                                "grade": "A",
                                "grade_description": "Strong earnings reaction",
                                "composite_score": 89.5,
                                "gap_pct": 7.8,
                                "current_price": 145.2,
                                "guidance": "Monitor for follow-through buying.",
                                "strongest_component": "Pre-Earnings Trend",
                                "weakest_component": "MA50 Position",
                            },
                        }
                    ],
                }
            if run_id == 31:
                return {
                    "id": 31,
                    "hits": [
                        {
                            "ticker": "AAA",
                            "passed": True,
                            "hit_payload_json": {
                                "ticker": "AAA",
                                "earnings_date": "2026-06-08",
                                "eligible_on": "2026-06-09",
                                "stage": "SIGNAL_READY",
                                "composite_score": 74.2,
                                "rating": "Good Setup",
                                "gap_pct": 7.8,
                                "current_price": 147.1,
                                "weeks_since_earnings": 1,
                                "breakout_pct": 0.0,
                                "risk_reward_ratio": 2.4,
                                "guidance": "Red candle formed, set alert for breakout.",
                            },
                        }
                    ],
                }
            return None

        self.service.history_service.list_runs = fake_list_runs  # type: ignore[method-assign]
        self.service.history_service.get_run = fake_get_run  # type: ignore[method-assign]

        with patch("src.webapp.services.earnings_calendar_service.load_configured_cookstock", return_value=_FakeCookstock(fake_events)):
            payload = self.service.get_next_week_calendar(reference_date=dt.date(2026, 6, 10), week_offset=0)

        entry = payload["days"][1]["before_market"][0]
        self.assertEqual(entry["earnings_trade_analysis"]["grade"], "A")
        self.assertEqual(entry["earnings_trade_analysis"]["composite_score"], 89.5)
        self.assertEqual(entry["pead_analysis"]["stage"], "SIGNAL_READY")
        self.assertEqual(entry["pead_analysis"]["risk_reward_ratio"], 2.4)
        self.assertEqual(entry["post_earnings_tracking"]["eligible_on"], "2026-06-09")
        self.assertTrue(entry["post_earnings_tracking"]["analyzer_ready"])
        self.assertTrue(entry["post_earnings_tracking"]["pead_ready"])


if __name__ == "__main__":
    unittest.main()
