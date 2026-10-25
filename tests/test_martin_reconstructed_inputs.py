from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
import sys
import unittest
from unittest import mock

import pandas as pd

from src.webapp.services.martin_reconstructed_inputs import (
    PLAN_SCANNER_IDS,
    evaluate_focus_price_sources,
    load_dated_scanner_evidence,
    preflight_session,
    source_manifest,
    _append_guru_board_scanner,
    _candidate_source_hit_evidence,
    _partial_source_candidate_state,
    _partition_missing_sources,
    _source_contributes_to_guru_board,
)
import src.webapp.services.martin_reconstructed_inputs as reconstructed_inputs
from src.webapp.services.martin_forward_test_service import MartinForwardTestService
from src.webapp.services.martin_forward_test_service import classify_candidate, select_sector_candidates
from src.webapp.services.strike_zone import build_strike_zone
from src.webapp.services.watchlist_service import _guru_row_sort_key
from src.config import load_app_config


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _sql, params):
        self.params = params

    def fetchall(self):
        return self.rows


class _QueuedCursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.params = []

    def execute(self, _sql, params):
        self.params.append(params)

    def fetchall(self):
        return self.responses.pop(0)


class _ReconstructionCursor:
    def __init__(self, universe, bars):
        self.universe = universe
        self.bars = bars
        self.queries = []
        self.current = ""

    def execute(self, sql, params):
        self.current = sql
        self.queries.append((sql, params))

    def fetchall(self):
        if "SELECT DISTINCT ticker FROM daily_bars" in self.current:
            return [{"ticker": item} for item in self.universe]
        if "SELECT ticker,trade_date,open,high,low,close" in self.current:
            return self.bars
        raise AssertionError(f"Unexpected query: {self.current}")

    def fetchone(self):
        return {"count": 0}


class _CapturingForwardRepository:
    def save_session(self, **kwargs):
        self.saved = kwargs
        return 12


class MartinReconstructedInputsTests(unittest.TestCase):
    def test_reconstruction_uses_target_day_universe_and_localizes_recent_ipo_warmup(self):
        day = dt.date(2026, 10, 23)
        class SessionLabel:
            def __init__(self, value): self.value = value
            def date(self): return self.value
        class FakeCalendar:
            def sessions_in_range(self, start, end):
                first, last = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
                return [SessionLabel(first + dt.timedelta(days=offset))
                    for offset in range((last - first).days + 1)
                    if (first + dt.timedelta(days=offset)).weekday() < 5]
        fake_exchange_calendars = SimpleNamespace(get_calendar=lambda *_args, **_kwargs: FakeCalendar())
        previous_session = dt.date(2026, 10, 22)
        long_history_id = sorted(reconstructed_inputs.FOCUS_SCANNERS)[0]
        scanner_ids = (set(reconstructed_inputs.FOCUS_SCANNERS) |
                       set(reconstructed_inputs.PLAN_SCANNER_IDS) |
                       set(reconstructed_inputs.DERIVED_SCANNER_IDS))
        catalog = {scanner_id: SimpleNamespace(id=scanner_id,
            total_trading_days=2 if scanner_id == long_history_id else 1,
            required_inputs={"daily_bars"}, evaluator=None) for scanner_id in scanner_ids}

        def bar(ticker, session):
            return {"ticker": ticker, "trade_date": session, "open": 10, "high": 11,
                "low": 9, "close": 10, "adj_close": 10, "volume": 1000,
                "source": "test", "split_factor": 1,
                "updated_at": dt.datetime(2026, 10, 24, tzinfo=dt.timezone.utc)}

        cursor = _ReconstructionCursor(["ABC", "IPO"], [bar("ABC", previous_session),
            bar("ABC", day), bar("IPO", day)])
        def evaluate(*, scanner_ids, **_kwargs):
            return {scanner_id: {"status": "executed_empty", "hit": {}, "metrics": {}}
                    for scanner_id in scanner_ids}

        fake_manifest = {"version": "test", "digest": "manifest",
            "code_fingerprint": "code", "sources": {"all": sorted(scanner_ids)}}
        with mock.patch.dict(sys.modules, {"exchange_calendars": fake_exchange_calendars}), \
             mock.patch.object(reconstructed_inputs, "source_manifest", return_value=fake_manifest), \
             mock.patch.object(reconstructed_inputs, "configuration_fingerprint", return_value="config"), \
             mock.patch.object(reconstructed_inputs, "build_screener_catalog", return_value=catalog), \
             mock.patch.object(reconstructed_inputs, "resolve_max_trading_days", return_value=2), \
             mock.patch.object(reconstructed_inputs, "load_published_rating_evidence", return_value=({}, {})), \
             mock.patch.object(reconstructed_inputs, "load_published_earnings_evidence", return_value={}), \
             mock.patch.object(reconstructed_inputs, "load_dated_scanner_evidence", return_value=({}, {})), \
             mock.patch.object(reconstructed_inputs, "evaluate_focus_price_sources", side_effect=evaluate):
            report = reconstructed_inputs.reconstruct_price_focus_session(cursor, day,
                next_open=dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc),
                database_url="", app_config=SimpleNamespace(benchmark_ticker="SPY"))

        universe_query, universe_params = cursor.queries[0]
        self.assertIn("WHERE trade_date=%s", universe_query)
        self.assertEqual(universe_params, (day,))
        self.assertEqual(report["coverage"]["universe_count"], 2)
        self.assertEqual(report["coverage"]["scanner_history_exclusions"][long_history_id], 1)
        self.assertNotIn(long_history_id, report["coverage"]["unavailable"])
        self.assertEqual(report["coverage"]["history_coverage_issues"][0]["ticker"], "IPO")
        self.assertTrue(report["ready"])

    def test_manifest_is_derived_and_versioned(self):
        manifest = source_manifest()
        self.assertEqual(manifest["version"], "martin_reconstructed_inputs_v1")
        self.assertEqual(len(manifest["digest"]), 64)
        self.assertTrue(PLAN_SCANNER_IDS.issubset(set(manifest["sources"]["all"])))
        self.assertEqual(len(manifest["sources"]["all"]), 47)
        self.assertEqual(len(manifest["sources"]["all"]), len(set(manifest["sources"]["all"])))

    def test_context_only_sources_do_not_change_scanner_count_or_guru_order(self):
        baseline = {"ticker": "ABC", "scanners": [{"id": "focus_scan"}],
            "scanner_count": 1, "strike_zone": {"state": "ready", "score": 50},
            "adr_suitability": "suitable", "stop_to_adr": 0.5, "daily_rs_rating": 90}
        with_context = {**baseline, "scanners": list(baseline["scanners"])}
        for scanner_id in ("rs_phase", "weinstein_stage_analysis"):
            self.assertFalse(_source_contributes_to_guru_board(scanner_id))
            self.assertFalse(_append_guru_board_scanner(with_context, scanner_id, {},
                signal_date="2026-10-23", as_of_date=dt.date(2026, 10, 23)))
        with_context["scanner_count"] = len({item["id"] for item in with_context["scanners"]})
        self.assertEqual(with_context["scanner_count"], 1)
        self.assertEqual(_guru_row_sort_key(with_context), _guru_row_sort_key(baseline))

        self.assertTrue(_source_contributes_to_guru_board("rmv_tightness"))
        with_rmv = {**baseline, "scanners": list(baseline["scanners"])}
        self.assertTrue(_append_guru_board_scanner(with_rmv, "rmv_tightness", {},
            signal_date="2026-10-23", as_of_date=dt.date(2026, 10, 23)))
        with_rmv["scanner_count"] = len({item["id"] for item in with_rmv["scanners"]})
        self.assertEqual(with_rmv["scanner_count"], 2)
        self.assertLess(_guru_row_sort_key(with_rmv), _guru_row_sort_key(baseline))

    def test_preflight_keeps_unavailable_distinct_from_executed_empty(self):
        day = dt.date(2026, 10, 23)
        cursor = _Cursor([{"id": 7, "strategy_id": "weekly_candidate_pool", "run_date": day,
                           "created_at": dt.datetime(2026, 10, 23, 21, tzinfo=dt.timezone.utc),
                           "hit_count": 0, "failure_count": 0, "config_hash": "cfg", "source_kind": "scan"}])
        report = preflight_session(cursor, day, next_open=dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc))
        coverage = report["coverage"]
        self.assertIn("weekly_candidate_pool", coverage["executed_empty"])
        self.assertNotIn("weekly_candidate_pool", coverage["unavailable"])
        self.assertTrue(coverage["unavailable"])
        self.assertFalse(report["ready"])
        self.assertEqual(cursor.params[1], day)

    def test_source_after_next_open_is_not_counted(self):
        day = dt.date(2026, 10, 23)
        cursor = _Cursor([])
        report = preflight_session(cursor, day, next_open=dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc))
        self.assertEqual(report["coverage"]["executed"], [])
        self.assertEqual(report["coverage"]["unavailable"], source_manifest()["sources"]["all"])
        self.assertEqual(report["rank_uncertainty"], "session_order_not_certifiable")

    def test_focus_price_evaluation_rejects_future_only_bars(self):
        day = dt.date(2026, 10, 23)
        future = pd.DataFrame([{"Open": 10, "High": 11, "Low": 9, "Close": 10, "Volume": 100}],
                              index=pd.to_datetime(["2026-10-26"]))
        results = evaluate_focus_price_sources(ticker="ABC", as_of_date=day, bars=future,
            benchmark_bars=pd.DataFrame(), metadata={"ticker": "ABC"}, app_config=object())
        self.assertTrue(results)
        self.assertTrue(all(item["status"] == "unavailable" for item in results.values()))
        self.assertTrue(all(item["reason"] == "future_or_missing_daily_bars" for item in results.values()))

    def test_focus_price_evaluation_rejects_stale_bars_without_target_session_close(self):
        day = dt.date(2026, 10, 23)
        stale = pd.DataFrame([{"Open": 10, "High": 11, "Low": 9, "Close": 10, "Volume": 100}],
                             index=pd.to_datetime(["2026-10-22"]))
        results = evaluate_focus_price_sources(ticker="ABC", as_of_date=day, bars=stale,
            benchmark_bars=pd.DataFrame(), metadata={"ticker": "ABC"}, app_config=object())
        self.assertTrue(results)
        self.assertTrue(all(item["status"] == "unavailable" for item in results.values()))
        self.assertTrue(all(item["reason"] == "future_or_missing_daily_bars" for item in results.values()))

    def test_dated_saved_source_excludes_future_hit_and_keeps_provenance(self):
        day = dt.date(2026, 10, 23)
        run = {"id": 18, "strategy_id": "best_winners", "run_date": dt.date(2026, 10, 22),
               "created_at": dt.datetime(2026, 10, 22, 21, tzinfo=dt.timezone.utc),
               "job_run_id": 91, "job_status": "success",
               "job_finished_at": dt.datetime(2026, 10, 22, 21, 5, tzinfo=dt.timezone.utc),
               "hit_count": 2, "failure_count": 0, "config_hash": "cfg", "source_kind": "batch"}
        hits = [
            {"screen_run_id": 18, "ticker": "ABC", "signal_date": day, "passed": True, "rank": 1,
             "metrics_json": {}, "reasons_json": [], "created_at": dt.datetime(2026, 10, 22, 21, 1, tzinfo=dt.timezone.utc),
             "hit_payload_json": {"signal_date": day.isoformat()}},
            {"screen_run_id": 18, "ticker": "XYZ", "signal_date": dt.date(2026, 10, 26), "passed": True,
             "rank": 2, "metrics_json": {}, "reasons_json": [],
             "created_at": dt.datetime(2026, 10, 26, 13, 31, tzinfo=dt.timezone.utc),
             "hit_payload_json": {"signal_date": "2026-10-26"}},
        ]
        cursor = _QueuedCursor([[run], hits])
        sources, by_source = load_dated_scanner_evidence(cursor, day,
            next_open=dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc),
            scanner_ids={"best_winners"})
        self.assertEqual(sources["best_winners"]["id"], 18)
        self.assertIn("ABC", by_source["best_winners"])
        self.assertNotIn("XYZ", by_source["best_winners"])
        self.assertEqual(by_source["best_winners"]["ABC"]["source_config_hash"], "cfg")

    def test_saved_source_old_screen_run_created_at_does_not_hide_late_replacement_job(self):
        day = dt.date(2026, 10, 23)
        cutoff = dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc)
        run = {"id": 19, "strategy_id": "best_winners", "run_date": dt.date(2026, 10, 22),
               "created_at": dt.datetime(2026, 10, 22, 21, tzinfo=dt.timezone.utc),
               "job_run_id": 92, "job_status": "success",
               "job_finished_at": cutoff + dt.timedelta(seconds=1),
               "hit_count": 1, "failure_count": 0, "config_hash": "cfg", "source_kind": "batch"}
        hits = [{"screen_run_id": 19, "ticker": "ABC", "signal_date": day, "passed": True,
                 "rank": 1, "metrics_json": {}, "reasons_json": [],
                 "hit_payload_json": {"signal_date": day.isoformat()}}]
        cursor = _QueuedCursor([[run], []])
        sources, by_source = load_dated_scanner_evidence(cursor, day,
            next_open=cutoff, scanner_ids={"best_winners"})
        self.assertEqual(sources["best_winners"]["status"], "unavailable")
        self.assertFalse(sources["best_winners"]["publication_provenance_valid"])
        self.assertNotIn("best_winners", by_source)
        self.assertEqual(cursor.params[1], ([19],))

    def test_unlinked_run_preserves_individually_published_positive_hit(self):
        day = dt.date(2026, 10, 23)
        cutoff = dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc)
        run = {"id": 20, "strategy_id": "best_winners", "run_date": dt.date(2026, 10, 22),
               "created_at": dt.datetime(2026, 10, 22, 21, tzinfo=dt.timezone.utc),
               "job_run_id": None, "job_status": None, "job_finished_at": None,
               "hit_count": 1, "failure_count": 0, "config_hash": "cfg", "source_kind": "batch"}
        hit = {"screen_run_id": 20, "ticker": "ABC", "signal_date": day, "passed": True,
               "rank": 1, "metrics_json": {}, "reasons_json": [],
               "created_at": dt.datetime(2026, 10, 22, 21, 5, tzinfo=dt.timezone.utc),
               "hit_payload_json": {"signal_date": day.isoformat()}}
        cursor = _QueuedCursor([[run], [hit]])
        sources, by_source = load_dated_scanner_evidence(cursor, day, next_open=cutoff,
            scanner_ids={"best_winners"})
        self.assertEqual(sources["best_winners"]["status"], "unavailable")
        self.assertIn("ABC", by_source["best_winners"])
        hit_evidence = _candidate_source_hit_evidence(sources["best_winners"],
            by_source["best_winners"]["ABC"])
        self.assertEqual(hit_evidence["status"], "saved_hit")
        self.assertEqual(hit_evidence["source_run_provenance"],
                         "unavailable_positive_hit_individually_published")

    def test_late_replacement_hit_is_not_recovered_from_old_parent_run_timestamp(self):
        day = dt.date(2026, 10, 23)
        cutoff = dt.datetime(2026, 10, 26, 13, 30, tzinfo=dt.timezone.utc)
        run = {"id": 21, "strategy_id": "best_winners", "run_date": dt.date(2026, 10, 22),
               "created_at": dt.datetime(2026, 10, 22, 21, tzinfo=dt.timezone.utc),
               "job_run_id": None, "job_status": None, "job_finished_at": None,
               "hit_count": 1, "failure_count": 0, "config_hash": "cfg", "source_kind": "batch"}
        hit = {"screen_run_id": 21, "ticker": "ABC", "signal_date": day, "passed": True,
               "rank": 1, "metrics_json": {}, "reasons_json": [],
               "created_at": cutoff + dt.timedelta(seconds=1),
               "hit_payload_json": {"signal_date": day.isoformat()}}
        cursor = _QueuedCursor([[run], [hit]])
        sources, by_source = load_dated_scanner_evidence(cursor, day, next_open=cutoff,
            scanner_ids={"best_winners"})
        self.assertEqual(sources["best_winners"]["status"], "unavailable")
        self.assertNotIn("best_winners", by_source)

    def test_partial_failure_run_keeps_known_positive_but_marks_no_hit_uncertain(self):
        run = {"id": 22, "status": "partial_failed", "failure_count": 3,
               "run_date": dt.date(2026, 10, 22)}
        hit = {"ticker": "ABC", "signal_date": "2026-10-22", "source_run_id": 22}
        positive_state = _partial_source_candidate_state("best_winners", run, hit)
        self.assertIsNotNone(positive_state)
        self.assertIsNone(positive_state[1])
        self.assertEqual(_candidate_source_hit_evidence(run, hit)["status"], "saved_hit")
        absent_state = _partial_source_candidate_state("best_winners", run, None)
        self.assertEqual(absent_state[1]["reason"], "historical_scanner_partial_run_no_candidate_hit")

    def test_missing_unregistered_source_is_not_published_but_active_source_is_unavailable(self):
        unavailable, not_published = _partition_missing_sources(
            {"high_volume_close", "best_winners"}, {}, set(), {"best_winners"})
        self.assertEqual(unavailable, ["best_winners"])
        self.assertEqual(not_published, ["high_volume_close"])

    def test_weekly_focus_is_unavailable_without_published_as_of_rating(self):
        day = dt.date(2026, 10, 23)
        bars = pd.DataFrame([{"Open": 10, "High": 11, "Low": 9, "Close": 10, "Volume": 100}],
                            index=pd.to_datetime(["2026-10-23"]))
        result = evaluate_focus_price_sources(ticker="ABC", as_of_date=day, bars=bars,
            benchmark_bars=pd.DataFrame(), metadata={"ticker": "ABC"}, app_config=load_app_config(),
            scanner_ids={"weekly_candidate_pool"}, technical_rating={}, fundamentals={})
        self.assertEqual(result["weekly_candidate_pool"]["status"], "unavailable")
        self.assertEqual(result["weekly_candidate_pool"]["reason"], "published_as_of_rating_missing")

    def test_unknown_sector_momentum_and_earnings_keep_saved_policy_semantics(self):
        day = dt.date(2026, 10, 23)
        row = {"ticker": "ABC", "market_cap": 2_000_000_000, "day_close": 100,
               "daily_rs_rating": 95, "scanner_ids": ["rs"], "sector": "Technology",
               "sector_momentum": None, "trade_readiness": "defined_risk", "adr14_pct": 6,
               "adr_suitability": "suitable", "trade_plan": {"state": "active", "trigger_price": 100,
                   "stop_price": 98, "max_entry_price": 103, "reference_risk_pct": 2,
                   "risk_status": "within_limit", "risk_valid": True}}
        row["strike_zone"] = build_strike_zone(row, as_of_date=day)
        self.assertNotEqual(row["strike_zone"]["state"], "avoid")
        self.assertTrue(any("Earnings date is unavailable" in warning for warning in row["strike_zone"]["warnings"]))
        self.assertEqual(classify_candidate(row, 1)["status"], "pending")
        sector_rows = select_sector_candidates([row], limit=10)
        self.assertEqual(sector_rows[0]["sector_quadrant"], "Unknown")
        self.assertEqual(sector_rows[0]["selection_tier"], "Primary")

    def test_reconstructed_empty_board_persists_without_fabricated_screen_run(self):
        day = dt.date(2026, 10, 23)
        repository = _CapturingForwardRepository()
        service = MartinForwardTestService(repository, bar_loader=lambda *_args, **_kwargs: {})
        snapshot = {"target_trading_date": day.isoformat(), "source_fingerprint": "source-fingerprint",
                    "guru_board": {"rows": []}, "replay_provenance": {
                        "origin": "historical_replay", "source_mode": "martin_reconstructed_inputs_v1"}}
        run_id = service._save_snapshot(day, snapshot, [], "martin_daily_v1")
        self.assertEqual(run_id, 12)
        self.assertEqual(repository.saved["source_run_id"], 0)
        self.assertEqual(repository.saved["fingerprint"], "source-fingerprint")


if __name__ == "__main__":
    unittest.main()
