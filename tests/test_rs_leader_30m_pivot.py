import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.webapp.services.rs_leader_30m_pivot import (evaluate_pivot_sequence, refine_pivot_ambiguity,
    load_frozen_30m_directory, support_decline_candidate)


def bar(opening, high, low, close):
    return {"open": opening, "high": high, "low": low, "close": close, "volume": 1000}


class RSPivotTests(unittest.TestCase):
    def test_replay_rejects_frozen_bundle_for_different_book_id(self):
        from scripts.replay_rs_leader_30m_pivot import validate_bundle_book_id
        with self.assertRaisesRegex(ValueError, "frozen_30m_book_id_mismatch"):
            validate_bundle_book_id({"book_id": "old"}, "new")

    def test_bundle_rebind_copies_verified_candle_bytes_and_keeps_source_unchanged(self):
        from scripts.rebind_rs_leader_30m_pivot_bundle import rebind_bundle
        from scripts.audit_martin_intraday_execution import digest
        with tempfile.TemporaryDirectory() as tmp:
            source, destination = Path(tmp) / "source", Path(tmp) / "copy"
            source.mkdir()
            candle = b'{"candles": []}\n'
            (source / "abc.json").write_bytes(candle)
            manifest = {"schema": "rs_leader_30m_pivot_audit_v1", "book_id": "old",
                        "records": [{"ticker": "ABC", "candle_file": "abc.json",
                                     "candle_fingerprint": digest({"candles": []})}]}
            manifest["fingerprint"] = digest(manifest)
            original = json.dumps(manifest, sort_keys=True)
            (source / "audit.json").write_text(json.dumps(manifest))
            rebind_bundle(source, destination, "rs_leader_30m_pivot_replay_new")
            copied = json.loads((destination / "audit.json").read_text())
            supplied = copied.pop("fingerprint")
            self.assertEqual(copied["book_id"], "rs_leader_30m_pivot_replay_new")
            self.assertEqual(supplied, digest(copied))
            self.assertEqual((destination / "abc.json").read_bytes(), candle)
            self.assertEqual(json.dumps(json.loads((source / "audit.json").read_text()), sort_keys=True), original)
            (source / "abc.json").write_text('{"candles": [1]}')
            with self.assertRaisesRegex(ValueError, "bundle_fingerprint_mismatch"):
                rebind_bundle(source, Path(tmp) / "badcopy", "rs_leader_30m_pivot_replay_other")

    def _frozen_bundle(self, actual="BATS:ABC", *, high=10.9, low=9.1):
        from scripts.audit_martin_intraday_execution import digest
        day = "2026-01-05"
        requested = "NASDAQ:ABC"
        starts = [int((1767623400 + 1800 * i)) for i in range(13)]
        candles = [dict({**bar(10, high, low, 10.1)}, time=stamp) for stamp in starts]
        data = {"requested_symbol": actual, "resolved_symbol": actual,
            "query": {"symbol": actual, "session": "regular", "adjustment": "splits",
                "timezone": "America/New_York", "timeframe": "30"},
            "metadata": {"full_name": actual, "subsession_id": "regular",
                "timezone": "America/New_York", "currency_code": "USD"}, "candles": candles}
        manifest = {"schema": "rs_leader_30m_pivot_audit_v1", "provider": "TradingView",
            "timeframe": "30m", "session": "regular", "adjustment": "splits"}
        manifest["fingerprint"] = digest(manifest)
        record = {"ticker": "ABC", "requested_symbol": requested, "resolved_symbol": actual,
            "timeframe": "30m", "session": "regular", "adjustment": "splits"}
        return {"manifest": manifest, "record": record, "data": data}, day

    def test_guarded_bats_requires_exact_ticker_complete_bars_and_daily_envelope(self):
        from src.webapp.services.rs_leader_30m_pivot import _frozen_30m_evidence
        from scripts import audit_martin_intraday_execution as audit
        bundle, day = self._frozen_bundle()
        with patch.object(audit, "session_starts", return_value=[1767623400 + 1800 * i for i in range(13)]):
            bars, evidence = _frozen_30m_evidence(bundle, day, {"open": 10, "high": 11, "low": 9,
                "close": 10.2, "volume": 1000})
        self.assertEqual(len(bars), 13)
        self.assertEqual(evidence["evidence_tier"], "guarded_alternate")
        self.assertTrue(evidence["feed_conflict"])
        outside, day = self._frozen_bundle(high=11.2)
        with patch.object(audit, "session_starts", return_value=[1767623400 + 1800 * i for i in range(13)]):
            with self.assertRaisesRegex(ValueError, "bats_outside_daily_envelope"):
                _frozen_30m_evidence(outside, day, {"open": 10, "high": 11, "low": 9,
                    "close": 10.2, "volume": 1000})
        wrong, day = self._frozen_bundle(actual="BATS:OTHER")
        with patch.object(audit, "session_starts", return_value=[1767623400 + 1800 * i for i in range(13)]):
            with self.assertRaisesRegex(ValueError, "ticker_mismatch"):
                _frozen_30m_evidence(wrong, day, {"open": 10, "high": 11, "low": 9,
                    "close": 10.2, "volume": 1000})

    def test_exact_requested_exchange_is_primary_evidence(self):
        from src.webapp.services.rs_leader_30m_pivot import _frozen_30m_evidence
        from scripts import audit_martin_intraday_execution as audit
        bundle, day = self._frozen_bundle(actual="NASDAQ:ABC")
        with patch.object(audit, "session_starts", return_value=[1767623400 + 1800 * i for i in range(13)]):
            _, evidence = _frozen_30m_evidence(bundle, day)
        self.assertEqual(evidence["evidence_tier"], "primary_exact")
        self.assertFalse(evidence["feed_conflict"])

    @unittest.skipUnless(importlib.util.find_spec("pandas"), "pandas is not installed")
    def test_daily_support_decline_thresholds_are_explicit_and_inferred(self):
        import pandas as pd
        dates = pd.bdate_range("2025-12-01", periods=60)
        frame = pd.DataFrame({"Open": [110] * 59 + [100], "High": [110] * 59 + [106],
                              "Low": [100] * 60, "Close": [108] * 59 + [102], "Volume": [1000] * 60},
                             index=dates)
        result = support_decline_candidate(frame, dates[-1].date(), saved_support=101)
        self.assertEqual(result["status"], "eligible")
        self.assertTrue(result["thresholds_inferred"])
        self.assertEqual(support_decline_candidate(frame, dates[-1].date())["status"], "rejected")
        future = pd.DataFrame({"Open": [200], "High": [300], "Low": [190], "Close": [250], "Volume": [1000]},
                              index=[dates[-1] + pd.Timedelta(days=1)])
        with_future = pd.concat([frame, future])
        self.assertEqual(support_decline_candidate(with_future, dates[-1].date(), saved_support=101), result)

    def test_pivot_requires_later_bar_after_decline_and_stop_first_cancels(self):
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                bar(10.3, 10.35, 9.9, 10.1)]
        self.assertEqual(evaluate_pivot_sequence(bars)["status"], "cancelled")
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                bar(10.3, 10.6, 10.2, 10.5)]
        self.assertEqual(evaluate_pivot_sequence(bars)["status"], "filled")

    def test_same_bar_entry_and_stop_is_not_counted_as_fill(self):
        result = evaluate_pivot_sequence([bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                                          bar(10.15, 10.6, 9.7, 10.2)])
        self.assertEqual(result["status"], "ambiguous")

    def test_green_bar_without_prior_intraday_decline_is_not_a_pivot(self):
        result = evaluate_pivot_sequence([bar(10, 10.2, 9.9, 10.1), bar(10.1, 10.5, 10, 10.4)])
        self.assertEqual(result["status"], "expired")

    def test_one_minute_bars_resolve_30m_ambiguity_when_order_is_known(self):
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                {**bar(10.15, 10.6, 9.7, 10.2), "time": 4600}]
        ambiguous = {"status": "ambiguous", "pivot_index": 1, "bar_index": 2,
                     "entry": 10.5, "stop": 10.1}
        fine = [{**bar(10.3, 10.4, 10.2, 10.35), "time": 4600 + 60 * i} for i in range(30)]
        fine[1] = {**bar(10.4, 10.55, 10.3, 10.5), "time": 4660}
        refined = refine_pivot_ambiguity(bars, fine, ambiguous)
        self.assertEqual(refined["status"], "filled")
        self.assertEqual(refined["fill_time"], 4660)
        self.assertEqual(refined["fill_time_granularity"], "1m")
        fine[2] = {**bar(10.4, 10.45, 10.0, 10.1), "time": 4720}
        stopped = refine_pivot_ambiguity(bars, fine, ambiguous)
        self.assertEqual(stopped["status"], "filled_stopped")
        self.assertEqual(stopped["fill_time"], 4660)
        self.assertEqual(stopped["fill_time_granularity"], "1m")
        fine[0] = {**bar(10.05, 10.2, 10, 10.1), "time": 4600}
        self.assertEqual(refine_pivot_ambiguity(bars, fine, ambiguous)["status"], "cancelled")
        fine[0] = {**bar(10.2, 10.6, 10, 10.2), "time": 4600}
        self.assertEqual(refine_pivot_ambiguity(bars, fine, ambiguous)["status"], "ambiguous")

    def test_invalid_intraday_data_blocks_entry(self):
        self.assertEqual(evaluate_pivot_sequence([bar(10, 9, 9.5, 10.2)])["status"], "blocked_data")

    def test_tampered_30m_manifest_is_rejected_before_loading_bars(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.json"
            path.write_text(json.dumps({"schema": "rs_leader_30m_pivot_audit_v1", "fingerprint": "bad"}))
            with self.assertRaisesRegex(ValueError, "fingerprint_mismatch"):
                load_frozen_30m_directory(directory, "2026-01-02")

    def test_retrospective_source_stays_blocked_by_default_and_is_hypothetical_when_enabled(self):
        from src.webapp.services.rs_leader_30m_pivot import candidates_from_saved_run
        source = {"strategy_id": "martin_bar_watchlist_v1", "rules_version": "martin_bar_watchlist_v1",
            "target_trading_date": "2026-01-02", "created_at": "2026-01-05T00:00:00Z",
            "source_run_id": 12, "source_fingerprint": "abc", "replay_provenance": {"retrospective": True},
            "hits": [{"ticker": "ABC", "discovery_categories": ["leader_30_session"]}]}
        with patch("src.webapp.services.rs_leader_30m_pivot.support_decline_candidate",
                   return_value={"status": "eligible"}):
            strict = candidates_from_saved_run(source, {}, "2026-01-02")
            hypothetical = candidates_from_saved_run(source, {}, "2026-01-02", simulate_backdated_buys=True)
        self.assertEqual(strict[0]["status"], "blocked_source")
        self.assertEqual(hypothetical[0]["status"], "pending")
        self.assertTrue(hypothetical[0]["backdated_buy_simulation"])
        self.assertEqual(hypothetical[0]["fill_basis"], "hypothetical_backdated_simulation")

    def test_saved_watchlist_priority_uses_adr_then_30_session_rank_then_ticker(self):
        from src.webapp.services.rs_leader_30m_pivot import candidates_from_saved_run, PRIORITY_VERSION
        source = {"strategy_id": "martin_bar_watchlist_v1", "rules_version": "martin_bar_watchlist_v1",
            "target_trading_date": "2026-01-02", "source_run_id": 12, "source_fingerprint": "abc",
            "hits": [
                {"ticker": "ZZZ", "adr14_pct": 4, "leader_rank": 1, "discovery_categories": ["leader_30_session"]},
                {"ticker": "BBB", "adr14_pct": 5, "leader_rank": 2, "discovery_categories": ["leader_30_session"]},
                {"ticker": "AAA", "adr14_pct": 5, "leader_rank": 2, "discovery_categories": ["leader_30_session"]},
            ]}
        with patch("src.webapp.services.rs_leader_30m_pivot.support_decline_candidate",
                   return_value={"status": "eligible"}):
            candidates = candidates_from_saved_run(source, {}, "2026-01-02")
        self.assertEqual([item["ticker"] for item in candidates], ["AAA", "BBB", "ZZZ"])
        self.assertEqual(candidates[0]["priority_version"], PRIORITY_VERSION)
        self.assertEqual(candidates[0]["priority_inputs"], {"saved_adr14_pct": 5.0,
            "leader_rank_30_session": 2})

    def test_pivot_fills_are_ordered_by_trigger_bar_before_priority_tie_break(self):
        from src.webapp.services.rs_leader_30m_pivot import _sort_pivot_evaluations
        later_high_priority = {"ticker": "AAA", "payload_json": {"priority_inputs": {
            "saved_adr14_pct": 8, "leader_rank_30_session": 1}}}
        earlier_lower_priority = {"ticker": "BBB", "payload_json": {"priority_inputs": {
            "saved_adr14_pct": 3, "leader_rank_30_session": 20}}}
        tied_high_priority = {"ticker": "CCC", "payload_json": {"priority_inputs": {
            "saved_adr14_pct": 9, "leader_rank_30_session": 1}}}
        evaluated = [(later_high_priority, {"status": "filled", "bar_index": 5}),
                     (earlier_lower_priority, {"status": "filled", "bar_index": 2}),
                     (tied_high_priority, {"status": "filled", "bar_index": 5})]
        ordered = _sort_pivot_evaluations(evaluated)
        self.assertEqual([order["ticker"] for order, _ in ordered], ["BBB", "CCC", "AAA"])

    def test_refined_earlier_fill_gets_the_only_open_slot_before_later_fill(self):
        from src.webapp.services.rs_leader_30m_pivot import refine_pivot_ambiguity, _sort_pivot_evaluations
        bars = [bar(10.5, 10.6, 10.2, 10.3), bar(10.2, 10.5, 10.1, 10.4),
                {**bar(10.15, 10.6, 9.7, 10.2), "time": 4600}]
        ambiguous = {"status": "ambiguous", "pivot_index": 1, "bar_index": 2,
                     "entry": 10.5, "stop": 10.1}
        fine = [{**bar(10.3, 10.4, 10.2, 10.35), "time": 4600 + 60 * i} for i in range(30)]
        fine[1] = {**bar(10.4, 10.55, 10.3, 10.5), "time": 4660}
        refined = refine_pivot_ambiguity(bars, fine, ambiguous)
        early = {"ticker": "EARLY", "payload_json": {"priority_inputs": {
            "saved_adr14_pct": 2, "leader_rank_30_session": 20}}}
        later = {"ticker": "LATER", "payload_json": {"priority_inputs": {
            "saved_adr14_pct": 9, "leader_rank_30_session": 1}}}
        evaluated = [(later, {"status": "filled", "bar_index": 3, "fill_time": 8200,
                              "fill_time_granularity": "30m"}), (early, refined)]
        ordered = _sort_pivot_evaluations(evaluated)
        # One position slot remains; capacity goes to the earlier refined trigger.
        available_slots = 1
        allocated = [order["ticker"] for order, result in ordered
                     if result["status"] in {"filled", "filled_stopped"}][:available_slots]
        self.assertEqual(allocated, ["EARLY"])

    def test_frozen_manifest_requires_matching_backdated_mode(self):
        from scripts.audit_martin_intraday_execution import digest
        with tempfile.TemporaryDirectory() as directory:
            manifest = {"schema": "rs_leader_30m_pivot_audit_v1", "provider": "TradingView",
                "timeframe": "30m", "session": "regular", "adjustment": "splits",
                "book_id": "rs_leader_30m_pivot_replay_test", "simulate_backdated_buys": True,
                "records": []}
            manifest["fingerprint"] = digest(manifest)
            (Path(directory) / "audit.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "backdated_mode_mismatch"):
                load_frozen_30m_directory(directory, "2026-01-02", simulate_backdated_buys=False)

    @unittest.skipUnless(shutil.which("node"), "node is not installed")
    def test_intraday_fetch_cli_accepts_30m_timeframe(self):
        script = Path(__file__).resolve().parents[1] / "scripts" / "fetch_tradingview_intraday.mjs"
        result = subprocess.run(["node", str(script), "/nonexistent", "NASDAQ:AAPL", "2026-01-02", "30"],
                                capture_output=True, text=True, check=False)
        self.assertNotEqual(result.returncode, 64)
        self.assertNotIn("[30|15|1]", result.stderr)


if __name__ == "__main__":
    unittest.main()
