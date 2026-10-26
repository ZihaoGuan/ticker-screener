import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import tempfile
from contextlib import nullcontext
from types import ModuleType
import unittest
from unittest.mock import patch

from src.webapp.services.martin_intraday_execution import evaluate_frozen_order, fingerprint
from scripts import audit_martin_intraday_execution as audit
from scripts.replay_martin_bar_retest_range import (
    auto_freeze_book_intraday, continue_book, frozen_intraday_for, main as replay_main,
)


def feed(candles, *, symbol="NASDAQ:ABC", full_name=None, timeframe="15"):
    return {"requested_symbol": symbol, "expected_starts": [row["time"] for row in candles],
        "data": {"query": {"symbol": symbol, "timeframe": timeframe, "session": "regular",
            "adjustment": "splits", "timezone": "America/New_York"},
            "metadata": {"full_name": full_name or symbol, "subsession_id": "regular",
                "timezone": "America/New_York", "currency_code": "USD"}, "candles": candles}}


def candle(time, low, *, opening=12, high=13):
    return {"time": time, "open": opening, "high": high, "low": low, "close": 12, "volume": 100}


class MartinIntradayExecutionTests(unittest.TestCase):
    day = dt.date(2026, 10, 23)
    daily = {"open": 12, "high": 13, "low": 9.8, "close": 12, "volume": 1000}

    def test_symbol_resolution_requires_one_exact_us_stock_listing(self):
        module = (Path(__file__).resolve().parents[1] / "scripts/tradingview_symbol_resolution.mjs").as_uri()
        code = f"""
            import assert from 'node:assert/strict';
            import {{ exactUsStocks }} from {json.dumps(module)};
            for (const ticker of ['ECO', 'GME', 'RXO']) {{
              assert.deepEqual(exactUsStocks([{{symbol:ticker, exchange:'NYSE', type:'stock'}}], ticker), [`NYSE:${{ticker}}`]);
              assert.deepEqual(exactUsStocks([{{symbol:ticker, exchange:'NYSE', type:'stock'}},
                {{symbol:ticker, exchange:'NASDAQ', type:'stock'}}], ticker), [`NYSE:${{ticker}}`, `NASDAQ:${{ticker}}`]);
            }}
            assert.deepEqual(exactUsStocks([{{symbol:'ECO', exchange:'NYSE', type:'crypto'}}], 'ECO'), []);
            assert.deepEqual(exactUsStocks([{{symbol:'ECON', exchange:'NYSE', type:'stock'}}], 'ECO'), []);
        """
        result = subprocess.run(["node", "--input-type=module", "-e", code], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_configured_tradingview_runtime_exports_search_markets(self):
        client_dir = os.environ.get("TRADINGVIEW_CLIENT_DIR")
        if not client_dir:
            self.skipTest("TRADINGVIEW_CLIENT_DIR is not set")
        entry = (Path(client_dir) / "dist/index.js").as_uri()
        result = subprocess.run(["node", "--input-type=module", "-e",
            f"const api = await import({json.dumps(entry)}); if (typeof api.searchMarkets !== 'function') process.exit(1);"],
            text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def evaluate(self, bundle):
        return evaluate_frozen_order(bundle, day=self.day, requested_symbol="NASDAQ:ABC",
                                     entry=11, stop=10, daily=self.daily)

    def test_sequential_evaluator_checks_later_bars_after_entry(self):
        result = self.evaluate(feed([candle(1, 10.5), candle(2, 9.8)]))
        self.assertEqual(result["status"], "filled_stopped")
        self.assertEqual(result["reason"], "stop_after_entry")
        self.assertEqual(result["bundle_fingerprint"].__len__(), 64)

    def test_missing_coverage_blocks_execution(self):
        result = self.evaluate({**feed([candle(1, 10.5), candle(2, 9.8)]), "expected_starts": [1, 2, 3]})
        self.assertEqual(result["status"], "blocked_data")
        self.assertEqual(result["reason"], "intraday_coverage_incomplete")

    def test_bats_requires_full_range_and_open_fill_equivalence(self):
        result = self.evaluate(feed([candle(1, 10.5)], symbol="NASDAQ:ABC", full_name="BATS:ABC"))
        self.assertEqual(result["status"], "filled")
        self.assertEqual(result["evidence_status"], "alternate_feed_supported")
        self.assertEqual(result["feed_basis"], "alternate_bats_supported")
        self.assertTrue(result["alternate_feed_supported"])
        exact = self.evaluate(feed([candle(1, 9.8)]))
        self.assertEqual(exact["evidence_status"], "exact_requested_feed")
        self.assertEqual(exact["evidence_tier"], "primary_exact")
        outside = {"open": 12, "high": 13, "low": 11, "close": 12, "volume": 1000}
        blocked = evaluate_frozen_order(feed([candle(1, 10.5)], full_name="BATS:ABC"), day=self.day,
            requested_symbol="NASDAQ:ABC", entry=11, stop=10, daily=outside)
        self.assertEqual(blocked["reason"], "intraday_feed_mismatch")

    def test_v3_uses_complete_in_range_bats_bars_when_openings_conflict(self):
        bars = [candle(1, 11.5, opening=12, high=12.5)]
        daily = {"open": 10, "high": 13, "low": 9.8, "close": 12, "volume": 1000}
        bundle = feed(bars, full_name="BATS:ABC")
        v2 = evaluate_frozen_order(bundle, day=self.day, requested_symbol="NASDAQ:ABC",
            entry=11, stop=9, daily=daily, execution_version="martin_bar_retest_execution_v2")
        v3 = evaluate_frozen_order(bundle, day=self.day, requested_symbol="NASDAQ:ABC",
            entry=11, stop=9, daily=daily, execution_version="martin_bar_retest_execution_v3")
        self.assertEqual(v2["status"], "blocked_data")
        self.assertEqual(v3["status"], "expired")
        self.assertFalse(v3["feed_mismatch"])
        self.assertTrue(v3["feed_conflict"])
        self.assertIn("15m bars", v3["feed_conflict_note"])

    def test_v3_rejects_bats_range_outside_daily_envelope(self):
        daily = {"open": 12, "high": 13, "low": 11, "close": 12, "volume": 1000}
        result = evaluate_frozen_order(feed([candle(1, 10.5)], full_name="BATS:ABC"),
            day=self.day, requested_symbol="NASDAQ:ABC", entry=11, stop=10, daily=daily,
            execution_version="martin_bar_retest_execution_v3")
        self.assertEqual(result["status"], "blocked_data")
        self.assertEqual(result["reason"], "intraday_feed_mismatch")

    def test_v3_invalid_bar_is_blocked_for_daily_fallback(self):
        invalid = {"time": 1, "open": 12, "high": 11, "low": 10, "close": 12, "volume": 100}
        result = evaluate_frozen_order(feed([invalid], full_name="BATS:ABC"),
            day=self.day, requested_symbol="NASDAQ:ABC", entry=11, stop=9, daily=self.daily,
            execution_version="martin_bar_retest_execution_v3")
        self.assertEqual(result["status"], "blocked_data")
        self.assertEqual(result["reason"], "intraday_bar_invalid")

    def test_replay_loads_frozen_audit_bundle_by_ticker_and_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ABC_2026-10-23.json"
            data = {"query": {"symbol": "NASDAQ:ABC"}, "candles": []}
            path.write_text(json.dumps(data))
            manifest = {"records": [{"ticker": "ABC", "date": "2026-10-23",
                "candle_file": path.name, "candle_fingerprint": fingerprint(data)}]}
            manifest["fingerprint"] = fingerprint(manifest)
            (Path(temporary) / "audit.json").write_text(json.dumps(manifest))
            bundles = frozen_intraday_for(temporary, self.day)
        self.assertIn("ABC", bundles)
        self.assertEqual(bundles["ABC"]["requested_symbol"], "NASDAQ:ABC")
        self.assertEqual(bundles["ABC"]["source_date"], "2026-10-23")

    def test_replay_rejects_tampered_frozen_bundle(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ABC_2026-10-23.json"
            data = {"query": {"symbol": "NASDAQ:ABC"}, "candles": []}
            path.write_text(json.dumps(data))
            manifest = {"records": [{"ticker": "ABC", "date": "2026-10-23",
                "candle_file": path.name, "candle_fingerprint": fingerprint(data)}]}
            manifest["fingerprint"] = fingerprint(manifest)
            (Path(temporary) / "audit.json").write_text(json.dumps(manifest))
            path.write_text(json.dumps({"query": {"symbol": "NASDAQ:ABC"}, "candles": [1]}))
            with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
                frozen_intraday_for(temporary, self.day)

    def test_missing_frozen_directory_returns_empty_input_for_daily_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.assertEqual(frozen_intraday_for(Path(temporary) / "missing", self.day), {})

    def test_new_v2_replay_requires_explicit_bundle_directory(self):
        with self.assertRaises(SystemExit) as raised:
            replay_main(["--from-date", "2026-10-20", "--to-date", "2026-10-23",
                "--book-id", "martin_range_bar_retest_v2", "--execution-version", "v2"])
        self.assertEqual(raised.exception.code, 2)

    def test_auto_freeze_writes_durable_bundle_and_fetches_one_minute_for_ambiguity(self):
        calls = []
        data15 = {"query": {"symbol": "NASDAQ:ABC", "timeframe": "15", "session": "regular",
                "adjustment": "splits", "timezone": "America/New_York"},
            "metadata": {"full_name": "BATS:ABC", "subsession_id": "regular",
                "timezone": "America/New_York", "currency_code": "USD"},
            "candles": [candle(1, 8)]}
        data1 = {**data15, "query": {**data15["query"], "timeframe": "1"},
            "candles": [candle(1, 10)]}
        def fetcher(_client, _symbol, _day, timeframe):
            calls.append(timeframe)
            return data15 if timeframe == "15" else data1
        with tempfile.TemporaryDirectory() as temporary, patch.object(audit, "session_starts", return_value=[1]):
            output, frozen = auto_freeze_book_intraday(book_id="martin_range_bar_retest_v2", day=self.day,
                pending_orders=[{"ticker": "ABC", "payload_json": {"max_entry": 10, "stop": 9}}],
                database_url="unused", artifacts_dir=temporary,
                daily_loader=lambda *_args: {"open": 12, "high": 13, "low": 8, "close": 12, "volume": 100},
                fetcher=fetcher)
            self.assertTrue((output / "audit.json").exists())
            self.assertEqual(frozen["ABC"]["source_date"], self.day.isoformat())
            self.assertIn("one_minute", frozen["ABC"]["data"])
        self.assertEqual(calls, ["15", "1"])

    def test_auto_freeze_records_provider_failure_for_daily_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            _output, frozen = auto_freeze_book_intraday(book_id="martin_range_bar_retest_v2", day=self.day,
                pending_orders=[{"ticker": "ABC", "payload_json": {"max_entry": 10, "stop": 9}}],
                database_url="unused", artifacts_dir=temporary, daily_loader=lambda *_args: None,
                fetcher=lambda *_args: None)
        self.assertEqual(frozen["ABC"]["freeze_error"], "provider_unavailable")
        self.assertEqual(frozen["ABC"]["source_date"], self.day.isoformat())

    def test_continuation_routes_v2_from_persisted_book_version(self):
        order = []
        class Repository:
            instance = None
            def __init__(self, **_kwargs): self.__class__.instance = self; self.metadata = None
            def latest_session_date(self, _book): return dt.date(2026, 10, 22)
            def _rows(self, *_args): return [{"summary_json": {"execution_book_version": "martin_bar_retest_execution_v2"}}]
            def session_run_id(self, *_args): return None
            def pending_orders(self, **_kwargs): return [{"ticker": "ABC", "payload_json": {"max_entry": 10, "stop": 9}}]
            def atomic_session(self, *_args): order.append("atomic"); return nullcontext()
            def set_run_metadata(self, _run_id, payload): self.metadata = payload

        class Executor:
            instance = None
            def __init__(self, *_args, **_kwargs): self.__class__.instance = self
            def run_session(self, **kwargs): self.kwargs = kwargs; return {"missing_bars": 0, "intraday_fallbacks": 0}

        modules = {
            "src.webapp.repositories.martin_forward_test_repository": {"MartinForwardTestRepository": Repository},
            "src.webapp.services.martin_bar_retest_service": {"MartinBarRetestService": object},
            "src.webapp.services.martin_bar_retest_execution": {"MartinBarRetestExecutionService": Executor},
            "src.webapp.services.martin_rotation": {"require_completed_session": lambda _day: None},
            "src.webapp.config": {"load_webapp_config": lambda: type("Config", (), {"artifacts_dir": "/artifacts"})()},
            "scripts.run_martin_bar_retest": {"_load_source": lambda *_args: None},
        }
        replacements = {name: ModuleType(name) for name in modules}
        for name, attrs in modules.items():
            for key, value in attrs.items(): setattr(replacements[name], key, value)
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict("sys.modules", replacements), patch("scripts.replay_martin_bar_retest_range.xnys_sessions",
                    return_value=[self.day]), \
                    patch("scripts.replay_martin_bar_retest_range.continuation_mark", return_value={
                        "mark_coverage": {"status": "complete"}}), \
                    patch("scripts.replay_martin_bar_retest_range.save_execution_only_run", return_value=77), \
                    patch("scripts.replay_martin_bar_retest_range.auto_freeze_book_intraday",
                        side_effect=lambda **_kwargs: (order.append("freeze") or Path(temporary), {})):
                result = continue_book(book_id="martin_range_bar_retest_v2", database_url="unused",
                    history=object())
        self.assertEqual(result["status"], "complete")
        self.assertEqual(Executor.instance.execution_version, "martin_bar_retest_execution_v2")
        self.assertEqual(Executor.instance.kwargs["frozen_intraday"], {})
        self.assertEqual(order, ["freeze", "atomic"])
        self.assertEqual(Repository.instance.metadata["execution_management_version"], "martin_bar_retest_management_v2")
        self.assertEqual(Repository.instance.metadata["trim_fraction"], "1/3")



if __name__ == "__main__":
    unittest.main()
