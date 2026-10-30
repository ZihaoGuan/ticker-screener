from __future__ import annotations

import json
import multiprocessing
import tempfile
import threading
import time
import unittest
from pathlib import Path

from src.webapp.services.martin_intraday_raw_cache import RawIntradayCache


def _fetch_cache_process(root, identity, counter, output):
    cache = RawIntradayCache(root)
    data = {"metadata": {"full_name": "NASDAQ:ABC"}, "candles": [{"time": 1, "open": 2.0}]}
    def fetch():
        with counter.get_lock(): counter.value += 1
        time.sleep(0.05)
        return data
    output.put(cache.get_or_fetch(identity, fetch))


class RawIntradayCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cache = RawIntradayCache(self.temp.name)
        self.identity = RawIntradayCache.identity(provider="TradingView", requested_symbol="nasdaq:abc",
            date="2026-10-29", timeframe="30", session="regular", adjustment="splits",
            symbol_resolution_mode="exchange_fallback")
        self.data = {"metadata": {"full_name": "NASDAQ:ABC"}, "candles": [{"time": 1, "open": 2.0}]}

    def tearDown(self):
        self.temp.cleanup()

    def test_identity_normalizes_and_separates_all_feed_dimensions(self):
        base = self.identity
        for update in ({"timeframe": "15"}, {"session": "extended"}, {"adjustment": "none"},
                       {"symbol_resolution_mode": "exact"}, {"requested_symbol": "NYSE:ABC"},
                       {"date": "2026-10-28"}):
            changed = dict(base, **update)
            self.assertNotEqual(RawIntradayCache.identity(**base), RawIntradayCache.identity(**changed))
        self.assertEqual(base["requested_symbol"], "NASDAQ:ABC")

    def test_success_is_reused_and_resolved_feed_is_verified(self):
        calls = []
        fetch = lambda: calls.append(1) or self.data
        first = self.cache.get_or_fetch(self.identity, fetch, validator=lambda data: bool(data["candles"]))
        second = self.cache.get_or_fetch(self.identity, fetch, validator=lambda data: bool(data["candles"]))
        self.assertEqual(first, self.data)
        self.assertEqual(second, self.data)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.cache.stats, {"hits": 1, "misses": 1, "fetch_failures": 0, "published": 1})
        mismatch = self.cache.get_or_fetch(self.identity, fetch, expected_resolved_symbol="NYSE:ABC")
        self.assertEqual(mismatch, self.data)
        self.assertEqual(len(calls), 2)

    def test_errors_malformed_payloads_and_none_are_retryable(self):
        attempts = iter([{"error_code": "LOOKUP_FAILED"}, None, {"metadata": {"full_name": "NASDAQ:ABC"}, "candles": []}, self.data])
        for _ in range(3):
            self.cache.get_or_fetch(self.identity, lambda: next(attempts), validator=lambda data: bool(data["candles"]))
        result = self.cache.get_or_fetch(self.identity, lambda: next(attempts), validator=lambda data: bool(data["candles"]))
        self.assertEqual(result, self.data)
        again = self.cache.get_or_fetch(self.identity, lambda: self.fail("success should be cached"), validator=lambda data: bool(data["candles"]))
        self.assertEqual(again, self.data)

    def test_corrupt_or_tampered_entry_is_refetched(self):
        self.cache.get_or_fetch(self.identity, lambda: self.data)
        entry = next(Path(self.temp.name).glob("*/*.json"))
        envelope = json.loads(entry.read_text())
        envelope["data"]["candles"][0]["open"] = 999
        entry.write_text(json.dumps(envelope))
        calls = []
        self.cache.get_or_fetch(self.identity, lambda: calls.append(True) or self.data)
        self.assertEqual(calls, [True])

    def test_same_key_fetches_once_across_threads(self):
        calls, results = [], []
        def fetch():
            calls.append(True)
            time.sleep(0.03)
            return self.data
        threads = [threading.Thread(target=lambda: results.append(self.cache.get_or_fetch(self.identity, fetch)))
                   for _ in range(6)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(len(calls), 1)
        self.assertEqual(results, [self.data] * 6)

    def test_same_key_fetches_once_across_processes(self):
        context = multiprocessing.get_context("fork")
        counter, output = context.Value("i", 0), context.Queue()
        processes = [context.Process(target=_fetch_cache_process,
            args=(self.temp.name, self.identity, counter, output)) for _ in range(3)]
        for process in processes: process.start()
        for process in processes: process.join(5)
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        self.assertEqual(counter.value, 1)
        self.assertEqual([output.get(timeout=1) for _ in processes], [self.data] * 3)


if __name__ == "__main__":
    unittest.main()
