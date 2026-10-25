from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.cookstock_bridge import (_apply_market_data_source_patches, _prefetched_market_data,
                                  use_prefetched_market_data, prefetched_cookstock_market_data)


class CookstockBridgeTests(unittest.TestCase):
    def test_strict_prefetch_rejects_missing_price_without_database_or_internet(self) -> None:
        class FakeFinancials:
            ticker = "ABC"
            benchmark_price_cache = {}

            def get_historical_price_data(self, *_args):
                raise AssertionError("internet fallback reached")

            def _get_benchmark_price_data(self, *_args):
                raise AssertionError("internet benchmark reached")

            def _resolve_benchmark_ticker(self, _ticker=None):
                return "SPY"

        fake_module = SimpleNamespace(algoParas=SimpleNamespace(MARKET_DATA_SOURCE="database-first"),
                                      cookFinancials=FakeFinancials)
        _apply_market_data_source_patches(fake_module, "database-first")
        with use_prefetched_market_data(ticker_frames={}, benchmark_frames={}, strict=True):
            with self.assertRaisesRegex(ValueError, "Replay price history unavailable"):
                FakeFinancials().get_historical_price_data("2026-01-01", "2026-10-23", "daily")
            with self.assertRaisesRegex(ValueError, "Replay benchmark history unavailable"):
                FakeFinancials()._get_benchmark_price_data("SPY")
            with patch("src.cookstock_bridge.load_many_ticker_windows",
                       side_effect=AssertionError("nested DB prefetch reached")):
                with prefetched_cookstock_market_data(SimpleNamespace(benchmark_ticker="SPY"), ["ABC"],
                        as_of_date=None, history_lookback_days=320):
                    pass

    def test_prefetched_benchmark_lookup_does_not_truthiness_check_dataframe(self) -> None:
        class FakeFinancials:
            benchmark_price_cache: dict[tuple[str, int], object] = {}

            def __init__(self):
                self.history_lookback_days = 365

            def _resolve_benchmark_ticker(self, benchmark_ticker=None):
                return benchmark_ticker or "SPY"

            def get_historical_price_data(self, *args, **kwargs):
                raise AssertionError("should not hit network path")

            def _get_benchmark_price_data(self, *args, **kwargs):
                raise AssertionError("patch not applied")

        fake_module = SimpleNamespace(
            algoParas=SimpleNamespace(MARKET_DATA_SOURCE="database-first"),
            cookFinancials=FakeFinancials,
            dt=SimpleNamespace(date=SimpleNamespace(today=lambda: None)),
        )

        frame = pd.DataFrame(
            {
                "Open": [10.0],
                "High": [11.0],
                "Low": [9.5],
                "Close": [10.5],
                "Volume": [1000],
            },
            index=pd.to_datetime(["2026-06-15"]),
        )

        with patch("src.cookstock_bridge.build_cookstock_price_list_from_frame", return_value=[{"close": 10.5}]):
            _apply_market_data_source_patches(fake_module, "database-first")
            instance = fake_module.cookFinancials()
            token = _prefetched_market_data.set({"ticker_frames": {}, "benchmark_frames": {"SPY": frame}})
            try:
                result = instance._get_benchmark_price_data("SPY")
            finally:
                _prefetched_market_data.reset(token)

        self.assertEqual(result, [{"close": 10.5}])

    def test_strict_prefetch_replaces_stale_shared_benchmark_cache(self) -> None:
        class FakeFinancials:
            benchmark_price_cache = {("SPY", 365): [{"close": 999.0}]}
            history_lookback_days = 365

            def _resolve_benchmark_ticker(self, _ticker=None):
                return "SPY"

            def get_historical_price_data(self, *_args):
                raise AssertionError("internet price path reached")

            def _get_benchmark_price_data(self, *_args):
                raise AssertionError("internet benchmark path reached")

        module = SimpleNamespace(algoParas=SimpleNamespace(MARKET_DATA_SOURCE="database-first"),
                                 cookFinancials=FakeFinancials)
        _apply_market_data_source_patches(module, "database-first")
        frame = pd.DataFrame({"Close": [10.5]}, index=pd.to_datetime(["2026-10-23"]))
        with patch("src.cookstock_bridge.build_cookstock_price_list_from_frame",
                   return_value=[{"close": 10.5}]):
            with use_prefetched_market_data(ticker_frames={}, benchmark_frames={"SPY": frame}, strict=True):
                self.assertEqual(FakeFinancials()._get_benchmark_price_data("SPY"), [{"close": 10.5}])


if __name__ == "__main__":
    unittest.main()
