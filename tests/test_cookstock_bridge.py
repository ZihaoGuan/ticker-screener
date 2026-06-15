from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from src.cookstock_bridge import _apply_market_data_source_patches, _prefetched_market_data


class CookstockBridgeTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
