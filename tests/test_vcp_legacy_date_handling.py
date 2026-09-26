from __future__ import annotations

import datetime as dt
from contextlib import nullcontext
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.universe import UniverseTicker
from src.vcp_screen import run_vcp_screen
from vendor.cookstock.src.cookStock import cookFinancials


class VcpLegacyDateHandlingTests(unittest.TestCase):
    def test_invalid_local_high_date_is_a_non_match_instead_of_a_strptime_error(self) -> None:
        financials = object.__new__(cookFinancials)
        financials.get_highest_in5days = lambda _date: (100.0, -1)  # type: ignore[method-assign]

        with patch("vendor.cookstock.src.cookStock._is_v2", return_value=False):
            matched, *_ = financials.find_one_contraction(dt.date.today() - dt.timedelta(days=10))

        self.assertFalse(matched)

    def test_vcp_scan_uses_corrected_v2_engine_without_changing_the_global_default(self) -> None:
        ticker = UniverseTicker(symbol="AAPL")
        config = SimpleNamespace(benchmark_ticker="SPY", rs_new_high_history_days=365)
        cookstock = SimpleNamespace(algoParas=SimpleNamespace(ENGINE_VERSION="v1", SCREEN_PROFILE="strict"))
        test_case = self

        class FakeFinancials:
            def combined_best_strategy(self, **_kwargs: object) -> bool:
                test_case.assertEqual(cookstock.algoParas.ENGINE_VERSION, "v2")
                return False

        cookstock.cookFinancials = lambda *_args, **_kwargs: FakeFinancials()

        with (
            patch("src.vcp_screen.load_configured_cookstock", return_value=cookstock),
            patch("src.vcp_screen.freeze_cookstock_today", return_value=nullcontext()),
            patch("src.vcp_screen.iter_prefetched_cookstock_batches", return_value=iter([[ticker]])),
        ):
            result = run_vcp_screen(config, [ticker], as_of_date=dt.date(2026, 9, 25))

        self.assertEqual(result.failed_tickers, [])
        self.assertEqual(cookstock.algoParas.ENGINE_VERSION, "v1")


if __name__ == "__main__":
    unittest.main()
