from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.screener_engine import ScreenerEvaluationResult, ScreenerSpec
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService


def _frame(start: str, count: int) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=count, freq="B")
    return pd.DataFrame(
        {
            "Open": [100.0 + idx for idx in range(count)],
            "High": [101.0 + idx for idx in range(count)],
            "Low": [99.0 + idx for idx in range(count)],
            "Close": [100.5 + idx for idx in range(count)],
            "Adj Close": [100.5 + idx for idx in range(count)],
            "Volume": [1_000_000 + idx for idx in range(count)],
        },
        index=index,
    )


class AdHocScreenServiceTests(unittest.TestCase):
    def test_run_prefetches_once_and_evaluates_selected_screeners(self) -> None:
        ticker_frame = _frame("2026-01-01", 40)
        benchmark_frame = _frame("2026-01-01", 40)
        captured_bundles: list[tuple[str, int]] = []

        def _evaluator(bundle):
            captured_bundles.append((bundle.ticker, len(bundle.bars)))
            return ScreenerEvaluationResult(
                passed=True,
                metrics={"close": float(bundle.bars["Close"].iloc[-1])},
                reasons=("ok",),
                hit={"ticker": bundle.ticker, "signal": "demo"},
            )

        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        service.catalog = {
            "demo": ScreenerSpec(
                id="demo",
                required_inputs=("daily_bars", "benchmark_bars", "metadata"),
                lookback_trading_days=25,
                warmup_trading_days=5,
                evaluator=_evaluator,
            )
        }

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ) as load_windows, patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology"}},
        ):
            payload = service.run(
                ticker="aapl",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["demo"],
            )

        self.assertEqual(captured_bundles, [("AAPL", 40)])
        self.assertEqual(payload["ticker"], "AAPL")
        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["timing"]["market_data_tickers_loaded"], ["AAPL", "SPY"])
        load_windows.assert_called_once()

    def test_run_rejects_unknown_screener(self) -> None:
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        service.catalog = {}

        with self.assertRaisesRegex(ValueError, "Unknown screener id"):
            service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["missing"],
            )
