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


def _ftd_sweep_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-05", periods=80, freq="B")
    close = [100.0 - (idx * 0.18) for idx in range(80)]
    open_values = [value + 0.45 for value in close]
    high = [max(op, cl) + 0.55 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.55 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 for _ in close]

    overrides = {
        21: (90.5, 89.8, 91.0, 89.0, 1_050_000.0),
        22: (90.2, 91.0, 91.4, 89.5, 1_050_000.0),
        23: (91.1, 90.8, 91.6, 90.2, 1_000_000.0),
        24: (90.7, 90.2, 91.0, 89.8, 1_000_000.0),
        25: (90.0, 89.7, 90.3, 89.3, 1_000_000.0),
        26: (89.6, 89.3, 89.9, 88.9, 1_000_000.0),
        27: (89.1, 88.8, 89.4, 88.4, 1_000_000.0),
        28: (88.5, 88.2, 88.8, 87.8, 1_000_000.0),
        29: (88.1, 87.6, 88.5, 87.2, 1_000_000.0),
        30: (85.4, 83.8, 85.8, 82.8, 1_050_000.0),
        31: (84.2, 86.0, 86.5, 84.0, 1_000_000.0),
        32: (85.4, 89.4, 90.0, 85.2, 1_900_000.0),
        33: (89.2, 89.9, 90.3, 88.9, 1_250_000.0),
        34: (90.3, 89.6, 90.5, 89.2, 700_000.0),
        35: (89.9, 89.2, 90.1, 88.9, 650_000.0),
        36: (89.4, 88.8, 89.6, 88.5, 600_000.0),
        37: (89.0, 91.6, 92.4, 88.9, 1_400_000.0),
        38: (91.8, 91.0, 92.0, 90.6, 1_000_000.0),
        39: (91.2, 90.8, 91.4, 90.2, 950_000.0),
        40: (90.9, 93.2, 93.6, 90.7, 1_300_000.0),
        41: (93.0, 93.6, 94.0, 92.8, 1_100_000.0),
        42: (93.5, 93.9, 94.3, 93.3, 1_100_000.0),
    }
    for idx in range(43, len(close)):
        close[idx] = 94.0 + ((idx - 43) * 0.12)
        open_values[idx] = close[idx] - 0.3
        high[idx] = close[idx] + 0.45
        low[idx] = open_values[idx] - 0.35
        volume[idx] = 1_050_000.0

    for idx, row in overrides.items():
        open_values[idx], close[idx], high[idx], low[idx], volume[idx] = row

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
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

    def test_run_supports_ftd_sweep_catalog_entry(self) -> None:
        ticker_frame = _ftd_sweep_frame().iloc[:43]
        benchmark_frame = _frame("2026-01-01", 60)
        service = AdHocScreenService(
            app_config=AppConfig(
                ftd_sweep_history_days=90,
                ftd_sweep_min_avg_volume=0,
                ftd_sweep_min_avg_dollar_volume=0.0,
            ),
            database_url="postgres://unit-test",
        )

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 3, 4),
                screener_ids=["ftd_sweep"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "ftd_sweep")
        self.assertTrue(payload["screeners"][0]["passed"])
