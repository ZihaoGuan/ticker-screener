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


def _fearzone_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    close = [90.0 + (idx * 0.16) for idx in range(240)]
    close.extend(
        [
            129.0,
            129.6,
            128.8,
            130.0,
            131.2,
            132.0,
            131.0,
            132.4,
            131.6,
            132.2,
            131.8,
            132.0,
            131.4,
            119.0,
            121.4,
            123.7,
            124.9,
            125.8,
            126.7,
            127.5,
        ]
    )
    open_values = [value * 1.003 for value in close]
    high = [max(op, cl) + 1.0 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 1.0 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_100_000.0 for _ in close]
    signal_index = len(close) - 5
    open_values[signal_index] = 126.5
    close[signal_index] = 119.0
    high[signal_index] = 127.0
    low[signal_index] = 118.2
    volume[signal_index] = 1_900_000.0
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


def _fearzone_zeiierman_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-01", periods=220, freq="B")
    close = [100.0 + ((80.0 / 219.0) * idx) for idx in range(220)]
    drop_offsets = [0.0, 6.1, 12.2, 18.3, 24.4, 30.5, 36.6, 42.7, 48.8, 55.0]
    for position, offset in enumerate(drop_offsets, start=len(close) - len(drop_offsets)):
        close[position] -= offset
    open_values = [value * 1.002 for value in close]
    high = [max(op, cl) + 1.0 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 1.0 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_250_000.0 for _ in close]
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


def _rs_rating_stock_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    close = 50.0 + (150.0 * (pd.Series(range(320), index=index) / 319.0) ** 1.2)
    return pd.DataFrame(
        {
            "Open": close * 0.998,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_250_000.0,
        },
        index=index,
    )


def _rs_rating_benchmark_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    close = pd.Series([100.0 + ((5.0 / 319.0) * idx) for idx in range(320)], index=index)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close,
            "Low": close,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_000_000.0,
        },
        index=index,
    )


def _hve_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=320, freq="B")
    open_values = [100.0 + (idx * 0.2) for idx in range(320)]
    close = [100.4 + (idx * 0.2) for idx in range(320)]
    high = [max(op, cl) + 0.8 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.7 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 + (idx * 2_500.0) for idx in range(320)]
    signal_index = len(index) - 1
    open_values[signal_index] = 163.0
    close[signal_index] = 170.0
    high[signal_index] = 171.2
    low[signal_index] = 162.1
    volume[signal_index] = 4_800_000.0
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


def _inside_dryup_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high: list[float] = []
    low: list[float] = []
    close: list[float] = []
    volume: list[float] = []

    price = 100.0
    for idx in range(250):
        price += 0.35
        open_price = price - 0.3
        close_price = price
        high_price = close_price + 0.8
        low_price = open_price - 0.7
        open_values.append(open_price)
        high.append(high_price)
        low.append(low_price)
        close.append(close_price)
        volume.append(1_250_000.0 + (idx * 1_000.0))

    tail = [
        (186.2, 186.8, 184.8, 185.3, 760_000.0),
        (185.1, 185.5, 183.9, 184.4, 700_000.0),
        (184.3, 184.8, 183.2, 183.8, 660_000.0),
        (183.6, 184.1, 182.9, 183.3, 610_000.0),
        (183.2, 183.8, 183.05, 183.4, 540_000.0),
        (183.35, 183.7, 183.2, 183.45, 500_000.0),
        (183.42, 183.62, 183.31, 183.5, 460_000.0),
        (183.46, 183.58, 183.36, 183.49, 430_000.0),
        (183.48, 183.55, 183.4, 183.5, 410_000.0),
        (183.49, 183.53, 183.43, 183.48, 390_000.0),
    ]
    for row in tail:
        open_price, high_price, low_price, close_price, volume_value = row
        open_values.append(open_price)
        high.append(high_price)
        low.append(low_price)
        close.append(close_price)
        volume.append(volume_value)

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

    def test_run_supports_fearzone_catalog_entry(self) -> None:
        ticker_frame = _fearzone_frame()
        benchmark_frame = _frame("2025-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 30),
                screener_ids=["fearzone"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "fearzone")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_fearzone_zeiierman_catalog_entry(self) -> None:
        ticker_frame = _fearzone_zeiierman_frame()
        benchmark_frame = _frame("2025-01-01", 220)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 11, 4),
                screener_ids=["fearzone_zeiierman"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "fearzone_zeiierman")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_hve_catalog_entry(self) -> None:
        ticker_frame = _hve_frame()
        benchmark_frame = _frame("2025-01-02", 320)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 3, 25),
                screener_ids=["hve"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "hve")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_rs_rating_catalog_entry(self) -> None:
        ticker_frame = _rs_rating_stock_frame()
        benchmark_frame = _rs_rating_benchmark_frame()
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 3, 21),
                screener_ids=["rs_rating"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "rs_rating")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_inside_dryup_catalog_entry(self) -> None:
        ticker_frame = _inside_dryup_frame()
        benchmark_frame = _frame("2025-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 31),
                screener_ids=["inside_dryup"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "inside_dryup")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_falls_back_to_internet_when_benchmark_missing_in_db(self) -> None:
        ticker_frame = _frame("2026-01-01", 40)
        benchmark_frame = _frame("2026-01-01", 40)

        def _evaluator(bundle):
            return ScreenerEvaluationResult(
                passed=True,
                metrics={"benchmark_close": float(bundle.benchmark_bars["Close"].iloc[-1])},
                reasons=("ok",),
                hit={"ticker": bundle.ticker},
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
            return_value={"AAPL": ticker_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology"}},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service._download_history_frame",
            return_value=benchmark_frame,
        ) as download_history:
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["demo"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "demo")
        self.assertTrue(payload["screeners"][0]["passed"])
        download_history.assert_called_once()
