from __future__ import annotations

from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.webapp.services.rrg_service import RrgService


def _rotation_frame(start: str, count: int, close_start: float = 100.0, close_step: float = 0.8) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=count, freq="B")
    close = [close_start + (idx * close_step) for idx in range(count)]
    open_values = [value - 0.2 for value in close]
    high = [value + 0.8 for value in close]
    low = [value - 1.1 for value in close]
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": [1_000_000.0 for _ in close],
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


def _mock_rotation_series(closes: pd.DataFrame) -> SimpleNamespace:
    trail_index = closes.index[-4:]
    trail = pd.DataFrame(
        {
            "x": [98.4, 99.7, 101.1, 102.3],
            "y": [97.6, 99.2, 100.7, 101.9],
        },
        index=trail_index,
    )
    return SimpleNamespace(trail=trail, quadrant="Leading", distance=3.21)


class RrgServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.service = RrgService(
            output_dir=Path(self.temp_dir.name),
            app_config=AppConfig(),
        )

    def test_get_universe_report_includes_fearzone_summary(self) -> None:
        benchmark_frame = _rotation_frame("2025-01-02", 260)
        ticker_frame = _fearzone_frame()

        def fake_fetch_history(ticker: str, period: str) -> pd.DataFrame:
            self.assertEqual(period, "3y")
            if ticker == "SPY":
                return benchmark_frame
            if ticker == "XLK":
                return ticker_frame
            raise AssertionError(f"unexpected ticker {ticker}")

        with patch.object(self.service, "_universe_entries", return_value=[("Technology", "XLK")]), patch(
            "src.webapp.services.rrg_service.fetch_history",
            side_effect=fake_fetch_history,
        ), patch(
            "src.webapp.services.rrg_service.compute_rotation_series",
            side_effect=lambda **kwargs: _mock_rotation_series(kwargs["closes"]),
        ):
            payload = self.service.get_universe_report(
                universe="sector",
                benchmark="SPY",
                period="3y",
                trail_weeks=12,
                cadence="weekly",
            )

        self.assertEqual(payload["meta"]["count"], 1)
        entry = payload["series"][0]
        self.assertEqual(entry["ticker"], "XLK")
        self.assertTrue(entry["fearzone"]["active"])
        self.assertIsNotNone(entry["fearzone"]["signal_date"])
        self.assertGreaterEqual(len(entry["fearzone"]["conditions"]), 6)
        above_ma200 = next(item for item in entry["fearzone"]["conditions"] if item["key"] == "above_ma200")
        self.assertTrue(above_ma200["active"])

    def test_daily_mode_refetches_longer_history_for_fearzone(self) -> None:
        benchmark_short = _rotation_frame("2026-03-02", 40)
        ticker_short = _rotation_frame("2026-03-02", 40, close_start=118.0, close_step=0.35)
        ticker_long = _fearzone_frame()
        calls: list[tuple[str, str]] = []

        def fake_fetch_history(ticker: str, period: str) -> pd.DataFrame:
            calls.append((ticker, period))
            if ticker == "SPY":
                return benchmark_short
            if ticker == "XLK" and period == "2mo":
                return ticker_short
            if ticker == "XLK" and period == "3y":
                return ticker_long
            raise AssertionError(f"unexpected request {(ticker, period)}")

        with patch.object(self.service, "_universe_entries", return_value=[("Technology", "XLK")]), patch(
            "src.webapp.services.rrg_service.fetch_history",
            side_effect=fake_fetch_history,
        ), patch(
            "src.webapp.services.rrg_service.compute_rotation_series",
            side_effect=lambda **kwargs: _mock_rotation_series(kwargs["closes"]),
        ):
            payload = self.service.get_universe_report(
                universe="sector",
                benchmark="SPY",
                period="3y",
                trail_weeks=12,
                cadence="daily-2m",
            )

        self.assertIn(("XLK", "3y"), calls)
        self.assertTrue(payload["series"][0]["fearzone"]["active"])

    def test_close_only_history_does_not_crash_fearzone_payload(self) -> None:
        benchmark_frame = _rotation_frame("2025-01-02", 260)
        ticker_frame = _fearzone_frame()[["Close", "Adj Close", "Volume"]].copy()

        def fake_fetch_history(ticker: str, period: str) -> pd.DataFrame:
            if ticker == "SPY":
                return benchmark_frame
            if ticker == "XLK":
                return ticker_frame
            raise AssertionError(f"unexpected ticker {ticker}")

        with patch.object(self.service, "_universe_entries", return_value=[("Technology", "XLK")]), patch(
            "src.webapp.services.rrg_service.fetch_history",
            side_effect=fake_fetch_history,
        ), patch(
            "src.webapp.services.rrg_service.compute_rotation_series",
            side_effect=lambda **kwargs: _mock_rotation_series(kwargs["closes"]),
        ):
            payload = self.service.get_universe_report(
                universe="sector",
                benchmark="SPY",
                period="3y",
                trail_weeks=12,
                cadence="weekly",
            )

        self.assertEqual(payload["series"][0]["ticker"], "XLK")
        self.assertIn("fearzone", payload["series"][0])


if __name__ == "__main__":
    unittest.main()
