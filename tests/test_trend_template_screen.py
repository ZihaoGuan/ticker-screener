from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.trend_template_screen import run_trend_template_screen
from src.universe import UniverseTicker


def _trend_template_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-02", periods=320, freq="B")
    close_values = [100.0 + (idx * 0.45) for idx in range(300)]
    close_values.extend([236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0])
    return pd.DataFrame(
        {
            "Open": [value - 1.0 for value in close_values],
            "High": [value + 1.5 for value in close_values],
            "Low": [value - 1.5 for value in close_values],
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": [1_400_000.0 for _ in close_values],
        },
        index=index,
    )


class TrendTemplateScreenTests(unittest.TestCase):
    def test_run_trend_template_screen_uses_db_frame_when_available(self) -> None:
        ticker = UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors", exchange="NASDAQ")
        frame = _trend_template_frame()
        as_of_date = frame.index[-1].date()

        with patch("src.trend_template_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.trend_template_screen.load_many_ticker_windows",
            return_value={"NVDA": frame},
        ), patch("src.trend_template_screen.load_configured_cookstock") as load_cookstock:
            result = run_trend_template_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertEqual(result.hits[0].criteria_passed, result.hits[0].criteria_total)
        self.assertLessEqual(result.hits[0].distance_from_52wk_high_pct, 25.0)
        self.assertGreaterEqual(result.hits[0].distance_from_52wk_low_pct, 25.0)
        load_cookstock.assert_not_called()

    def test_run_trend_template_screen_filters_failed_stack(self) -> None:
        ticker = UniverseTicker(symbol="TSLA", sector="Consumer", industry="Auto", exchange="NASDAQ")
        frame = _trend_template_frame().copy()
        as_of_date = frame.index[-1].date()
        frame.loc[frame.index[-10:], "Close"] = 150.0
        frame.loc[frame.index[-10:], "High"] = 151.0
        frame.loc[frame.index[-10:], "Low"] = 149.0

        with patch("src.trend_template_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.trend_template_screen.load_many_ticker_windows",
            return_value={"TSLA": frame},
        ):
            result = run_trend_template_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 0)
