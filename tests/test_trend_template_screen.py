from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.trend_template_screen import evaluate_trend_template, run_trend_template_screen
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
        frame = _trend_template_frame().copy()
        as_of_date = frame.index[-1].date()
        for start, end, delta in ((131, 139, 20.0), (287, 299, -60.0), (281, 300, -50.0)):
            frame.loc[frame.index[start:end], "Close"] += delta
            frame.loc[frame.index[start:end], "High"] += delta
            frame.loc[frame.index[start:end], "Low"] += delta

        with patch("src.trend_template_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.trend_template_screen.load_many_ticker_windows",
            return_value={"NVDA": frame},
        ), patch("src.trend_template_screen.load_configured_cookstock") as load_cookstock:
            result = run_trend_template_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertEqual(result.hits[0].criteria_passed, result.hits[0].criteria_total)
        self.assertLessEqual(result.hits[0].distance_from_52wk_high_pct, 25.0)
        self.assertGreaterEqual(result.hits[0].distance_from_52wk_low_pct, 30.0)
        self.assertGreater(result.hits[0].rs_rating, 70.0)
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

    def test_evaluate_trend_template_uses_30pct_above_52_week_low_threshold(self) -> None:
        index = pd.date_range("2025-01-02", periods=320, freq="B")
        close_values = [182.0 + (idx * 0.15) for idx in range(300)]
        close_values.extend([236.0, 238.0, 237.0, 239.0, 238.5, 240.0, 239.5, 241.0, 240.5, 242.0, 241.5, 243.0, 242.5, 244.0, 243.5, 245.0, 244.5, 246.0, 245.5, 247.0])
        frame = pd.DataFrame(
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

        snapshot = evaluate_trend_template(frame)

        assert snapshot is not None
        self.assertGreaterEqual(snapshot.distance_from_52wk_low_pct, 30.0)
        self.assertLess(snapshot.distance_from_52wk_low_pct, 35.0)
        self.assertTrue(snapshot.criteria["price_30pct_above_52w_low"])
        self.assertFalse(snapshot.criteria["rs_rating_above_70"])

    def test_run_trend_template_screen_requires_rs_above_70(self) -> None:
        ticker = UniverseTicker(symbol="CRWD", sector="Technology", industry="Software", exchange="NASDAQ")
        frame = _trend_template_frame().copy()
        as_of_date = frame.index[-1].date()
        frame.loc[frame.index[275:], "Close"] = 223.75
        frame.loc[frame.index[275:], "High"] = 225.25
        frame.loc[frame.index[275:], "Low"] = 222.25

        snapshot = evaluate_trend_template(frame)
        assert snapshot is not None
        self.assertFalse(snapshot.matched)
        self.assertFalse(snapshot.criteria["rs_rating_above_70"])
        self.assertTrue(all(passed for key, passed in snapshot.criteria.items() if key != "rs_rating_above_70"))

        with patch("src.trend_template_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.trend_template_screen.load_many_ticker_windows",
            return_value={"CRWD": frame},
        ):
            result = run_trend_template_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 0)



