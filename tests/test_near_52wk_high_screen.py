from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.near_52wk_high_screen import evaluate_near_52wk_high_frame, run_near_52wk_high_screen
from src.universe import UniverseTicker


def _frame(*, latest_close: float) -> pd.DataFrame:
    dates = pd.bdate_range("2025-06-23", periods=252)
    rows: list[dict[str, object]] = []
    for index, day in enumerate(dates):
        high_value = 100.0
        close_value = 95.0
        if index == len(dates) - 1:
            close_value = latest_close
            high_value = max(100.0, latest_close + 1.0)
        rows.append(
            {
                "Date": day,
                "High": high_value,
                "Close": close_value,
                "Volume": 1_000_000 + index,
            }
        )
    return pd.DataFrame(rows).set_index("Date")


class Near52WeekHighScreenTests(unittest.TestCase):
    def test_evaluate_accepts_name_within_20pct_of_52_week_high(self) -> None:
        hit = evaluate_near_52wk_high_frame(
            _frame(latest_close=90.0),
            ticker=UniverseTicker(symbol="TEST", sector="Technology", industry="Software"),
            signal_date=dt.date(2026, 6, 12),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertAlmostEqual(hit.distance_from_52wk_high_pct, ((100.0 / 90.0) - 1.0) * 100.0)
        self.assertEqual(hit.signal_date, "2026-06-12")

    def test_evaluate_rejects_name_more_than_20pct_below_high(self) -> None:
        hit = evaluate_near_52wk_high_frame(
            _frame(latest_close=80.0),
            ticker=UniverseTicker(symbol="LAG"),
            signal_date=dt.date(2026, 6, 12),
        )

        self.assertIsNone(hit)

    def test_run_prefers_db_frame_when_available(self) -> None:
        ticker = UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors")
        as_of_date = dt.date(2026, 6, 12)
        frame = _frame(latest_close=90.0)

        with patch("src.near_52wk_high_screen.resolve_database_url", return_value="postgres://example"), patch(
            "src.near_52wk_high_screen.load_many_ticker_windows",
            return_value={"NVDA": frame.copy()},
        ), patch("src.near_52wk_high_screen.load_configured_cookstock") as load_cookstock:
            result = run_near_52wk_high_screen(AppConfig(), [ticker], as_of_date=as_of_date)

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        load_cookstock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
