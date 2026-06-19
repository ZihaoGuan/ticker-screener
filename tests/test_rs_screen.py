from __future__ import annotations

import contextlib
import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.rs_screen import _compute_rs_new_high_flags, run_rs_screen
from src.universe import UniverseTicker


def _rows_from_close_series(closes: list[float], start: str = "2024-01-02") -> list[dict[str, object]]:
    index = dt.date.fromisoformat(start)
    rows: list[dict[str, object]] = []
    cursor = index
    generated = 0
    while generated < len(closes):
        if cursor.weekday() < 5:
            close = float(closes[generated])
            rows.append(
                {
                    "formatted_date": cursor.isoformat(),
                    "open": close - 0.2,
                    "high": close + 0.8,
                    "low": close - 0.8,
                    "close": close,
                    "volume": 1_000_000.0,
                }
            )
            generated += 1
        cursor += dt.timedelta(days=1)
    return rows


class _FakeFinancials:
    def __init__(self, stock_rows: list[dict[str, object]], benchmark_rows: list[dict[str, object]]) -> None:
        self.priceData = {"AAPL": {"prices": stock_rows}}
        self._stock_rows = stock_rows
        self._benchmark_rows = benchmark_rows

    def _get_clean_price_data(self):
        return self._stock_rows

    def _get_benchmark_price_data(self, _benchmark_ticker=None):
        return self._benchmark_rows

    def get_rs_new_high_before_price_summary(self, sectorName=None, benchmarkTicker=None, signalProfile="daily"):
        latest = self._stock_rows[-1]
        return {
            "signal_date": latest["formatted_date"],
            "benchmark_ticker": benchmarkTicker or "SPY",
            "current_price": latest["close"],
            "current_high": latest["high"],
            "current_rs_line": 1.25,
            "daily_rs_line_high": 1.25,
            "daily_price_high": latest["high"] + 5.0,
            "daily_lookback_days": 250,
            "weekly_lookback_weeks": 52,
            "daily_rs_new_high": True,
            "daily_rs_new_high_before_price": True,
            "weekly_rs_new_high": True,
            "weekly_rs_new_high_before_price": True,
            "weekly_rs_new_high_recent": True,
            "weekly_signal_weeks_ago": 0,
            "weekly_recent_signal_weeks": 4,
            "require_before_price": True,
            "is_near_year_high": True,
            "year_high": latest["high"] + 5.0,
            "distance_from_year_high_pct": 0.02,
            "is_strong_rs": True,
            "stock_return_vs_rs_window_pct": 35.0,
            "benchmark_return_vs_rs_window_pct": 12.0,
            "rs_line_high": 1.25,
            "is_sector_etf_strong": False,
            "sector_etf": "XLK",
            "sector_etf_near_year_high": False,
            "sector_etf_distance_from_year_high_pct": "n/a",
            "sector_etf_return_vs_rs_window_pct": "n/a",
            "sector_benchmark_return_vs_rs_window_pct": "n/a",
            "reasons": ["daily RS new high before price"],
        }


class _FakeCookstockModule:
    def __init__(self, stock_rows: list[dict[str, object]], benchmark_rows: list[dict[str, object]]) -> None:
        self._stock_rows = stock_rows
        self._benchmark_rows = benchmark_rows

    def cookFinancials(self, ticker, benchmarkTicker=None, historyLookbackDays=365):
        return _FakeFinancials(self._stock_rows, self._benchmark_rows)


class RsScreenTests(unittest.TestCase):
    def test_compute_rs_new_high_flags_matches_pine_before_price_rule(self) -> None:
        index = pd.date_range("2026-01-05", periods=5, freq="B")
        rs_line = pd.Series([1.00, 1.05, 1.08, 1.12, 1.20], index=index)
        price_high = pd.Series([10.0, 10.4, 10.8, 11.5, 11.1], index=index)

        new_high, before_price = _compute_rs_new_high_flags(rs_line, price_high, lookback=5)

        self.assertTrue(bool(new_high.iloc[-1]))
        self.assertTrue(bool(before_price.iloc[-1]))

    def test_compute_rs_new_high_flags_rejects_when_price_also_sets_high(self) -> None:
        index = pd.date_range("2026-01-05", periods=5, freq="B")
        rs_line = pd.Series([1.00, 1.05, 1.08, 1.12, 1.20], index=index)
        price_high = pd.Series([10.0, 10.4, 10.8, 11.5, 11.8], index=index)

        new_high, before_price = _compute_rs_new_high_flags(rs_line, price_high, lookback=5)

        self.assertTrue(bool(new_high.iloc[-1]))
        self.assertFalse(bool(before_price.iloc[-1]))

    def test_run_rs_screen_uses_clean_stock_rows_for_rs_rating(self) -> None:
        stock_closes = [100.0 + (idx * 0.9) for idx in range(320)]
        benchmark_closes = [100.0 + (idx * 0.2) for idx in range(320)]
        stock_rows = _rows_from_close_series(stock_closes)
        benchmark_rows = _rows_from_close_series(benchmark_closes)
        fake_module = _FakeCookstockModule(stock_rows, benchmark_rows)

        with patch("src.rs_screen.load_configured_cookstock", return_value=fake_module), patch(
            "src.rs_screen.freeze_cookstock_today",
            side_effect=lambda _module, _as_of_date: contextlib.nullcontext(),
        ), patch(
            "src.rs_screen.iter_prefetched_cookstock_batches",
            return_value=[[UniverseTicker(symbol="AAPL", sector="Technology", exchange="NASDAQ")]],
        ):
            result = run_rs_screen(
                AppConfig(),
                [UniverseTicker(symbol="AAPL", sector="Technology", exchange="NASDAQ")],
                as_of_date=dt.date(2026, 6, 8),
            )

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(len(result.hits), 1)
        self.assertGreaterEqual(result.hits[0].rs_rating, 0.0)
        self.assertLessEqual(result.hits[0].rs_rating, 99.0)


if __name__ == "__main__":
    unittest.main()
