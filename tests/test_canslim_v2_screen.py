from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.canslim_v2_screen import WATCHLIST_MIN_SCORE_EXCLUSIVE, run_canslim_v2_screen
from src.config import AppConfig
from src.universe import UniverseTicker


def _frame(*, close_start: float, close_end: float, volume: float = 1_500_000.0) -> pd.DataFrame:
    index = pd.date_range(end="2026-06-22", periods=320, freq="B")
    closes = [close_start + ((close_end - close_start) * idx / (len(index) - 1)) for idx in range(len(index))]
    return pd.DataFrame(
        {
            "Open": [value - 1.0 for value in closes],
            "High": [value + 1.5 for value in closes],
            "Low": [value - 1.5 for value in closes],
            "Close": closes,
            "Volume": [volume for _ in closes],
        },
        index=index,
    )


class CanslimV2ScreenTests(unittest.TestCase):
    def test_run_canslim_v2_scores_and_filters_watchlist_threshold(self) -> None:
        fundamentals = {
            "NVDA": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Semiconductors",
                "eps_qq_pct": 72.0,
                "sales_qq_pct": 44.0,
                "eps_this_y_pct": 58.0,
                "eps_next_5y_pct": 29.0,
                "roe_pct": 27.0,
                "institutional_ownership_pct": 68.0,
                "institutional_transactions_pct": 7.0,
                "insider_ownership_pct": 3.0,
                "insider_transactions_pct": 1.2,
                "shares_float": 420_000_000.0,
                "shares_outstanding": 430_000_000.0,
            },
            "PLTR": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Software",
                "eps_qq_pct": 26.0,
                "sales_qq_pct": 19.0,
                "eps_this_y_pct": 21.0,
                "eps_next_5y_pct": 18.0,
                "roe_pct": 16.0,
                "institutional_ownership_pct": 12.0,
                "institutional_transactions_pct": -3.0,
                "insider_ownership_pct": 0.3,
                "insider_transactions_pct": -0.8,
                "shares_float": 1_500_000_000.0,
                "shares_outstanding": 1_600_000_000.0,
            },
        }
        technical = {
            "NVDA": {"technical_status": "ok", "leadership_score": 94.0},
            "PLTR": {"technical_status": "ok", "leadership_score": 74.0},
        }
        frames = {
            "SPY": _frame(close_start=450.0, close_end=520.0, volume=8_000_000.0),
            "NVDA": _frame(close_start=90.0, close_end=198.0, volume=2_400_000.0),
            "PLTR": _frame(close_start=20.0, close_end=34.0, volume=1_800_000.0),
        }
        universe = [UniverseTicker(symbol="NVDA"), UniverseTicker(symbol="PLTR")]
        insider_signal_map = {
            "NVDA": {
                "buy_amount": 1_250_000.0,
                "discretionary_sell_amount": 0.0,
                "net_amount_excl_10b5_1": 1_250_000.0,
            },
            "PLTR": {
                "buy_amount": 0.0,
                "discretionary_sell_amount": 4_100_000.0,
                "net_amount_excl_10b5_1": -4_100_000.0,
            },
        }

        with patch("src.canslim_v2_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers", return_value=fundamentals), patch(
            "src.canslim_v2_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value=technical,
        ), patch("src.canslim_v2_screen.load_many_ticker_windows", return_value=frames), patch(
            "src.canslim_v2_screen.load_finviz_insider_signal_map",
            return_value=insider_signal_map,
        ):
            result = run_canslim_v2_screen(AppConfig(), universe, as_of_date=dt.date(2026, 6, 22), database_url="postgres://example")

        self.assertEqual(result.total_tickers, 2)
        self.assertEqual(result.passed_tickers, 2)
        self.assertEqual(result.watchlist_passed_tickers, 1)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertGreater(result.hits[0].composite_score, WATCHLIST_MIN_SCORE_EXCLUSIVE)
        self.assertLess(result.hits[1].composite_score, WATCHLIST_MIN_SCORE_EXCLUSIVE)
        self.assertEqual(result.hits[0].rating, "Exceptional+")
        self.assertEqual(result.hits[0].rank, 1)


if __name__ == "__main__":
    unittest.main()
