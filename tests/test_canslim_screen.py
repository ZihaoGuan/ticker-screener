from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.canslim_screen import CANSLIM_MIN_SCORE, run_canslim_screen
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


def _accumulation_frame(*, close_start: float, close_end: float, volume: float) -> pd.DataFrame:
    frame = _frame(close_start=close_start, close_end=close_end, volume=volume)
    closes = frame["Close"].tolist()
    start = len(closes) - 20
    current = closes[start - 1]
    for offset in range(20):
        current += 2.0 if offset % 4 != 3 else -0.5
        closes[start + offset] = current
    frame["Close"] = closes
    frame["Open"] = frame["Close"] - 1.0
    frame["High"] = frame["Close"] + 1.5
    frame["Low"] = frame["Close"] - 1.5
    volumes = frame["Volume"].tolist()
    for offset in range(20):
        volumes[start + offset] = volume * (1.4 if offset % 4 != 3 else 0.4)
    frame["Volume"] = volumes
    return frame


class CanslimScreenTests(unittest.TestCase):
    def test_run_canslim_screen_ranks_high_score_names_and_skips_missing_data(self) -> None:
        fundamentals = {
            "NVDA": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Semiconductors",
                "eps_qq_pct": 62.0,
                "sales_qq_pct": 31.0,
                "eps_this_y_pct": 55.0,
                "eps_next_5y_pct": 24.0,
                "roe_pct": 22.0,
                "institutional_ownership_pct": 41.0,
                "institutional_transactions_pct": 6.0,
                "insider_ownership_pct": 2.5,
                "insider_transactions_pct": 0.8,
                "shares_float": 420_000_000.0,
                "shares_outstanding": 430_000_000.0,
            },
            "PLTR": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Software",
                "eps_qq_pct": 24.0,
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
            "NVDA": {"technical_status": "ok", "leadership_score": 91.0},
            "PLTR": {"technical_status": "ok", "leadership_score": 74.0},
        }
        frames = {
            "SPY": _frame(close_start=450.0, close_end=520.0, volume=8_000_000.0),
            "NVDA": _frame(close_start=90.0, close_end=198.0),
            "PLTR": _frame(close_start=20.0, close_end=34.0),
        }
        universe = [UniverseTicker(symbol="NVDA"), UniverseTicker(symbol="PLTR"), UniverseTicker(symbol="MSFT")]
        insider_signal_map = {
            "NVDA": {
                "buy_count": 2,
                "sell_count": 0,
                "buy_amount": 1_250_000.0,
                "sell_amount": 0.0,
                "discretionary_sell_count": 0,
                "discretionary_sell_amount": 0.0,
                "net_amount_excl_10b5_1": 1_250_000.0,
            },
            "PLTR": {
                "buy_count": 0,
                "sell_count": 3,
                "buy_amount": 0.0,
                "sell_amount": 4_100_000.0,
                "discretionary_sell_count": 3,
                "discretionary_sell_amount": 4_100_000.0,
                "net_amount_excl_10b5_1": -4_100_000.0,
            },
        }

        with patch("src.canslim_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers", return_value=fundamentals), patch(
            "src.canslim_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value=technical,
        ), patch("src.canslim_screen.load_many_ticker_windows", return_value=frames), patch(
            "src.canslim_screen.load_finviz_insider_signal_map",
            return_value=insider_signal_map,
        ):
            result = run_canslim_screen(AppConfig(), universe, as_of_date=dt.date(2026, 6, 22), database_url="postgres://example")

        self.assertEqual(result.total_tickers, 3)
        self.assertEqual(result.passed_tickers, 2)
        self.assertEqual(result.minimum_score, CANSLIM_MIN_SCORE)
        self.assertEqual(result.hits[0].ticker, "NVDA")
        self.assertGreater(result.hits[0].score, result.hits[1].score)
        self.assertEqual(result.hits[0].rank, 1)
        self.assertTrue(result.hits[0].letter_passes["M"])
        self.assertEqual(result.hits[0].letter_scores["S"], 2)
        self.assertEqual(result.hits[0].letter_scores["I"], 2)
        self.assertEqual(result.hits[1].letter_scores["S"], 0)
        self.assertEqual(result.hits[1].letter_scores["I"], 0)
        self.assertEqual(result.hits[0].metrics["insider_net_amount_excl_10b5_1"], 1_250_000.0)
        self.assertTrue(any(item["ticker"] == "MSFT" for item in result.failed_tickers))

    def test_run_canslim_screen_allows_older_technical_snapshot_lookup(self) -> None:
        fundamentals = {
            "NVDA": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Semiconductors",
                "eps_qq_pct": 62.0,
                "sales_qq_pct": 31.0,
                "eps_this_y_pct": 55.0,
                "eps_next_5y_pct": 24.0,
                "roe_pct": 22.0,
                "institutional_ownership_pct": 41.0,
                "institutional_transactions_pct": 6.0,
                "insider_ownership_pct": 2.5,
                "insider_transactions_pct": 0.8,
                "shares_float": 420_000_000.0,
                "shares_outstanding": 430_000_000.0,
            },
        }
        technical = {
            "NVDA": {"technical_status": "ok", "leadership_score": 91.0, "as_of_date": "2026-06-20"},
        }
        frames = {
            "SPY": _frame(close_start=450.0, close_end=520.0, volume=8_000_000.0),
            "NVDA": _frame(close_start=90.0, close_end=198.0),
        }
        universe = [UniverseTicker(symbol="NVDA")]

        with patch("src.canslim_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers", return_value=fundamentals), patch(
            "src.canslim_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value=technical,
        ) as technical_loader, patch("src.canslim_screen.load_many_ticker_windows", return_value=frames), patch(
            "src.canslim_screen.load_finviz_insider_signal_map",
            return_value={},
        ):
            result = run_canslim_screen(AppConfig(), universe, as_of_date=dt.date(2026, 6, 22), database_url="postgres://example")

        technical_loader.assert_called_once_with(
            ["NVDA"],
            as_of_date=dt.date(2026, 6, 22),
            allow_older_as_of_date=True,
        )
        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.failed_tickers, [])

    def test_run_canslim_screen_allows_mega_caps_to_earn_partial_s_score(self) -> None:
        fundamentals = {
            "NVDA": {
                "as_of_date": "2026-06-22",
                "parse_status": "ok",
                "sector": "Technology",
                "industry": "Semiconductors",
                "eps_qq_pct": 62.0,
                "sales_qq_pct": 31.0,
                "eps_this_y_pct": 55.0,
                "eps_next_5y_pct": 24.0,
                "roe_pct": 22.0,
                "institutional_ownership_pct": 70.28,
                "institutional_transactions_pct": 0.0,
                "insider_ownership_pct": 4.09,
                "insider_transactions_pct": -0.28,
                "shares_float": 23_200_000_000.0,
                "shares_outstanding": 24_220_000_000.0,
            },
        }
        technical = {
            "NVDA": {"technical_status": "ok", "leadership_score": 89.2},
        }
        frames = {
            "SPY": _frame(close_start=450.0, close_end=520.0, volume=8_000_000.0),
            "NVDA": _accumulation_frame(close_start=90.0, close_end=198.0, volume=54_018_362.0),
        }
        universe = [UniverseTicker(symbol="NVDA")]

        with patch("src.canslim_screen.RatingsRepository.load_latest_fundamentals_snapshots_for_tickers", return_value=fundamentals), patch(
            "src.canslim_screen.RatingsRepository.load_latest_technical_rating_snapshots_for_tickers",
            return_value=technical,
        ), patch("src.canslim_screen.load_many_ticker_windows", return_value=frames), patch(
            "src.canslim_screen.load_finviz_insider_signal_map",
            return_value={},
        ):
            result = run_canslim_screen(AppConfig(), universe, as_of_date=dt.date(2026, 6, 22), database_url="postgres://example")

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.hits[0].letter_scores["S"], 1)
        self.assertEqual(result.hits[0].letter_scores["I"], 1)


if __name__ == "__main__":
    unittest.main()
