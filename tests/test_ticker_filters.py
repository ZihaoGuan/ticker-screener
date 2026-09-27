from __future__ import annotations

from dataclasses import dataclass
import unittest

from src.ticker_filters import (
    filter_earnings_events,
    filter_pre_earnings_events,
    filter_symbols,
    filter_universe_tickers,
    is_leveraged_or_inverse_fund_name,
    leveraged_or_inverse_catalog_symbols,
)


@dataclass(frozen=True)
class _UniverseTicker:
    symbol: str


@dataclass(frozen=True)
class _EarningsEvent:
    ticker: str


@dataclass(frozen=True)
class _PreEarningsEvent:
    ticker: str


class TickerFiltersTests(unittest.TestCase):
    def test_filter_symbols_skips_special_suffix_tickers(self) -> None:
        filtered = filter_symbols(
            ["AAPL", "ABCDW", "ABCDU", "ABCWS", "BRK.A", "ABCUW", "ABCU"],
            excluded=set(),
        )

        self.assertEqual(filtered, ["AAPL", "BRK.A", "ABCU"])

    def test_filter_universe_tickers_skips_special_suffix_tickers(self) -> None:
        filtered = filter_universe_tickers(
            [
                _UniverseTicker(symbol="AAPL"),
                _UniverseTicker(symbol="ABCDW"),
                _UniverseTicker(symbol="ABCDU"),
                _UniverseTicker(symbol="ABCWS"),
                _UniverseTicker(symbol="TSLA"),
            ],
            excluded=set(),
        )

        self.assertEqual([item.symbol for item in filtered], ["AAPL", "TSLA"])

    def test_event_filters_skip_special_suffix_tickers(self) -> None:
        earnings = filter_earnings_events(
            [_EarningsEvent(ticker="AAPL"), _EarningsEvent(ticker="ABCDW"), _EarningsEvent(ticker="TSLA")],
            excluded=set(),
        )
        pre_earnings = filter_pre_earnings_events(
            [_PreEarningsEvent(ticker="AAPL"), _PreEarningsEvent(ticker="ABCWS"), _PreEarningsEvent(ticker="TSLA")],
            excluded=set(),
        )

        self.assertEqual([item.ticker for item in earnings], ["AAPL", "TSLA"])
        self.assertEqual([item.ticker for item in pre_earnings], ["AAPL", "TSLA"])

    def test_detects_leveraged_or_inverse_fund_names_without_matching_common_stocks(self) -> None:
        self.assertTrue(is_leveraged_or_inverse_fund_name("Direxion Daily MU Bull 2X Shares"))
        self.assertTrue(is_leveraged_or_inverse_fund_name("GraniteShares 2x Long MU Daily ETF"))
        self.assertTrue(is_leveraged_or_inverse_fund_name("ProShares UltraPro Short QQQ"))
        self.assertFalse(is_leveraged_or_inverse_fund_name("SPDR S&P 500 ETF Trust"))
        self.assertFalse(is_leveraged_or_inverse_fund_name("Ultra Clean Holdings, Inc."))

    def test_extracts_only_leveraged_or_inverse_catalog_symbols(self) -> None:
        symbols = leveraged_or_inverse_catalog_symbols(
            [
                {"ticker": "SOXL", "name": "Direxion Daily Semiconductor Bull 3X ETF"},
                {"ticker": "SPY", "name": "SPDR S&P 500 ETF Trust"},
            ]
        )

        self.assertEqual(symbols, {"SOXL"})


if __name__ == "__main__":
    unittest.main()
