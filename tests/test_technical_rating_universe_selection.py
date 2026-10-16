from __future__ import annotations

import unittest

from scripts.build_technical_indicator_ratings import _select_target_tickers as select_indicator_tickers
from scripts.build_technical_ratings import _select_target_tickers as select_technical_tickers


class _Repository:
    def __init__(self, tickers: list[str]) -> None:
        self.tickers = tickers
        self.calls: list[dict[str, object]] = []

    def list_active_tickers(self, *, tickers=None, sectors=None, limit=None) -> list[str]:
        self.calls.append({"tickers": tickers, "sectors": sectors, "limit": limit})
        candidates = self.tickers
        if tickers:
            candidates = [ticker for ticker in candidates if ticker in tickers]
        return candidates


class TechnicalRatingUniverseSelectionTests(unittest.TestCase):
    def test_both_builders_filter_exclusions_before_applying_limit(self) -> None:
        for select_tickers in (select_technical_tickers, select_indicator_tickers):
            with self.subTest(builder=select_tickers.__module__):
                repository = _Repository(["AAA", "BAD", "ABCDR", "GOOD", "NEXT"])

                selected = select_tickers(
                    repository,
                    tickers=None,
                    sectors=("Technology",),
                    limit=2,
                    excluded={"BAD"},
                )

                self.assertEqual(selected, ["AAA", "GOOD"])
                self.assertEqual(repository.calls, [{"tickers": None, "sectors": ("Technology",), "limit": None}])

    def test_both_builders_preserve_explicit_ticker_selection_and_filter_it(self) -> None:
        for select_tickers in (select_technical_tickers, select_indicator_tickers):
            with self.subTest(builder=select_tickers.__module__):
                repository = _Repository(["AAA", "BAD", "ABCDR", "GOOD"])

                selected = select_tickers(
                    repository,
                    tickers=("AAA", "BAD", "ABCDR", "GOOD"),
                    sectors=None,
                    limit=None,
                    excluded={"BAD"},
                )

                self.assertEqual(selected, ["AAA", "GOOD"])
                self.assertEqual(
                    repository.calls,
                    [{"tickers": ("AAA", "BAD", "ABCDR", "GOOD"), "sectors": None, "limit": None}],
                )


if __name__ == "__main__":
    unittest.main()
