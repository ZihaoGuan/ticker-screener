from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

from src.candidate_universe import select_candidates


class CandidateUniverseTests(unittest.TestCase):
    def test_explicit_tickers_bypass_snapshot_filtering(self) -> None:
        result = select_candidates("trend_template", dt.date(2026, 10, 16), ["abc", "def"], explicit_tickers=True)
        self.assertEqual(result.tickers, ["ABC", "DEF"])
        self.assertEqual(result.fallback_reason, "explicit_tickers_bypass")

    @patch("src.candidate_universe._connect")
    def test_weinstein_uses_exhaustive_universe_without_connecting_to_snapshot(self, connect) -> None:
        eligible = ["AAA", "BBB", "CCC"]
        result = select_candidates(
            "weinstein_stage2_early",
            dt.date(2026, 10, 16),
            eligible,
            limit=2,
        )
        self.assertEqual(result.tickers, eligible[:2])
        self.assertEqual(result.eligible_count, len(eligible))
        self.assertEqual(result.fallback_reason, "no_safe_prefilter")
        connect.assert_not_called()

    @patch("src.candidate_universe._connect")
    def test_stale_or_low_coverage_snapshot_falls_back_to_full_universe(self, connect) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [("ABC", 10, 40, 5, 4, 3, 20, 19)]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connect.return_value = connection
        eligible = [f"T{i}" for i in range(10)]
        result = select_candidates("one_year_winners", dt.date(2026, 10, 16), eligible)
        self.assertEqual(result.tickers, eligible)
        self.assertEqual(result.fallback_reason, "stale_or_incomplete_snapshot")

    @patch("src.candidate_universe._connect")
    def test_safe_necessary_condition_filters_and_limit_runs_after_filter(self, connect) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("AAA", 30, 40, 10, 8, 6, 20, 19),
            ("BBB", 15, 50, 10, 8, 6, 20, 19),
            ("CCC", 25, 20, 10, 8, 6, 20, 19),
        ]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connect.return_value = connection
        result = select_candidates("one_year_winners", dt.date(2026, 10, 16), ["AAA", "BBB", "CCC"], limit=1)
        self.assertEqual(result.tickers, ["AAA"])
        self.assertEqual(result.eligible_count, 3)
        self.assertIsNone(result.fallback_reason)

    @patch("src.candidate_universe._connect")
    def test_pilot_necessary_conditions_keep_a_valid_candidate(self, connect) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = [("AAA", 150, 40, 120, 110, 100, 130, 125)]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        connect.return_value = connection
        for strategy in ("trend_template", "weinstein_stage2_early"):
            with self.subTest(strategy=strategy):
                result = select_candidates(strategy, dt.date(2026, 10, 16), ["AAA"])
                self.assertEqual(result.tickers, ["AAA"])


if __name__ == "__main__":
    unittest.main()
