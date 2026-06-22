from __future__ import annotations

import unittest

from src.canslim_screen import CanslimHit
from src.canslim_watchlist_builder import build_canslim_watchlist


def _hit(ticker: str, score: int, rank: int) -> CanslimHit:
    return CanslimHit(
        ticker=ticker,
        sector="Technology",
        industry="Software",
        exchange="NASDAQ",
        as_of_date="2026-06-22",
        score=score,
        max_score=14,
        rank=rank,
        letter_scores={"C": 2, "A": 2, "N": 1, "S": 1, "L": 1, "I": 1, "M": 2},
        letter_passes={"C": True, "A": True, "N": True, "S": True, "L": True, "I": True, "M": True},
        metrics={"close": 100.0, "leadership_score": 85.0, "distance_from_52w_high_pct": 4.0},
        reasons=["EPS Q/Q 40.0%", "Sales Q/Q 30.0%", "Leadership score 85.0"],
        leader_flags=["leader"],
    )


class CanslimWatchlistBuilderTests(unittest.TestCase):
    def test_build_watchlist_prefers_high_score_names(self) -> None:
        watchlist = build_canslim_watchlist([_hit("NVDA", 12, 1), _hit("PLTR", 9, 2), _hit("MSFT", 8, 3)])

        self.assertEqual([item["ticker"] for item in watchlist], ["NVDA", "PLTR", "MSFT"])
        self.assertEqual(watchlist[0]["score"], 12)
        self.assertIn("CANSLIM score", watchlist[0]["summary"])
        self.assertIn("letter_scores", watchlist[0])


if __name__ == "__main__":
    unittest.main()
