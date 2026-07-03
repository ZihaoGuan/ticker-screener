from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

from src.industry_group_rs_rank_screen import run_industry_group_rs_rank_screen


class IndustryGroupRsRankScreenTests(unittest.TestCase):
    def test_excludes_invalid_industry_group_labels(self) -> None:
        snapshots = {
            "GOOD": {
                "as_of_date": "2026-07-02",
                "sector": "Technology",
                "industry": "Software",
                "industry_group": "Software",
                "industry_group_rs_rank": 98.0,
                "industry_group_member_count": 12,
                "daily_rs_rating": 95.0,
                "leadership_score": 92.0,
                "overall_rating": 90.0,
                "rating_band": "elite",
            },
            "BADTIME": {
                "as_of_date": "2026-07-02",
                "sector": "(0.00%)",
                "industry": "04:00PM ET",
                "industry_group": "04:00PM ET",
                "industry_group_rs_rank": 99.0,
                "industry_group_member_count": 2,
                "daily_rs_rating": 99.0,
                "leadership_score": 95.0,
                "overall_rating": 91.0,
                "rating_band": "elite",
            },
            "BADBLANK": {
                "as_of_date": "2026-07-02",
                "sector": "Finance",
                "industry": "",
                "industry_group": "",
                "industry_group_rs_rank": 97.0,
                "industry_group_member_count": 5,
                "daily_rs_rating": 94.0,
                "leadership_score": 90.0,
                "overall_rating": 88.0,
                "rating_band": "elite",
            },
        }

        with patch("src.industry_group_rs_rank_screen.RatingsRepository") as repository_cls:
            repository = repository_cls.return_value
            repository.list_active_tickers.return_value = ["GOOD", "BADTIME", "BADBLANK"]
            repository.load_latest_technical_rating_snapshots_for_tickers.return_value = snapshots

            result = run_industry_group_rs_rank_screen(
                database_url="postgres://example",
                as_of_date=dt.date(2026, 7, 2),
            )

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual([hit.ticker for hit in result.hits], ["GOOD"])


if __name__ == "__main__":
    unittest.main()
