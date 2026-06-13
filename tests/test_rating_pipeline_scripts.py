from __future__ import annotations

import datetime as dt
from argparse import Namespace
import unittest
from unittest.mock import MagicMock, patch

from src.ratings.models import FundamentalsSnapshot


class RatingPipelineScriptTests(unittest.TestCase):
    def test_build_sector_rating_baselines_respects_include_sectors(self) -> None:
        import scripts.build_sector_rating_baselines as script

        repository = MagicMock()
        repository.load_fundamentals_for_date.return_value = []
        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(as_of_date="2026-06-13", include_sectors=["Technology"], database_url="postgres://example"),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(script, "build_sector_baselines", return_value=[]):
            result = script.main()

        self.assertEqual(result, 0)
        repository.load_fundamentals_for_date.assert_called_once_with(dt.date(2026, 6, 13), sectors=("Technology",))
        repository.replace_sector_metric_baselines.assert_called_once_with(dt.date(2026, 6, 13), [], sectors=("Technology",))

    def test_build_ticker_ratings_respects_include_sectors_and_ticker_replace_scope(self) -> None:
        import scripts.build_ticker_ratings as script

        snapshot = FundamentalsSnapshot(ticker="ARM", as_of_date=dt.date(2026, 6, 13), sector="Technology", industry="Semiconductors")
        rating = MagicMock(rating_status="ok")
        repository = MagicMock()
        repository.load_fundamentals_for_date.return_value = [snapshot]
        repository.load_sector_baselines_for_date.return_value = {"Technology": {}}
        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(
                as_of_date="2026-06-13",
                include_sectors=["Technology"],
                min_sector_peers=20,
                min_category_metrics=1.0,
                database_url="postgres://example",
            ),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(script, "build_ticker_rating", return_value=rating):
            result = script.main()

        self.assertEqual(result, 0)
        repository.load_fundamentals_for_date.assert_called_once_with(dt.date(2026, 6, 13), sectors=("Technology",))
        repository.load_sector_baselines_for_date.assert_called_once_with(dt.date(2026, 6, 13), sectors=("Technology",))
        repository.replace_rating_snapshots.assert_called_once_with(
            dt.date(2026, 6, 13),
            [rating],
            tickers=["ARM"],
        )

    def test_sync_finviz_fundamentals_filters_universe_before_limit(self) -> None:
        import scripts.sync_finviz_fundamentals as script

        with patch.object(script, "load_app_config", return_value=object()), patch.object(
            script,
            "load_universe",
            return_value=[
                script.UniverseTicker(symbol="AAPL", sector="Technology"),
                script.UniverseTicker(symbol="JPM", sector="Finance"),
                script.UniverseTicker(symbol="MSFT", sector="Technology"),
            ],
        ):
            universe = script._load_target_universe(
                Namespace(
                    tickers=None,
                    config="",
                    limit=1,
                    include_sectors=["Technology"],
                )
            )

        self.assertEqual([item.symbol for item in universe], ["AAPL"])


if __name__ == "__main__":
    unittest.main()
