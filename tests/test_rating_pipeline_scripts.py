from __future__ import annotations

import datetime as dt
from argparse import Namespace
import json
from pathlib import Path
import tempfile
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

    def test_build_technical_ratings_respects_scope_and_replace_scope(self) -> None:
        import scripts.build_technical_ratings as script

        technical_rating = MagicMock(technical_status="ok")
        repository = MagicMock()
        repository.list_active_tickers.return_value = ["ARM"]
        benchmark_frame = MagicMock(empty=False)
        repository.replace_technical_rating_snapshots.return_value = 1
        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(
                as_of_date="2026-06-13",
                tickers=["ARM"],
                include_sectors=["Technology"],
                limit=10,
                benchmark_ticker="SPY",
                database_url="postgres://example",
            ),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example", benchmark_ticker="SPY")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(
            script,
            "load_many_ticker_windows",
            return_value={"ARM": MagicMock(), "SPY": benchmark_frame},
        ), patch.object(
            script,
            "_build_technical_snapshot_input",
            return_value=MagicMock(),
        ), patch.object(
            script,
            "build_technical_rating",
            return_value=technical_rating,
        ):
            result = script.main()

        self.assertEqual(result, 0)
        repository.list_active_tickers.assert_called_once_with(
            tickers=("ARM",),
            sectors=("Technology",),
            limit=10,
        )
        repository.replace_technical_rating_snapshots.assert_called_once_with(
            dt.date(2026, 6, 13),
            [technical_rating],
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

    def test_sync_postgres_market_data_default_universe_also_includes_rotation_etfs(self) -> None:
        import scripts.sync_postgres_market_data as script

        with patch.object(script, "load_app_config", return_value=object()), patch.object(
            script,
            "load_excluded_tickers",
            return_value=set(),
        ), patch(
            "src.universe.load_universe",
            return_value=[script.UniverseTicker(symbol="AAPL", sector="Technology")],
        ), patch.object(
            script,
            "build_theme_universe",
            return_value=[("AI Theme", "CHAT")],
        ):
            _config, universe = script._load_target_universe(
                Namespace(
                    tickers=None,
                    config="",
                    limit=None,
                    include_excluded_tickers=False,
                    rotation_only=False,
                )
            )

        symbols = [item.symbol for item in universe]
        self.assertIn("AAPL", symbols)
        self.assertIn("XLK", symbols)
        self.assertIn("SOXX", symbols)
        self.assertIn("CHAT", symbols)

    def test_sync_postgres_market_data_rotation_only_targets_rotation_universe(self) -> None:
        import scripts.sync_postgres_market_data as script

        with patch.object(script, "load_app_config", return_value=object()), patch.object(
            script,
            "load_excluded_tickers",
            return_value={"XLK"},
        ), patch.object(
            script,
            "build_theme_universe",
            return_value=[("AI Theme", "CHAT")],
        ):
            _config, universe = script._load_target_universe(
                Namespace(
                    tickers=None,
                    config="",
                    limit=3,
                    include_excluded_tickers=False,
                    rotation_only=True,
                )
            )

        self.assertEqual([item.symbol for item in universe], ["XLC", "XLY", "XLP"])

    def test_sync_finviz_fundamentals_does_not_skip_existing_failed_snapshot(self) -> None:
        import scripts.sync_finviz_fundamentals as script

        repository = MagicMock()
        repository.load_latest_fundamentals_statuses.return_value = {
            "AAPL": {"as_of_date": dt.date(2026, 6, 13), "parse_status": "scrape_failed"}
        }
        snapshot = FundamentalsSnapshot(
            ticker="AAPL",
            as_of_date=dt.date(2026, 6, 13),
            sector="Technology",
            industry="Consumer Electronics",
            parse_status="ok",
        )
        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(
                config="",
                as_of_date="2026-06-13",
                limit=None,
                tickers=["AAPL"],
                resume_from="",
                delay_min_seconds=0.0,
                delay_max_seconds=0.0,
                batch_size_before_rest=500,
                rest_seconds=0.0,
                overwrite_policy="skip-existing",
                include_sectors=None,
                database_url="postgres://example",
                manifest_path="",
                retry_failed_from_manifest=False,
                circuit_breaker_consecutive_503=25,
            ),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(
            script,
            "_load_target_universe",
            return_value=[script.UniverseTicker(symbol="AAPL", sector="Technology", industry="Consumer Electronics")],
        ), patch.object(
            script,
            "fetch_finviz_api_snapshot",
            return_value=snapshot,
        ), patch.object(script, "snapshot_needs_fallback", return_value=False), patch.object(
            script, "_write_manifest"
        ), patch.object(script, "_sleep_with_jitter"):
            result = script.main()

        self.assertEqual(result, 0)
        repository.upsert_fundamentals_snapshots.assert_called_once()

    def test_sync_finviz_fundamentals_skips_existing_ok_snapshot(self) -> None:
        import scripts.sync_finviz_fundamentals as script

        repository = MagicMock()
        repository.load_latest_fundamentals_statuses.return_value = {
            "AAPL": {"as_of_date": dt.date(2026, 6, 13), "parse_status": "ok"}
        }
        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(
                config="",
                as_of_date="2026-06-13",
                limit=None,
                tickers=["AAPL"],
                resume_from="",
                delay_min_seconds=0.0,
                delay_max_seconds=0.0,
                batch_size_before_rest=500,
                rest_seconds=0.0,
                overwrite_policy="skip-existing",
                include_sectors=None,
                database_url="postgres://example",
                manifest_path="",
                retry_failed_from_manifest=False,
                circuit_breaker_consecutive_503=25,
            ),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(
            script,
            "_load_target_universe",
            return_value=[script.UniverseTicker(symbol="AAPL", sector="Technology", industry="Consumer Electronics")],
        ), patch.object(script, "_write_manifest"):
            result = script.main()

        self.assertEqual(result, 0)
        repository.upsert_fundamentals_snapshots.assert_not_called()

    def test_sync_finviz_fundamentals_retry_failed_from_manifest_uses_failed_and_blocked_tickers(self) -> None:
        import scripts.sync_finviz_fundamentals as script

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "failed_tickers": [{"ticker": "AAPL", "reason": "Unexpected HTTP status: 503"}],
                        "blocked_tickers": ["MSFT"],
                    }
                ),
                encoding="utf-8",
            )
            repository = MagicMock()
            repository.load_latest_fundamentals_statuses.return_value = {}
            snapshot = FundamentalsSnapshot(
                ticker="AAPL",
                as_of_date=dt.date(2026, 6, 13),
                sector="Technology",
                industry="Consumer Electronics",
                parse_status="ok",
            )
            snapshot_msft = FundamentalsSnapshot(
                ticker="MSFT",
                as_of_date=dt.date(2026, 6, 13),
                sector="Technology",
                industry="Software",
                parse_status="ok",
            )
            snapshots = {"AAPL": snapshot, "MSFT": snapshot_msft}

            with patch.object(
                script,
                "parse_args",
                return_value=Namespace(
                    config="",
                    as_of_date="2026-06-13",
                    limit=None,
                    tickers=None,
                    resume_from="",
                    delay_min_seconds=0.0,
                    delay_max_seconds=0.0,
                    batch_size_before_rest=500,
                    rest_seconds=0.0,
                    overwrite_policy="skip-existing",
                    include_sectors=None,
                    database_url="postgres://example",
                    manifest_path=str(manifest_path),
                    retry_failed_from_manifest=True,
                    circuit_breaker_consecutive_503=25,
                ),
            ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
                script, "RatingsRepository", return_value=repository
            ), patch.object(
                script,
                "fetch_finviz_api_snapshot",
                side_effect=lambda ticker, **kwargs: snapshots[ticker],
            ), patch.object(script, "snapshot_needs_fallback", return_value=False), patch.object(
                script, "_write_manifest"
            ), patch.object(script, "_sleep_with_jitter"):
                result = script.main()

        self.assertEqual(result, 0)
        self.assertEqual(repository.upsert_fundamentals_snapshots.call_count, 2)

    def test_sync_finviz_fundamentals_stops_after_consecutive_503_threshold(self) -> None:
        import scripts.sync_finviz_fundamentals as script

        repository = MagicMock()
        repository.load_latest_fundamentals_statuses.return_value = {}
        fail_a = FundamentalsSnapshot(
            ticker="AAPL",
            as_of_date=dt.date(2026, 6, 13),
            sector=None,
            industry=None,
            parse_status="scrape_failed",
            parse_error="Unexpected HTTP status: 503",
        )
        fail_b = FundamentalsSnapshot(
            ticker="MSFT",
            as_of_date=dt.date(2026, 6, 13),
            sector=None,
            industry=None,
            parse_status="scrape_failed",
            parse_error="Unexpected HTTP status: 503",
        )
        later_ok = FundamentalsSnapshot(
            ticker="NVDA",
            as_of_date=dt.date(2026, 6, 13),
            sector="Technology",
            industry="Semiconductors",
            parse_status="ok",
        )
        snapshots = {"AAPL": fail_a, "MSFT": fail_b, "NVDA": later_ok}

        with patch.object(
            script,
            "parse_args",
            return_value=Namespace(
                config="",
                as_of_date="2026-06-13",
                limit=None,
                tickers=["AAPL", "MSFT", "NVDA"],
                resume_from="",
                delay_min_seconds=0.0,
                delay_max_seconds=0.0,
                batch_size_before_rest=500,
                rest_seconds=0.0,
                overwrite_policy="replace-date",
                include_sectors=None,
                database_url="postgres://example",
                manifest_path="",
                retry_failed_from_manifest=False,
                circuit_breaker_consecutive_503=2,
            ),
        ), patch.object(script, "load_webapp_config", return_value=Namespace(database_url="postgres://example")), patch.object(
            script, "RatingsRepository", return_value=repository
        ), patch.object(
            script,
            "fetch_finviz_api_snapshot",
            side_effect=lambda ticker, **kwargs: snapshots[ticker],
        ), patch.object(script, "snapshot_needs_fallback", return_value=False), patch.object(
            script, "_write_manifest"
        ) as write_manifest, patch.object(script, "_sleep_with_jitter"):
            result = script.main()

        self.assertEqual(result, 1)
        self.assertEqual(repository.upsert_fundamentals_snapshots.call_count, 2)
        self.assertTrue(write_manifest.called)


if __name__ == "__main__":
    unittest.main()
