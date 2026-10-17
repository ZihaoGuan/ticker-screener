from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import MagicMock, patch

from src.daily_screening_features import build_daily_screening_features
from src.universe import UniverseTicker


class DailyScreeningFeatureBuilderTests(unittest.TestCase):
    trade_date = dt.date(2026, 10, 16)

    def _db_connection(self, stored_count: int = 10):
        cursor = MagicMock()
        cursor.fetchone.return_value = (stored_count,)
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        return connection, cursor

    def _patch_market_loaders(self, tickers: list[str]):
        universe = [UniverseTicker(symbol=ticker) for ticker in tickers]
        return (
            patch("src.daily_screening_features.load_active_universe_from_db", return_value=universe),
            patch(
                "src.daily_screening_features.load_many_ticker_windows",
                side_effect=lambda requested, *_args, **_kwargs: {ticker: object() for ticker in requested},
            ),
        )

    def test_shared_exclusions_are_applied_before_building_features(self) -> None:
        tickers = [f"T{i}" for i in range(10)]
        connection, cursor = self._db_connection(9)
        universe_patch, frame_patch = self._patch_market_loaders(tickers)
        with (
            patch("src.daily_screening_features._connect", return_value=connection),
            universe_patch,
            frame_patch as load_frames,
            patch("src.daily_screening_features.filter_universe_tickers", side_effect=lambda rows, excluded: rows[:-1]) as filter_rows,
            patch("src.daily_screening_features._feature_row", side_effect=lambda _frame, ticker, date: (date, ticker)),
        ):
            result = build_daily_screening_features(
                self.trade_date,
                database_url="postgres://test",
                excluded_tickers={"T9"},
            )

        filter_rows.assert_called_once()
        self.assertEqual(filter_rows.call_args.args[1], {"T9"})
        self.assertEqual(load_frames.call_args.args[0], tickers[:-1])
        self.assertEqual(result["eligible_tickers"], 9)
        self.assertEqual(result["feature_rows"], 9)
        self.assertEqual(cursor.executemany.call_args.args[1], [(self.trade_date, ticker) for ticker in tickers[:-1]])
        connection.commit.assert_called_once()

    def test_below_90_percent_coverage_keeps_prior_snapshot_without_delete(self) -> None:
        tickers = [f"T{i}" for i in range(10)]
        connection, cursor = self._db_connection()
        universe_patch, frame_patch = self._patch_market_loaders(tickers)
        with (
            patch("src.daily_screening_features._connect", return_value=connection),
            universe_patch,
            frame_patch,
            patch(
                "src.daily_screening_features._feature_row",
                side_effect=lambda _frame, ticker, date: (date, ticker) if ticker not in {"T8", "T9"} else None,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "below required 90.0%"):
                build_daily_screening_features(self.trade_date, excluded_tickers=set())

        connection.cursor.assert_not_called()
        connection.close.assert_called_once()
        cursor.execute.assert_not_called()
        cursor.executemany.assert_not_called()
        connection.commit.assert_not_called()

    def test_success_replaces_date_in_one_transaction_and_checks_row_count(self) -> None:
        tickers = [f"T{i}" for i in range(10)]
        connection, cursor = self._db_connection(10)
        universe_patch, frame_patch = self._patch_market_loaders(tickers)
        with (
            patch("src.daily_screening_features._connect", return_value=connection),
            universe_patch,
            frame_patch,
            patch("src.daily_screening_features._feature_row", side_effect=lambda _frame, ticker, date: (date, ticker)),
        ):
            result = build_daily_screening_features(self.trade_date, excluded_tickers=set())

        self.assertTrue(cursor.execute.call_args_list[0].args[0].startswith("DELETE FROM ticker_daily_screening_features"))
        self.assertEqual(cursor.executemany.call_count, 1)
        self.assertIn("SELECT COUNT(*)", cursor.execute.call_args_list[1].args[0])
        self.assertEqual(result["feature_rows"], 10)
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once()

    def test_insert_and_count_failures_roll_back_replacement(self) -> None:
        for failure in ("insert", "count"):
            with self.subTest(failure=failure):
                tickers = [f"T{i}" for i in range(10)]
                connection, cursor = self._db_connection(9 if failure == "count" else 10)
                if failure == "insert":
                    cursor.executemany.side_effect = RuntimeError("insert failed")
                else:
                    cursor.execute.side_effect = [None, RuntimeError("count failed")]
                universe_patch, frame_patch = self._patch_market_loaders(tickers)
                with (
                    patch("src.daily_screening_features._connect", return_value=connection),
                    universe_patch,
                    frame_patch,
                    patch("src.daily_screening_features._feature_row", side_effect=lambda _frame, ticker, date: (date, ticker)),
                ):
                    with self.assertRaisesRegex(RuntimeError, "failed"):
                        build_daily_screening_features(self.trade_date, excluded_tickers=set())

                connection.rollback.assert_called_once()
                connection.commit.assert_not_called()
                connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
