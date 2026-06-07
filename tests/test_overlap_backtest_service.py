from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.webapp.services.overlap_backtest_service import OverlapBacktestService


class _FakeFrameIloc:
    def __init__(self, rows):
        self.rows = rows

    def __getitem__(self, index: int):
        return self.rows[index]


class _FakeFrame:
    def __init__(self, dates: list[dt.date], closes: list[float]) -> None:
        self.index = [dt.datetime.combine(item, dt.time(0, 0)) for item in dates]
        self._rows = [{"Close": value} for value in closes]
        self.iloc = _FakeFrameIloc(self._rows)
        self.empty = False


class _FakeRepository:
    def __init__(self) -> None:
        self.database_url = "postgresql://example"
        self.overlap_summary = None
        self.overlap_members = None
        self.backtest_summary = None
        self.backtest_trades = None

    def is_configured(self) -> bool:
        return True

    def load_cached_signals(self, **_: object):
        return [
            {"ticker": "AAPL", "strategy_id": "rs"},
            {"ticker": "AAPL", "strategy_id": "vcp"},
            {"ticker": "AAPL", "strategy_id": "gap_fill"},
            {"ticker": "AAPL", "strategy_id": "fearzone"},
            {"ticker": "MSFT", "strategy_id": "rs"},
            {"ticker": "MSFT", "strategy_id": "vcp"},
        ]

    def upsert_overlap_run(self, **kwargs: object):
        self.overlap_summary = kwargs
        return 17

    def replace_overlap_run_members(self, overlap_run_id: int | None, rows):
        self.overlap_members = {"overlap_run_id": overlap_run_id, "rows": rows}

    def list_signal_cache_calendar(self, **_: object):
        return [
            {"run_date": dt.date(2026, 1, 2), "strategy_id": "rs"},
            {"run_date": dt.date(2026, 1, 2), "strategy_id": "vcp"},
            {"run_date": dt.date(2026, 1, 2), "strategy_id": "gap_fill"},
        ]

    def list_overlap_runs(self, **_: object):
        return [
            {
                "id": 17,
                "run_date": dt.date(2026, 1, 2),
                "summary_json": {
                    "candidate_count": 1,
                    "overlap_two_plus_count": 2,
                    "overlap_three_plus_count": 1,
                    "overlap_four_plus_count": 1,
                },
                "created_at": "2026-01-02T22:00:00+00:00",
            }
        ]

    def list_overlap_run_members(self, **_: object):
        return [
            {
                "run_date": dt.date(2026, 1, 2),
                "ticker": "AAPL",
                "signal_count": 4,
                "contributing_strategies_json": ["rs", "vcp", "gap_fill", "fearzone"],
                "metadata_json": {"sector": "Tech"},
            }
        ]

    def create_backtest_run(self, **kwargs: object):
        self.backtest_summary = kwargs
        return 91

    def replace_backtest_run_trades(self, backtest_run_id: int | None, rows):
        self.backtest_trades = {"backtest_run_id": backtest_run_id, "rows": rows}

    def list_backtest_runs_v2(self, *, limit: int = 30):
        _ = limit
        return []

    def get_backtest_run_v2(self, run_id: int):
        _ = run_id
        return None


class OverlapBacktestServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.repository = _FakeRepository()
        self.service = OverlapBacktestService(
            database_url="postgresql://example",
            artifacts_dir=Path(self.tmpdir.name),
            repository=self.repository,  # type: ignore[arg-type]
        )

    def test_build_overlap_for_date_persists_summary_and_members(self) -> None:
        dates = pd.date_range(start="2025-10-01", periods=80, freq="B")
        aapl_frame = pd.DataFrame(
            {
                "Open": [100.0 + (idx * 0.5) for idx in range(len(dates))],
                "High": [101.8 + (idx * 0.55) for idx in range(len(dates))],
                "Low": [99.2 + (idx * 0.45) for idx in range(len(dates))],
                "Close": [100.5 + (idx * 0.5) for idx in range(len(dates))],
                "Adj Close": [100.5 + (idx * 0.5) for idx in range(len(dates))],
                "Volume": [1_000_000.0 for _ in range(len(dates))],
            },
            index=dates,
        )
        msft_frame = pd.DataFrame(
            {
                "Open": [80.0 + (idx * 0.2) for idx in range(len(dates))],
                "High": [81.0 + (idx * 0.22) for idx in range(len(dates))],
                "Low": [79.4 + (idx * 0.18) for idx in range(len(dates))],
                "Close": [80.2 + (idx * 0.2) for idx in range(len(dates))],
                "Adj Close": [80.2 + (idx * 0.2) for idx in range(len(dates))],
                "Volume": [900_000.0 for _ in range(len(dates))],
            },
            index=dates,
        )
        with patch("src.webapp.services.overlap_backtest_service.load_ticker_metadata_map", return_value={"AAPL": {"sector": "Tech"}, "MSFT": {"sector": "Tech"}}), patch(
            "src.webapp.services.overlap_backtest_service.load_many_ticker_windows",
            return_value={"AAPL": aapl_frame, "MSFT": msft_frame},
        ):
            payload = self.service.build_overlap_for_date(
                run_date=dt.date(2026, 1, 2),
                strategy_ids=["rs", "vcp", "gap_fill", "fearzone"],
                candidate_threshold=4,
            )

        self.assertEqual(payload["candidate_count"], 1)
        self.assertEqual(payload["overlap_four_plus_count"], 1)
        self.assertEqual(payload["overlap_two_plus_count"], 2)
        self.assertEqual(payload["overlap_two_plus"][0]["ticker"], "AAPL")
        self.assertGreater(payload["overlap_two_plus"][0]["pipeline_count"], payload["overlap_two_plus"][1]["pipeline_count"])
        self.assertIsNotNone(payload["overlap_two_plus"][0]["adr14_pct"])
        self.assertIsNotNone(payload["overlap_two_plus"][0]["atr14"])
        self.assertIn("trim_warning", payload["overlap_two_plus"][0])
        self.assertEqual(self.repository.overlap_summary["strategy_set_key"], "fearzone,gap_fill,rs,vcp")
        self.assertEqual(self.repository.overlap_members["overlap_run_id"], 17)
        self.assertEqual(len(self.repository.overlap_members["rows"]), 2)
        self.assertIn("adr14_pct", self.repository.overlap_members["rows"][0]["metadata_json"])
        self.assertTrue((Path(self.tmpdir.name) / "raw" / "daily_overlap_summary_2026-01-02.json").exists())

    def test_list_overlap_coverage_marks_missing_and_ready(self) -> None:
        days = self.service.list_overlap_coverage(
            strategy_ids=["rs", "vcp", "gap_fill", "fearzone"],
            start_date=dt.date(2026, 1, 2),
            end_date=dt.date(2026, 1, 3),
            candidate_threshold=4,
        )

        self.assertEqual(days[0]["screen_status"], "partial")
        self.assertEqual(days[0]["missing_strategy_ids"], ["fearzone"])
        self.assertTrue(days[0]["overlap_ready"])
        self.assertEqual(days[0]["overlap_four_plus_count"], 1)
        self.assertEqual(days[1]["screen_status"], "none")

    def test_run_backtest_calculates_returns_and_spy_excess(self) -> None:
        dates = [
            dt.date(2026, 1, 2),
            dt.date(2026, 1, 5),
            dt.date(2026, 1, 6),
            dt.date(2026, 1, 7),
            dt.date(2026, 1, 8),
            dt.date(2026, 1, 9),
            dt.date(2026, 1, 12),
            dt.date(2026, 1, 13),
            dt.date(2026, 1, 14),
            dt.date(2026, 1, 15),
            dt.date(2026, 1, 16),
        ]
        aapl_frame = _FakeFrame(dates, [100, 101, 102, 103, 104, 106, 108, 109, 110, 111, 112])
        spy_frame = _FakeFrame(dates, [400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410])
        with patch(
            "src.webapp.services.overlap_backtest_service.load_many_ticker_windows_for_range",
            return_value={"AAPL": aapl_frame, "SPY": spy_frame},
        ):
            payload = self.service.run_backtest(
                start_date=dt.date(2026, 1, 2),
                end_date=dt.date(2026, 1, 10),
                strategy_ids=["rs", "vcp", "gap_fill", "fearzone"],
                entry_signal_threshold=4,
                hold_periods=[5, 10],
                job_run_id=33,
            )

        self.assertEqual(payload["backtest_run_id"], 91)
        self.assertEqual(payload["summary"]["trade_count"], 1)
        self.assertAlmostEqual(payload["summary"]["holds"]["5"]["avg_return_pct"], 6.0, places=4)
        self.assertAlmostEqual(payload["summary"]["holds"]["10"]["avg_return_pct"], 12.0, places=4)
        self.assertEqual(self.repository.backtest_trades["backtest_run_id"], 91)
        self.assertEqual(len(self.repository.backtest_trades["rows"]), 1)


if __name__ == "__main__":
    unittest.main()
