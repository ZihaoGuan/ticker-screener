from __future__ import annotations

import datetime as dt
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from src.webapp.services.backtest_service import BacktestService


class _FakeBacktestRepository:
    def __init__(self) -> None:
        self.created_payload: dict[str, object] | None = None

    def list_backtest_runs(self, *, limit: int = 20):
        return []

    def load_cached_signals(self, *, screener_ids: list[str], start_date: dt.date, end_date: dt.date, include_deleted: bool = False):
        _ = include_deleted
        return [
            {"strategy_id": "rs", "signal_date": dt.date(2026, 1, 2), "ticker": "AAPL"},
            {"strategy_id": "vcp", "signal_date": dt.date(2026, 1, 2), "ticker": "AAPL"},
            {"strategy_id": "rs", "signal_date": dt.date(2026, 1, 3), "ticker": "MSFT"},
        ]

    def create_backtest_run(self, **kwargs: object) -> int:
        self.created_payload = dict(kwargs)
        return 88


def _frame(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=["trade_date", "Open", "High", "Low", "Close"])
    frame["Adj Close"] = frame["Close"]
    frame["Volume"] = 1_000_000
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame.set_index("trade_date")


class BacktestServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.artifacts_dir = Path(self.temp_dir.name)
        self.repository = _FakeBacktestRepository()
        self.service = BacktestService(database_url="postgres://unit-test", artifacts_dir=self.artifacts_dir, repository=self.repository)  # type: ignore[arg-type]

    def test_run_backtest_generates_summary_and_artifacts(self) -> None:
        frames = {
            "AAPL": _frame(
                [
                    ("2026-01-02", 10, 10.5, 9.8, 10.0),
                    ("2026-01-05", 10.1, 10.8, 10.0, 10.5),
                    ("2026-01-06", 10.5, 11.2, 10.4, 11.0),
                    ("2026-01-07", 11.1, 11.3, 10.7, 10.8),
                ]
            )
        }
        payload = {
            "entry_rule": {"mode": "min_count_same_day", "screener_ids": ["rs", "vcp"], "min_count": 2},
            "date_range": {"start_date": "2026-01-01", "end_date": "2026-01-31"},
            "exit_rules": [{"kind": "fixed_hold", "trading_days": 1}, {"kind": "take_profit_pct", "percent": 5}],
            "position_rules": {},
            "signal_cache_policy": "reuse_then_fill",
            "market_data_mode": "database_only",
        }

        with patch("src.webapp.services.backtest_service.load_many_ticker_windows_for_range", return_value=frames):
            result = self.service.run_backtest(payload, job_run_id=7)

        self.assertEqual(result["backtest_run_id"], 88)
        self.assertTrue(Path(result["json_report_path"]).exists())
        self.assertTrue(Path(result["html_report_path"]).exists())
        assert self.repository.created_payload is not None
        self.assertEqual(self.repository.created_payload["strategy_id"], "combo:rs+vcp")
        self.assertEqual(result["summary"]["entry_count"], 1)
        self.assertIn("hold_1d", result["summary"]["results_by_rule"])

    def test_first_of_prefers_earliest_exit(self) -> None:
        frame = _frame(
            [
                ("2026-01-02", 10, 10.2, 9.9, 10.0),
                ("2026-01-05", 9.5, 10.0, 9.0, 9.2),
                ("2026-01-06", 11.0, 11.5, 10.9, 11.2),
            ]
        )
        entry = {
            "ticker": "AAPL",
            "signal_date": dt.date(2026, 1, 2),
            "entry_date": dt.date(2026, 1, 2),
            "entry_index": 0,
            "entry_price": 10.0,
            "matched_count": 2,
            "matched_screener_ids": ["rs", "vcp"],
        }

        result = self.service._evaluate_rule(
            entry,
            frame,
            {
                "kind": "first_of",
                "rules": [
                    {"kind": "fixed_hold", "trading_days": 2},
                    {"kind": "stop_loss_pct", "percent": 5},
                ],
            },
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["exit_date"], "2026-01-05")
        self.assertLess(result["return_pct"], 0)


if __name__ == "__main__":
    unittest.main()
