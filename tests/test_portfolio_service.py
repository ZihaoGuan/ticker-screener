from __future__ import annotations

import datetime as dt
import unittest

import pandas as pd

from src.webapp.services import portfolio_service as portfolio_service_module
from src.webapp.services.portfolio_service import PortfolioService


class _FakePortfolioRepository:
    def __init__(self) -> None:
        self.portfolios: dict[int, dict[str, object]] = {}
        self.positions: dict[int, dict[str, object]] = {}
        self.transactions: dict[int, dict[str, object]] = {}
        self.advice: dict[int, dict[str, object]] = {}
        self.import_batches: list[dict[str, object]] = []
        self.next_portfolio_id = 1
        self.next_position_id = 1
        self.next_transaction_id = 1

    @property
    def database_url(self) -> str:
        return "postgres://example"

    def is_configured(self) -> bool:
        return True

    def list_portfolios(self):
        return [dict(item) for item in self.portfolios.values()]

    def get_or_create_portfolio(self, *, name: str, created_by_user_id: int | None = None):
        for item in self.portfolios.values():
            if item["name"] == name:
                return dict(item)
        payload = {
            "id": self.next_portfolio_id,
            "name": name,
            "created_by_user_id": created_by_user_id,
            "created_at": dt.datetime(2026, 6, 6, tzinfo=dt.timezone.utc),
            "updated_at": dt.datetime(2026, 6, 6, tzinfo=dt.timezone.utc),
        }
        self.portfolios[self.next_portfolio_id] = payload
        self.next_portfolio_id += 1
        return dict(payload)

    def ensure_ticker_metadata_stub(self, ticker: str) -> None:
        _ = ticker

    def create_position(self, **kwargs: object):
        payload = {
            "id": self.next_position_id,
            "created_at": dt.datetime(2026, 6, 6, tzinfo=dt.timezone.utc),
            "updated_at": dt.datetime(2026, 6, 6, tzinfo=dt.timezone.utc),
            **kwargs,
        }
        self.positions[self.next_position_id] = payload
        self.next_position_id += 1
        return dict(payload)

    def update_position(self, position_id: int, **kwargs: object):
        existing = self.positions.get(position_id)
        if existing is None:
            return None
        updated = {**existing, **kwargs, "updated_at": dt.datetime(2026, 6, 6, 1, tzinfo=dt.timezone.utc)}
        self.positions[position_id] = updated
        return dict(updated)

    def get_position(self, position_id: int):
        item = self.positions.get(position_id)
        return dict(item) if item else None

    def delete_position(self, position_id: int):
        if position_id not in self.positions:
            return False
        self.positions.pop(position_id)
        self.advice.pop(position_id, None)
        return True

    def create_transaction(self, **kwargs: object):
        payload = {
            "id": self.next_transaction_id,
            "created_at": dt.datetime(2026, 6, 6, 3, tzinfo=dt.timezone.utc),
            **kwargs,
        }
        self.transactions[self.next_transaction_id] = payload
        self.next_transaction_id += 1
        return dict(payload)

    def list_position_transactions(self, position_ids: list[int] | None = None):
        rows = list(self.transactions.values())
        if position_ids:
            rows = [item for item in rows if int(item["position_id"]) in position_ids]
        rows.sort(key=lambda item: (item["trade_date"], item["id"]))
        return [dict(item) for item in rows]

    def list_positions(self):
        rows: list[dict[str, object]] = []
        for payload in self.positions.values():
            portfolio = self.portfolios[int(payload["portfolio_id"])]
            advice = self.advice.get(int(payload["id"]), {})
            rows.append({**payload, "portfolio_name": portfolio["name"], **advice})
        rows.sort(key=lambda item: (str(item["portfolio_name"]), str(item["ticker"])))
        return rows

    def upsert_advice_snapshot(self, position_id: int, **kwargs: object):
        self.advice[position_id] = {
            **kwargs,
            "refreshed_at": dt.datetime(2026, 6, 6, 2, tzinfo=dt.timezone.utc),
            "signal_context_json": kwargs.get("signal_context_json") or {},
        }

    def create_import_batch(self, **kwargs: object):
        self.import_batches.append(kwargs)
        return len(self.import_batches)

    def list_recent_signal_hits(self, tickers: list[str], *, lookback_days: int = 45):
        _ = lookback_days
        return {
            ticker: [
                {
                    "ticker": ticker,
                    "strategy_id": "weekly_htf_pullback",
                    "signal_date": "2026-06-05",
                    "reasons_json": ["trend intact"],
                    "metrics_json": {},
                }
            ]
            for ticker in tickers
        }


def _frame(*, end_date: dt.date, close: float = 150.0, volume: int = 1_500_000) -> pd.DataFrame:
    index = pd.bdate_range(end=end_date, periods=260)
    closes = pd.Series([close * (0.96 + (idx / 1000.0)) for idx in range(len(index))], index=index)
    payload = pd.DataFrame(
        {
            "Open": closes * 0.99,
            "High": closes * 1.01,
            "Low": closes * 0.98,
            "Close": closes,
            "Adj Close": closes,
            "Volume": [volume for _ in range(len(index))],
        },
        index=index,
    )
    return payload


class PortfolioServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = _FakePortfolioRepository()
        self.service = PortfolioService(repository=self.repo)
        self.original_load_many_ticker_windows = portfolio_service_module.load_many_ticker_windows
        self.original_load_ticker_metadata_map = portfolio_service_module.load_ticker_metadata_map
        self.original_find_recent_hve_hit = portfolio_service_module.find_recent_hve_hit
        self.original_find_recent_inside_dryup_hit = portfolio_service_module.find_recent_inside_dryup_hit
        self.original_find_recent_ftd_sweep_hit = portfolio_service_module.find_recent_ftd_sweep_hit
        portfolio_service_module.load_ticker_metadata_map = lambda tickers, database_url=None: {
            ticker: {"ticker": ticker, "sector": "Tech", "industry": "Software", "exchange": "NASDAQ"}
            for ticker in tickers
        }
        portfolio_service_module.find_recent_hve_hit = lambda frame, ticker: object()
        portfolio_service_module.find_recent_inside_dryup_hit = lambda frame, ticker: None
        portfolio_service_module.find_recent_ftd_sweep_hit = lambda frame, ticker, benchmark_ticker, config: None

    def tearDown(self) -> None:
        portfolio_service_module.load_many_ticker_windows = self.original_load_many_ticker_windows
        portfolio_service_module.load_ticker_metadata_map = self.original_load_ticker_metadata_map
        portfolio_service_module.find_recent_hve_hit = self.original_find_recent_hve_hit
        portfolio_service_module.find_recent_inside_dryup_hit = self.original_find_recent_inside_dryup_hit
        portfolio_service_module.find_recent_ftd_sweep_hit = self.original_find_recent_ftd_sweep_hit

    def test_create_and_import_positions(self) -> None:
        created = self.service.create_position(
            ticker="aapl",
            shares="10",
            entry_price="180.5",
            opened_at="2026-05-01",
            notes="starter",
            portfolio_name="Core",
            actor_user_id=7,
        )

        self.assertEqual(created["ticker"], "AAPL")
        self.assertEqual(created["portfolio_name"], "Core")

        imported = self.service.import_csv(
            csv_text="ticker,shares,entry_price,opened_at,notes\nMSFT,5,420.1,2026-05-02,added\nBAD,,44,2026-05-01,oops\n",
            portfolio_name="Core",
            actor_user_id=7,
        )

        self.assertEqual(imported["accepted_count"], 1)
        self.assertEqual(imported["error_count"], 1)
        self.assertEqual(imported["accepted"][0]["position"]["ticker"], "MSFT")
        self.assertIn("shares", imported["errors"][0]["message"])

    def test_refresh_advice_builds_targets_and_adjusted_cost(self) -> None:
        position = self.service.create_position(
            ticker="NVDA",
            shares=10,
            entry_price=100,
            opened_at="2026-05-01",
            portfolio_name="Main",
        )
        portfolio_service_module.load_many_ticker_windows = lambda tickers, as_of_date, trading_days_needed, database_url=None: {
            "NVDA": _frame(end_date=as_of_date, close=140.0)
        }

        result = self.service.refresh_advice(position_id=position["id"])

        self.assertEqual(result["refreshed_count"], 1)
        payload = self.service.get_context()
        advice = payload["positions"][0]["advice"]
        self.assertEqual(advice["market_data_status"], "ready")
        self.assertIsNotNone(advice["stop_loss_price"])
        self.assertIsNotNone(advice["tp1_price"])
        self.assertIsNotNone(advice["tp2_price"])
        self.assertIsNotNone(advice["net_cost_after_tp1"])
        self.assertIsNotNone(advice["average_up_price"])
        self.assertIsNotNone(advice["blended_entry_after_average_up"])
        self.assertIn(advice["signal_status"], {"hold", "raise_stop"})

    def test_transactions_update_current_shares_and_average_cost(self) -> None:
        position = self.service.create_position(
            ticker="AMD",
            shares=10,
            entry_price=100,
            opened_at="2026-05-01",
            portfolio_name="Main",
        )

        buy = self.service.record_transaction(
            position["id"],
            side="buy",
            shares=5,
            price=120,
            trade_date="2026-05-10",
            fees=0,
        )
        self.assertEqual(buy["side"], "buy")

        sell = self.service.record_transaction(
            position["id"],
            side="sell",
            shares=4,
            price=130,
            trade_date="2026-05-15",
            fees=0,
        )
        self.assertEqual(sell["side"], "sell")

        payload = self.service.get_context()
        item = payload["positions"][0]
        self.assertEqual(item["shares"], 11.0)
        self.assertAlmostEqual(item["entry_price"], 106.67, places=2)
        self.assertAlmostEqual(item["realized_pl"], 93.33, places=2)
        self.assertEqual(len(item["transactions"]), 3)
        self.assertEqual(item["transactions"][0]["notes"], "Initial position")

    def test_refresh_advice_marks_stale_market_data(self) -> None:
        position = self.service.create_position(
            ticker="TSLA",
            shares=8,
            entry_price=220,
            opened_at="2026-05-01",
            portfolio_name="Main",
        )
        stale_end = dt.date.today() - dt.timedelta(days=12)
        portfolio_service_module.load_many_ticker_windows = lambda tickers, as_of_date, trading_days_needed, database_url=None: {
            "TSLA": _frame(end_date=stale_end, close=205.0)
        }

        self.service.refresh_advice(position_id=position["id"])

        payload = self.service.get_context()
        advice = payload["positions"][0]["advice"]
        self.assertEqual(advice["market_data_status"], "stale")
        self.assertEqual(advice["signal_status"], "review")
        self.assertIsNone(advice["tp1_price"])


if __name__ == "__main__":
    unittest.main()
