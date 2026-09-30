from __future__ import annotations

import contextlib
import datetime as dt
import unittest
from unittest.mock import patch

from src.config import AppConfig
from src.elite_rs_screen import run_elite_rs_screen
from src.universe import UniverseTicker


def _price_rows(*, volume_event_age: int, stock: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = dt.date(2025, 1, 2)
    cursor = start
    while len(rows) < 320:
        if cursor.weekday() < 5:
            index = len(rows)
            close = 100.0 + index * (1.0 if stock else 0.1)
            rows.append(
                {
                    "formatted_date": cursor.isoformat(),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 5_000_000.0 if stock and index == 319 - volume_event_age else 1_000_000.0,
                }
            )
        cursor += dt.timedelta(days=1)
    return rows


class _FakeFinancials:
    def __init__(self, stock_rows: list[dict[str, object]], benchmark_rows: list[dict[str, object]]) -> None:
        self._stock_rows = stock_rows
        self._benchmark_rows = benchmark_rows

    def _get_clean_price_data(self) -> list[dict[str, object]]:
        return self._stock_rows

    def _get_benchmark_price_data(self, _benchmark_ticker: str) -> list[dict[str, object]]:
        return self._benchmark_rows


class _FakeCookstock:
    def __init__(self, rows_by_ticker: dict[str, list[dict[str, object]]], benchmark_rows: list[dict[str, object]]) -> None:
        self._rows_by_ticker = rows_by_ticker
        self._benchmark_rows = benchmark_rows

    def cookFinancials(self, ticker: str, **_kwargs: object) -> _FakeFinancials:
        return _FakeFinancials(self._rows_by_ticker[ticker], self._benchmark_rows)


class EliteRsScreenTests(unittest.TestCase):
    def test_hv1_profile_only_accepts_events_in_latest_three_trading_bars(self) -> None:
        recent = UniverseTicker(symbol="RECENT", sector="Technology", exchange="NASDAQ")
        stale = UniverseTicker(symbol="STALE", sector="Technology", exchange="NASDAQ")
        benchmark_rows = _price_rows(volume_event_age=0, stock=False)
        cookstock = _FakeCookstock(
            {
                "RECENT": _price_rows(volume_event_age=2, stock=True),
                "STALE": _price_rows(volume_event_age=3, stock=True),
            },
            benchmark_rows,
        )

        with patch("src.elite_rs_screen.load_configured_cookstock", return_value=cookstock), patch(
            "src.elite_rs_screen.freeze_cookstock_today",
            side_effect=lambda _module, _as_of_date: contextlib.nullcontext(),
        ), patch(
            "src.elite_rs_screen.iter_prefetched_cookstock_batches",
            return_value=[[recent, stale]],
        ):
            result = run_elite_rs_screen(
                AppConfig(),
                [recent, stale],
                profile="hv1",
                as_of_date=dt.date(2026, 3, 25),
            )

        self.assertEqual([hit.ticker for hit in result.hits], ["RECENT"])
        self.assertEqual(result.hits[0].volume_signal_age_days, 2)


if __name__ == "__main__":
    unittest.main()
