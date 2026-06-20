from __future__ import annotations

from contextlib import contextmanager
import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.peg_screen import EarningsEvent, assess_peg_event_quality, run_peg_screen
from src.peg_strategy import assess_sean_post_earnings_gap_setup


def _price_data(*, hv_signal: str | None = "HVE") -> tuple[list[dict[str, object]], str]:
    index = pd.date_range(start="2025-01-02", periods=320, freq="B")
    peg_index = 314
    bars: list[dict[str, object]] = []
    for idx, date_value in enumerate(index):
        close_value = 100.0 + (idx * 0.35)
        open_value = close_value - 0.6
        high_value = close_value + 2.2
        low_value = close_value - 2.2
        volume_value = 850_000.0 + ((idx % 5) * 10_000.0)
        if hv_signal == "NONE" and idx == 56:
            volume_value = 4_200_000.0
        if idx == peg_index:
            previous_close = bars[-1]["close"] if bars else close_value - 1.0
            open_value = float(previous_close) * 1.12
            close_value = float(previous_close) * 1.145
            high_value = close_value + 2.4
            low_value = float(previous_close) * 1.08
            volume_value = 5_500_000.0
            if hv_signal == "HV1":
                volume_value = 3_600_000.0
                bars[20]["volume"] = 4_100_000.0
            elif hv_signal == "NONE":
                volume_value = 3_500_000.0
                bars[20]["volume"] = 4_100_000.0
                bars[80]["volume"] = 3_900_000.0
        bars.append(
            {
                "formatted_date": date_value.date().isoformat(),
                "open": round(open_value, 4),
                "high": round(high_value, 4),
                "low": round(low_value, 4),
                "close": round(close_value, 4),
                "volume": volume_value,
            }
        )
    return bars, bars[peg_index]["formatted_date"]


def _peg_setup(price_data: list[dict[str, object]], peg_date: str) -> dict[str, object]:
    peg_bar = next(item for item in price_data if item["formatted_date"] == peg_date)
    peg_index = next(idx for idx, item in enumerate(price_data) if item["formatted_date"] == peg_date)
    previous_close = float(price_data[peg_index - 1]["close"])
    current_price = float(price_data[-1]["close"])
    peg_low = float(peg_bar["low"])
    return {
        "setup_type": "peg",
        "peg_date": peg_date,
        "peg_open": float(peg_bar["open"]),
        "peg_high": float(peg_bar["high"]),
        "peg_low": peg_low,
        "peg_close": float(peg_bar["close"]),
        "previous_close": previous_close,
        "gap_pct": (float(peg_bar["close"]) - previous_close) / previous_close,
        "open_gap_pct": (float(peg_bar["open"]) - previous_close) / previous_close,
        "volume_ratio": float(peg_bar["volume"]) / 900_000.0,
        "close_position_ratio": 0.7,
        "entry_distance_pct": max((current_price - peg_low) / peg_low, 0.0),
        "current_price": current_price,
        "hvc": float(peg_bar["close"]),
        "hvc5": float(peg_bar["close"]) * 0.95,
        "gdh": float(peg_bar["high"]),
        "gdl": float(peg_bar["low"]),
        "gap_fill_floor": previous_close,
        "gap_fully_filled": False,
        "earnings_actual_eps": 1.5,
        "earnings_estimated_eps": 1.0,
        "earnings_surprise_pct": 50.0,
    }


def _make_recent_pullback_light_volume(price_data: list[dict[str, object]], *, days: int = 8, volume: float = 450_000.0) -> None:
    start_index = max(0, len(price_data) - days)
    for idx in range(start_index, len(price_data)):
        price_data[idx]["volume"] = volume


def _extend_quiet_post_gap_bars(price_data: list[dict[str, object]], *, extra_days: int = 8, volume: float = 450_000.0) -> None:
    last_date = dt.date.fromisoformat(str(price_data[-1]["formatted_date"]))
    last_close = float(price_data[-1]["close"])
    for offset in range(1, extra_days + 1):
        next_date = last_date + dt.timedelta(days=offset)
        close_value = round(last_close + (offset * 0.15), 4)
        price_data.append(
            {
                "formatted_date": next_date.isoformat(),
                "open": round(close_value - 0.4, 4),
                "high": round(close_value + 2.4, 4),
                "low": round(close_value - 2.4, 4),
                "close": close_value,
                "volume": volume,
            }
        )


class _FakeFinancials:
    def __init__(
        self,
        price_data: list[dict[str, object]],
        peg_setup: dict[str, object],
        *,
        ema50: float = 105.0,
        demand_dry: bool = True,
    ) -> None:
        self._price_rows = price_data
        self._peg_setup = peg_setup
        self._ema50 = ema50
        self._demand_dry = demand_dry

    def _get_clean_price_data(self) -> list[dict[str, object]]:
        return self._price_rows

    def _get_latest_ema_value(self, length: int) -> float | None:
        if length == 50:
            return self._ema50
        if length == 21:
            return float(self._price_rows[-1]["close"]) * 0.99
        return None

    def is_demand_dry(self) -> tuple[bool, dict[str, object]]:
        return self._demand_dry, {}

    def find_recent_power_earnings_gap_event(self, recency_days: int = 30) -> dict[str, object] | None:
        return self._peg_setup

    def find_recent_power_earnings_gap(self) -> dict[str, object] | None:
        return self._peg_setup

    def get_peg_trade_plan(self, peg_setup: dict[str, object]) -> dict[str, object]:
        return {
            "primary_entry_label": "peg_low",
            "primary_entry": peg_setup["peg_low"],
            "distribution_warning": False,
            "distribution_days_count": 0,
            "latest_distribution_date": None,
            "latest_distribution_volume_ratio": None,
            "distribution_volume_ratio_threshold": 1.5,
        }


class _FakeCookstock:
    def __init__(self, financials: _FakeFinancials) -> None:
        self._financials = financials

    def cookFinancials(self, ticker: str, benchmarkTicker: str | None = None) -> _FakeFinancials:
        return self._financials


@contextmanager
def _freeze_stub(_cookstock: object, _as_of_date: dt.date | None):
    yield


class PegScreenTests(unittest.TestCase):
    def test_assess_peg_event_quality_accepts_hve_gap_bar(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HVE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))

        assessment = assess_peg_event_quality(
            financials,
            peg_date,
            as_of_date=dt.date.fromisoformat(price_data[-1]["formatted_date"]),
            config=AppConfig(),
        )

        self.assertTrue(assessment.qualifies)
        self.assertEqual(assessment.volume_signal_kind, "HVE")
        self.assertGreater(assessment.adr_pct_20 or 0.0, 2.0)
        self.assertGreater(assessment.avg_volume_20 or 0.0, 500_000.0)
        self.assertTrue(assessment.price_above_ema50)

    def test_assess_peg_event_quality_accepts_hv1_gap_bar_using_52_week_logic(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HV1")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))

        assessment = assess_peg_event_quality(
            financials,
            peg_date,
            as_of_date=dt.date.fromisoformat(price_data[-1]["formatted_date"]),
            config=AppConfig(),
        )

        self.assertTrue(assessment.qualifies)
        self.assertEqual(assessment.volume_signal_kind, "HV1")

    def test_assess_peg_event_quality_rejects_non_hve_non_hv1_gap_bar(self) -> None:
        price_data, peg_date = _price_data(hv_signal="NONE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))

        assessment = assess_peg_event_quality(
            financials,
            peg_date,
            as_of_date=dt.date.fromisoformat(price_data[-1]["formatted_date"]),
            config=AppConfig(),
        )

        self.assertFalse(assessment.qualifies)
        self.assertIsNone(assessment.volume_signal_kind)
        self.assertIn("PEG gap bar is not HVE/HV1", assessment.notes)

    def test_run_peg_screen_filters_out_non_hve_gap_events(self) -> None:
        price_data, peg_date = _price_data(hv_signal="NONE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))
        cookstock = _FakeCookstock(financials)

        with patch("src.peg_screen.load_configured_cookstock", return_value=cookstock), patch(
            "src.peg_screen.freeze_cookstock_today",
            _freeze_stub,
        ):
            result = run_peg_screen(
                AppConfig(),
                [EarningsEvent(ticker="TEST")],
                as_of_date=dt.date.fromisoformat(price_data[-1]["formatted_date"]),
            )

        self.assertEqual(result.passed_tickers, 0)
        self.assertEqual(result.recent_event_tickers, 0)

    def test_run_peg_screen_keeps_hve_gap_events(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HVE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))
        cookstock = _FakeCookstock(financials)

        with patch("src.peg_screen.load_configured_cookstock", return_value=cookstock), patch(
            "src.peg_screen.freeze_cookstock_today",
            _freeze_stub,
        ):
            result = run_peg_screen(
                AppConfig(),
                [EarningsEvent(ticker="TEST")],
                as_of_date=dt.date.fromisoformat(price_data[-1]["formatted_date"]),
            )

        self.assertEqual(result.passed_tickers, 1)
        self.assertEqual(result.recent_event_tickers, 1)
        self.assertEqual(result.hits[0].peg_volume_signal_kind, "HVE")

    def test_sean_gap_up_can_qualify_even_with_distribution_warning(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HVE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))

        assessment = assess_sean_post_earnings_gap_setup(
            financials,
            peg_date,
            True,
            AppConfig(),
        )

        self.assertTrue(assessment.qualifies)
        self.assertIn("recent distribution warning", assessment.notes)

    def test_sean_gap_up_can_qualify_without_breakout_or_dema_support_ready(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HVE")
        financials = _FakeFinancials(price_data, _peg_setup(price_data, peg_date))
        config = AppConfig(
            peg_sean_tight_range_max_pct=0.000001,
            peg_sean_breakout_proximity_pct=0.0,
            peg_sean_dema_tolerance_pct=0.0,
        )

        assessment = assess_sean_post_earnings_gap_setup(
            financials,
            peg_date,
            False,
            config,
        )

        self.assertTrue(assessment.qualifies)
        self.assertFalse(assessment.breakout_ready)
        self.assertFalse(assessment.dema_support_ready)
        self.assertIn("not near breakout or 8 DEMA support entry", assessment.notes)

    def test_sean_gap_up_does_not_require_demand_dry_or_pullback_volume_check(self) -> None:
        price_data, peg_date = _price_data(hv_signal="HVE")
        financials = _FakeFinancials(
            price_data,
            _peg_setup(price_data, peg_date),
            demand_dry=False,
        )

        assessment = assess_sean_post_earnings_gap_setup(
            financials,
            peg_date,
            False,
            AppConfig(),
        )

        self.assertFalse(assessment.demand_dry)
        self.assertFalse(assessment.low_volume_pullback)
        self.assertTrue(assessment.qualifies)
        self.assertIn("pullback volume not drying up", assessment.notes)


if __name__ == "__main__":
    unittest.main()
