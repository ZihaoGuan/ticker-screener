from __future__ import annotations

import unittest

import pandas as pd

from src.sean_breakout_screen import find_recent_sean_breakout_hit
from src.universe import UniverseTicker


def _sean_breakout_frame(*, close_ok: bool, ema_ok: bool, volume_ok: bool, adr_ok: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=80, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        base_close = 3.4 + (idx * 0.08)
        if not close_ok and idx == len(index) - 1:
            close_value = 2.95
        elif not ema_ok and idx >= len(index) - 6:
            close_value = 6.6 - ((idx - (len(index) - 6)) * 0.45)
        else:
            close_value = base_close

        if adr_ok:
            bar_range = 0.42 + ((idx % 3) * 0.03)
        else:
            bar_range = 0.10 + ((idx % 2) * 0.01)

        open_value = close_value - (bar_range * 0.15)
        high_value = max(open_value, close_value) + (bar_range * 0.45)
        low_value = min(open_value, close_value) - (bar_range * 0.40)
        volume_value = 720_000.0 if volume_ok else 420_000.0

        close_values.append(close_value)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(volume_value)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


class SeanBreakoutScreenTests(unittest.TestCase):
    def test_find_recent_sean_breakout_hit_returns_signal_when_all_filters_pass(self) -> None:
        hit = find_recent_sean_breakout_hit(
            _sean_breakout_frame(close_ok=True, ema_ok=True, volume_ok=True, adr_ok=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_kind, "sean_breakout")
        self.assertGreaterEqual(hit.current_price, 3.0)
        self.assertGreater(hit.current_price, hit.ema21_value)
        self.assertGreater(hit.current_price, hit.ema50_value)
        self.assertGreater(hit.avg_volume_10, 500_000.0)
        self.assertGreaterEqual(hit.adr_pct_20, 2.0)

    def test_find_recent_sean_breakout_hit_returns_none_when_close_below_minimum(self) -> None:
        hit = find_recent_sean_breakout_hit(
            _sean_breakout_frame(close_ok=False, ema_ok=True, volume_ok=True, adr_ok=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)

    def test_find_recent_sean_breakout_hit_returns_none_when_close_not_above_emas(self) -> None:
        hit = find_recent_sean_breakout_hit(
            _sean_breakout_frame(close_ok=True, ema_ok=False, volume_ok=True, adr_ok=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)

    def test_find_recent_sean_breakout_hit_returns_none_when_volume_or_adr_fail(self) -> None:
        low_volume_hit = find_recent_sean_breakout_hit(
            _sean_breakout_frame(close_ok=True, ema_ok=True, volume_ok=False, adr_ok=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )
        low_adr_hit = find_recent_sean_breakout_hit(
            _sean_breakout_frame(close_ok=True, ema_ok=True, volume_ok=True, adr_ok=False),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(low_volume_hit)
        self.assertIsNone(low_adr_hit)


if __name__ == "__main__":
    unittest.main()
