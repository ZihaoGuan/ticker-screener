from __future__ import annotations

import unittest

import pandas as pd

from src.bb_squeeze_screen import find_recent_bb_squeeze_hit
from src.universe import UniverseTicker


def _bb_squeeze_frame(*, squeeze: bool, positive_cci: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=80, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 55:
            center = 100.0 + (idx * 0.28)
            bar_range = 3.2 - min(idx * 0.02, 1.0)
            close_offset = 0.12 if idx % 2 == 0 else -0.1
        else:
            if squeeze:
                drift = 0.03 if positive_cci else -0.10
                center = 115.0 + ((idx - 55) * drift)
                bar_range = 1.8
                close_offset = 0.05 if positive_cci else (-0.08 if idx % 2 == 0 else -0.16)
            else:
                drift = 0.35 if positive_cci else -0.35
                center = 115.0 + ((idx - 55) * drift)
                bar_range = 0.8
                close_offset = 0.18 if idx % 2 == 0 else 0.08
        open_value = center - (bar_range * 0.2)
        close_value = center + close_offset
        high_value = max(open_value, close_value) + (bar_range * 0.4)
        low_value = min(open_value, close_value) - (bar_range * 0.4)
        close_values.append(close_value)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(1_000_000.0)

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


class BbSqueezeScreenTests(unittest.TestCase):
    def test_find_recent_bb_squeeze_hit_returns_positive_cci_signal(self) -> None:
        hit = find_recent_bb_squeeze_hit(
            _bb_squeeze_frame(squeeze=True, positive_cci=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertLess(hit.bb_squeeze_ratio, 1.0)
        self.assertEqual(hit.signal_kind, "positive_cci")
        self.assertGreater(hit.cci_value, 0.0)

    def test_find_recent_bb_squeeze_hit_returns_non_positive_cci_signal(self) -> None:
        hit = find_recent_bb_squeeze_hit(
            _bb_squeeze_frame(squeeze=True, positive_cci=False),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertLess(hit.bb_squeeze_ratio, 1.0)
        self.assertEqual(hit.signal_kind, "non_positive_cci")
        self.assertLessEqual(hit.cci_value, 0.0)

    def test_find_recent_bb_squeeze_hit_returns_none_without_squeeze(self) -> None:
        hit = find_recent_bb_squeeze_hit(
            _bb_squeeze_frame(squeeze=False, positive_cci=True),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
