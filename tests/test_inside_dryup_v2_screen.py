from __future__ import annotations

import unittest

import pandas as pd

from src.inside_dryup_v2_screen import find_recent_inside_dryup_v2_hit
from src.universe import UniverseTicker


def _inside_dryup_v2_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for idx in range(240):
        close_value = 80.0 + (idx * 0.42)
        open_value = close_value - 0.45
        high_value = close_value + 0.9
        low_value = open_value - 0.8
        volume_value = 1_300_000.0 + ((idx % 7) * 25_000.0)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
        volume_values.append(volume_value)

    tail = [
        (180.0, 181.0, 179.1, 180.4, 1_020_000.0),
        (180.5, 181.4, 179.8, 181.0, 980_000.0),
        (181.1, 182.0, 180.5, 181.7, 940_000.0),
        (181.8, 182.8, 181.2, 182.2, 910_000.0),
        (182.4, 183.3, 181.8, 182.9, 860_000.0),
        (183.0, 184.0, 182.5, 183.6, 820_000.0),
        (183.5, 184.6, 183.0, 184.1, 780_000.0),
        (184.0, 185.1, 183.5, 184.6, 740_000.0),
        (184.4, 185.4, 183.9, 184.9, 700_000.0),
        (184.8, 185.8, 184.2, 185.1, 650_000.0),
        (185.2, 186.2, 184.6, 185.5, 620_000.0),
        (185.6, 186.4, 184.9, 185.8, 590_000.0),
        (185.9, 186.6, 185.1, 186.0, 560_000.0),
        (186.1, 186.7, 185.3, 186.2, 530_000.0),
        (186.2, 186.8, 185.5, 186.4, 500_000.0),
        (186.4, 186.9, 185.7, 186.5, 470_000.0),
        (186.5, 187.0, 185.9, 186.6, 440_000.0),
        (186.6, 187.1, 186.0, 186.7, 410_000.0),
        (186.68, 186.95, 186.18, 186.52, 165_000.0 if passes else 410_000.0),
        (186.50, 186.82, 186.32, 186.46, 135_000.0 if passes else 360_000.0),
    ]
    for open_value, high_value, low_value, close_value, volume_value in tail:
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
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


class InsideDryupV2ScreenTests(unittest.TestCase):
    def test_find_recent_inside_dryup_v2_hit_returns_hit_when_inside_day_and_extreme_dry(self) -> None:
        hit = find_recent_inside_dryup_v2_hit(
            _inside_dryup_v2_frame(passes=True),
            ticker=UniverseTicker(symbol="AAPL", sector="Technology", industry="Software", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertLess(hit.price_volume_ratio, 0.30)
        self.assertGreaterEqual(hit.dry_count, 1)
        self.assertTrue(hit.qualified_extreme)

    def test_find_recent_inside_dryup_v2_hit_returns_none_when_not_extreme_dry(self) -> None:
        hit = find_recent_inside_dryup_v2_hit(
            _inside_dryup_v2_frame(passes=False),
            ticker=UniverseTicker(symbol="AAPL"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
