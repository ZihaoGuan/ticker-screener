from __future__ import annotations

import unittest

import pandas as pd

from src.universe import UniverseTicker
from src.wyckoff_analysis import compute_wyckoff_markers, find_recent_wyckoff_signal_hit


def _wyckoff_buy_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=230, freq="B")
    rows: list[tuple[float, float, float, float, float]] = []
    for i in range(180):
        close_value = 200.0 - (i * 0.5)
        open_value = close_value + 0.4
        high_value = max(open_value, close_value) + 1.0
        low_value = min(open_value, close_value) - 1.0
        volume_value = 1_000_000.0 + ((i % 5) * 20_000.0)
        rows.append((open_value, high_value, low_value, close_value, volume_value))
    for j in range(40):
        base_value = 110.0 + ((j % 4) * 0.2)
        rows.append((base_value - 0.2, base_value + 0.8, base_value - 0.8, base_value + 0.1, 420_000.0 - (j * 2_000.0)))
    rows.extend(
        [
            (110.2, 111.0, 108.6, 110.8, 650_000.0),
            (110.8, 111.6, 110.1, 111.3, 680_000.0),
            (111.3, 112.1, 110.8, 111.9, 700_000.0),
            (111.8, 112.6, 111.2, 112.4, 740_000.0),
            (112.4, 113.2, 111.8, 112.9, 760_000.0),
            (112.8, 113.6, 112.2, 113.3, 790_000.0),
            (113.4, 114.4, 112.9, 114.1, 820_000.0),
            (114.1, 115.2, 113.8, 114.9, 850_000.0),
            (114.8, 116.2, 114.4, 115.8, 900_000.0),
            (115.6, 117.0, 115.2, 116.7, 950_000.0),
        ]
    )
    frame = pd.DataFrame(rows, columns=["Open", "High", "Low", "Close", "Volume"], index=index[: len(rows)])
    return frame.loc[: "2025-11-05"]


def _wyckoff_sell_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=260, freq="B")
    rows: list[tuple[float, float, float, float, float]] = []
    for i in range(180):
        close_value = 80.0 + (i * 0.5)
        open_value = close_value - 0.4
        high_value = max(open_value, close_value) + 1.0
        low_value = min(open_value, close_value) - 1.0
        volume_value = 1_000_000.0 + ((i % 5) * 20_000.0)
        rows.append((open_value, high_value, low_value, close_value, volume_value))
    for j in range(40):
        base_value = 170.0 - ((j % 4) * 0.15)
        rows.append((base_value + 0.2, base_value + 0.8, base_value - 0.8, base_value - 0.1, 430_000.0 - (j * 1_500.0)))
    rows.extend(
        [
            (170.2, 172.6, 169.8, 170.1, 900_000.0),
            (170.1, 172.8, 169.7, 169.4, 1_200_000.0),
            (169.3, 170.0, 167.8, 168.1, 950_000.0),
            (168.2, 168.7, 166.6, 167.0, 980_000.0),
            (167.1, 167.6, 165.4, 165.9, 1_020_000.0),
            (166.0, 166.4, 164.1, 164.7, 1_050_000.0),
        ]
    )
    frame = pd.DataFrame(rows, columns=["Open", "High", "Low", "Close", "Volume"], index=index[: len(rows)])
    return frame.loc[: "2025-11-05"]


def _wyckoff_bc_frame() -> pd.DataFrame:
    index = pd.date_range("2025-01-01", periods=230, freq="B")
    rows: list[tuple[float, float, float, float, float]] = []
    for i in range(220):
        close_value = 100.0 + (i * 0.25)
        open_value = close_value - 0.3
        high_value = close_value + 0.7
        low_value = open_value - 0.5
        volume_value = 900_000.0 + ((i % 5) * 10_000.0)
        rows.append((open_value, high_value, low_value, close_value, volume_value))
    rows.extend(
        [
            (154.8, 155.4, 154.1, 155.0, 920_000.0),
            (155.1, 155.6, 154.4, 155.3, 930_000.0),
            (155.4, 156.2, 154.7, 156.0, 2_100_000.0),
        ]
    )
    return pd.DataFrame(rows, columns=["Open", "High", "Low", "Close", "Volume"], index=index[: len(rows)])


class WyckoffAnalysisTests(unittest.TestCase):
    def test_find_recent_wyckoff_buy_signal_hit_returns_hit(self) -> None:
        hit = find_recent_wyckoff_signal_hit(
            _wyckoff_buy_frame(),
            ticker=UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors", exchange="NASDAQ"),
            signal_type="buy",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_type, "buy")
        self.assertEqual(hit.phase, "ACCUMULATION")
        self.assertIn("SPRING", hit.event_flags)

    def test_find_recent_wyckoff_sell_signal_hit_returns_hit(self) -> None:
        hit = find_recent_wyckoff_signal_hit(
            _wyckoff_sell_frame(),
            ticker=UniverseTicker(symbol="TSLA", sector="Consumer Cyclical", industry="Auto", exchange="NASDAQ"),
            signal_type="sell",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.signal_type, "sell")
        self.assertEqual(hit.phase, "DISTRIBUTION")
        self.assertIn("UTAD", hit.event_flags)

    def test_compute_wyckoff_markers_emits_buying_climax_buy_sell_and_hold(self) -> None:
        visible_dates = {
            "2025-09-15",
            "2025-09-18",
            "2025-11-05",
            "2025-11-07",
        }
        markers = []
        markers.extend(compute_wyckoff_markers(_wyckoff_buy_frame(), visible_dates=visible_dates))
        markers.extend(compute_wyckoff_markers(_wyckoff_sell_frame(), visible_dates=visible_dates))
        markers.extend(compute_wyckoff_markers(_wyckoff_bc_frame(), visible_dates=visible_dates))

        marker_kinds = {(str(marker["time"]), str(marker["kind"])) for marker in markers}
        self.assertIn(("2025-11-05", "wyckoff_buy_signal"), marker_kinds)
        self.assertIn(("2025-11-05", "wyckoff_sell_signal"), marker_kinds)
        self.assertIn(("2025-11-07", "wyckoff_buying_climax"), marker_kinds)
        self.assertIn(("2025-09-18", "wyckoff_hold_signal"), marker_kinds)
        self.assertIn(("2025-09-15", "wyckoff_hold_signal"), marker_kinds)


if __name__ == "__main__":
    unittest.main()
