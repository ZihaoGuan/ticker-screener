from __future__ import annotations

import pandas as pd
import unittest

from src.universe import UniverseTicker
from src.vcp_spec_screen import find_recent_vcp_spec_hit


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame.set_index("Date")


def _good_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=320, freq="B")
    rows: list[dict[str, object]] = []
    for idx, ts in enumerate(index):
        if idx < 220:
            close = 22.0 + (idx * 0.40)
            high = close * 1.015
            low = close * 0.985
            volume = 1_000_000
        elif idx < 236:
            ratio = idx - 220
            close = 110.0 - (ratio * 1.25)
            high = close * 1.008
            low = close * 0.992
            volume = 1_000_000
        elif idx < 246:
            ratio = idx - 236
            close = 91.5 + (ratio * 1.25)
            high = close * 1.006
            low = close * 0.994
            volume = 900_000
        elif idx < 256:
            ratio = idx - 246
            close = 104.0 - (ratio * 1.0)
            high = close * 1.006
            low = close * 0.994
            volume = 820_000
        elif idx < 266:
            ratio = idx - 256
            close = 95.0 + (ratio * 0.70)
            high = close * 1.005
            low = close * 0.995
            volume = 700_000
        elif idx < 273:
            ratio = idx - 266
            close = 101.3 - (ratio * 0.55)
            high = close * 1.004
            low = close * 0.996
            volume = 620_000
        elif idx < 282:
            ratio = idx - 273
            close = 97.8 + (ratio * 0.33)
            high = close * 1.003
            low = close * 0.997
            volume = 580_000
        else:
            ratio = idx - 282
            close = 100.45 + ((ratio % 4) * 0.06)
            high = close * 1.002
            low = close * 0.998
            volume = 540_000
        rows.append(
            {
                "Date": ts.isoformat(),
                "Open": close * 0.997,
                "High": round(high, 4),
                "Low": round(low, 4),
                "Close": round(close, 4),
                "Volume": volume,
            }
        )
    return _frame(rows)


class VcpSpecScreenTests(unittest.TestCase):
    def test_find_recent_vcp_spec_hit_returns_pre_breakout_candidate(self) -> None:
        hit = find_recent_vcp_spec_hit(
            _good_frame(),
            ticker=UniverseTicker(symbol="TEST", sector="Technology", industry="Software"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.category, "pre_breakout")
        self.assertGreaterEqual(hit.contractions_count, 3)
        self.assertTrue(all(hit.criteria_pass[f"criterion_{index}"] for index in range(1, 8)))
        self.assertLessEqual(abs(hit.pivot_within_top_pct), 1.5)

    def test_find_recent_vcp_spec_hit_rejects_loose_base(self) -> None:
        frame = _good_frame().copy()
        latest_slice = frame.iloc[220:].copy()
        frame.loc[latest_slice.index, "High"] = frame.loc[latest_slice.index, "High"] * 1.12
        frame.loc[latest_slice.index, "Low"] = frame.loc[latest_slice.index, "Low"] * 0.88

        hit = find_recent_vcp_spec_hit(
            frame,
            ticker=UniverseTicker(symbol="LOOSE"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
