from __future__ import annotations

from decimal import Decimal
import unittest

import pandas as pd

from src.market_extension import build_moving_average, compute_extension_frame, find_extension_peaks, resample_to_weekly


class MarketExtensionTests(unittest.TestCase):
    def test_build_moving_average_supports_sma_and_ema(self) -> None:
        series = pd.Series([10.0, 11.0, 12.0, 13.0, 14.0])

        sma = build_moving_average(series, length=3, ma_type="sma")
        ema = build_moving_average(series, length=3, ma_type="ema")

        self.assertAlmostEqual(float(sma.iloc[-1]), 13.0)
        self.assertAlmostEqual(float(ema.iloc[-1]), 13.0625)

    def test_compute_extension_frame_marks_warning_and_extreme(self) -> None:
        index = pd.date_range(start="2026-01-02", periods=6, freq="W-FRI")
        frame = pd.DataFrame(
            {
                "Open": [100.0, 102.0, 104.0, 107.0, 109.0, 115.0],
                "High": [101.0, 103.0, 105.0, 108.0, 111.0, 118.0],
                "Low": [99.0, 101.0, 103.0, 106.0, 108.0, 114.0],
                "Close": [100.0, 102.0, 104.0, 107.0, 110.0, 118.0],
                "Volume": [1_000_000.0] * 6,
            },
            index=index,
        )

        result = compute_extension_frame(frame, length=3, ma_type="sma", warning_pct=5.0, extreme_pct=5.5)

        self.assertEqual(result["threshold_state"].iloc[-2], "normal")
        self.assertEqual(result["threshold_state"].iloc[-1], "extreme")

    def test_compute_extension_frame_coerces_decimal_db_values(self) -> None:
        index = pd.date_range(start="2026-01-02", periods=6, freq="W-FRI")
        frame = pd.DataFrame(
            {
                "Open": [Decimal("100.0"), Decimal("102.0"), Decimal("104.0"), Decimal("107.0"), Decimal("109.0"), Decimal("115.0")],
                "High": [Decimal("101.0"), Decimal("103.0"), Decimal("105.0"), Decimal("108.0"), Decimal("111.0"), Decimal("118.0")],
                "Low": [Decimal("99.0"), Decimal("101.0"), Decimal("103.0"), Decimal("106.0"), Decimal("108.0"), Decimal("114.0")],
                "Close": [Decimal("100.0"), Decimal("102.0"), Decimal("104.0"), Decimal("107.0"), Decimal("110.0"), Decimal("118.0")],
                "Volume": [Decimal("1000000"), Decimal("1000000"), Decimal("1000000"), Decimal("1000000"), Decimal("1000000"), Decimal("1000000")],
            },
            index=index,
        )

        result = compute_extension_frame(frame, length=3, ma_type="sma", warning_pct=5.0, extreme_pct=5.5)

        self.assertEqual(result["threshold_state"].iloc[-1], "extreme")
        self.assertAlmostEqual(float(result["extension_pct"].iloc[-1]), 5.67, places=2)

    def test_find_extension_peaks_returns_local_maximum(self) -> None:
        index = pd.date_range(start="2026-01-02", periods=7, freq="W-FRI")
        frame = pd.DataFrame(
            {
                "Open": [100.0, 102.0, 104.0, 106.0, 108.0, 107.0, 106.0],
                "High": [101.0, 103.0, 105.0, 109.0, 111.0, 108.0, 107.0],
                "Low": [99.0, 101.0, 103.0, 105.0, 107.0, 106.0, 105.0],
                "Close": [100.0, 102.0, 104.0, 108.0, 112.0, 107.0, 106.0],
                "Volume": [1_000_000.0] * 7,
            },
            index=index,
        )

        peaks = find_extension_peaks(frame, length=3, ma_type="sma", min_extension_pct=3.0, max_extension_pct=20.0)

        self.assertEqual(len(peaks), 1)
        self.assertEqual(peaks[0].trade_date, "2026-01-30")

    def test_resample_to_weekly_collapses_daily_bars(self) -> None:
        index = pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"])
        frame = pd.DataFrame(
            {
                "Open": [100.0, 101.0, 102.0, 103.0, 104.0],
                "High": [101.0, 102.0, 103.0, 104.0, 106.0],
                "Low": [99.0, 100.0, 101.0, 102.0, 103.0],
                "Close": [100.5, 101.5, 102.5, 103.5, 105.5],
                "Volume": [10.0, 11.0, 12.0, 13.0, 14.0],
            },
            index=index,
        )

        weekly = resample_to_weekly(frame)

        self.assertEqual(len(weekly), 1)
        self.assertEqual(float(weekly["Open"].iloc[0]), 100.0)
        self.assertEqual(float(weekly["High"].iloc[0]), 106.0)
        self.assertEqual(float(weekly["Low"].iloc[0]), 99.0)
        self.assertEqual(float(weekly["Close"].iloc[0]), 105.5)
        self.assertEqual(float(weekly["Volume"].iloc[0]), 60.0)


if __name__ == "__main__":
    unittest.main()
