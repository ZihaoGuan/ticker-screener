from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import pandas as pd

from src.config import AppConfig
from src.rs_screen import _compute_latest_rs_rating, _compute_weekly_rs_before_price_context
from src.screener_engine import ScreenerEvaluationResult, ScreenerSpec
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService


def _frame(start: str, count: int) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=count, freq="B")
    return pd.DataFrame(
        {
            "Open": [100.0 + idx for idx in range(count)],
            "High": [101.0 + idx for idx in range(count)],
            "Low": [99.0 + idx for idx in range(count)],
            "Close": [100.5 + idx for idx in range(count)],
            "Adj Close": [100.5 + idx for idx in range(count)],
            "Volume": [1_000_000 + idx for idx in range(count)],
        },
        index=index,
    )


def _ftd_sweep_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-05", periods=80, freq="B")
    close = [100.0 - (idx * 0.18) for idx in range(80)]
    open_values = [value + 0.45 for value in close]
    high = [max(op, cl) + 0.55 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.55 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 for _ in close]

    overrides = {
        21: (90.5, 89.8, 91.0, 89.0, 1_050_000.0),
        22: (90.2, 91.0, 91.4, 89.5, 1_050_000.0),
        23: (91.1, 90.8, 91.6, 90.2, 1_000_000.0),
        24: (90.7, 90.2, 91.0, 89.8, 1_000_000.0),
        25: (90.0, 89.7, 90.3, 89.3, 1_000_000.0),
        26: (89.6, 89.3, 89.9, 88.9, 1_000_000.0),
        27: (89.1, 88.8, 89.4, 88.4, 1_000_000.0),
        28: (88.5, 88.2, 88.8, 87.8, 1_000_000.0),
        29: (88.1, 87.6, 88.5, 87.2, 1_000_000.0),
        30: (85.4, 83.8, 85.8, 82.8, 1_050_000.0),
        31: (84.2, 86.0, 86.5, 84.0, 1_000_000.0),
        32: (85.4, 89.4, 90.0, 85.2, 1_900_000.0),
        33: (89.2, 89.9, 90.3, 88.9, 1_250_000.0),
        34: (90.3, 89.6, 90.5, 89.2, 700_000.0),
        35: (89.9, 89.2, 90.1, 88.9, 650_000.0),
        36: (89.4, 88.8, 89.6, 88.5, 600_000.0),
        37: (89.0, 91.6, 92.4, 88.9, 1_400_000.0),
        38: (91.8, 91.0, 92.0, 90.6, 1_000_000.0),
        39: (91.2, 90.8, 91.4, 90.2, 950_000.0),
        40: (90.9, 93.2, 93.6, 90.7, 1_300_000.0),
        41: (93.0, 93.6, 94.0, 92.8, 1_100_000.0),
        42: (93.5, 93.9, 94.3, 93.3, 1_100_000.0),
    }
    for idx in range(43, len(close)):
        close[idx] = 94.0 + ((idx - 43) * 0.12)
        open_values[idx] = close[idx] - 0.3
        high[idx] = close[idx] + 0.45
        low[idx] = open_values[idx] - 0.35
        volume[idx] = 1_050_000.0

    for idx, row in overrides.items():
        open_values[idx], close[idx], high[idx], low[idx], volume[idx] = row

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _fearzone_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    close = [90.0 + (idx * 0.16) for idx in range(240)]
    close.extend(
        [
            129.0,
            129.6,
            128.8,
            130.0,
            131.2,
            132.0,
            131.0,
            132.4,
            131.6,
            132.2,
            131.8,
            132.0,
            131.4,
            119.0,
            121.4,
            123.7,
            124.9,
            125.8,
            126.7,
            127.5,
        ]
    )
    open_values = [value * 1.003 for value in close]
    high = [max(op, cl) + 1.0 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 1.0 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_100_000.0 for _ in close]
    signal_index = len(close) - 5
    open_values[signal_index] = 126.5
    close[signal_index] = 119.0
    high[signal_index] = 127.0
    low[signal_index] = 118.2
    volume[signal_index] = 1_900_000.0
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _fearzone_zeiierman_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-01", periods=220, freq="B")
    close = [100.0 + ((80.0 / 219.0) * idx) for idx in range(220)]
    drop_offsets = [0.0, 6.1, 12.2, 18.3, 24.4, 30.5, 36.6, 42.7, 48.8, 55.0]
    for position, offset in enumerate(drop_offsets, start=len(close) - len(drop_offsets)):
        close[position] -= offset
    open_values = [value * 1.002 for value in close]
    high = [max(op, cl) + 1.0 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 1.0 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_250_000.0 for _ in close]
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _rs_rating_stock_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    close = 50.0 + (150.0 * (pd.Series(range(320), index=index) / 319.0) ** 1.2)
    return pd.DataFrame(
        {
            "Open": close * 0.998,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_250_000.0,
        },
        index=index,
    )


def _rs_rating_benchmark_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-01", periods=320, freq="B")
    close = pd.Series([100.0 + ((5.0 / 319.0) * idx) for idx in range(320)], index=index)
    return pd.DataFrame(
        {
            "Open": close,
            "High": close,
            "Low": close,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_000_000.0,
        },
        index=index,
    )


def _weekly_rs_rows(*, before_price: bool) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    stock_rows: list[dict[str, object]] = []
    benchmark_rows: list[dict[str, object]] = []
    daily_index = pd.date_range(start="2025-01-06", periods=60 * 5, freq="B")
    for week in range(60):
        week_dates = daily_index[week * 5 : (week + 1) * 5]
        benchmark_close = 100.0 + (week * 0.3)
        if week < 55:
            stock_close = 100.0 + (week * 0.45)
            stock_high = stock_close + 1.0
        elif before_price:
            stock_close = 121.0 + ((week - 55) * 0.2)
            stock_high = 125.0
        else:
            stock_close = 124.0 + ((week - 55) * 0.8)
            stock_high = stock_close + 1.5
        if before_price and week >= 55:
            benchmark_close = 95.0 - ((week - 55) * 0.6)
        for date_value in week_dates:
            stock_rows.append(
                {
                    "formatted_date": date_value.date().isoformat(),
                    "close": float(stock_close),
                    "high": float(stock_high),
                }
            )
            benchmark_rows.append(
                {
                    "formatted_date": date_value.date().isoformat(),
                    "close": float(benchmark_close),
                }
            )
    return stock_rows, benchmark_rows


def _hve_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=320, freq="B")
    open_values = [100.0 + (idx * 0.2) for idx in range(320)]
    close = [100.4 + (idx * 0.2) for idx in range(320)]
    high = [max(op, cl) + 0.8 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.7 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 + (idx * 2_500.0) for idx in range(320)]
    signal_index = len(index) - 1
    open_values[signal_index] = 163.0
    close[signal_index] = 170.0
    high[signal_index] = 171.2
    low[signal_index] = 162.1
    volume[signal_index] = 4_800_000.0
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _hv1_only_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-02", periods=320, freq="B")
    open_values = [100.0 + (idx * 0.2) for idx in range(320)]
    close = [100.4 + (idx * 0.2) for idx in range(320)]
    high = [max(op, cl) + 0.8 for op, cl in zip(open_values, close, strict=False)]
    low = [min(op, cl) - 0.7 for op, cl in zip(open_values, close, strict=False)]
    volume = [1_000_000.0 + (idx * 2_500.0) for idx in range(320)]

    old_hve_index = 20
    open_values[old_hve_index] = 104.0
    close[old_hve_index] = 106.0
    high[old_hve_index] = 106.8
    low[old_hve_index] = 103.5
    volume[old_hve_index] = 6_500_000.0

    signal_index = len(index) - 1
    open_values[signal_index] = 163.0
    close[signal_index] = 169.0
    high[signal_index] = 170.2
    low[signal_index] = 162.3
    volume[signal_index] = 4_900_000.0
    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _inside_dryup_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high: list[float] = []
    low: list[float] = []
    close: list[float] = []
    volume: list[float] = []

    price = 100.0
    for idx in range(250):
        price += 0.35
        open_price = price - 0.3
        close_price = price
        high_price = close_price + 0.8
        low_price = open_price - 0.7
        open_values.append(open_price)
        high.append(high_price)
        low.append(low_price)
        close.append(close_price)
        volume.append(1_250_000.0 + (idx * 1_000.0))

    tail = [
        (186.2, 186.8, 184.8, 185.3, 760_000.0),
        (185.1, 185.5, 183.9, 184.4, 700_000.0),
        (184.3, 184.8, 183.2, 183.8, 660_000.0),
        (183.6, 184.1, 182.9, 183.3, 610_000.0),
        (183.2, 183.8, 183.05, 183.4, 540_000.0),
        (183.35, 183.7, 183.2, 183.45, 500_000.0),
        (183.42, 183.62, 183.31, 183.5, 460_000.0),
        (183.46, 183.58, 183.36, 183.49, 430_000.0),
        (183.48, 183.55, 183.4, 183.5, 410_000.0),
        (183.49, 183.53, 183.43, 183.48, 390_000.0),
    ]
    for row in tail:
        open_price, high_price, low_price, close_price, volume_value = row
        open_values.append(open_price)
        high.append(high_price)
        low.append(low_price)
        close.append(close_price)
        volume.append(volume_value)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


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
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _bearish_td9_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=20, freq="B")
    close = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 99.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0]
    return pd.DataFrame(
        {
            "Open": [value - 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Adj Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _bullish_td9_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=20, freq="B")
    close = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 101.0, 99.0, 98.0, 97.0, 96.0, 95.0, 94.0, 93.0, 92.0, 91.0]
    return pd.DataFrame(
        {
            "Open": [value + 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Adj Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _macd_golden_cross_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=60, freq="B")
    close = [100.0 - (idx * 0.35) for idx in range(55)]
    close.extend([80.6, 80.1, 79.8, 80.0, 81.8])
    return pd.DataFrame(
        {
            "Open": [value - 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Adj Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _macd_dead_cross_frame() -> pd.DataFrame:
    index = pd.date_range(start="2026-01-02", periods=60, freq="B")
    close = [100.0 + (idx * 0.35) for idx in range(55)]
    close.extend([119.4, 119.8, 120.0, 119.5, 117.5])
    return pd.DataFrame(
        {
            "Open": [value + 0.2 for value in close],
            "High": [value + 0.8 for value in close],
            "Low": [value - 0.8 for value in close],
            "Close": close,
            "Adj Close": close,
            "Volume": [1_000_000.0 for _ in close],
        },
        index=index,
    )


def _rsi_ma_bb_bullish_frame() -> pd.DataFrame:
    frame = _rsi_ma_bb_unit_bullish_frame().copy()
    frame["Adj Close"] = frame["Close"]
    return frame


def _rsi_ma_bb_bearish_frame() -> pd.DataFrame:
    frame = _rsi_ma_bb_unit_bearish_frame().copy()
    frame["Adj Close"] = frame["Close"]
    return frame


def _rsi_ma_bb_unit_bullish_frame() -> pd.DataFrame:
    from tests.test_rsi_ma_bb_screen import _bullish_bb_frame

    return _bullish_bb_frame()


def _rsi_ma_bb_unit_bearish_frame() -> pd.DataFrame:
    from tests.test_rsi_ma_bb_screen import _bearish_bb_frame

    return _bearish_bb_frame()


def _active_base_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-10-01", periods=160, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)
    close.extend(
        [
            115.0,
            114.4,
            114.8,
            113.7,
            114.2,
            113.8,
            114.3,
            113.9,
            114.5,
            114.1,
            113.6,
            114.0,
            113.5,
            114.2,
            113.9,
            114.4,
            114.1,
            113.7,
            114.0,
            113.8,
            114.2,
            114.0,
            113.9,
            114.1,
            114.0,
        ]
    )
    while len(close) < len(index):
        step = len(close) - 126
        close.append(114.0 + ((step % 6) * 0.18))

    open_values = [value - 0.25 for value in close]
    high = [value + 0.65 for value in close]
    low = [value - 0.65 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.2
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.0

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 118.9)
        low[idx] = max(low[idx], 108.0)
        close[idx] = min(close[idx], 118.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _active_cup_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-09-01", periods=145, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)
    close.extend(
        [
            116.0,
            114.0,
            112.0,
            110.0,
            108.0,
            106.0,
            104.0,
            102.0,
            100.0,
            98.0,
            96.0,
            95.0,
            94.5,
            94.0,
            93.5,
            93.5,
            93.8,
            94.0,
            94.2,
            94.5,
            95.0,
            95.5,
            96.0,
            97.0,
            98.0,
            99.0,
            100.0,
            101.0,
            102.0,
            103.0,
            104.5,
            106.0,
            108.0,
            110.0,
            111.5,
            112.5,
            113.5,
            114.2,
            114.8,
            115.2,
            115.5,
            115.8,
            116.0,
            116.2,
        ]
    )

    open_values = [value - 0.25 for value in close]
    high = [value + 0.75 for value in close]
    low = [value - 0.75 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.3
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.5

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 118.9)
        close[idx] = min(close[idx], 117.0)
        low[idx] = max(low[idx], 92.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _active_double_bottom_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-09-01", periods=135, freq="B")
    close: list[float] = []
    for idx in range(100):
        close.append(80.0 + (idx * 0.28))
    close.append(118.0)
    close.extend(
        [
            114.0,
            110.0,
            106.0,
            102.0,
            99.0,
            96.0,
            94.0,
            95.0,
            98.0,
            101.0,
            104.0,
            107.0,
            110.0,
            111.5,
            112.0,
            112.6,
            111.8,
            110.0,
            107.0,
            104.0,
            100.0,
            96.0,
            91.0,
            92.0,
            95.0,
            98.0,
            101.0,
            104.0,
            107.0,
            109.0,
            110.0,
            110.5,
            111.0,
            111.2,
        ]
    )

    open_values = [value - 0.25 for value in close]
    high = [value + 0.85 for value in close]
    low = [value - 0.85 for value in close]
    volume = [1_000_000.0 for _ in close]

    candidate_index = 100
    open_values[candidate_index] = 117.3
    close[candidate_index] = 118.0
    high[candidate_index] = 120.0
    low[candidate_index] = 116.5

    for idx in range(candidate_index + 1, len(close)):
        high[idx] = min(high[idx], 112.85)
        low[idx] = max(low[idx], 89.8)
        close[idx] = min(close[idx], 112.1)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high,
            "Low": low,
            "Close": close,
            "Adj Close": close,
            "Volume": volume,
        },
        index=index,
    )


def _weekly_tight_close_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-06", periods=105, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for week in range(21):
        week_dates = index[week * 5 : (week + 1) * 5]
        if week < 17:
            week_open = 100.0 + (week * 1.2)
            week_high = week_open + 3.0
            week_low = week_open - 3.0
            week_close = week_open + 1.0
        elif week == 17:
            week_open = 120.5
            week_high = 122.0
            week_low = 119.0
            week_close = 121.0
        elif week == 18:
            week_open = 120.8
            week_high = 122.2
            week_low = 119.4
            week_close = 121.2
        elif week == 19:
            week_open = 121.0
            week_high = 122.1
            week_low = 119.5
            week_close = 121.1
        else:
            week_open = 121.8
            week_high = 123.8
            week_low = 120.9
            week_close = 123.1

        for day_index, _date in enumerate(week_dates):
            open_values.append(week_open + (day_index * 0.02))
            high_values.append(week_high - (0.1 * (4 - day_index)))
            low_values.append(week_low + (0.1 * day_index))
            close_values.append(week_close if day_index == 4 else week_open + (day_index * 0.15))
            volume_values.append(1_000_000.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _three_weeks_tight_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-06", periods=30, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    weekly_closes = [100.0, 108.0, 114.0, 120.0, 120.8, 121.4]
    weekly_highs = [101.0, 109.0, 115.0, 121.0, 121.7, 121.9]
    weekly_lows = [98.5, 106.5, 112.5, 118.0, 119.8, 120.2]

    for week in range(6):
        week_dates = index[week * 5 : (week + 1) * 5]
        week_open = weekly_closes[week] - 1.0
        week_close = weekly_closes[week]
        week_high = weekly_highs[week]
        week_low = weekly_lows[week]
        for day_index, _date in enumerate(week_dates):
            open_values.append(week_open + (day_index * 0.1))
            high_values.append(week_high - (0.1 * (4 - day_index)))
            low_values.append(week_low + (0.1 * day_index))
            close_values.append(week_close if day_index == 4 else week_open + (day_index * 0.2))
            volume_values.append(1_000_000.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _rti_frame() -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=8, freq="B")
    close_values = [100.0 + (idx * 0.4) for idx in range(8)]
    open_values = [value - 0.2 for value in close_values]
    volume_values = [1_000_000.0 for _ in close_values]
    ranges = [6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.2, 1.8]
    high_values = [close + (bar_range / 2.0) for close, bar_range in zip(close_values, ranges)]
    low_values = [close - (bar_range / 2.0) for close, bar_range in zip(close_values, ranges)]

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


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
                close_value_offset = 0.05 if positive_cci else (-0.08 if idx % 2 == 0 else -0.16)
            else:
                drift = 0.35 if positive_cci else -0.35
                center = 115.0 + ((idx - 55) * drift)
                bar_range = 0.8
                close_value_offset = 0.18 if idx % 2 == 0 else 0.08
            close_offset = close_value_offset
        open_value = center - (bar_range * 0.2)
        close_value = center + close_offset
        high_value = max(open_value, close_value) + (bar_range * 0.4)
        low_value = min(open_value, close_value) - (bar_range * 0.4)
        open_values.append(open_value)
        close_values.append(close_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(1_000_000.0)

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _sean_breakout_frame(*, close_ok: bool = True, ema_ok: bool = True, volume_ok: bool = True, adr_ok: bool = True) -> pd.DataFrame:
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


def _sepa_vcp_stock_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-02", periods=320, freq="B")
    close_values: list[float] = []
    for idx in range(len(index)):
        if idx < 315:
            close_values.append(80.0 + (idx * 0.35))
        else:
            close_values.extend([189.2, 189.8, 190.1, 189.9, 190.3])
            break
    return pd.DataFrame(
        {
            "Open": [value - 0.35 for value in close_values],
            "High": [value + 0.85 for value in close_values],
            "Low": [value - 0.95 for value in close_values],
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": [1_200_000.0 for _ in close_values],
        },
        index=index,
    )


def _sepa_vcp_benchmark_frame() -> pd.DataFrame:
    index = pd.date_range(start="2024-01-02", periods=320, freq="B")
    close_values = [100.0 + (idx * 0.05) for idx in range(len(index))]
    return pd.DataFrame(
        {
            "Open": [value - 0.1 for value in close_values],
            "High": [value + 0.4 for value in close_values],
            "Low": [value - 0.4 for value in close_values],
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": [2_000_000.0 for _ in close_values],
        },
        index=index,
    )


def _vcs_frame(*, compressed: bool, variant: int = 0) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=90, freq="B")
    close_values: list[float] = []
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 70:
            center = 100.0 + (idx * (0.35 + (0.03 * variant)))
            bar_range = 5.0 + (variant * 0.4) - min(idx * 0.02, 1.0)
            volume = 2_000_000.0 + (variant * 100_000.0) - (idx * 2_000.0)
        else:
            if compressed:
                center = 124.0 + ((idx - 70) * 0.02)
                bar_range = 0.35
                volume = 450_000.0
            else:
                center = 124.0 + ((idx - 70) * (0.25 + (0.05 * variant)))
                bar_range = 1.6 + (variant * 1.2) + (((idx - 70) % 3) * 0.7)
                volume = 1_200_000.0 + (variant * 300_000.0)
        open_value = center - (bar_range * 0.35)
        close_value = center + (bar_range * (0.25 if idx % 2 == 0 else -0.15))
        high_value = max(open_value, close_value) + (bar_range * 0.3)
        low_value = min(open_value, close_value) - (bar_range * 0.35)
        open_values.append(open_value)
        close_values.append(close_value)
        high_values.append(high_value)
        low_values.append(low_value)
        volume_values.append(max(volume, 100_000.0))

    return pd.DataFrame(
        {
            "Open": open_values,
            "High": high_values,
            "Low": low_values,
            "Close": close_values,
            "Adj Close": close_values,
            "Volume": volume_values,
        },
        index=index,
    )


def _high_tight_flag_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 200:
            close_value = 45.0 + (idx * 0.4)
            bar_range = 8.5 - min(idx * 0.01, 2.0)
            volume_value = 1_700_000.0 + (idx * 2_000.0)
        elif idx < 220:
            close_value = 100.0 + ((idx - 200) * 1.0)
            bar_range = 6.0 - ((idx - 200) * 0.08)
            volume_value = 2_000_000.0 - ((idx - 200) * 12_000.0)
        else:
            close_value = 145.0 + ((idx - 220) * (2.2 if passes else 0.8))
            bar_range = 4.2 - ((idx - 220) * 0.05)
            volume_value = 1_760_000.0 - ((idx - 220) * 20_000.0)
        open_value = close_value - (bar_range * 0.15)
        high_value = close_value + (bar_range * 0.5)
        low_value = close_value - (bar_range * 0.5)
        open_values.append(open_value)
        high_values.append(high_value)
        low_values.append(low_value)
        close_values.append(close_value)
        volume_values.append(max(volume_value, 500_000.0))

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


def _leif_benchmark_frame() -> pd.DataFrame:
    return pd.DataFrame({"Close": [100.0 + (index_value * 0.18) for index_value in range(260)]}, index=pd.date_range(start="2025-01-02", periods=260, freq="B"))


def _leif_high_tight_flag_frame(*, passes: bool) -> pd.DataFrame:
    index = pd.date_range(start="2025-01-02", periods=260, freq="B")
    open_values: list[float] = []
    high_values: list[float] = []
    low_values: list[float] = []
    close_values: list[float] = []
    volume_values: list[float] = []

    for idx, _date in enumerate(index):
        if idx < 220:
            close_value = 40.0 + (idx * 0.12)
            volume_value = 1_000_000.0 + ((idx % 5) * 15_000.0)
        elif idx < 240:
            close_value = 66.4 + ((idx - 220) * 3.2)
            volume_value = 2_200_000.0 + ((idx - 220) * 12_000.0)
        elif idx < 259:
            flag_closes = [
                126.8,
                125.9,
                125.2,
                123.8,
                122.1,
                120.6,
                118.4,
                116.5,
                114.2,
                112.4,
                110.8,
                111.6,
                112.7,
                114.0,
                115.4,
                117.1,
                118.9,
                120.5,
                122.4,
            ]
            close_value = flag_closes[idx - 240]
            volume_value = 1_050_000.0 - ((idx - 240) * 10_000.0)
        else:
            close_value = 127.2 if passes else 126.7
            volume_value = 2_450_000.0 if passes else 1_300_000.0
        open_value = close_value * 0.992
        high_value = close_value * 1.01
        low_value = close_value * 0.99
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


class AdHocScreenServiceTests(unittest.TestCase):
    def test_run_prefetches_once_and_evaluates_selected_screeners(self) -> None:
        ticker_frame = _frame("2026-01-01", 40)
        benchmark_frame = _frame("2026-01-01", 40)
        captured_bundles: list[tuple[str, int]] = []

        def _evaluator(bundle):
            captured_bundles.append((bundle.ticker, len(bundle.bars)))
            return ScreenerEvaluationResult(
                passed=True,
                metrics={"close": float(bundle.bars["Close"].iloc[-1])},
                reasons=("ok",),
                hit={"ticker": bundle.ticker, "signal": "demo"},
            )

        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        service.catalog = {
            "demo": ScreenerSpec(
                id="demo",
                required_inputs=("daily_bars", "benchmark_bars", "metadata"),
                lookback_trading_days=25,
                warmup_trading_days=5,
                evaluator=_evaluator,
            )
        }

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ) as load_windows, patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology"}},
        ):
            payload = service.run(
                ticker="aapl",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["demo"],
            )

        self.assertEqual(captured_bundles, [("AAPL", 40)])
        self.assertEqual(payload["ticker"], "AAPL")
        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["timing"]["market_data_tickers_loaded"], ["AAPL", "SPY"])
        load_windows.assert_called_once()

    def test_run_rejects_unknown_screener(self) -> None:
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        service.catalog = {}

        with self.assertRaisesRegex(ValueError, "Unknown screener id"):
            service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["missing"],
            )

    def test_run_supports_ftd_sweep_catalog_entry(self) -> None:
        ticker_frame = _ftd_sweep_frame().iloc[:43]
        benchmark_frame = _frame("2026-01-01", 60)
        service = AdHocScreenService(
            app_config=AppConfig(
                ftd_sweep_history_days=90,
                ftd_sweep_min_avg_volume=0,
                ftd_sweep_min_avg_dollar_volume=0.0,
            ),
            database_url="postgres://unit-test",
        )

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 3, 4),
                screener_ids=["ftd_sweep"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "ftd_sweep")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_fearzone_catalog_entry(self) -> None:
        ticker_frame = _fearzone_frame()
        benchmark_frame = _frame("2025-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 30),
                screener_ids=["fearzone"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "fearzone")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_fearzone_zeiierman_catalog_entry(self) -> None:
        ticker_frame = _fearzone_zeiierman_frame()
        benchmark_frame = _frame("2025-01-01", 220)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 11, 4),
                screener_ids=["fearzone_zeiierman"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "fearzone_zeiierman")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_hve_catalog_entry(self) -> None:
        ticker_frame = _hve_frame()
        benchmark_frame = _frame("2025-01-02", 320)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 3, 25),
                screener_ids=["hve"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "hve")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_hv1_only_signal_for_hve_catalog_entry(self) -> None:
        ticker_frame = _hv1_only_frame()
        benchmark_frame = _frame("2024-01-02", 320)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 3, 24),
                screener_ids=["hve"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "hve")
        self.assertTrue(payload["screeners"][0]["passed"])
        self.assertEqual(payload["screeners"][0]["hit"]["signal_kind"], "HV1")

    def test_rs_rating_helper_returns_expected_threshold_behavior(self) -> None:
        strong_metrics = _compute_latest_rs_rating(
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in _rs_rating_stock_frame()["Close"].items()
            ],
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in _rs_rating_benchmark_frame()["Close"].items()
            ],
        )

        self.assertIsNotNone(strong_metrics)
        assert strong_metrics is not None
        self.assertGreaterEqual(strong_metrics[1], 0.0)
        self.assertGreaterEqual(strong_metrics[1], 90.0)
        self.assertLessEqual(strong_metrics[1], 99.0)

        weak_frame = _frame("2024-01-01", 320)
        weak_benchmark = _frame("2024-01-01", 320)
        weak_metrics = _compute_latest_rs_rating(
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in weak_frame["Close"].items()
            ],
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in weak_benchmark["Close"].items()
            ],
        )

        self.assertIsNotNone(weak_metrics)
        assert weak_metrics is not None
        self.assertGreaterEqual(weak_metrics[1], 0.0)
        self.assertLess(weak_metrics[1], 90.0)
        self.assertLessEqual(weak_metrics[1], 99.0)

    def test_rs_rating_helper_supports_shorter_history_like_pine_caps(self) -> None:
        shorter_stock = _rs_rating_stock_frame().iloc[-180:]
        shorter_benchmark = _rs_rating_benchmark_frame().iloc[-180:]
        metrics = _compute_latest_rs_rating(
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in shorter_stock["Close"].items()
            ],
            [
                {
                    "formatted_date": index.date().isoformat(),
                    "close": float(close),
                }
                for index, close in shorter_benchmark["Close"].items()
            ],
        )

        self.assertIsNotNone(metrics)
        assert metrics is not None
        self.assertGreaterEqual(metrics[1], 0.0)
        self.assertLessEqual(metrics[1], 99.0)

    def test_weekly_rs_before_price_context_requires_price_not_at_new_high(self) -> None:
        before_price_stock, before_price_benchmark = _weekly_rs_rows(before_price=True)
        before_price_context = _compute_weekly_rs_before_price_context(
            before_price_stock,
            before_price_benchmark,
            weekly_lookback_weeks=52,
            recent_signal_weeks=4,
        )

        self.assertIsNotNone(before_price_context)
        assert before_price_context is not None
        self.assertTrue(bool(before_price_context["recent_weekly_before_price"]))

        plain_new_high_stock, plain_new_high_benchmark = _weekly_rs_rows(before_price=False)
        plain_new_high_context = _compute_weekly_rs_before_price_context(
            plain_new_high_stock,
            plain_new_high_benchmark,
            weekly_lookback_weeks=52,
            recent_signal_weeks=4,
        )

        self.assertIsNotNone(plain_new_high_context)
        assert plain_new_high_context is not None
        self.assertFalse(bool(plain_new_high_context["recent_weekly_before_price"]))

    def test_run_supports_inside_dryup_catalog_entry(self) -> None:
        ticker_frame = _inside_dryup_frame()
        benchmark_frame = _frame("2025-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 31),
                screener_ids=["inside_dryup"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "inside_dryup")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_inside_dryup_v2_catalog_entry(self) -> None:
        ticker_frame = _inside_dryup_v2_frame(passes=True)
        benchmark_frame = _frame("2025-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 31),
                screener_ids=["inside_dryup_v2"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "inside_dryup_v2")
        self.assertLess(payload["screeners"][0]["hit"]["price_volume_ratio"], 0.30)
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_td9_bullish_catalog_entry(self) -> None:
        ticker_frame = _bullish_td9_frame()
        benchmark_frame = _frame("2026-01-02", 40)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 1, 29),
                screener_ids=["td9_bullish"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "td9_bullish")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_td9_bearish_catalog_entry(self) -> None:
        ticker_frame = _bearish_td9_frame()
        benchmark_frame = _frame("2026-01-02", 40)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"TSLA": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"TSLA": {"ticker": "TSLA", "sector": "Consumer Cyclical", "industry": "Auto", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="TSLA",
                as_of_date=dt.date(2026, 1, 29),
                screener_ids=["td9_bearish"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "td9_bearish")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_macd_golden_cross_catalog_entry(self) -> None:
        ticker_frame = _macd_golden_cross_frame()
        benchmark_frame = _frame("2026-01-02", 80)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 3, 26),
                screener_ids=["macd_golden_cross"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "macd_golden_cross")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_macd_dead_cross_catalog_entry(self) -> None:
        ticker_frame = _macd_dead_cross_frame()
        benchmark_frame = _frame("2026-01-02", 80)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"TSLA": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"TSLA": {"ticker": "TSLA", "sector": "Consumer Cyclical", "industry": "Auto", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="TSLA",
                as_of_date=dt.date(2026, 3, 26),
                screener_ids=["macd_dead_cross"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "macd_dead_cross")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_rsi_ma_bb_bullish_catalog_entry(self) -> None:
        ticker_frame = _rsi_ma_bb_bullish_frame()
        benchmark_frame = _frame("2026-01-02", 80)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 4, 9),
                screener_ids=["rsi_ma_bb_bullish"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "rsi_ma_bb_bullish")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_rsi_ma_bb_bearish_catalog_entry(self) -> None:
        ticker_frame = _rsi_ma_bb_bearish_frame()
        benchmark_frame = _frame("2026-01-02", 80)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"TSLA": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"TSLA": {"ticker": "TSLA", "sector": "Consumer Cyclical", "industry": "Auto", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="TSLA",
                as_of_date=dt.date(2026, 4, 9),
                screener_ids=["rsi_ma_bb_bearish"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "rsi_ma_bb_bearish")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_base_detection_catalog_entry(self) -> None:
        ticker_frame = _active_base_frame()
        benchmark_frame = _frame("2026-01-02", 160)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 5, 12),
                screener_ids=["base_detection"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "base_detection")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_cup_detection_catalog_entry(self) -> None:
        ticker_frame = _active_cup_frame()
        benchmark_frame = _frame("2026-01-02", 145)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 5, 12),
                screener_ids=["cup_detection"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "cup_detection")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_double_bottom_detection_catalog_entry(self) -> None:
        ticker_frame = _active_double_bottom_frame()
        benchmark_frame = _frame("2026-01-02", 135)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 5, 12),
                screener_ids=["double_bottom_detection"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "double_bottom_detection")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_weekly_tight_close_catalog_entry(self) -> None:
        ticker_frame = _weekly_tight_close_frame()
        benchmark_frame = _frame("2026-01-02", 105)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 5, 23),
                screener_ids=["weekly_tight_close"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "weekly_tight_close")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_weekly_tight_close_breakout_catalog_entry(self) -> None:
        ticker_frame = _weekly_tight_close_frame()
        benchmark_frame = _frame("2026-01-02", 105)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 6, 27),
                screener_ids=["weekly_tight_close_breakout"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "weekly_tight_close_breakout")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_three_weeks_tight_catalog_entry(self) -> None:
        ticker_frame = _three_weeks_tight_frame()
        benchmark_frame = _frame("2026-01-02", 40)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 2, 14),
                screener_ids=["three_weeks_tight"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "three_weeks_tight")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_rti_catalog_entry(self) -> None:
        ticker_frame = _rti_frame()
        benchmark_frame = _frame("2026-01-02", 20)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 1, 13),
                screener_ids=["rti"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "rti")
        self.assertEqual(payload["screeners"][0]["hit"]["signal_kind"], "range_expansion")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_bb_squeeze_catalog_entry(self) -> None:
        ticker_frame = _bb_squeeze_frame(squeeze=True, positive_cci=True)
        benchmark_frame = _frame("2026-01-02", 120)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 4, 23),
                screener_ids=["bb_squeeze"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "bb_squeeze")
        self.assertEqual(payload["screeners"][0]["hit"]["signal_kind"], "positive_cci")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_high_tight_flag_catalog_entry(self) -> None:
        ticker_frame = _high_tight_flag_frame(passes=True)
        benchmark_frame = _frame("2026-01-02", 260)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 31),
                screener_ids=["high_tight_flag"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "high_tight_flag")
        self.assertGreater(payload["screeners"][0]["hit"]["runup_40_ratio"], 1.9)
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_leif_high_tight_flag_catalog_entry(self) -> None:
        ticker_frame = _leif_high_tight_flag_frame(passes=True)
        benchmark_frame = _leif_benchmark_frame()
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 12, 31),
                screener_ids=["leif_high_tight_flag"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "leif_high_tight_flag")
        self.assertGreaterEqual(payload["screeners"][0]["hit"]["score"], 5.0)
        self.assertGreaterEqual(payload["screeners"][0]["hit"]["rs_rating"], 80.0)
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_sean_breakout_catalog_entry(self) -> None:
        ticker_frame = _sean_breakout_frame()
        benchmark_frame = _frame("2026-01-02", 120)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 4, 23),
                screener_ids=["sean_breakout"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "sean_breakout")
        self.assertEqual(payload["screeners"][0]["hit"]["signal_kind"], "sean_breakout")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_sepa_vcp_catalog_entry(self) -> None:
        ticker_frame = _sepa_vcp_stock_frame()
        benchmark_frame = _sepa_vcp_benchmark_frame()
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 3, 24),
                screener_ids=["sepa_vcp"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "sepa_vcp")
        self.assertEqual(payload["screeners"][0]["hit"]["tpr_status"], "PASSED")
        self.assertEqual(payload["screeners"][0]["hit"]["buy_risk_status"], "Low Risk")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_vcs_setup_stage_catalog_entry(self) -> None:
        ticker_frame = _vcs_frame(compressed=False, variant=1)
        benchmark_frame = _frame("2026-01-02", 120)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 5, 7),
                screener_ids=["vcs_setup_stage"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "vcs_setup_stage")
        self.assertEqual(payload["screeners"][0]["hit"]["stage"], "setup")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_supports_vcs_critical_tightness_catalog_entry(self) -> None:
        ticker_frame = _vcs_frame(compressed=True)
        benchmark_frame = _frame("2026-01-02", 120)
        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame, "SPY": benchmark_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology", "industry": "Software", "exchange": "NASDAQ"}},
        ):
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2025, 5, 7),
                screener_ids=["vcs_critical_tightness"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "vcs_critical_tightness")
        self.assertEqual(payload["screeners"][0]["hit"]["stage"], "critical")
        self.assertTrue(payload["screeners"][0]["passed"])

    def test_run_falls_back_to_internet_when_benchmark_missing_in_db(self) -> None:
        ticker_frame = _frame("2026-01-01", 40)
        benchmark_frame = _frame("2026-01-01", 40)

        def _evaluator(bundle):
            return ScreenerEvaluationResult(
                passed=True,
                metrics={"benchmark_close": float(bundle.benchmark_bars["Close"].iloc[-1])},
                reasons=("ok",),
                hit={"ticker": bundle.ticker},
            )

        service = AdHocScreenService(app_config=AppConfig(), database_url="postgres://unit-test")
        service.catalog = {
            "demo": ScreenerSpec(
                id="demo",
                required_inputs=("daily_bars", "benchmark_bars", "metadata"),
                lookback_trading_days=25,
                warmup_trading_days=5,
                evaluator=_evaluator,
            )
        }

        with patch(
            "src.webapp.services.ad_hoc_screen_service.load_many_ticker_windows",
            return_value={"AAPL": ticker_frame},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service.load_ticker_metadata_map",
            return_value={"AAPL": {"ticker": "AAPL", "sector": "Technology"}},
        ), patch(
            "src.webapp.services.ad_hoc_screen_service._download_history_frame",
            return_value=benchmark_frame,
        ) as download_history:
            payload = service.run(
                ticker="AAPL",
                as_of_date=dt.date(2026, 2, 27),
                screener_ids=["demo"],
            )

        self.assertEqual(payload["summary"]["passed_screener_count"], 1)
        self.assertEqual(payload["screeners"][0]["id"], "demo")
        self.assertTrue(payload["screeners"][0]["passed"])
        download_history.assert_called_once()
