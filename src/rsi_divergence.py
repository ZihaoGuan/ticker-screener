from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RsiBearishDivergenceSignal:
    signal_date: str
    signal_price: float
    previous_signal_date: str
    previous_signal_price: float
    signal_rsi: float
    previous_signal_rsi: float
    bars_apart: int
    price_change_pct: float
    overbought_threshold: float
    is_overbought: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date,
            "signal_price": self.signal_price,
            "previous_signal_date": self.previous_signal_date,
            "previous_signal_price": self.previous_signal_price,
            "signal_rsi": self.signal_rsi,
            "previous_signal_rsi": self.previous_signal_rsi,
            "bars_apart": self.bars_apart,
            "price_change_pct": self.price_change_pct,
            "overbought_threshold": self.overbought_threshold,
            "is_overbought": self.is_overbought,
        }


def compute_wilder_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    delta = numeric.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    avg_up = up.ewm(alpha=1 / float(length), adjust=False, min_periods=length).mean()
    avg_down = down.ewm(alpha=1 / float(length), adjust=False, min_periods=length).mean()
    rs = avg_up / avg_down.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_down != 0, 100.0)
    rsi = rsi.where(avg_up != 0, 0.0).where(~((avg_down == 0) & (avg_up > 0)), 100.0)
    return rsi


def find_latest_regular_bearish_rsi_divergence(
    frame: pd.DataFrame,
    *,
    rsi_length: int = 14,
    minimum_bars: int = 6,
    maximum_bars: int = 1000,
    minimum_price_change_pct: float = 0.1,
    maximum_price_change_pct: float = 100.0,
    overbought_threshold: float = 75.0,
) -> RsiBearishDivergenceSignal | None:
    if frame.empty or len(frame) < max(30, rsi_length + 10):
        return None

    bars = frame.copy()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    bars = bars.sort_index()
    required = {"High"}
    if not required.issubset(bars.columns):
        return None

    oscillator_high = compute_wilder_rsi(bars["High"], length=rsi_length)
    tops: list[dict[str, Any]] = []

    for idx in range(4, len(bars)):
        center = idx - 2
        window = oscillator_high.iloc[idx - 4 : idx + 1]
        if window.isna().any():
            continue
        values = window.tolist()
        if not (values[0] < values[2] and values[1] < values[2] and values[2] > values[3] and values[2] > values[4]):
            continue
        tops.append(
            {
                "center_index": center,
                "date": bars.index[center].date().isoformat(),
                "price": float(bars["High"].iloc[center]),
                "rsi": float(oscillator_high.iloc[center]),
            }
        )

    if len(tops) < 2:
        return None

    latest_signal: RsiBearishDivergenceSignal | None = None
    for current, previous in zip(tops[1:], tops[:-1], strict=False):
        if current["price"] <= previous["price"]:
            continue
        if current["rsi"] >= previous["rsi"]:
            continue
        bars_apart = int(current["center_index"] - previous["center_index"])
        if bars_apart < int(minimum_bars) or bars_apart > int(maximum_bars):
            continue
        if previous["price"] <= 0:
            continue
        price_change_pct = abs(current["price"] - previous["price"]) / previous["price"] * 100.0
        if price_change_pct < float(minimum_price_change_pct) or price_change_pct > float(maximum_price_change_pct):
            continue
        latest_signal = RsiBearishDivergenceSignal(
            signal_date=str(current["date"]),
            signal_price=round(float(current["price"]), 2),
            previous_signal_date=str(previous["date"]),
            previous_signal_price=round(float(previous["price"]), 2),
            signal_rsi=round(float(current["rsi"]), 2),
            previous_signal_rsi=round(float(previous["rsi"]), 2),
            bars_apart=bars_apart,
            price_change_pct=round(float(price_change_pct), 2),
            overbought_threshold=float(overbought_threshold),
            is_overbought=bool(float(current["rsi"]) >= float(overbought_threshold)),
        )

    return latest_signal
