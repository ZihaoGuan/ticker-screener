from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RsiDivergenceTopSignal:
    signal_date: str
    signal_price: float
    previous_signal_date: str
    previous_signal_price: float
    signal_rsi: float
    previous_signal_rsi: float
    bars_apart: int
    bars_since_signal: int
    active_bars: int
    fresh_bars: int
    reset_rsi_threshold: float
    current_close: float
    current_rsi: float
    current_ema21: float
    distance_from_signal_pct: float
    state: str
    label: str
    lift_reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date,
            "signal_price": self.signal_price,
            "previous_signal_date": self.previous_signal_date,
            "previous_signal_price": self.previous_signal_price,
            "signal_rsi": self.signal_rsi,
            "previous_signal_rsi": self.previous_signal_rsi,
            "bars_apart": self.bars_apart,
            "bars_since_signal": self.bars_since_signal,
            "active_bars": self.active_bars,
            "fresh_bars": self.fresh_bars,
            "reset_rsi_threshold": self.reset_rsi_threshold,
            "current_close": self.current_close,
            "current_rsi": self.current_rsi,
            "current_ema21": self.current_ema21,
            "distance_from_signal_pct": self.distance_from_signal_pct,
            "state": self.state,
            "label": self.label,
            "lift_reason": self.lift_reason,
        }


def compute_wilder_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    delta = numeric.diff()
    gain = delta.where(delta >= 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1 / float(length), adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1 / float(length), adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.where(avg_loss != 0, 100.0)
    rsi = rsi.where(avg_gain != 0, 0.0).where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    return rsi


def find_latest_bearish_rsi_divergence_top(
    frame: pd.DataFrame,
    *,
    rsi_length: int = 14,
    top_rsi_min: float = 60.0,
    active_bars: int = 35,
    fresh_bars: int = 10,
    reset_rsi_threshold: float = 45.0,
    below_ema_bars_needed: int = 3,
    invalid_break_pct: float = 2.0,
) -> RsiDivergenceTopSignal | None:
    if frame.empty or len(frame) < max(30, rsi_length + 10):
        return None

    bars = frame.copy()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    bars = bars.sort_index()
    required = {"Close"}
    if not required.issubset(bars.columns):
        return None

    close = pd.to_numeric(bars["Close"], errors="coerce")
    rsi = compute_wilder_rsi(close, length=rsi_length)
    ema21 = close.ewm(span=21, adjust=False).mean()

    tops: list[dict[str, Any]] = []
    previous_top = False
    for idx in range(4, len(bars)):
        center = idx - 2
        values = rsi.iloc[idx - 4 : idx + 1]
        if values.isna().any():
            previous_top = False
            continue
        left_avg = (float(values.iloc[0]) + float(values.iloc[1])) / 2.0
        center_value = float(values.iloc[2])
        right_avg = (float(values.iloc[3]) + float(values.iloc[4])) / 2.0
        top = left_avg < center_value and right_avg < center_value and center_value > float(top_rsi_min) and not previous_top
        previous_top = top
        if not top:
            continue
        tops.append(
            {
                "center_index": center,
                "date": bars.index[center].date().isoformat(),
                "price": float(close.iloc[center]),
                "rsi": center_value,
            }
        )

    if len(tops) < 2:
        return None

    latest_pair: tuple[dict[str, Any], dict[str, Any]] | None = None
    for current, previous in zip(tops[1:], tops[:-1], strict=False):
        if current["rsi"] >= previous["rsi"]:
            continue
        if current["price"] <= previous["price"]:
            continue
        latest_pair = (current, previous)

    if latest_pair is None:
        return None

    current, previous = latest_pair
    latest_close = float(close.iloc[-1])
    latest_rsi = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else float("nan")
    latest_ema21 = float(ema21.iloc[-1]) if pd.notna(ema21.iloc[-1]) else float("nan")
    bars_since_signal = int(len(bars) - 1 - current["center_index"])
    bars_apart = int(current["center_index"] - previous["center_index"])
    distance_from_signal_pct = ((latest_close / current["price"]) - 1.0) * 100.0 if current["price"] else 0.0

    consecutive_below_ema = 0
    for value_close, value_ema in zip(reversed(close.tolist()), reversed(ema21.tolist()), strict=False):
        if pd.isna(value_close) or pd.isna(value_ema) or float(value_close) >= float(value_ema):
            break
        consecutive_below_ema += 1

    lift_reason: str | None = None
    lift_by_ema = consecutive_below_ema >= int(below_ema_bars_needed)
    lift_by_rsi = pd.notna(rsi.iloc[-1]) and float(rsi.iloc[-1]) < float(reset_rsi_threshold)
    lift_by_age = bars_since_signal > int(active_bars)
    invalidated = (
        pd.notna(rsi.iloc[-1])
        and latest_close > current["price"] * (1.0 + (float(invalid_break_pct) / 100.0))
        and float(rsi.iloc[-1]) > current["rsi"]
    )

    if invalidated:
        state = "invalidated"
        label = "Top Invalidated"
    elif lift_by_rsi:
        state = "lifted"
        label = "Top Lifted"
        lift_reason = "rsi_reset"
    elif lift_by_ema:
        state = "lifted"
        label = "Top Lifted"
        lift_reason = "below_ema21"
    elif lift_by_age:
        state = "lifted"
        label = "Top Lifted"
        lift_reason = "expired"
    elif bars_since_signal <= int(fresh_bars):
        state = "fresh_top_warning"
        label = "Fresh Top Warning"
    else:
        state = "active_top_warning"
        label = "Active Top Warning"

    return RsiDivergenceTopSignal(
        signal_date=str(current["date"]),
        signal_price=round(float(current["price"]), 2),
        previous_signal_date=str(previous["date"]),
        previous_signal_price=round(float(previous["price"]), 2),
        signal_rsi=round(float(current["rsi"]), 2),
        previous_signal_rsi=round(float(previous["rsi"]), 2),
        bars_apart=bars_apart,
        bars_since_signal=bars_since_signal,
        active_bars=int(active_bars),
        fresh_bars=int(fresh_bars),
        reset_rsi_threshold=float(reset_rsi_threshold),
        current_close=round(latest_close, 2),
        current_rsi=round(latest_rsi, 2),
        current_ema21=round(latest_ema21, 2),
        distance_from_signal_pct=round(float(distance_from_signal_pct), 2),
        state=state,
        label=label,
        lift_reason=lift_reason,
    )
