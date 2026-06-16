from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from typing import Literal

import pandas as pd


MovingAverageType = Literal["sma", "ema"]


@dataclass(frozen=True)
class ExtensionPeak:
    trade_date: str
    close: float
    moving_average: float
    extension_pct: float
    threshold_state: str


def normalize_ma_type(value: str) -> MovingAverageType:
    normalized = str(value or "sma").strip().lower()
    if normalized not in {"sma", "ema"}:
        raise ValueError(f"Unsupported moving average type: {value}")
    return normalized  # type: ignore[return-value]


def build_moving_average(series: pd.Series, *, length: int, ma_type: str) -> pd.Series:
    if int(length) <= 0:
        raise ValueError("length must be positive")
    normalized = normalize_ma_type(ma_type)
    numeric = pd.to_numeric(series, errors="coerce")
    if normalized == "ema":
        return numeric.ewm(span=int(length), adjust=False).mean()
    return numeric.rolling(int(length), min_periods=int(length)).mean()


def resample_to_weekly(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    normalized = frame.copy()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    normalized = normalized.sort_index()
    weekly = normalized.resample("W-FRI").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    weekly = weekly.dropna(subset=["Open", "High", "Low", "Close"])
    return weekly


def compute_extension_frame(
    frame: pd.DataFrame,
    *,
    length: int,
    ma_type: str,
    warning_pct: float = 11.0,
    extreme_pct: float = 15.0,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    bars = frame.copy()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    bars = bars.sort_index()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column in bars.columns:
            bars[column] = pd.to_numeric(bars[column], errors="coerce")
    bars["moving_average"] = build_moving_average(bars["Close"], length=length, ma_type=ma_type)
    bars["extension_pct"] = ((bars["Close"] / bars["moving_average"]) - 1.0) * 100.0
    bars["warning_pct"] = float(warning_pct)
    bars["extreme_pct"] = float(extreme_pct)
    bars["is_warning"] = bars["extension_pct"] >= float(warning_pct)
    bars["is_extreme"] = bars["extension_pct"] >= float(extreme_pct)
    bars["threshold_state"] = "normal"
    bars.loc[bars["is_warning"], "threshold_state"] = "warning"
    bars.loc[bars["is_extreme"], "threshold_state"] = "extreme"
    return bars


def find_extension_peaks(
    frame: pd.DataFrame,
    *,
    length: int,
    ma_type: str,
    warning_pct: float = 11.0,
    extreme_pct: float = 15.0,
    min_extension_pct: float | None = None,
    max_extension_pct: float | None = None,
) -> list[ExtensionPeak]:
    enriched = compute_extension_frame(
        frame,
        length=length,
        ma_type=ma_type,
        warning_pct=warning_pct,
        extreme_pct=extreme_pct,
    )
    if enriched.empty or "extension_pct" not in enriched:
        return []

    series = enriched["extension_pct"]
    peaks: list[ExtensionPeak] = []
    for idx in range(1, len(enriched) - 1):
        current = series.iloc[idx]
        if pd.isna(current):
            continue
        if current < series.iloc[idx - 1] or current < series.iloc[idx + 1]:
            continue
        if min_extension_pct is not None and current < float(min_extension_pct):
            continue
        if max_extension_pct is not None and current > float(max_extension_pct):
            continue
        row = enriched.iloc[idx]
        moving_average = row["moving_average"]
        if pd.isna(moving_average):
            continue
        peaks.append(
            ExtensionPeak(
                trade_date=enriched.index[idx].date().isoformat(),
                close=round(float(row["Close"]), 2),
                moving_average=round(float(moving_average), 2),
                extension_pct=round(float(current), 2),
                threshold_state=str(row["threshold_state"]),
            )
        )
    return peaks


def filter_extension_window(
    frame: pd.DataFrame,
    *,
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame.copy()
    if not isinstance(filtered.index, pd.DatetimeIndex):
        filtered.index = pd.to_datetime(filtered.index)
    if start_date is not None:
        filtered = filtered[filtered.index.date >= start_date]
    if end_date is not None:
        filtered = filtered[filtered.index.date <= end_date]
    return filtered
