from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import pandas as pd

from .market_extension import resample_to_weekly


def normalize_price_frame(frame: pd.DataFrame, *, include_volume: bool = True) -> pd.DataFrame:
    required = ["High", "Low", "Close", "Volume"] if include_volume else ["Close"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    selected_columns = [available[column.lower()] for column in required]
    normalized = frame[selected_columns].copy()
    normalized.columns = required
    if include_volume:
        if "open" in available:
            normalized.insert(0, "Open", frame[available["open"]].copy())
        else:
            normalized.insert(0, "Open", normalized["Close"].copy())
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def to_weekly_price_frame(frame: pd.DataFrame, *, include_volume: bool = True) -> pd.DataFrame:
    normalized = normalize_price_frame(frame, include_volume=include_volume)
    if normalized.empty:
        return pd.DataFrame()
    if include_volume:
        return resample_to_weekly(normalized)
    aggregations: dict[str, str] = {"Close": "last"}
    if "Open" in normalized.columns:
        aggregations["Open"] = "first"
    if "High" in normalized.columns:
        aggregations["High"] = "max"
    if "Low" in normalized.columns:
        aggregations["Low"] = "min"
    weekly = normalized.resample("W-FRI").agg(aggregations)
    return weekly.dropna(subset=["Close"])


@contextmanager
def temporary_attr_overrides(target: Any, **overrides: Any) -> Iterator[None]:
    sentinel = object()
    previous: dict[str, Any] = {}
    for name, value in overrides.items():
        previous[name] = getattr(target, name, sentinel)
        setattr(target, name, value)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is sentinel:
                delattr(target, name)
            else:
                setattr(target, name, value)
