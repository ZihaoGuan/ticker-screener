from __future__ import annotations

import math
from typing import Iterable

import numpy as np

from .constants import ALL_RATING_METRICS
from .models import FundamentalsSnapshot, SectorMetricBaseline


def remove_outliers(values: Iterable[float], std_multiplier: float = 2.0) -> list[float]:
    filtered = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if not filtered:
        return []
    array = np.asarray(filtered, dtype=float)
    first_pass = array[np.abs(array - np.mean(array)) <= (std_multiplier * np.std(array))]
    if first_pass.size == 0:
        return []
    second_pass = first_pass[np.abs(first_pass - np.mean(first_pass)) <= (std_multiplier * np.std(first_pass))]
    return [float(value) for value in second_pass]


def build_sector_baselines(
    snapshots: Iterable[FundamentalsSnapshot],
    *,
    as_of_date,
) -> list[SectorMetricBaseline]:
    grouped: dict[tuple[str, str], list[float]] = {}
    for snapshot in snapshots:
        sector = str(snapshot.sector or "").strip()
        if not sector:
            continue
        for metric_name in ALL_RATING_METRICS:
            value = getattr(snapshot, metric_name)
            if value is None:
                continue
            grouped.setdefault((sector, metric_name), []).append(float(value))

    baselines: list[SectorMetricBaseline] = []
    for (sector, metric_name), values in grouped.items():
        filtered = remove_outliers(values, 2.0)
        if filtered:
            filtered_array = np.asarray(filtered, dtype=float)
            median_value = float(np.median(filtered_array))
            pct10_value = float(np.quantile(filtered_array, 0.1))
            pct90_value = float(np.quantile(filtered_array, 0.9))
            std_value = float(np.std(filtered_array, axis=0))
            std_step_value = std_value / 5.0
        else:
            median_value = pct10_value = pct90_value = std_value = std_step_value = None
        baselines.append(
            SectorMetricBaseline(
                as_of_date=as_of_date,
                sector=sector,
                metric_name=metric_name,
                sample_size=len(values),
                filtered_sample_size=len(filtered),
                median_value=median_value,
                pct10_value=pct10_value,
                pct90_value=pct90_value,
                std_value=std_value,
                std_step_value=std_step_value,
            )
        )
    return baselines
