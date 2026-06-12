from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd


VCS_SHORT_LENGTH = 13
VCS_LONG_LENGTH = 63
VCS_VOLUME_LENGTH = 50
VCS_SENSITIVITY = 2.0
VCS_TREND_PENALTY_WEIGHT = 1.0
VCS_STRUCTURE_LOOKBACK = 63
VCS_PENALTY_FACTOR = 0.75
VCS_BONUS_MAX = 15
VCS_IS_TIGHT_LEVEL = 70.0
VCS_SETUP_LEVEL = 60.0
VCS_CRITICAL_LEVEL = 80.0


@dataclass(frozen=True)
class VcsSnapshot:
    score: float
    stage: str
    stage_label: str
    color_zone: str
    is_setup_stage: bool
    is_critical_tightness: bool
    tr_short: float
    tr_long_avg: float
    std_short: float
    std_long_avg: float
    vol_short_avg: float
    vol_avg: float
    trend_factor: float
    efficiency: float
    days_tight: int
    is_higher_low: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _true_range(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["Close"].shift(1)
    return pd.concat(
        [
            frame["High"] - frame["Low"],
            (frame["High"] - previous_close).abs(),
            (frame["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def compute_vcs_series(frame: pd.DataFrame) -> pd.DataFrame:
    bars = frame.copy().sort_index()
    if bars.empty:
        return pd.DataFrame(index=bars.index)

    tr_val = _true_range(bars)
    tr_short = tr_val.rolling(VCS_SHORT_LENGTH, min_periods=1).mean()
    tr_long_avg = tr_val.rolling(VCS_LONG_LENGTH, min_periods=1).mean()
    ratio_atr = tr_short / tr_long_avg.clip(lower=1e-6)

    std_short = bars["Close"].rolling(VCS_SHORT_LENGTH, min_periods=1).std(ddof=0)
    std_long_avg = bars["Close"].rolling(VCS_LONG_LENGTH, min_periods=1).std(ddof=0)
    ratio_std = std_short / std_long_avg.clip(lower=1e-6)

    vol_avg = bars["Volume"].rolling(VCS_VOLUME_LENGTH, min_periods=1).mean()
    vol_short_avg = bars["Volume"].rolling(5, min_periods=1).mean()
    vol_ratio = vol_short_avg / vol_avg.clip(lower=1.0)

    net_change = (bars["Close"] - bars["Close"].shift(VCS_SHORT_LENGTH)).abs()
    total_travel = tr_val.rolling(VCS_SHORT_LENGTH, min_periods=1).sum()
    efficiency = net_change / total_travel.clip(lower=1e-6)
    trend_factor = (1.0 - (efficiency.fillna(0.0) * VCS_TREND_PENALTY_WEIGHT)).clip(lower=0.0)

    low_recent = bars["Low"].rolling(VCS_SHORT_LENGTH, min_periods=1).min()
    low_base = bars["Low"].rolling(VCS_STRUCTURE_LOOKBACK, min_periods=1).min().shift(VCS_SHORT_LENGTH)
    has_history = pd.Series(np.arange(len(bars)) >= VCS_SHORT_LENGTH, index=bars.index)
    is_higher_low = pd.Series(True, index=bars.index)
    is_higher_low.loc[has_history] = low_recent.loc[has_history] >= low_base.loc[has_history]

    s_atr = (1.0 - ratio_atr.fillna(1.0)).clip(lower=0.0) * VCS_SENSITIVITY
    s_std = (1.0 - ratio_std.fillna(1.0)).clip(lower=0.0) * VCS_SENSITIVITY
    s_vol = (1.0 - vol_ratio.fillna(1.0)).clip(lower=0.0)
    raw_score = (s_atr * 0.4) + (s_std * 0.4) + (s_vol * 0.2)
    filtered_score = raw_score * trend_factor
    physics_score = (filtered_score * 100.0).clip(upper=100.0)
    smooth_physics = physics_score.ewm(span=3, adjust=False).mean()

    is_tight = smooth_physics >= VCS_IS_TIGHT_LEVEL
    days_tight_values: list[int] = []
    current_streak = 0
    for tight in is_tight.fillna(False).tolist():
        if tight:
            current_streak += 1
        else:
            current_streak = 0
        days_tight_values.append(current_streak)
    days_tight = pd.Series(days_tight_values, index=bars.index, dtype=float)

    weight_physics = (100.0 - VCS_BONUS_MAX) / 100.0
    weighted_physics_score = smooth_physics * weight_physics
    consistency_score = days_tight.clip(upper=VCS_BONUS_MAX)
    total_score = weighted_physics_score + consistency_score
    final_score = total_score.where(is_higher_low, total_score * VCS_PENALTY_FACTOR).fillna(0.0)

    return pd.DataFrame(
        {
            "score": final_score,
            "tr_short": tr_short,
            "tr_long_avg": tr_long_avg,
            "std_short": std_short,
            "std_long_avg": std_long_avg,
            "vol_short_avg": vol_short_avg,
            "vol_avg": vol_avg,
            "trend_factor": trend_factor,
            "efficiency": efficiency.fillna(0.0),
            "days_tight": days_tight,
            "is_higher_low": is_higher_low,
        },
        index=bars.index,
    )


def classify_vcs_score(score: float) -> tuple[str, str, str]:
    if score >= VCS_CRITICAL_LEVEL:
        return "critical", "Critical Tightness", "green"
    if score >= VCS_SETUP_LEVEL:
        return "setup", "Setup Stage", "blue"
    return "base", "Base", "base"


def latest_vcs_snapshot(frame: pd.DataFrame) -> VcsSnapshot | None:
    vcs_frame = compute_vcs_series(frame)
    if vcs_frame.empty:
        return None
    latest = vcs_frame.iloc[-1]
    score = float(latest["score"])
    stage, stage_label, color_zone = classify_vcs_score(score)
    return VcsSnapshot(
        score=score,
        stage=stage,
        stage_label=stage_label,
        color_zone=color_zone,
        is_setup_stage=VCS_SETUP_LEVEL <= score < VCS_CRITICAL_LEVEL,
        is_critical_tightness=score >= VCS_CRITICAL_LEVEL,
        tr_short=float(latest["tr_short"]),
        tr_long_avg=float(latest["tr_long_avg"]),
        std_short=float(latest["std_short"]) if pd.notna(latest["std_short"]) else 0.0,
        std_long_avg=float(latest["std_long_avg"]) if pd.notna(latest["std_long_avg"]) else 0.0,
        vol_short_avg=float(latest["vol_short_avg"]),
        vol_avg=float(latest["vol_avg"]),
        trend_factor=float(latest["trend_factor"]),
        efficiency=float(latest["efficiency"]),
        days_tight=int(round(float(latest["days_tight"]))),
        is_higher_low=bool(latest["is_higher_low"]),
    )
