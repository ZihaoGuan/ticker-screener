from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from .config import AppConfig


@dataclass(frozen=True)
class SeanPegAssessment:
    strategy_profile: str
    qualifies: bool
    setup_score: int
    setup_label: str
    peg_age_days: int | None
    avg_volume_20: float | None
    adr_pct_20: float | None
    ema_21: float | None
    ema_50: float | None
    dema_8: float | None
    price_above_ema21: bool
    price_above_ema50: bool
    ema21_distance_pct: float | None
    inside_day: bool
    inside_day_at_ema21: bool
    demand_dry: bool
    low_volume_pullback: bool
    recent_range_pct: float | None
    pullback_from_peg_high_pct: float | None
    breakout_trigger: float | None
    breakout_ready: bool
    dema_support_ready: bool
    notes: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _compute_dema(closes: np.ndarray, length: int) -> float | None:
    if length <= 0 or closes.size == 0:
        return None
    alpha = 2.0 / (float(length) + 1.0)
    ema1 = np.nan
    ema2 = np.nan
    for value in closes:
        if np.isnan(value):
            continue
        if np.isnan(ema1):
            ema1 = value
            ema2 = value
            continue
        ema1 = alpha * value + (1.0 - alpha) * ema1
        ema2 = alpha * ema1 + (1.0 - alpha) * ema2
    if np.isnan(ema1) or np.isnan(ema2):
        return None
    return float((2.0 * ema1) - ema2)


def assess_sean_post_earnings_gap_setup(
    financials: object,
    peg_date: str | None,
    distribution_warning: bool,
    config: AppConfig,
) -> SeanPegAssessment:
    price_data = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not price_data:
        return SeanPegAssessment(
            strategy_profile="sean-peg",
            qualifies=False,
            setup_score=0,
            setup_label="insufficient_data",
            peg_age_days=None,
            avg_volume_20=None,
            adr_pct_20=None,
            ema_21=None,
            ema_50=None,
            dema_8=None,
            price_above_ema21=False,
            price_above_ema50=False,
            ema21_distance_pct=None,
            inside_day=False,
            inside_day_at_ema21=False,
            demand_dry=False,
            low_volume_pullback=False,
            recent_range_pct=None,
            pullback_from_peg_high_pct=None,
            breakout_trigger=None,
            breakout_ready=False,
            dema_support_ready=False,
            notes=["missing clean price data"],
        )

    closes = np.asarray([item.get("close") or np.nan for item in price_data], dtype=float)
    highs = np.asarray([item.get("high") or np.nan for item in price_data], dtype=float)
    lows = np.asarray([item.get("low") or np.nan for item in price_data], dtype=float)
    volumes = np.asarray([item.get("volume") or np.nan for item in price_data], dtype=float)
    current_price = float(closes[-1])

    recent_20_slice = slice(max(0, len(price_data) - 20), len(price_data))
    avg_volume_20 = float(np.nanmean(volumes[recent_20_slice])) if len(volumes[recent_20_slice]) else None
    adr_pct_20 = None
    if len(highs[recent_20_slice]) and current_price > 0:
        adr_values = ((highs[recent_20_slice] - lows[recent_20_slice]) / closes[recent_20_slice]) * 100.0
        adr_pct_20 = float(np.nanmean(adr_values))

    ema_21 = financials._get_latest_ema_value(21)  # type: ignore[attr-defined]
    ema_50 = financials._get_latest_ema_value(50)  # type: ignore[attr-defined]
    dema_8 = _compute_dema(closes, 8)
    price_above_ema21 = bool(ema_21 is not None and current_price > float(ema_21))
    price_above_ema50 = bool(ema_50 is not None and current_price > float(ema_50))
    ema21_distance_pct = None
    if ema_21 is not None and float(ema_21) > 0:
        ema21_distance_pct = ((current_price - float(ema_21)) / float(ema_21)) * 100.0

    peg_index = None
    peg_high = None
    peg_volume = None
    peg_age_days = None
    if peg_date:
        for idx, item in enumerate(price_data):
            if item.get("formatted_date") == peg_date:
                peg_index = idx
                peg_high = item.get("high")
                peg_volume = item.get("volume")
                peg_age_days = len(price_data) - idx - 1
                break

    recent_window = max(3, int(config.peg_sean_recent_window_days))
    recent_bars = price_data[-recent_window:]
    recent_highs = [float(item["high"]) for item in recent_bars if item.get("high") is not None]
    recent_lows = [float(item["low"]) for item in recent_bars if item.get("low") is not None]
    recent_volumes = [float(item["volume"]) for item in recent_bars if item.get("volume") is not None]
    recent_range_pct = None
    if recent_highs and recent_lows and current_price > 0:
        recent_range_pct = (max(recent_highs) - min(recent_lows)) / current_price
    inside_day = False
    if len(price_data) >= 2:
        latest_bar = price_data[-1]
        previous_bar = price_data[-2]
        latest_high = latest_bar.get("high")
        latest_low = latest_bar.get("low")
        previous_high = previous_bar.get("high")
        previous_low = previous_bar.get("low")
        if (
            latest_high is not None
            and latest_low is not None
            and previous_high is not None
            and previous_low is not None
        ):
            inside_day = bool(float(latest_high) < float(previous_high) and float(latest_low) > float(previous_low))
    inside_day_at_ema21 = bool(
        inside_day
        and ema_21 is not None
        and price_above_ema21
        and ema21_distance_pct is not None
        and ema21_distance_pct <= float(config.peg_sean_ema21_tolerance_pct) * 100.0
    )

    demand_dry = bool(financials.is_demand_dry()[0])  # type: ignore[attr-defined]
    recent_avg_volume = float(np.mean(recent_volumes)) if recent_volumes else None
    low_volume_pullback = bool(
        demand_dry
        or (
            recent_avg_volume is not None
            and avg_volume_20 is not None
            and recent_avg_volume <= avg_volume_20
            and peg_volume is not None
            and recent_avg_volume < float(peg_volume)
        )
    )

    breakout_trigger = None
    if peg_high is not None:
        post_peg_highs = [
            float(item["high"])
            for item in price_data[(peg_index + 1 if peg_index is not None else 0):]
            if item.get("high") is not None
        ]
        breakout_trigger = max(post_peg_highs) if post_peg_highs else float(peg_high)

    breakout_ready = bool(
        breakout_trigger is not None
        and breakout_trigger > 0
        and recent_range_pct is not None
        and recent_range_pct <= float(config.peg_sean_tight_range_max_pct)
        and current_price >= breakout_trigger * (1.0 - float(config.peg_sean_breakout_proximity_pct))
    )
    dema_support_ready = bool(
        dema_8 is not None
        and dema_8 > 0
        and abs(current_price - float(dema_8)) / float(dema_8) <= float(config.peg_sean_dema_tolerance_pct)
        and current_price >= float(dema_8) * 0.98
    )
    pullback_from_peg_high_pct = None
    if peg_high is not None and peg_high > 0:
        pullback_from_peg_high_pct = ((float(peg_high) - current_price) / float(peg_high)) * 100.0

    notes: list[str] = []
    score = 0

    if peg_age_days is not None and peg_age_days >= int(config.peg_sean_min_setup_age_days):
        score += 1
    else:
        notes.append("setup too fresh after earnings gap")
    if adr_pct_20 is not None and adr_pct_20 >= float(config.peg_sean_min_adr_pct):
        score += 1
    else:
        notes.append("ADR below Sean threshold")
    if avg_volume_20 is not None and avg_volume_20 >= int(config.peg_sean_min_avg_volume):
        score += 1
    else:
        notes.append("average volume below Sean threshold")
    if price_above_ema50:
        score += 1
    else:
        notes.append("price not above 50 EMA")
    if low_volume_pullback:
        score += 1
    else:
        notes.append("pullback volume not drying up")
    if not distribution_warning:
        score += 1
    else:
        notes.append("recent distribution warning")
    if breakout_ready or dema_support_ready:
        score += 1
    else:
        notes.append("not near breakout or 8 DEMA support entry")
    if inside_day_at_ema21:
        score += 1
        notes.append("inside day at 21 EMA")
    elif inside_day:
        notes.append("inside day but extended from 21 EMA")
    elif price_above_ema21 and ema21_distance_pct is not None and ema21_distance_pct <= 5.0:
        notes.append("near 21 EMA")

    qualifies = (
        peg_age_days is not None
        and peg_age_days >= int(config.peg_sean_min_setup_age_days)
        and adr_pct_20 is not None
        and adr_pct_20 >= float(config.peg_sean_min_adr_pct)
        and avg_volume_20 is not None
        and avg_volume_20 >= int(config.peg_sean_min_avg_volume)
        and price_above_ema50
        and low_volume_pullback
        and not distribution_warning
        and (breakout_ready or dema_support_ready)
    )

    setup_label = "post_earnings_gap_breakout" if breakout_ready else "post_earnings_gap_dema_support"
    if not (breakout_ready or dema_support_ready):
        setup_label = "post_earnings_gap_watch"

    return SeanPegAssessment(
        strategy_profile="sean-peg",
        qualifies=qualifies,
        setup_score=score,
        setup_label=setup_label,
        peg_age_days=peg_age_days,
        avg_volume_20=avg_volume_20,
        adr_pct_20=adr_pct_20,
        ema_21=ema_21,
        ema_50=ema_50,
        dema_8=dema_8,
        price_above_ema21=price_above_ema21,
        price_above_ema50=price_above_ema50,
        ema21_distance_pct=ema21_distance_pct,
        inside_day=inside_day,
        inside_day_at_ema21=inside_day_at_ema21,
        demand_dry=demand_dry,
        low_volume_pullback=low_volume_pullback,
        recent_range_pct=recent_range_pct,
        pullback_from_peg_high_pct=pullback_from_peg_high_pct,
        breakout_trigger=breakout_trigger,
        breakout_ready=breakout_ready,
        dema_support_ready=dema_support_ready,
        notes=notes,
    )
