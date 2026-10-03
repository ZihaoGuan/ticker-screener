from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


PULLBACK_QUALITY_LOOKBACK_DAYS = 8
PULLBACK_QUALITY_BASELINE_DAYS = 20


@dataclass(frozen=True)
class PullbackQuality:
    """Explain whether a move into support is controlled or distributional."""

    score: int
    state: str
    selling_pressure_score: int
    volume_score: int
    support_score: int
    structure_score: int
    down_volume_ratio: float | None
    volume_trend_ratio: float | None
    downside_range_atr: float | None
    aggressive_downside_days: int
    distribution_days: int
    support_distance_atr: float
    close_location_pct: float | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def evaluate_pullback_quality(
    bars: pd.DataFrame,
    *,
    support_price: float,
    atr14: float,
) -> PullbackQuality | None:
    """Score the path into a moving-average support level without look-ahead.

    The score is evidence for review, not an automated buy verdict.  It deliberately
    separates controlled price/volume contraction from selling pressure that is
    expanding into support.
    """
    required = ("Open", "High", "Low", "Close", "Volume")
    if atr14 <= 0 or support_price <= 0 or any(column not in bars for column in required):
        return None
    source = bars.loc[:, required].dropna().astype(float)
    required_days = PULLBACK_QUALITY_LOOKBACK_DAYS + PULLBACK_QUALITY_BASELINE_DAYS
    if len(source) < required_days:
        return None

    recent = source.tail(PULLBACK_QUALITY_LOOKBACK_DAYS)
    baseline = source.iloc[-required_days:-PULLBACK_QUALITY_LOOKBACK_DAYS]
    day_range_atr = (recent["High"] - recent["Low"]) / atr14
    body_atr = (recent["Close"] - recent["Open"]).abs() / atr14
    close_location = ((recent["Close"] - recent["Low"]) / (recent["High"] - recent["Low"]).replace(0, pd.NA)).fillna(0.5)
    red_days = recent["Close"] < recent["Open"]
    aggressive_days = red_days & ((day_range_atr >= 1.35) | (body_atr >= 0.80)) & (close_location <= 0.35)
    aggressive_count = int(aggressive_days.sum())
    downside_range = float(day_range_atr[red_days].max()) if bool(red_days.any()) else 0.0

    baseline_volume = float(baseline["Volume"].mean())
    down_volume = float(recent.loc[red_days, "Volume"].mean()) if bool(red_days.any()) else float(recent["Volume"].mean())
    down_volume_ratio = down_volume / baseline_volume if baseline_volume > 0 else None
    recent_volume = float(recent["Volume"].tail(3).mean())
    prior_recent_volume = float(recent["Volume"].head(5).mean())
    volume_trend_ratio = recent_volume / prior_recent_volume if prior_recent_volume > 0 else None
    prior_close = recent["Close"].shift(1)
    distribution_days = red_days & (recent["Close"] < prior_close) & (day_range_atr >= 1.0) & (recent["Volume"] >= baseline_volume * 1.2)
    distribution_count = int(distribution_days.sum())

    if aggressive_count == 0:
        selling_score = 30
    elif aggressive_count == 1:
        selling_score = 18
    elif aggressive_count == 2:
        selling_score = 8
    else:
        selling_score = 0

    if down_volume_ratio is None:
        volume_score = 12
    elif down_volume_ratio <= 0.80 and (volume_trend_ratio is None or volume_trend_ratio <= 1.0):
        volume_score = 30
    elif down_volume_ratio <= 1.0:
        volume_score = 24
    elif down_volume_ratio <= 1.15:
        volume_score = 14
    else:
        volume_score = 4
    volume_score = max(0, volume_score - distribution_count * 8)

    last_close = float(recent["Close"].iloc[-1])
    support_distance_atr = (last_close - support_price) / atr14
    recent_low_distance_atr = (float(recent["Low"].min()) - support_price) / atr14
    if support_distance_atr >= -0.05:
        support_score = 25
    elif support_distance_atr >= -0.35:
        support_score = 15
    else:
        support_score = 0
    if recent_low_distance_atr < -1.0:
        support_score = max(0, support_score - 8)

    recent_range = float(day_range_atr.tail(3).mean())
    prior_range = float(day_range_atr.head(5).mean())
    range_ratio = recent_range / prior_range if prior_range > 0 else 1.0
    drawdown_atr = (float(recent["Close"].max()) - last_close) / atr14
    if range_ratio <= 0.95:
        structure_score = 15
    elif range_ratio <= 1.20:
        structure_score = 10
    elif range_ratio <= 1.50:
        structure_score = 5
    else:
        structure_score = 0
    if drawdown_atr > 5.0:
        structure_score = max(0, structure_score - 5)

    score = int(selling_score + volume_score + support_score + structure_score)
    hard_failure = support_distance_atr < -0.75 and (down_volume_ratio is None or down_volume_ratio > 1.05 or distribution_count >= 2)
    if hard_failure:
        state = "failed"
    elif score >= 70 and aggressive_count == 0 and distribution_count == 0:
        state = "constructive"
    elif score >= 50:
        state = "neutral"
    else:
        state = "warning"

    volume_text = "unavailable" if down_volume_ratio is None else f"{down_volume_ratio * 100:.0f}% of baseline"
    trend_text = "unavailable" if volume_trend_ratio is None else f"{volume_trend_ratio * 100:.0f}% of prior pullback volume"
    reasons = [
        f"Down-day volume {volume_text}; recent volume {trend_text}.",
        f"{aggressive_count} aggressive downside candle(s); largest red range {downside_range:.2f} ATR.",
        f"Close is {support_distance_atr:+.2f} ATR versus moving-average support.",
        f"Recent range is {range_ratio * 100:.0f}% of the earlier pullback range.",
    ]
    if distribution_count:
        reasons.append(f"{distribution_count} high-volume distribution day(s) during the pullback.")
    if hard_failure:
        reasons.append("Support break is confirmed by expanding selling pressure.")

    return PullbackQuality(
        score=score,
        state=state,
        selling_pressure_score=selling_score,
        volume_score=volume_score,
        support_score=support_score,
        structure_score=structure_score,
        down_volume_ratio=round(down_volume_ratio, 3) if down_volume_ratio is not None else None,
        volume_trend_ratio=round(volume_trend_ratio, 3) if volume_trend_ratio is not None else None,
        downside_range_atr=round(downside_range, 3),
        aggressive_downside_days=aggressive_count,
        distribution_days=distribution_count,
        support_distance_atr=round(support_distance_atr, 3),
        close_location_pct=round(float(close_location.iloc[-1]) * 100, 1),
        reasons=reasons,
    )
