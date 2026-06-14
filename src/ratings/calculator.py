from __future__ import annotations

from typing import Mapping

from .constants import (
    CATEGORY_METRICS,
    GRADE_ORDER,
    GRADE_SCORES,
    LESS_IS_BETTER_METRICS,
    MIN_SECTOR_PEERS_DEFAULT,
    RATING_STATUS_INSUFFICIENT_SECTOR_PEERS,
    RATING_STATUS_MISSING_METRICS,
    RATING_STATUS_MISSING_SECTOR,
    RATING_STATUS_OK,
    TECHNICAL_RATING_STATUS_MISSING_METRICS,
    TECHNICAL_REQUIRED_METRICS,
)
from .models import FundamentalsSnapshot, RatingSnapshot, SectorMetricBaseline, TechnicalRatingSnapshot, TechnicalSnapshotInput


def _convert_score_to_letter_grade(value: float | None) -> str | None:
    if value is None:
        return None
    for grade in GRADE_ORDER:
        if value >= GRADE_SCORES[grade]:
            return grade
    return "F"


def _metric_grade(
    metric_name: str,
    metric_value: float,
    baseline: SectorMetricBaseline,
) -> str | None:
    if baseline.std_step_value is None:
        return None
    less_is_better = metric_name in LESS_IS_BETTER_METRICS
    start = baseline.pct10_value if less_is_better else baseline.pct90_value
    if start is None:
        return None
    change = baseline.std_step_value
    for index, grade in enumerate(GRADE_ORDER):
        comparison = start + (change * index) if less_is_better else start - (change * index)
        if less_is_better and metric_value < comparison:
            return grade
        if not less_is_better and metric_value > comparison:
            return grade
    return "C"


def build_ticker_rating(
    snapshot: FundamentalsSnapshot,
    baselines_by_metric: Mapping[str, SectorMetricBaseline],
    *,
    min_sector_peers: int = MIN_SECTOR_PEERS_DEFAULT,
) -> RatingSnapshot:
    rating = RatingSnapshot(ticker=snapshot.ticker, as_of_date=snapshot.as_of_date, sector=snapshot.sector)
    if not snapshot.sector:
        rating.rating_status = RATING_STATUS_MISSING_SECTOR
        rating.rating_status_reason = "Sector unavailable in ticker_metadata and Finviz snapshot."
        return rating

    missing_metric_names: list[str] = []
    insufficient_metrics: list[str] = []
    category_scores: dict[str, float] = {}
    category_grades: dict[str, str] = {}

    for category_name, metric_names in CATEGORY_METRICS.items():
        metric_grade_scores: list[float] = []
        for metric_name in metric_names:
            metric_value = getattr(snapshot, metric_name)
            if metric_value is None:
                missing_metric_names.append(metric_name)
                continue
            baseline = baselines_by_metric.get(metric_name)
            if baseline is None or baseline.filtered_sample_size < int(min_sector_peers):
                insufficient_metrics.append(metric_name)
                continue
            grade = _metric_grade(metric_name, float(metric_value), baseline)
            if grade is None:
                insufficient_metrics.append(metric_name)
                continue
            metric_grade_scores.append(float(GRADE_SCORES[grade]))
        if len(metric_grade_scores) != len(metric_names):
            continue
        score = round(sum(metric_grade_scores) / len(metric_grade_scores), 2)
        category_scores[category_name] = score
        category_grades[category_name] = _convert_score_to_letter_grade(score) or "F"

    if missing_metric_names:
        rating.rating_status = RATING_STATUS_MISSING_METRICS
        rating.rating_status_reason = "One or more required rating metrics are missing."
        rating.missing_metric_names = sorted(set(missing_metric_names))
        return rating
    if insufficient_metrics:
        rating.rating_status = RATING_STATUS_INSUFFICIENT_SECTOR_PEERS
        rating.rating_status_reason = "Sector peer baselines are missing or too small for one or more metrics."
        rating.insufficient_baseline_metrics = sorted(set(insufficient_metrics))
        return rating
    if len(category_scores) != len(CATEGORY_METRICS):
        rating.rating_status = RATING_STATUS_MISSING_METRICS
        rating.rating_status_reason = "Incomplete category scores."
        return rating

    rating.valuation_score = category_scores["valuation"]
    rating.profitability_score = category_scores["profitability"]
    rating.growth_score = category_scores["growth"]
    rating.performance_score = category_scores["performance"]
    rating.overall_rating = round(sum(category_scores.values()) * 6.2, 2)
    rating.valuation_grade = category_grades["valuation"]
    rating.profitability_grade = category_grades["profitability"]
    rating.growth_grade = category_grades["growth"]
    rating.performance_grade = category_grades["performance"]
    rating.rating_status = RATING_STATUS_OK
    rating.rating_status_reason = None
    return rating


def _clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, value))


def _annualized_speed(current: float, previous: float, lookback_days: int) -> float | None:
    if previous <= 0 or lookback_days <= 0:
        return None
    return ((current / previous) - 1.0) * (63.0 / float(lookback_days))


def _score_rating_band(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 90.0:
        return "elite"
    if value >= 80.0:
        return "strong"
    if value >= 70.0:
        return "constructive"
    if value >= 60.0:
        return "mixed"
    return "weak"


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _build_trend_regime_score(snapshot: TechnicalSnapshotInput) -> float:
    score = 0.0
    close = float(snapshot.close or 0.0)
    if snapshot.sma20 is not None and close > float(snapshot.sma20):
        score += 10.0
    if snapshot.sma50 is not None and close > float(snapshot.sma50):
        score += 15.0
    if snapshot.sma100 is not None and close > float(snapshot.sma100):
        score += 10.0
    if snapshot.sma200 is not None and close > float(snapshot.sma200):
        score += 15.0
    if snapshot.sma20 is not None and snapshot.sma50 is not None and float(snapshot.sma20) > float(snapshot.sma50):
        score += 10.0
    if snapshot.sma50 is not None and snapshot.sma100 is not None and float(snapshot.sma50) > float(snapshot.sma100):
        score += 10.0
    if snapshot.sma100 is not None and snapshot.sma200 is not None and float(snapshot.sma100) > float(snapshot.sma200):
        score += 10.0
    if snapshot.sma50 is not None and snapshot.sma50_20d_ago is not None and float(snapshot.sma50) > float(snapshot.sma50_20d_ago):
        score += 10.0
    if snapshot.sma200 is not None and snapshot.sma200_20d_ago is not None and float(snapshot.sma200) > float(snapshot.sma200_20d_ago):
        score += 10.0
    return _clamp(score)


def _build_dma_speed_score(snapshot: TechnicalSnapshotInput) -> float:
    g20 = _annualized_speed(float(snapshot.sma20), float(snapshot.sma20_5d_ago), 5) if snapshot.sma20 is not None and snapshot.sma20_5d_ago is not None else None
    g50 = _annualized_speed(float(snapshot.sma50), float(snapshot.sma50_10d_ago), 10) if snapshot.sma50 is not None and snapshot.sma50_10d_ago is not None else None
    g100 = _annualized_speed(float(snapshot.sma100), float(snapshot.sma100_10d_ago), 10) if snapshot.sma100 is not None and snapshot.sma100_10d_ago is not None else None
    g200 = _annualized_speed(float(snapshot.sma200), float(snapshot.sma200_20d_ago), 20) if snapshot.sma200 is not None and snapshot.sma200_20d_ago is not None else None
    score = 0.0
    if g20 is not None and g20 > 0:
        score += 20.0
    if g50 is not None and g50 > 0:
        score += 25.0
    if g100 is not None and g100 > 0:
        score += 20.0
    if g200 is not None and g200 > 0:
        score += 15.0
    if g50 is not None and g200 is not None and g50 >= g200:
        score += 10.0
    if g20 is not None and g50 is not None and g20 >= (g50 * 0.8) and g20 <= (g50 * 2.5):
        score += 10.0
    return _clamp(score)


def _build_divergence_health_score(snapshot: TechnicalSnapshotInput) -> float:
    close = float(snapshot.close or 0.0)
    atr20 = float(snapshot.atr20 or 0.0)
    score = 100.0
    d20 = _safe_ratio(close, float(snapshot.sma20)) - 1.0 if snapshot.sma20 else None
    d50 = _safe_ratio(close, float(snapshot.sma50)) - 1.0 if snapshot.sma50 else None
    z20 = ((close - float(snapshot.sma20)) / atr20) if snapshot.sma20 and atr20 > 0 else None

    if d20 is not None:
        if 0.05 < d20 <= 0.12:
            score -= 10.0
        elif 0.12 < d20 <= 0.20:
            score -= 25.0
        elif d20 > 0.20:
            score -= 45.0
        elif -0.08 <= d20 < -0.03:
            score -= 5.0
        elif d20 < -0.08:
            score -= 20.0

    if d50 is not None:
        if 0.12 < d50 <= 0.25:
            score -= 10.0
        elif 0.25 < d50 <= 0.35:
            score -= 25.0
        elif d50 > 0.35:
            score -= 40.0
        elif -0.06 <= d50 < 0.00:
            score += 5.0
        elif -0.12 <= d50 < -0.06:
            score -= 5.0
        elif d50 < -0.12:
            score -= 25.0

    if z20 is not None:
        if 2.0 < z20 <= 3.0:
            score -= 10.0
        elif 3.0 < z20 <= 4.0:
            score -= 20.0
        elif z20 > 4.0:
            score -= 35.0
        elif -1.0 <= z20 < 0.0:
            score += 5.0
        elif z20 < -3.0:
            score -= 20.0

    if snapshot.sma100 is not None and close < float(snapshot.sma100):
        score = min(score, 60.0)
    if snapshot.sma200 is not None and close < float(snapshot.sma200):
        score = min(score, 35.0)
    return _clamp(score)


def _build_leadership_score(snapshot: TechnicalSnapshotInput) -> float:
    score = (float(snapshot.daily_rs_rating or 0.0) * 0.45) + (float(snapshot.weekly_rs_rating or 0.0) * 0.35)
    rs_bonus = 0.0
    if snapshot.rs_line is not None and snapshot.rs_line_sma50 is not None and float(snapshot.rs_line) > float(snapshot.rs_line_sma50):
        rs_bonus += 10.0
    if snapshot.rs_line is not None and snapshot.rs_line_3m_high is not None and float(snapshot.rs_line) >= float(snapshot.rs_line_3m_high):
        rs_bonus += 5.0
    if snapshot.rs_line is not None and snapshot.rs_line_12m_high is not None and float(snapshot.rs_line) >= float(snapshot.rs_line_12m_high):
        rs_bonus += 5.0
    return _clamp(score + rs_bonus)


def _build_structure_volume_score(snapshot: TechnicalSnapshotInput) -> float:
    score = 0.0
    close = float(snapshot.close or 0.0)
    if snapshot.high_52w is not None and snapshot.low_52w is not None and float(snapshot.high_52w) > float(snapshot.low_52w):
        range_position = (close - float(snapshot.low_52w)) / (float(snapshot.high_52w) - float(snapshot.low_52w))
        if range_position >= 0.75:
            score += 20.0
        high_distance = (float(snapshot.high_52w) - close) / float(snapshot.high_52w) if float(snapshot.high_52w) > 0 else None
        if high_distance is not None and high_distance <= 0.15:
            score += 20.0
    if snapshot.tr_10d_avg is not None and snapshot.tr_20d_avg is not None and float(snapshot.tr_10d_avg) < float(snapshot.tr_20d_avg):
        score += 15.0
    if snapshot.close_above_bar_midpoint_count_10d is not None and int(snapshot.close_above_bar_midpoint_count_10d) >= 6:
        score += 10.0
    if snapshot.up_down_volume_ratio_20d is not None and float(snapshot.up_down_volume_ratio_20d) > 1.2:
        score += 15.0
    if snapshot.breakout_volume_ratio is not None and float(snapshot.breakout_volume_ratio) > 1.5:
        score += 20.0
    if snapshot.distribution_day_count_20d is not None and int(snapshot.distribution_day_count_20d) >= 2:
        score -= 20.0
    return _clamp(score)


def _build_technical_flags(snapshot: TechnicalSnapshotInput) -> list[str]:
    flags: list[str] = []
    close = float(snapshot.close or 0.0)
    if snapshot.sma200 is not None and close > float(snapshot.sma200):
        flags.append("above_200dma")
    if (
        snapshot.sma20 is not None
        and snapshot.sma50 is not None
        and snapshot.sma100 is not None
        and snapshot.sma200 is not None
        and float(snapshot.sma20) > float(snapshot.sma50) > float(snapshot.sma100) > float(snapshot.sma200)
    ):
        flags.append("ma_stack_bullish")
    if snapshot.daily_rs_rating is not None and snapshot.weekly_rs_rating is not None and float(snapshot.daily_rs_rating) >= 90.0 and float(snapshot.weekly_rs_rating) >= 85.0:
        flags.append("rs_leader")
    if snapshot.high_52w is not None and float(snapshot.high_52w) > 0 and ((float(snapshot.high_52w) - close) / float(snapshot.high_52w)) <= 0.15:
        flags.append("near_52w_high")
    if snapshot.up_down_volume_ratio_20d is not None and float(snapshot.up_down_volume_ratio_20d) > 1.2 and snapshot.distribution_day_count_20d is not None and int(snapshot.distribution_day_count_20d) <= 1:
        flags.append("volume_supportive")
    if snapshot.sma50 is not None and close > (float(snapshot.sma50) * 1.35):
        flags.append("extended")
    if snapshot.atr20 is not None and snapshot.sma20 is not None and float(snapshot.atr20) > 0:
        z20 = (close - float(snapshot.sma20)) / float(snapshot.atr20)
        if z20 > 4.0:
            flags.append("escape_risk")
    return flags


def build_technical_rating(snapshot: TechnicalSnapshotInput) -> TechnicalRatingSnapshot:
    rating = TechnicalRatingSnapshot(ticker=snapshot.ticker, as_of_date=snapshot.as_of_date)
    missing_metric_names = [name for name in TECHNICAL_REQUIRED_METRICS if getattr(snapshot, name) is None]
    if missing_metric_names:
        rating.technical_status = TECHNICAL_RATING_STATUS_MISSING_METRICS
        rating.technical_status_reason = "One or more required technical rating metrics are missing."
        rating.missing_metric_names = sorted(missing_metric_names)
        return rating

    rating.trend_regime_score = _build_trend_regime_score(snapshot)
    rating.dma_speed_score = _build_dma_speed_score(snapshot)
    rating.divergence_health_score = _build_divergence_health_score(snapshot)
    rating.leadership_score = _build_leadership_score(snapshot)
    rating.structure_volume_score = _build_structure_volume_score(snapshot)
    rating.flags = _build_technical_flags(snapshot)

    overall = (
        (rating.trend_regime_score * 0.30)
        + (rating.dma_speed_score * 0.20)
        + (rating.divergence_health_score * 0.20)
        + (rating.leadership_score * 0.20)
        + (rating.structure_volume_score * 0.10)
    )

    close = float(snapshot.close or 0.0)
    if snapshot.sma200 is not None and close < float(snapshot.sma200):
        overall = min(overall, 59.0)
    if snapshot.sma50 is not None and snapshot.sma50_20d_ago is not None and float(snapshot.sma50) <= float(snapshot.sma50_20d_ago) and close < float(snapshot.sma50):
        overall = min(overall, 64.0)
    if snapshot.daily_rs_rating is not None and float(snapshot.daily_rs_rating) < 70.0:
        overall = min(overall, 74.0)
    if snapshot.sma50 is not None and close > (float(snapshot.sma50) * 1.35):
        overall -= 8.0
    if snapshot.atr20 is not None and snapshot.sma20 is not None and float(snapshot.atr20) > 0 and ((close - float(snapshot.sma20)) / float(snapshot.atr20)) > 4.0:
        overall -= 8.0
    if snapshot.sma100 is not None and close < float(snapshot.sma100) and snapshot.weekly_rs_rating is not None and float(snapshot.weekly_rs_rating) < 70.0:
        overall -= 10.0

    rating.overall_rating = round(_clamp(overall), 2)
    rating.rating_band = _score_rating_band(rating.overall_rating)
    rating.technical_status = RATING_STATUS_OK
    rating.technical_status_reason = None
    return rating
