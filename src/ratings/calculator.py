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
)
from .models import FundamentalsSnapshot, RatingSnapshot, SectorMetricBaseline


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
