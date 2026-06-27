from __future__ import annotations

from typing import Any


COMPONENT_WEIGHTS = {
    "gap_size": 0.25,
    "pre_earnings_trend": 0.30,
    "volume_trend": 0.20,
    "ma200_position": 0.15,
    "ma50_position": 0.10,
}

GRADE_THRESHOLDS = [
    (85, "A", "Strong earnings reaction with institutional accumulation"),
    (70, "B", "Good earnings reaction worth monitoring"),
    (55, "C", "Mixed signals, use caution"),
    (0, "D", "Weak setup, avoid"),
]

GRADE_GUIDANCE = {
    "A": "Consider entry on pullback to gap support or breakout continuation. High conviction setup.",
    "B": "Monitor for follow-through buying. Wait for pullback to key support or volume confirmation.",
    "C": "Additional analysis needed. Consider waiting for clearer price action or catalyst.",
    "D": "Avoid trading. Weak setup with poor risk/reward profile.",
}


def normalize_timing(time_value: Any) -> str:
    if not time_value:
        return "unknown"
    timing = str(time_value).strip().lower()
    if timing in {"bmo", "pre-market", "before market open"}:
        return "bmo"
    if timing in {"amc", "after-market", "after market close"}:
        return "amc"
    return "unknown"


def analyze_stock(
    daily_prices: list[dict[str, Any]],
    earnings_date: str,
    timing: str,
) -> dict[str, Any]:
    gap_result = _calculate_gap(daily_prices, earnings_date, timing)
    trend_result = _calculate_pre_earnings_trend(daily_prices, earnings_date)
    volume_result = _calculate_volume_trend(daily_prices, earnings_date)
    ma200_result = _calculate_ma_position(daily_prices, window=200)
    ma50_result = _calculate_ma_position(daily_prices, window=50)

    composite = _calculate_composite_score(
        gap_score=float(gap_result["score"]),
        trend_score=float(trend_result["score"]),
        volume_score=float(volume_result["score"]),
        ma200_score=float(ma200_result["score"]),
        ma50_score=float(ma50_result["score"]),
    )

    return {
        "gap": gap_result,
        "pre_earnings_trend": trend_result,
        "volume_trend": volume_result,
        "ma200_position": ma200_result,
        "ma50_position": ma50_result,
        "composite": composite,
    }


def apply_entry_filter(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for result in results:
        price = float(result.get("current_price") or 0)
        gap_pct = abs(float(result.get("gap_pct") or 0))
        score = float(result.get("composite_score") or 0)
        if price < 30:
            continue
        if gap_pct >= 10 and score >= 85:
            continue
        filtered.append(result)
    return filtered


def _find_index_by_date(daily_prices: list[dict[str, Any]], target_date: str) -> int:
    for index, bar in enumerate(daily_prices):
        if str(bar.get("date") or "") == target_date:
            return index
    return -1


def _calculate_gap(
    daily_prices: list[dict[str, Any]],
    earnings_date: str,
    timing: str,
) -> dict[str, Any]:
    earnings_idx = _find_index_by_date(daily_prices, earnings_date)
    timing_used = normalize_timing(timing)
    if earnings_idx == -1:
        return {
            "gap_pct": 0.0,
            "gap_type": "up",
            "base_price": 0.0,
            "gap_price": 0.0,
            "timing_used": timing_used,
            "score": 0.0,
            "warning": f"Earnings date {earnings_date} not found in price data",
        }

    if timing_used == "bmo":
        prev_idx = earnings_idx + 1
        if prev_idx >= len(daily_prices):
            return {
                "gap_pct": 0.0,
                "gap_type": "up",
                "base_price": 0.0,
                "gap_price": 0.0,
                "timing_used": timing_used,
                "score": 0.0,
                "warning": "No previous trading day available for BMO gap calculation",
            }
        base_price = float(daily_prices[prev_idx].get("close") or 0)
        gap_price = float(daily_prices[earnings_idx].get("open") or 0)
    else:
        next_idx = earnings_idx - 1
        if next_idx < 0:
            return {
                "gap_pct": 0.0,
                "gap_type": "up",
                "base_price": 0.0,
                "gap_price": 0.0,
                "timing_used": timing_used,
                "score": 0.0,
                "warning": "No next trading day available for AMC gap calculation",
            }
        base_price = float(daily_prices[earnings_idx].get("close") or 0)
        gap_price = float(daily_prices[next_idx].get("open") or 0)

    if base_price <= 0:
        return {
            "gap_pct": 0.0,
            "gap_type": "up",
            "base_price": round(base_price, 2),
            "gap_price": round(gap_price, 2),
            "timing_used": timing_used,
            "score": 0.0,
            "warning": "Base price is zero, cannot calculate gap",
        }

    gap_pct = ((gap_price / base_price) - 1.0) * 100.0
    abs_gap = abs(gap_pct)
    if abs_gap >= 10.0:
        score = 100.0
    elif abs_gap >= 7.0:
        score = 85.0
    elif abs_gap >= 5.0:
        score = 70.0
    elif abs_gap >= 3.0:
        score = 55.0
    elif abs_gap >= 1.0:
        score = 35.0
    else:
        score = 15.0

    return {
        "gap_pct": round(gap_pct, 2),
        "gap_type": "up" if gap_pct >= 0 else "down",
        "base_price": round(base_price, 2),
        "gap_price": round(gap_price, 2),
        "timing_used": timing_used,
        "score": score,
    }


def _calculate_pre_earnings_trend(
    daily_prices: list[dict[str, Any]],
    earnings_date: str,
) -> dict[str, Any]:
    earnings_idx = _find_index_by_date(daily_prices, earnings_date)
    if earnings_idx == -1:
        return {
            "return_20d_pct": 0.0,
            "trend_direction": "up",
            "score": 0.0,
            "warning": f"Earnings date {earnings_date} not found in price data",
        }

    lookback_idx = earnings_idx + 20
    if lookback_idx >= len(daily_prices):
        return {
            "return_20d_pct": 0.0,
            "trend_direction": "up",
            "score": 0.0,
            "warning": "Insufficient data for 20-day pre-earnings trend calculation",
        }

    close_at_earnings = float(daily_prices[earnings_idx].get("close") or 0)
    close_20d_before = float(daily_prices[lookback_idx].get("close") or 0)
    if close_20d_before <= 0:
        return {
            "return_20d_pct": 0.0,
            "trend_direction": "up",
            "score": 0.0,
            "warning": "Price 20 days before earnings is zero",
        }

    return_pct = ((close_at_earnings / close_20d_before) - 1.0) * 100.0
    if return_pct >= 15.0:
        score = 100.0
    elif return_pct >= 10.0:
        score = 85.0
    elif return_pct >= 5.0:
        score = 70.0
    elif return_pct >= 0.0:
        score = 50.0
    elif return_pct >= -5.0:
        score = 30.0
    else:
        score = 15.0

    return {
        "return_20d_pct": round(return_pct, 2),
        "trend_direction": "up" if return_pct >= 0 else "down",
        "score": score,
    }


def _calculate_volume_trend(
    daily_prices: list[dict[str, Any]],
    earnings_date: str,
) -> dict[str, Any]:
    earnings_idx = _find_index_by_date(daily_prices, earnings_date)
    if earnings_idx == -1:
        return {
            "vol_ratio_20_60": 0.0,
            "recent_avg_volume": 0,
            "longer_avg_volume": 0,
            "score": 0.0,
            "warning": f"Earnings date {earnings_date} not found in price data",
        }

    end_20 = min(earnings_idx + 20, len(daily_prices))
    if end_20 - earnings_idx < 5:
        return {
            "vol_ratio_20_60": 0.0,
            "recent_avg_volume": 0,
            "longer_avg_volume": 0,
            "score": 0.0,
            "warning": "Insufficient data for 20-day volume calculation",
        }
    volumes_20 = [float(daily_prices[i].get("volume") or 0) for i in range(earnings_idx, end_20)]
    recent_avg = sum(volumes_20) / len(volumes_20)

    end_60 = min(earnings_idx + 60, len(daily_prices))
    if end_60 - earnings_idx < 20:
        return {
            "vol_ratio_20_60": 0.0,
            "recent_avg_volume": int(recent_avg),
            "longer_avg_volume": 0,
            "score": 0.0,
            "warning": "Insufficient data for 60-day volume calculation",
        }
    volumes_60 = [float(daily_prices[i].get("volume") or 0) for i in range(earnings_idx, end_60)]
    longer_avg = sum(volumes_60) / len(volumes_60)
    if longer_avg <= 0:
        return {
            "vol_ratio_20_60": 0.0,
            "recent_avg_volume": int(recent_avg),
            "longer_avg_volume": 0,
            "score": 0.0,
            "warning": "60-day average volume is zero",
        }

    ratio = recent_avg / longer_avg
    if ratio >= 2.0:
        score = 100.0
    elif ratio >= 1.5:
        score = 80.0
    elif ratio >= 1.2:
        score = 60.0
    elif ratio >= 1.0:
        score = 40.0
    else:
        score = 20.0

    return {
        "vol_ratio_20_60": round(ratio, 2),
        "recent_avg_volume": int(recent_avg),
        "longer_avg_volume": int(longer_avg),
        "score": score,
    }


def _calculate_ma_position(
    daily_prices: list[dict[str, Any]],
    *,
    window: int,
) -> dict[str, Any]:
    label = f"ma{window}"
    if len(daily_prices) < window:
        return {
            label: 0.0,
            "distance_pct": 0.0,
            f"above_{label}": False,
            "score": 0.0,
            "warning": f"Insufficient data for MA{window}: {len(daily_prices)} days available, {window} required",
        }

    closes = [float(daily_prices[i].get("close") or 0) for i in range(window)]
    ma_value = sum(closes) / float(window)
    current_price = float(daily_prices[0].get("close") or 0)
    if ma_value <= 0:
        return {
            label: 0.0,
            "distance_pct": 0.0,
            f"above_{label}": False,
            "score": 0.0,
            "warning": f"MA{window} is zero",
        }

    distance_pct = ((current_price / ma_value) - 1.0) * 100.0
    if window == 200:
        if distance_pct >= 20.0:
            score = 100.0
        elif distance_pct >= 10.0:
            score = 85.0
        elif distance_pct >= 5.0:
            score = 70.0
        elif distance_pct >= 0.0:
            score = 55.0
        elif distance_pct >= -5.0:
            score = 35.0
        else:
            score = 15.0
    else:
        if distance_pct >= 10.0:
            score = 100.0
        elif distance_pct >= 5.0:
            score = 80.0
        elif distance_pct >= 0.0:
            score = 60.0
        elif distance_pct >= -5.0:
            score = 35.0
        else:
            score = 15.0

    return {
        label: round(ma_value, 2),
        "distance_pct": round(distance_pct, 2),
        f"above_{label}": distance_pct >= 0,
        "score": score,
    }


def _calculate_composite_score(
    *,
    gap_score: float,
    trend_score: float,
    volume_score: float,
    ma200_score: float,
    ma50_score: float,
) -> dict[str, Any]:
    components = {
        "Gap Size": {"score": gap_score, "weight": COMPONENT_WEIGHTS["gap_size"]},
        "Pre-Earnings Trend": {"score": trend_score, "weight": COMPONENT_WEIGHTS["pre_earnings_trend"]},
        "Volume Trend": {"score": volume_score, "weight": COMPONENT_WEIGHTS["volume_trend"]},
        "MA200 Position": {"score": ma200_score, "weight": COMPONENT_WEIGHTS["ma200_position"]},
        "MA50 Position": {"score": ma50_score, "weight": COMPONENT_WEIGHTS["ma50_position"]},
    }
    composite_score = round(sum(item["score"] * item["weight"] for item in components.values()), 1)

    grade = "D"
    grade_description = "Weak setup, avoid"
    for threshold, candidate_grade, description in GRADE_THRESHOLDS:
        if composite_score >= threshold:
            grade = candidate_grade
            grade_description = description
            break

    weakest_name = min(components, key=lambda key: components[key]["score"])
    strongest_name = max(components, key=lambda key: components[key]["score"])
    return {
        "composite_score": composite_score,
        "grade": grade,
        "grade_description": grade_description,
        "guidance": GRADE_GUIDANCE.get(grade, ""),
        "weakest_component": weakest_name,
        "weakest_score": components[weakest_name]["score"],
        "strongest_component": strongest_name,
        "strongest_score": components[strongest_name]["score"],
        "component_breakdown": {
            name: {
                "score": item["score"],
                "weight": item["weight"],
                "weighted_score": round(item["score"] * item["weight"], 1),
            }
            for name, item in components.items()
        },
    }
