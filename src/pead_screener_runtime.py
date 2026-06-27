from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


COMPONENT_WEIGHTS = {
    "setup_quality": 0.30,
    "breakout_strength": 0.25,
    "liquidity": 0.25,
    "risk_reward": 0.20,
}

COMPONENT_LABELS = {
    "setup_quality": "Setup Quality",
    "breakout_strength": "Breakout Strength",
    "liquidity": "Liquidity",
    "risk_reward": "Risk/Reward",
}


def analyze_stock(
    *,
    symbol: str,
    daily_prices: list[dict[str, Any]],
    earnings_date: str,
    earnings_timing: str,
    gap_pct: float,
    current_price: float,
    watch_weeks: int = 5,
) -> dict[str, Any] | None:
    if not daily_prices or len(daily_prices) < 5:
        return None

    weekly_candles = _daily_to_weekly(daily_prices, earnings_date=earnings_date)
    if not weekly_candles:
        return None

    pattern = _analyze_weekly_pattern(weekly_candles, earnings_date, watch_weeks=watch_weeks)
    setup_score = _calculate_setup_quality(gap_pct, pattern)
    red_candle = pattern.get("red_candle")
    breakout = (
        _calculate_breakout(weekly_candles, red_candle, current_price)
        if isinstance(red_candle, dict)
        else {"is_breakout": False, "breakout_pct": 0.0, "volume_confirmation": False, "score": 0.0}
    )
    liquidity = _calculate_liquidity(daily_prices, current_price)
    risk_reward = (
        _calculate_risk_reward(current_price, red_candle)
        if isinstance(red_candle, dict)
        else {
            "entry_price": current_price,
            "stop_price": 0.0,
            "target_price": 0.0,
            "risk_pct": 0.0,
            "reward_pct": 0.0,
            "risk_reward_ratio": 0.0,
            "score": 25.0,
        }
    )
    composite = _calculate_composite_score(
        setup_score=setup_score,
        breakout_score=float(breakout["score"]),
        liquidity_score=float(liquidity["score"]),
        rr_score=float(risk_reward["score"]),
    )
    return {
        "symbol": symbol,
        "stage": pattern["stage"],
        "earnings_date": earnings_date,
        "earnings_timing": earnings_timing,
        "gap_pct": gap_pct,
        "weeks_since_earnings": pattern["weeks_since_earnings"],
        "red_candle": red_candle,
        "current_price": current_price,
        "breakout_pct": breakout["breakout_pct"],
        "entry_price": risk_reward["entry_price"],
        "stop_price": risk_reward["stop_price"],
        "target_price": risk_reward["target_price"],
        "risk_pct": risk_reward["risk_pct"],
        "risk_reward_ratio": risk_reward["risk_reward_ratio"],
        "adv20_dollar": liquidity["adv20_dollar"],
        "composite_score": composite["composite_score"],
        "rating": composite["rating"],
        "guidance": composite["guidance"],
        "components": composite["component_breakdown"],
    }


def _daily_to_weekly(
    daily_prices: list[dict[str, Any]],
    earnings_date: str | None = None,
) -> list[dict[str, Any]]:
    if not daily_prices:
        return []

    earnings_dt = _parse_date(earnings_date) if earnings_date else None
    sorted_prices = sorted(daily_prices, key=lambda item: str(item.get("date") or ""))
    week_groups: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for day in sorted_prices:
        day_dt = _parse_date(str(day["date"]))
        week_key = day_dt.isocalendar()[:2]
        if earnings_dt:
            earnings_week_key = earnings_dt.isocalendar()[:2]
            if week_key == earnings_week_key and day_dt < earnings_dt:
                continue
        week_groups.setdefault(week_key, []).append(day)

    latest_dt = _parse_date(str(sorted_prices[-1]["date"]))
    latest_week_key = latest_dt.isocalendar()[:2]
    latest_partial = latest_dt.isocalendar()[2] < 5

    weekly: list[dict[str, Any]] = []
    for week_key in sorted(week_groups):
        days = week_groups[week_key]
        if not days:
            continue
        iso_year, iso_week = week_key
        monday = _iso_week_to_monday(iso_year, iso_week)
        partial = week_key == latest_week_key and latest_partial
        if earnings_dt and week_key == earnings_dt.isocalendar()[:2] and len(days) < 5:
            partial = True
        week_open = float(days[0].get("open") or 0)
        week_close = float(days[-1].get("close") or 0)
        weekly.append(
            {
                "week_start": monday.isoformat(),
                "year": iso_year,
                "week": iso_week,
                "open": round(week_open, 2),
                "high": round(max(float(day.get("high") or 0) for day in days), 2),
                "low": round(min(float(day.get("low") or 0) for day in days), 2),
                "close": round(week_close, 2),
                "volume": int(sum(float(day.get("volume") or 0) for day in days)),
                "is_green": week_close >= week_open,
                "partial_week": partial,
                "trading_days": len(days),
            }
        )
    weekly.reverse()
    return weekly


def _analyze_weekly_pattern(
    weekly_candles: list[dict[str, Any]],
    earnings_date: str,
    *,
    watch_weeks: int = 5,
) -> dict[str, Any]:
    result = {
        "weeks_since_earnings": 0,
        "earnings_week_idx": None,
        "red_candle": None,
        "is_breakout": False,
        "breakout_pct": 0.0,
        "stage": "MONITORING",
    }
    if not weekly_candles:
        return result

    earnings_dt = _parse_date(earnings_date)
    earnings_year, earnings_week, _ = earnings_dt.isocalendar()
    earnings_week_idx = None
    for index, candle in enumerate(weekly_candles):
        if int(candle["year"]) == earnings_year and int(candle["week"]) == earnings_week:
            earnings_week_idx = index
            break
    result["earnings_week_idx"] = earnings_week_idx

    if earnings_week_idx is not None:
        result["weeks_since_earnings"] = earnings_week_idx
    else:
        latest_dt = _parse_date(str(weekly_candles[0]["week_start"]))
        result["weeks_since_earnings"] = max(0, (latest_dt - earnings_dt).days // 7)

    if result["weeks_since_earnings"] > watch_weeks:
        result["stage"] = "EXPIRED"
        return result

    red_candle = _find_red_candle(weekly_candles, earnings_week_idx=earnings_week_idx)
    result["red_candle"] = red_candle
    if not red_candle:
        return result

    current_candle = weekly_candles[0]
    if bool(current_candle["is_green"]) and float(current_candle["close"]) > float(red_candle["high"]):
        result["is_breakout"] = True
        result["breakout_pct"] = round(
            ((float(current_candle["close"]) - float(red_candle["high"])) / float(red_candle["high"])) * 100.0,
            2,
        )
        result["stage"] = "BREAKOUT"
    else:
        result["stage"] = "SIGNAL_READY"
    return result


def _find_red_candle(
    weekly_candles: list[dict[str, Any]],
    *,
    earnings_week_idx: int | None,
) -> dict[str, Any] | None:
    if not weekly_candles:
        return None
    end_idx = earnings_week_idx if earnings_week_idx is not None else len(weekly_candles) - 1
    for index in range(0, min(end_idx, len(weekly_candles))):
        candle = weekly_candles[index]
        if bool(candle["is_green"]):
            continue
        candle_range = float(candle["high"]) - float(candle["low"])
        if candle_range > 0:
            body_low = min(float(candle["open"]), float(candle["close"]))
            lower_wick_pct = ((body_low - float(candle["low"])) / candle_range) * 100.0
        else:
            lower_wick_pct = 0.0
        surrounding_volumes = [
            float(weekly_candles[i].get("volume") or 0)
            for i in range(max(0, index - 2), min(len(weekly_candles), index + 3))
            if i != index
        ]
        avg_volume = (sum(surrounding_volumes) / len(surrounding_volumes)) if surrounding_volumes else 0.0
        volume_vs_avg = (float(candle.get("volume") or 0) / avg_volume) if avg_volume > 0 else 1.0
        return {
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "open": float(candle["open"]),
            "close": float(candle["close"]),
            "week_start": candle["week_start"],
            "week_index": index,
            "lower_wick_pct": round(lower_wick_pct, 1),
            "volume_vs_avg": round(volume_vs_avg, 2),
        }
    return None


def _calculate_setup_quality(gap_pct: float, pattern_result: dict[str, Any]) -> float:
    score = 0.0
    if gap_pct >= 10.0:
        score += 50
    elif gap_pct >= 7.0:
        score += 40
    elif gap_pct >= 5.0:
        score += 30
    elif gap_pct >= 3.0:
        score += 20
    else:
        score += 10

    stage = str(pattern_result.get("stage") or "MONITORING")
    weeks = int(pattern_result.get("weeks_since_earnings") or 0)
    red_candle = pattern_result.get("red_candle")
    if stage == "BREAKOUT":
        score += 50
    elif stage == "SIGNAL_READY":
        score += 40
        if isinstance(red_candle, dict) and float(red_candle.get("lower_wick_pct") or 0) > 30:
            score += 5
    elif stage == "MONITORING":
        score += 25 if weeks <= 2 else 15
    return min(100.0, score)


def _calculate_breakout(
    weekly_candles: list[dict[str, Any]],
    red_candle: dict[str, Any],
    current_price: float,
) -> dict[str, Any]:
    result = {
        "is_breakout": False,
        "breakout_pct": 0.0,
        "volume_confirmation": False,
        "score": 0.0,
    }
    red_high = float(red_candle.get("high") or 0)
    if red_high <= 0 or not weekly_candles:
        return result

    breakout_pct = ((current_price - red_high) / red_high) * 100.0
    result["breakout_pct"] = round(breakout_pct, 2)
    if breakout_pct <= 0:
        return result

    result["is_breakout"] = True
    prior_volumes = [
        float(weekly_candles[i].get("volume") or 0)
        for i in range(1, min(5, len(weekly_candles)))
        if float(weekly_candles[i].get("volume") or 0) > 0
    ]
    avg_volume = (sum(prior_volumes) / len(prior_volumes)) if prior_volumes else 0.0
    volume_confirmation = avg_volume > 0 and float(weekly_candles[0].get("volume") or 0) > avg_volume
    result["volume_confirmation"] = volume_confirmation

    if breakout_pct >= 3.0 and volume_confirmation:
        result["score"] = 100.0
    elif breakout_pct >= 2.0 and volume_confirmation:
        result["score"] = 85.0
    elif breakout_pct >= 1.0:
        result["score"] = 70.0
    else:
        result["score"] = 55.0
    return result


def _calculate_liquidity(daily_prices: list[dict[str, Any]], current_price: float) -> dict[str, Any]:
    result = {
        "adv20_dollar": 0.0,
        "avg_volume_20d": 0.0,
        "price": current_price,
        "passes_all": False,
        "score": 15.0,
    }
    if not daily_prices or current_price <= 0:
        return result

    recent_20 = daily_prices[:20]
    volumes = [float(item.get("volume") or 0) for item in recent_20]
    if not volumes:
        return result
    avg_volume = sum(volumes) / len(volumes)
    dollar_volumes = [
        float(item.get("volume") or 0) * float(item.get("close") or 0)
        for item in recent_20
        if float(item.get("volume") or 0) > 0 and float(item.get("close") or 0) > 0
    ]
    adv20 = (sum(dollar_volumes) / len(dollar_volumes)) if dollar_volumes else avg_volume * current_price
    passes_adv20 = adv20 >= 25_000_000
    passes_volume = avg_volume >= 1_000_000
    passes_price = current_price >= 10.0
    passes_count = sum([passes_adv20, passes_volume, passes_price])

    if passes_count == 3:
        score = 100.0 if adv20 > 100_000_000 else 85.0 if adv20 > 50_000_000 else 70.0
    elif passes_count == 2:
        score = 40.0
    else:
        score = 15.0

    result.update(
        {
            "adv20_dollar": round(adv20, 0),
            "avg_volume_20d": round(avg_volume, 0),
            "passes_all": passes_count == 3,
            "score": score,
        }
    )
    return result


def _calculate_risk_reward(current_price: float, red_candle: dict[str, Any]) -> dict[str, Any]:
    result = {
        "entry_price": current_price,
        "stop_price": 0.0,
        "target_price": 0.0,
        "risk_pct": 0.0,
        "reward_pct": 0.0,
        "risk_reward_ratio": 0.0,
        "score": 25.0,
    }
    stop_price = float(red_candle.get("low") or 0)
    if current_price <= 0 or stop_price <= 0 or stop_price >= current_price:
        return result

    risk = current_price - stop_price
    reward = risk * 2.0
    target_price = current_price + reward
    risk_pct = (risk / current_price) * 100.0
    reward_pct = (reward / current_price) * 100.0
    rr_ratio = reward / risk if risk > 0 else 0.0
    if rr_ratio >= 3.0:
        score = 100.0
    elif rr_ratio >= 2.5:
        score = 85.0
    elif rr_ratio >= 2.0:
        score = 70.0
    elif rr_ratio >= 1.5:
        score = 50.0
    else:
        score = 25.0

    result.update(
        {
            "entry_price": round(current_price, 2),
            "stop_price": round(stop_price, 2),
            "target_price": round(target_price, 2),
            "risk_pct": round(risk_pct, 2),
            "reward_pct": round(reward_pct, 2),
            "risk_reward_ratio": round(rr_ratio, 2),
            "score": score,
        }
    )
    return result


def _calculate_composite_score(
    *,
    setup_score: float,
    breakout_score: float,
    liquidity_score: float,
    rr_score: float,
) -> dict[str, Any]:
    component_scores = {
        "setup_quality": setup_score,
        "breakout_strength": breakout_score,
        "liquidity": liquidity_score,
        "risk_reward": rr_score,
    }
    composite = round(
        sum(component_scores[key] * weight for key, weight in COMPONENT_WEIGHTS.items()),
        1,
    )
    weakest_key = min(component_scores, key=component_scores.get)
    strongest_key = max(component_scores, key=component_scores.get)
    rating_info = _get_rating(composite)
    return {
        "composite_score": composite,
        "rating": rating_info["rating"],
        "rating_description": rating_info["description"],
        "guidance": rating_info["guidance"],
        "weakest_component": COMPONENT_LABELS[weakest_key],
        "weakest_score": component_scores[weakest_key],
        "strongest_component": COMPONENT_LABELS[strongest_key],
        "strongest_score": component_scores[strongest_key],
        "component_breakdown": {
            key: {
                "score": component_scores[key],
                "weight": weight,
                "weighted": round(component_scores[key] * weight, 1),
                "label": COMPONENT_LABELS[key],
            }
            for key, weight in COMPONENT_WEIGHTS.items()
        },
    }


def _get_rating(composite: float) -> dict[str, str]:
    if composite >= 85:
        return {
            "rating": "Strong Setup",
            "description": "High-conviction PEAD trade with all components aligned",
            "guidance": "High-conviction PEAD trade, full position size",
        }
    if composite >= 70:
        return {
            "rating": "Good Setup",
            "description": "Solid PEAD setup with minor imperfections",
            "guidance": "Solid PEAD setup, standard position size",
        }
    if composite >= 55:
        return {
            "rating": "Developing",
            "description": "PEAD pattern forming but not yet fully actionable",
            "guidance": "Watchlist, wait for cleaner breakout",
        }
    return {
        "rating": "Weak",
        "description": "Insufficient PEAD characteristics for trading",
        "guidance": "Not actionable",
    }


def _parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _iso_week_to_monday(iso_year: int, iso_week: int) -> date:
    jan4 = date(iso_year, 1, 4)
    week1_monday = jan4 - timedelta(days=jan4.weekday())
    return week1_monday + timedelta(weeks=iso_week - 1)
