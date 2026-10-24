"""Pure rules for the experimental Martin-inspired daily gap retest book."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any

STRATEGY_ID = "martin_daily_gap_retest_v1"
REPLAY_STRATEGY_ID = "martin_daily_gap_retest_replay_20261019_v1"
REPLAY_V2_STRATEGY_ID = "martin_daily_gap_retest_replay_20261019_v2"
REPLAY_V3_STRATEGY_ID = "martin_daily_gap_retest_replay_20261019_v3"
REPLAY_V4_STRATEGY_ID = "martin_daily_gap_retest_replay_20261019_v4"
BOOK_KEY = "gap_retest"
DISCOVERY_SOURCE = "power_gap_volume_watch"
DISCOVERY_RULES_VERSION = "power_gap_volume_watch_v1"
ENTRY_POLICY_VERSION = "gap_daily_reclaim_next_open_v1"
STOP_POLICY_VERSION = "gap_reclaim_candle_low_v1"
MANAGEMENT_POLICY_VERSION = "gap_trim10_3r_ema9_v1"
CONFIG = {
    "version": STRATEGY_ID, "history_sessions": 260, "load_sessions": 326,
    "age_min": 1, "age_max": 60, "episode_sessions": 5, "touch_adr_fraction": .25,
    "minimum_price": 5, "minimum_adv20_dollars": 20_000_000, "max_stop_pct": 5,
    "sizing_base": 100_000, "risk_per_trade": 500, "allocation_cap": 20_000,
    "max_positions": 8, "committed_risk_cap": 2_500, "adv_participation": .001,
    "slippage_bps": 10, "trim_fraction": .10, "trim_r": 3,
}
CONFIG_HASH = hashlib.sha256(json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _day(value: Any) -> date:
    return value.date() if hasattr(value, "date") else date.fromisoformat(str(value)[:10])


def _rows(frame: Any, as_of: date) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if hasattr(frame, "iterrows"):
        source = frame.iterrows()
    else:
        source = ((row.get("date", row.get("trade_date")), row) for row in frame)
    result = []
    for index, row in source:
        day = _day(index)
        if day > as_of:
            continue
        values = {str(key).lower().replace("adj close", "adj_close"): value for key, value in (row.items() if hasattr(row, "items") else row.items())}
        bar = {key: _finite(values.get(key)) for key in ("open", "high", "low", "close", "volume")}
        bar["date"] = day
        result.append(bar)
    return sorted(result, key=lambda x: x["date"])


def _valid_bars(rows: list[dict[str, Any]]) -> bool:
    dates = [row["date"] for row in rows]
    if len(set(dates)) != len(dates):
        return False
    for row in rows:
        o, h, l, c, v = (row[k] for k in ("open", "high", "low", "close", "volume"))
        if any(x is None or x <= 0 for x in (o, h, l, c, v)) or not l <= min(o, c) <= max(o, c) <= h:
            return False
    return True


def _indicators(rows: list[dict[str, Any]]) -> list[dict[str, float]]:
    closes = [row["close"] for row in rows]
    result = []
    for i, close in enumerate(closes):
        start = max(0, i - 259)
        ema = {}
        for span in (9, 21):
            value = closes[start]
            alpha = 2 / (span + 1)
            for item in closes[start + 1:i + 1]:
                value += alpha * (item - value)
            ema[span] = value
        recent = rows[max(0, i - 13):i + 1]
        adr = (sum((r["high"] - r["low"]) / r["close"] * 100 for r in recent) / len(recent)
               if len(recent) == 14 and all(r["high"] > r["low"] for r in recent) else None)
        adv = sum(r["close"] * r["volume"] for r in rows[max(0, i - 19):i + 1]) / 20 if i >= 19 else None
        sma50 = sum(closes[i - 49:i + 1]) / 50 if i >= 49 else None
        result.append({"ema9": ema[9], "ema21": ema[21], "adr14_pct": adr, "adv20_dollars": adv, "sma50": sma50})
    return result


def daily_indicators(frame_as_of_day: Any) -> dict[str, float | None]:
    """Return the final as-of EMA9/21, ADR14, ADV20, and SMA50 values."""
    if frame_as_of_day is None or (hasattr(frame_as_of_day, "empty") and frame_as_of_day.empty):
        return {}
    rows = _rows(frame_as_of_day, _day(frame_as_of_day.index[-1] if hasattr(frame_as_of_day, "index") else frame_as_of_day[-1]["date"]))
    if not _valid_bars(rows):
        return {}
    return _indicators(rows)[-1] if rows else {}


def _intersects(bar: dict[str, Any], center: float, tolerance: float) -> bool:
    return bar["high"] >= center - tolerance and bar["low"] <= center + tolerance


def evaluate_gap_retest(frame_as_of_day: Any, original_event: dict[str, Any], prior_watch_state: dict[str, Any] | None = None,
                        *, ticker: str | None = None, calendar_sessions: list[date] | None = None) -> dict[str, Any]:
    """Evaluate one event using only bars through the final frame session."""
    if frame_as_of_day is None or (hasattr(frame_as_of_day, "empty") and frame_as_of_day.empty):
        return {"ticker": str(ticker or (original_event or {}).get("ticker") or "").upper(),
                "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
    rows = _rows(frame_as_of_day, _day(frame_as_of_day.index[-1] if hasattr(frame_as_of_day, "index") else frame_as_of_day[-1]["date"]))
    event = dict(original_event or {})
    symbol = str(ticker or event.get("ticker") or "").strip().upper()
    try:
        gap_day = _day(event.get("original_gap_date", event.get("gap_date")))
    except (TypeError, ValueError):
        return {"ticker": symbol, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["invalid_gap_date"]}
    if not rows or not _valid_bars(rows):
        return {"ticker": symbol, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["invalid_ohlcv"]}
    index = {row["date"]: i for i, row in enumerate(rows)}
    if gap_day not in index:
        return {"ticker": symbol, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
    gap_i, end_i = index[gap_day], len(rows) - 1
    if calendar_sessions:
        sessions = list(calendar_sessions)
        if not sessions or sessions[-1] != rows[end_i]["date"] or gap_day not in sessions:
            return {"ticker": symbol, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
        gap_session_i, end_session_i = sessions.index(gap_day), len(sessions) - 1
        age = end_session_i - gap_session_i
        # Missing exchange-session bars invalidate the reconstructed EMA and episode history.
        required = sessions[max(0, gap_session_i - 259):end_session_i + 1]
        if [row["date"] for row in rows if row["date"] in set(required)] != required:
            return {"ticker": symbol, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
    else:
        age = end_i - gap_i
    gap_low = _finite(event.get("gap_day_low")) or rows[gap_i]["low"]
    indicators = _indicators(rows)
    base = {"ticker": symbol, "original_gap_date": gap_day.isoformat(), "gap_age_sessions": age,
            "event_id": event.get("event_id"), "source_run_id": event.get("source_run_id"),
            "discovery_rules_version": event.get("discovery_rules_version", DISCOVERY_RULES_VERSION),
            "config_version": STRATEGY_ID, "config_hash": CONFIG_HASH}
    if age > 60:
        return {**base, "status": "retired", "gap_state": "retired", "reasons": ["gap_event_expired"]}
    if age < 1:
        return {**base, "status": "watch", "gap_state": "watch", "reasons": ["awaiting_first_retest"]}
    if any(row["close"] < gap_low for row in rows[gap_i + 1:end_i + 1]):
        return {**base, "status": "retired", "gap_state": "retired", "reasons": ["gap_support_close_failed"]}
    if gap_i < 259 or end_i + 1 < 260:
        return {**base, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
    episode = prior_watch_state if prior_watch_state and prior_watch_state.get("episode_start_date") else None
    selected = int(episode["selected_ema_period"]) if episode else None
    start_i = index.get(_day(episode["episode_start_date"])) if episode else None
    # Reconstruct the first episode from the gap when no saved watch exists.
    if episode is None:
        for i in range(gap_i + 1, end_i + 1):
            if i < 1 or indicators[i]["adr14_pct"] is None:
                continue
            tolerance = .25 * rows[i]["close"] * indicators[i]["adr14_pct"] / 100
            qualifying = []
            for period in (9, 21):
                prev_value, value = indicators[i - 1][f"ema{period}"], indicators[i][f"ema{period}"]
                if rows[i - 1]["close"] >= prev_value and rows[i]["low"] < rows[i - 1]["low"] and _intersects(rows[i], value, tolerance):
                    qualifying.append(period)
            if qualifying:
                selected, start_i = min(qualifying), i
                break
    if start_i is None:
        return {**base, "status": "watch", "gap_state": "watch", "reasons": ["awaiting_first_retest"]}
    if end_i - start_i >= 5:
        return {**base, "status": "retired", "gap_state": "retired", "episode_start_date": rows[start_i]["date"].isoformat(), "selected_ema_period": selected,
                "reasons": ["first_episode_already_expired"] if episode is None else ["first_episode_expired"]}
    chosen = f"ema{selected}"
    today, bar = indicators[end_i], rows[end_i]
    adr = today["adr14_pct"]
    tolerance = .25 * bar["close"] * adr / 100 if adr is not None else None
    touched = tolerance is not None and _intersects(bar, today[chosen], tolerance)
    gates = {"history": end_i + 1 >= 260, "trend_close_above_sma50": today["sma50"] is not None and bar["close"] > today["sma50"],
             "ema21_rising_5": end_i >= 5 and today["ema21"] > indicators[end_i - 5]["ema21"],
             "adv20": today["adv20_dollars"] is not None and today["adv20_dollars"] >= 20_000_000,
             "price": bar["close"] >= 5, "adr": adr is not None, "chosen_ema_band_touch": bool(touched),
             "close_above_ema": bar["close"] > today[chosen], "bullish_close": bar["close"] > bar["open"]}
    evidence = {**base, "episode_start_date": rows[start_i]["date"].isoformat(), "episode_age_sessions": end_i - start_i + 1,
                "selected_ema_period": selected, "selected_ema_value": today[chosen], "touch_tolerance": tolerance,
                "adr14_pct": adr, "adv20_dollars": today["adv20_dollars"], "sma50": today["sma50"],
                "ema21_five_sessions_ago": indicators[end_i - 5]["ema21"] if end_i >= 5 else None,
                "confirmation_date": bar["date"].isoformat(), "candle": {k: bar[k] for k in ("open", "high", "low", "close", "volume")}, "gates": gates}
    failed = [name for name, value in gates.items() if value is not True]
    if failed:
        reason_map = {"history": "history_missing_sessions", "trend_close_above_sma50": "trend_not_qualified", "ema21_rising_5": "trend_not_qualified",
                      "adv20": "liquidity_below_minimum", "price": "price_below_minimum", "adr": "missing_adr", "chosen_ema_band_touch": "awaiting_daily_reclaim",
                      "close_above_ema": "awaiting_daily_reclaim", "bullish_close": "awaiting_daily_reclaim"}
        return {**evidence, "status": "support_zone", "gap_state": "support_zone", "reasons": list(dict.fromkeys(reason_map[x] for x in failed))}
    reference_money, stop_money = _money(bar["close"]), _money(bar["low"])
    risk_money = 100 * (reference_money - stop_money) / reference_money
    reference, stop, risk = float(reference_money), float(stop_money), float(risk_money)
    max_entry = min(stop_money / Decimal("0.95"), reference_money)
    result = {**evidence, "status": "confirmed", "gap_state": "confirmed", "reasons": [], "reference_entry": reference,
              "stop": stop, "stop_policy_version": STOP_POLICY_VERSION, "reference_risk_pct": risk,
              "max_entry": float(_money(max_entry, ROUND_DOWN)), "stop_to_adr": risk / adr if adr else None,
              "wide_stop_caution": bool(adr and risk / adr > .5), "entry_policy_version": ENTRY_POLICY_VERSION}
    result.update(entry=reference, limit_price=result["max_entry"], plan={"reference_entry": reference, "initial_stop": stop,
        "stop": stop, "max_entry": result["max_entry"], "reference_risk_pct": risk, "adr14_pct": adr,
        "stop_to_adr": result["stop_to_adr"], "stop_basis": "daily_reclaim_low", "stop_policy_version": STOP_POLICY_VERSION})
    if stop <= 0 or stop >= reference:
        result.update(status="blocked_data", gap_state="blocked_data", reasons=["opened_at_or_below_stop"])
    elif risk_money > 5:
        result.update(status="waiting_risk", gap_state="waiting_risk", reasons=["risk_exceeds_5pct"])
    return result


def _money(value: Any, rounding=ROUND_HALF_UP) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.000001"), rounding=rounding)


def rank_confirmations(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def descending(item: dict[str, Any], field: str) -> float:
        value = _finite(item.get(field))
        return -(value if value is not None else float("-inf"))
    return sorted(candidates, key=lambda item: (descending(item, "relative_return20_pct"),
        descending(item, "adr14_pct"), descending(item, "adv20_dollars"), str(item.get("ticker") or "")))


def size_gap_order(candidate: dict[str, Any], *, available_cash: Any, available_risk: Any, slots_available: int,
                   committed_positions: int = 0) -> dict[str, Any]:
    stop, cap, adv = (_money(candidate[k]) for k in ("stop", "max_entry", "adv20_dollars"))
    if slots_available <= 0:
        return {"shares": 0, "status": "waiting_capacity", "reasons": ["capacity_full"]}
    if cap <= stop:
        return {"shares": 0, "status": "waiting_risk", "reasons": ["risk_exceeds_5pct"]}
    distance = cap - stop
    limits = [Decimal("500") / distance, Decimal("20000") / cap, _money(available_cash) / cap,
              _money(available_risk) / distance, Decimal("0.001") * adv / cap]
    shares = max(0, int(min(limits).to_integral_value(rounding=ROUND_DOWN)))
    if shares <= 0:
        return {"shares": 0, "status": "waiting_capacity", "reasons": ["cash_or_risk_budget"]}
    return {"shares": shares, "status": "pending", "reasons": [], "reservation_cash": float(_money(shares * cap)),
            "planned_stop_risk": float(_money(shares * distance)), "maximum_entry": float(cap), "stop": float(stop)}


def resolve_gap_next_open(order: dict[str, Any], execution_bar: dict[str, Any]) -> dict[str, Any]:
    opening, stop, cap = (_finite(execution_bar.get("open")), _finite(order.get("stop")), _finite(order.get("max_entry", order.get("maximum_entry"))))
    if opening is None or stop is None or cap is None:
        return {"status": "blocked_data", "reason": "missing_execution_bar"}
    if opening <= stop:
        return {"status": "cancelled", "reason": "opened_at_or_below_stop"}
    fill = _money(Decimal(str(opening)) * Decimal("1.001"))
    if fill > _money(cap):
        return {"status": "expired", "reason": "opened_above_max_entry", "model_fill": float(fill)}
    return {"status": "filled", "fill_price": float(fill), "shares": int(order.get("shares", 0))}
