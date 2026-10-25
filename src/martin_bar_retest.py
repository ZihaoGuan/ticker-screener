"""Paper-only Martin Bar Watchlist retest evaluator."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Any

import pandas as pd

STRATEGY_ID = "martin_bar_retest_v1"
RULE_VERSION = STRATEGY_ID


def _rows(frame: Any, day: date) -> tuple[list[dict[str, Any]], str | None]:
    if frame is None or getattr(frame, "empty", False):
        return [], "history_missing_sessions"
    data = frame.loc[pd.to_datetime(frame.index).date <= day].sort_index()
    required = ("Open", "High", "Low", "Close", "Volume")
    if any(key not in data for key in required) or data.index.has_duplicates:
        return [], "invalid_or_duplicate_ohlcv"
    rows = []
    for idx, row in data.iterrows():
        try:
            values = {k.lower(): float(row[k]) for k in required}
        except (TypeError, ValueError, OverflowError):
            return [], "invalid_or_duplicate_ohlcv"
        if (any(not math.isfinite(v) or v <= 0 for k, v in values.items() if k != "volume")
                or not math.isfinite(values["volume"]) or values["volume"] < 0
                or not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]):
            return [], "invalid_or_duplicate_ohlcv"
        rows.append({"date": idx.date(), **values})
    return rows, None


def evaluate(frame: Any, event: dict[str, Any], day: date, sessions: list[date]) -> dict[str, Any]:
    ticker = str(event.get("ticker") or "").upper()
    identity = event.get("event_identity") or {}
    base = {"ticker": ticker, "event_date": str(event.get("event_date") or "")[:10],
            "event_identity": identity, "source_run_id": event.get("source_run_id"),
            "gap_low": event.get("gap_low"), "category": event.get("category"),
            "rule_version": RULE_VERSION, "decision_date": day.isoformat(),
            "invalidation": ({"basis": "original_gap_low", "price": event.get("gap_low")} if event.get("gap_low") is not None else
                {"basis": "first_pullback_episode_and_30_session_expiry"})}
    try:
        event_day = date.fromisoformat(str(event.get("event_date"))[:10])
    except (TypeError, ValueError):
        return {**base, "status": "blocked_data", "reasons": ["invalid_event_date"]}
    try:
        day_index = sessions.index(day)
        event_index = sessions.index(event_day)
        age = day_index - event_index
    except ValueError:
        return {**base, "status": "blocked_data", "reasons": ["history_missing_sessions"]}
    rows, row_error = _rows(frame, day)
    if row_error:
        return {**base, "status": "blocked_data", "reasons": [row_error]}
    row_dates = [r["date"] for r in rows]
    needed = sessions[max(0, event_index - 119):day_index + 1]
    session_set = set(sessions)
    needed_set = set(needed)
    if not rows or row_dates[-1:] != [day] or event_day not in row_dates or any(d not in session_set for d in row_dates) or [d for d in row_dates if d in needed_set] != needed:
        return {**base, "status": "blocked_data", "reasons": ["history_missing_sessions"]}
    # Seed all indicators and reconstruct the first episode from this exact validated window.
    rows = [row for row in rows if row["date"] in needed_set]
    base["event_date"] = event_day.isoformat()
    base["event_age_sessions"] = age
    if age < 1:
        return {**base, "status": "watch", "reasons": ["awaiting_first_post_event_session"]}
    if age > 30:
        return {**base, "status": "retired", "reasons": ["event_expired_30_sessions"]}
    index = {r["date"]: i for i, r in enumerate(rows)}
    if event_day not in index:
        return {**base, "status": "blocked_data", "reasons": ["history_missing_sessions"]}
    ei, end = index[event_day], len(rows) - 1
    gap_low = event.get("gap_low")
    if gap_low is not None and any(r["close"] < float(gap_low) for r in rows[ei+1:end+1]):
        return {**base, "status": "retired", "reasons": ["gap_low_close_failed"]}
    closes = [r["close"] for r in rows]
    emas = {}
    for span in (9, 21):
        vals = pd.Series(closes).ewm(span=span, adjust=False).mean().tolist()
        emas[span] = vals
    adr = [((r["high"] - r["low"]) / r["close"] * 100) for r in rows]
    selected = None
    start = None
    for i in range(ei + 1, end + 1):
        if i == 0:
            continue
        tol = .25 * rows[i]["close"] * (sum(adr[max(0, i-13):i+1]) / 14 if i >= 13 else 0) / 100
        match = [p for p in (9, 21) if rows[i-1]["close"] >= emas[p][i-1] and rows[i]["low"] < rows[i-1]["low"] and rows[i]["low"] <= emas[p][i] + tol and rows[i]["high"] >= emas[p][i] - tol]
        if match:
            selected, start = min(match), i
            break
    if start is None:
        return {**base, "status": "watch", "reasons": ["awaiting_first_ema_support_pullback"]}
    episode_age = end - start + 1
    chosen = emas[selected][end]
    sma50 = sum(closes[end-49:end+1]) / 50 if end >= 49 else None
    adv20 = sum(r["close"] * r["volume"] for r in rows[end-19:end+1]) / 20 if end >= 19 else None
    adr14 = sum(adr[end-13:end+1]) / 14 if end >= 13 else None
    bar = rows[end]
    tolerance = .25 * bar["close"] * adr14 / 100 if adr14 is not None else None
    current_touch = tolerance is not None and bar["high"] >= chosen - tolerance and bar["low"] <= chosen + tolerance
    evidence = {**base, "episode_start_date": rows[start]["date"].isoformat(), "episode_age_sessions": episode_age,
        "selected_ema_period": selected, "ema9": emas[9][end], "ema21": emas[21][end], "sma50": sma50,
        "adv20_dollars": adv20, "adr14_pct": adr14, "ema_touch_tolerance": tolerance,
        "current_ema_band_touch": bool(current_touch), "candle": {k: bar[k] for k in ("open","high","low","close","volume")}}
    if episode_age > 5:
        return {**evidence, "status": "retired", "reasons": ["first_support_episode_expired"]}
    reasons = []
    if bar["close"] < 5: reasons.append("price_below_5")
    if adv20 is None or adv20 < 20_000_000: reasons.append("adv20_below_20m")
    if sma50 is None or bar["close"] <= sma50: reasons.append("close_not_above_sma50")
    if end < 5 or emas[21][end] <= emas[21][end-5]: reasons.append("ema21_not_rising")
    if not current_touch: reasons.append("awaiting_current_ema_band_touch")
    if bar["close"] <= chosen or bar["close"] <= bar["open"]: reasons.append("awaiting_bullish_ema_reclaim")
    if reasons:
        return {**evidence, "status": "support_zone", "reasons": reasons}
    risk = 100 * (bar["close"] - bar["low"]) / bar["close"]
    result = {**evidence, "reference_entry": bar["close"], "stop": bar["low"], "reference_risk_pct": risk,
        "max_entry": round(min(bar["close"], bar["low"] / .95), 6),
        "stop_to_adr": risk / adr14 if adr14 else None, "wide_stop_caution": bool(adr14 and risk / adr14 > .5),
        "status": "waiting_risk" if risk > 5 else "confirmed", "reasons": ["risk_exceeds_5pct"] if risk > 5 else []}
    return result


def fingerprint(frame: Any, as_of: date | None = None) -> str:
    if frame is not None and as_of is not None:
        frame = frame.loc[pd.to_datetime(frame.index).date <= as_of]
    payload = frame.to_json(orient="split", date_format="iso", double_precision=15) if frame is not None else "missing"
    return hashlib.sha256(payload.encode()).hexdigest()
