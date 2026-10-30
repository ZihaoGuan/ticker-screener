"""Explicit hindsight earnings guard used by isolated replay books."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
from typing import Any

POLICY = "earnings_risk_retrospective_v1"
HOLDINGS_POLICY = "earnings_holdings_retrospective_v2"


def json_safe(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return value


def load_earnings(connection: Any, tickers: set[str], day: dt.date) -> dict[str, dict[str, Any]]:
    """Load recorded future events. This deliberately does not claim point-in-time availability."""
    if not tickers:
        return {}
    with connection.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT ON (ticker) ticker, earnings_date, session_timing, source
                          FROM earnings_events WHERE ticker=ANY(%s) AND earnings_date >= %s
                          ORDER BY ticker, earnings_date""", (sorted(tickers), day))
        rows = cursor.fetchall()
    return {str(row[0]).upper(): {"date": row[1], "timing": row[2], "source": row[3]} for row in rows}


def sessions_to_event(day: dt.date, event_date: dt.date) -> int | None:
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=7), end=event_date + dt.timedelta(days=7))
    # Use the calendar's bounded session index directly. `sessions_in_range` rejects a
    # holiday endpoint when the calendar's last available session is the prior day.
    sessions = [stamp.date() for stamp in calendar.sessions]
    start_index = next((index for index, session in enumerate(sessions) if session >= day), None)
    event_session = next((session for session in sessions if session >= event_date), None)
    result = (None if start_index is None or event_session is None or event_session < day else
              sessions.index(event_session) - start_index)
    return result


def cached_sessions_to_event(day: dt.date, event_date: dt.date,
                             cache: dict[tuple[dt.date, dt.date], int | None]) -> int | None:
    key = (day, event_date)
    if key not in cache:
        if len(cache) >= 512:
            cache.clear()
        cache[key] = sessions_to_event(day, event_date)
    return cache[key]


def entry_decision(event: dict[str, Any] | None, day: dt.date, *, evidence_mode: str = POLICY,
                   distance_cache: dict[tuple[dt.date, dt.date], int | None] | None = None) -> dict[str, Any]:
    if not event:
        return {"allowed": True, "status": "unknown", "reason": "earnings_date_unknown",
                "evidence_mode": evidence_mode, "retrospective_lookup": True}
    event_date = event["date"] if isinstance(event["date"], dt.date) else dt.date.fromisoformat(str(event["date"])[:10])
    distance = cached_sessions_to_event(day, event_date, distance_cache) if distance_cache is not None else sessions_to_event(day, event_date)
    if distance is None:
        return {"allowed": True, "status": "unassessable", "reason": "earnings_session_unavailable",
                "earnings_date": event_date.isoformat(), "session_timing": event.get("timing"),
                "evidence_mode": evidence_mode, "retrospective_lookup": True}
    blocked = distance <= 3
    return {"allowed": not blocked, "status": "known", "earnings_date": event_date.isoformat(),
            "session_timing": event.get("timing"), "trading_sessions_to_earnings": distance,
            "reason": "within_three_sessions_of_earnings" if blocked else None,
            "evidence_mode": evidence_mode, "retrospective_lookup": True}


def weak_cushion_trim(event: dict[str, Any] | None, day: dt.date, entry_price: float,
                      close: float, adr14_pct: float | None,
                      distance_cache: dict[tuple[dt.date, dt.date], int | None] | None = None) -> dict[str, Any] | None:
    if not event:
        return None
    event_date = event["date"] if isinstance(event["date"], dt.date) else dt.date.fromisoformat(str(event["date"])[:10])
    timing = str(event.get("timing") or "").lower()
    # A next-open order must be placed early enough to fill before a BMO/AMC release.
    lead = 1 if timing in {"after_market", "amc", "after-market"} else 2
    distance = cached_sessions_to_event(day, event_date, distance_cache) if distance_cache is not None else sessions_to_event(day, event_date)
    if distance is None:
        return {"weak": False, "status": "unassessable_earnings_session", "reason": "earnings_session_unavailable",
                "earnings_date": event_date.isoformat(), "session_timing": event.get("timing"),
                "evidence_mode": POLICY, "retrospective_lookup": True}
    if distance != lead or entry_price <= 0:
        return None
    if adr14_pct is None:
        return {"weak": False, "status": "unassessable_adr14", "reason": "current_adr14_missing",
                "gain_pct": (close / entry_price - 1) * 100, "cushion_threshold_pct": None,
                "earnings_date": event_date.isoformat(), "session_timing": event.get("timing"),
                "execution": "next_session_open", "decision_date": day.isoformat(),
                "evidence_mode": POLICY, "retrospective_lookup": True}
    gain = (close / entry_price - 1) * 100
    threshold = max(10.0, float(adr14_pct))
    return {"weak": gain < threshold, "gain_pct": gain, "cushion_threshold_pct": threshold,
            "earnings_date": event_date.isoformat(), "session_timing": event.get("timing"),
            "execution": "next_session_open", "decision_date": day.isoformat(),
            "evidence_mode": POLICY, "retrospective_lookup": True}


def holdings_decision(event: dict[str, Any] | None, day: dt.date, entry_price: float,
                      close: float, adr14: float | None,
                      distance_cache: dict[tuple[dt.date, dt.date], int | None] | None = None) -> dict[str, Any] | None:
    """Choose next-open earnings exposure action from completed close data."""
    base = {"evidence_mode": HOLDINGS_POLICY, "retrospective_lookup": True,
            "decision_date": day.isoformat(), "execution": "next_session_open"}
    if not event:
        return {**base, "status": "unknown_earnings", "earnings_known": False,
                "adr14_known": adr14 is not None, "action": "normal_exits"}
    event_date = event["date"] if isinstance(event["date"], dt.date) else dt.date.fromisoformat(str(event["date"])[:10])
    timing = str(event.get("timing") or "").lower()
    lead = 1 if timing in {"after_market", "amc", "after-market"} else 2
    distance = cached_sessions_to_event(day, event_date, distance_cache) if distance_cache is not None else sessions_to_event(day, event_date)
    if distance is None:
        return {**base, "status": "unassessable_earnings_session", "earnings_known": True,
                "adr14_known": adr14 is not None, "action": "normal_exits",
                "reason": "earnings_session_unavailable", "earnings_date": event_date.isoformat(),
                "session_timing": event.get("timing")}
    if distance != lead:
        return None
    try:
        entry_value, close_value = Decimal(str(entry_price)), Decimal(str(close))
    except (InvalidOperation, TypeError, ValueError):
        entry_value, close_value = Decimal(0), Decimal(0)
    if not entry_value.is_finite() or entry_value <= 0:
        return {**base, "status": "unassessable_entry_price", "earnings_known": True,
                "adr14_known": adr14 is not None, "action": "normal_exits",
                "reason": "entry_price_missing_or_invalid", "earnings_date": event_date.isoformat(),
                "session_timing": event.get("timing")}
    if not close_value.is_finite() or close_value <= 0:
        return {**base, "status": "unassessable_close", "earnings_known": True,
                "adr14_known": adr14 is not None, "action": "normal_exits",
                "reason": "completed_close_missing_or_invalid", "earnings_date": event_date.isoformat(),
                "session_timing": event.get("timing")}
    gain_value = (close_value / entry_value - Decimal(1)) * Decimal(100)
    gain = float(gain_value)
    data = {**base, "status": "assessed", "earnings_known": True, "adr14_known": adr14 is not None,
            "earnings_date": event_date.isoformat(), "session_timing": event.get("timing"),
            "trading_sessions_to_earnings": distance, "gain_pct": gain}
    if gain_value <= 0:
        return {**data, "action": "exit_all", "reason": "nonpositive_earnings_cushion",
                "cushion_threshold_pct": None}
    if adr14 is None:
        if gain_value < Decimal(15):
            return {**data, "status": "unassessable_adr14", "action": "exit_all",
                    "reason": "below_minimum_cushion_with_adr_unknown", "cushion_threshold_pct": 15.0}
        return {**data, "status": "unassessable_adr14", "action": "normal_exits",
                "reason": "current_adr14_missing", "cushion_threshold_pct": None}
    threshold = max(15.0, float(adr14))
    below_threshold = gain_value < Decimal(str(threshold))
    return {**data, "action": "exit_all" if below_threshold else "normal_exits",
            "reason": "below_earnings_cushion" if below_threshold else "earnings_cushion_sufficient",
            "cushion_threshold_pct": threshold}
def adr14_pct(frame: Any, day: dt.date) -> float | None:
    """Current completed-bar ADR14, matching Martin Bar Retest's high-low/close metric."""
    if frame is None or getattr(frame, "empty", True):
        return None
    rows = frame.loc[frame.index.date <= day].tail(14)
    if len(rows) < 14:
        return None
    try:
        values = [float((row["High"] - row["Low"]) / row["Close"] * 100) for _, row in rows.iterrows()]
    except (KeyError, TypeError, ValueError, ZeroDivisionError, OverflowError):
        return None
    return sum(values) / 14 if all(0 <= value < float("inf") for value in values) else None
