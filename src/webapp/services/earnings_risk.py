"""Explicit hindsight earnings guard used by isolated replay books."""
from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

POLICY = "earnings_risk_retrospective_v1"


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
    sessions = [stamp.date() for stamp in calendar.sessions_in_range(day.isoformat(), (event_date + dt.timedelta(days=7)).isoformat())]
    event_session = next((session for session in sessions if session >= event_date), None)
    return sessions.index(event_session) if event_session is not None else None


def entry_decision(event: dict[str, Any] | None, day: dt.date) -> dict[str, Any]:
    if not event:
        return {"allowed": True, "status": "unknown", "reason": "earnings_date_unknown",
                "evidence_mode": POLICY, "retrospective_lookup": True}
    event_date = event["date"] if isinstance(event["date"], dt.date) else dt.date.fromisoformat(str(event["date"])[:10])
    distance = sessions_to_event(day, event_date)
    blocked = distance is None or distance <= 3
    return {"allowed": not blocked, "status": "known", "earnings_date": event_date.isoformat(),
            "session_timing": event.get("timing"), "trading_sessions_to_earnings": distance,
            "reason": "within_three_sessions_of_earnings" if blocked else None,
            "evidence_mode": POLICY, "retrospective_lookup": True}


def weak_cushion_trim(event: dict[str, Any] | None, day: dt.date, entry_price: float,
                      close: float, adr14_pct: float | None) -> dict[str, Any] | None:
    if not event:
        return None
    event_date = event["date"] if isinstance(event["date"], dt.date) else dt.date.fromisoformat(str(event["date"])[:10])
    timing = str(event.get("timing") or "").lower()
    # A next-open order must be placed early enough to fill before a BMO/AMC release.
    lead = 1 if timing in {"after_market", "amc", "after-market"} else 2
    if sessions_to_event(day, event_date) != lead or entry_price <= 0:
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
