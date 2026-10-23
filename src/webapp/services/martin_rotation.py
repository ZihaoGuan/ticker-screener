"""Deterministic, paper-only RS Expanded rotation policy; no broker execution."""
from __future__ import annotations

import datetime as dt
import hashlib
import math
from typing import Any

import pandas as pd

ROTATION_STRATEGY_ID = "martin_rs_expanded_rotation_v1"
ROTATION_POLICY_VERSION = "rotation_v1"
ROTATION_LIMITS = {"max_positions": 8, "sizing_base": 100_000.0, "risk_per_trade": 500.0,
                   "max_position_allocation": 20_000.0, "committed_risk_limit": 2_500.0}


def session_dates(day: dt.date, count: int = 35) -> list[dt.date]:
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=500), end=day + dt.timedelta(days=15))
    return [label.date() for label in calendar.sessions_in_range((day - dt.timedelta(days=500)).isoformat(), day.isoformat())][-count:]


def require_completed_session(day: dt.date) -> None:
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=500), end=day + dt.timedelta(days=15))
    if not calendar.is_session(day.isoformat()) or pd.Timestamp.now(tz="UTC") < calendar.session_close(day.isoformat()):
        raise ValueError("Rotation strategy requires a completed exchange session")


def valid_history(frame: Any, dates: list[dt.date]) -> bool:
    if frame is None or frame.empty or not frame.index.is_unique or not frame.index.is_monotonic_increasing:
        return False
    rows = {index.date(): row for index, row in frame.iterrows()}
    for day in dates:
        row = rows.get(day)
        if row is None:
            return False
        try:
            opening, high, low, close, volume = (float(row[key]) for key in ("Open", "High", "Low", "Close", "Volume"))
        except (KeyError, TypeError, ValueError):
            return False
        if not all(math.isfinite(value) and value > 0 for value in (opening, high, low, close, volume)) or not low <= min(opening, close) <= max(opening, close) <= high:
            return False
    return True


def metrics(frame: Any, day: dt.date, dates: list[dt.date]) -> dict[str, float] | None:
    if len(dates) < 26 or not valid_history(frame, dates):
        return None
    past = frame.loc[frame.index.date <= day]
    closes = past["Close"].astype(float)
    ema9, ema21 = closes.ewm(span=9, adjust=False).mean(), closes.ewm(span=21, adjust=False).mean()
    result = {"close": float(closes.iloc[-1]), "ema9": float(ema9.iloc[-1]), "ema21": float(ema21.iloc[-1]),
            "ema21_prior5": float(ema21.iloc[-6]), "return5_pct": float((closes.iloc[-1] / closes.iloc[-6] - 1) * 100),
            "adv20_dollars": float((past["Close"] * past["Volume"]).tail(20).mean())}
    return result if all(math.isfinite(value) for value in result.values()) else None


def account_budget(positions: list[dict], orders: list[dict], realized_pnl: float) -> dict[str, float]:
    open_cost = risk = pending_cost = giveback = 0.0
    for row in positions:
        p = row.get("payload_json") or {}
        shares, entry, stop = int(p.get("remaining_shares") or 0), float(p.get("entry_price") or 0), float(p.get("stop") or 0)
        open_cost += shares * entry
        risk += shares * max(entry - stop, 0)
        giveback += shares * max(float(p.get("last_close") or entry) - stop, 0)
    for row in orders:
        p = row.get("payload_json") or {}
        if p.get("rotation_intent"):
            continue  # Its outgoing position still owns the slot and cash until the paired exit fills.
        shares, limit, stop = int(p.get("shares") or 0), float(p.get("limit_price") or 0), float(p.get("stop") or 0)
        pending_cost += shares * limit
        risk += shares * max(limit - stop, 0)
    return {"available_cash": 100_000.0 + realized_pnl - open_cost - pending_cost,
            "committed_stop_risk": risk, "mark_to_stop_exposure": giveback,
            "available_risk": max(0.0, 2_500.0 - risk)}


def size_order(candidate: dict, budget: dict[str, float], *, price: float | None = None) -> int:
    limit, stop = float(price if price is not None else candidate["limit_price"]), float(candidate["stop"])
    if not 0 < stop < limit:
        return 0
    shares = min(int(500 / (limit - stop)), int(20_000 / limit), int(max(0.0, budget["available_cash"]) / limit),
                 int(max(0.0, budget["available_risk"]) / (limit - stop)))
    if candidate.get("rs_tier") == "emerging":
        shares = min(shares, int(.001 * float((candidate.get("rs_exception_metrics") or {}).get("adv20_dollars") or 0) / limit))
    return max(0, shares)


def rotation_decision(*, day: dt.date, positions: list[dict], candidates: list[dict], rows: list[dict],
                      frames: dict, last_rotation: dt.date | None, dates: list[dt.date]) -> dict:
    elapsed = sum(value > last_rotation for value in dates) if last_rotation else 5
    remaining = max(0, 5 - elapsed)
    result = {"rotation_policy_version": ROTATION_POLICY_VERSION, "decision_date": day.isoformat(), "status": "not_scheduled",
              "cooldown_sessions_remaining": remaining, "last_rotation_exit": last_rotation.isoformat() if last_rotation else None}
    if last_rotation and elapsed < 5:
        return {**result, "reason": "rotation_cooldown"}
    benchmarks = {ticker: metrics(frames.get(ticker), day, dates) for ticker in ("SPY", "QQQ")}
    if any(value is None for value in benchmarks.values()):
        return {**result, "reason": "rotation_benchmark_data_missing"}
    if any(not value["close"] > value["ema21"] > value["ema21_prior5"] for value in benchmarks.values()):
        return {**result, "reason": "rotation_market_regime"}
    spy_return = benchmarks["SPY"]["return5_pct"]
    outgoing = []
    for position in positions:
        p, ticker = position.get("payload_json") or {}, str(position["ticker"]).upper()
        m = metrics(frames.get(ticker), day, dates)
        if m is None or p.get("trimmed") or p.get("exit_next_open"):
            continue
        opened = dt.date.fromisoformat(str(position.get("opened_on") or p.get("opened_on")))
        held = max(sum(value > opened for value in dates), int(p.get("holding_sessions") or 0) - 1)
        risk, entry = float(p.get("initial_r_per_share") or 0), float(p.get("entry_price") or 0)
        # MFE is persisted daily, including entry-day highs (a conservative daily-bar approximation).
        mfe = p.get("max_favorable_r")
        if risk <= 0 or mfe is None:
            continue
        current_r = (m["close"] - entry) / risk
        relative = m["return5_pct"] - spy_return
        if held >= 5 and current_r < .5 and float(mfe) < 1 and relative <= -2:
            outgoing.append({"ticker": ticker, "position_id": position["id"], "return5_pct": m["return5_pct"],
                             "relative5_pct": relative, "current_r": current_r, "max_favorable_r": mfe, "held_sessions": held})
    outgoing.sort(key=lambda item: (item["relative5_pct"], item["current_r"], -item["held_sessions"], item["ticker"]))
    if not outgoing:
        return {**result, "reason": "rotation_no_stalled_holding"}
    old = outgoing[0]
    row_map = {str(row["ticker"]).upper(): row for row in rows}
    incoming = []
    for candidate in candidates:
        ticker = str(candidate["ticker"]).upper()
        m = replacement_metrics(candidate, row_map.get(ticker, {}), frames.get(ticker), day, dates)
        if candidate.get("status") != "waiting_capacity" or m is None:
            continue
        if m["return5_pct"] - old["return5_pct"] < 2:
            continue
        incoming.append({"ticker": ticker, "rs_rating": candidate["rs_rating"], **m, "candidate": candidate})
    incoming.sort(key=lambda item: (-item["rs_rating"], -(item["return5_pct"] - spy_return), -item["adv20_dollars"], item["ticker"]))
    if not incoming:
        return {**result, "reason": "rotation_no_qualified_replacement"}
    new = incoming[0]
    identity = f"{ROTATION_STRATEGY_ID}:{day}:{old['position_id']}:{new['ticker']}"
    return {**result, "status": "scheduled", "decision_id": hashlib.sha256(identity.encode()).hexdigest()[:24],
            "outgoing": old, "incoming": {key: value for key, value in new.items() if key != "candidate"},
            "reason": "stalled_relative_laggard_replaced_by_confirmed_leader"}


def replacement_metrics(candidate: dict, row: dict, frame: Any, day: dt.date, dates: list[dt.date]) -> dict | None:
    m = metrics(frame, day, dates)
    try:
        earnings = dt.date.fromisoformat(str(row.get("earnings_date")))
        limit, stop, reference = float(candidate["limit_price"]), float(candidate["stop"]), float(candidate.get("entry") or 0)
    except (ValueError, TypeError, KeyError):
        return None
    if (candidate.get("rs_tier") != "preferred" or m is None or str(candidate.get("signal_date")) != day.isoformat() or
            (earnings - day).days <= 7 or not m["close"] > m["ema9"] > m["ema21"] > m["ema21_prior5"] or
            m["adv20_dollars"] < 20_000_000 or m["close"] > limit or not 0 < stop < reference or
            (reference - stop) / reference > .05):
        return None
    return m
