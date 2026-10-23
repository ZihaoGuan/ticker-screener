"""Versioned, snapshot-time stop policies shared by readers and forward books."""
from __future__ import annotations

import copy
import math
from datetime import date
from typing import Any

from .ma_pullback_retest_screen import structural_retest_plan

CURRENT_STOP_POLICY = "current_v1"
STRUCTURAL_STOP_POLICY = "martin_daily_structural_v1"
POLICY_FIELDS = ("trade_plan", "trade_readiness", "pullback_quality", "adr14_pct", "stop_to_adr", "adr_suitability", "adr_reason")


def number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def current_plan_payload(row: dict[str, Any], as_of_date: date) -> dict[str, Any]:
    return {"version": CURRENT_STOP_POLICY, "as_of_date": as_of_date.isoformat(),
            "availability": "available" if isinstance(row.get("trade_plan"), dict) else "unavailable",
            "unavailable_reason": None if isinstance(row.get("trade_plan"), dict) else "no_supported_plan",
            **{key: copy.deepcopy(row.get(key)) for key in POLICY_FIELDS}, "evidence": {}}


def structural_plan_payload(row: dict[str, Any], as_of_date: date, frame: Any) -> dict[str, Any]:
    """Compute once when publishing the snapshot; never from a forward-test read."""
    legacy = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
    plan = copy.deepcopy(legacy)
    evidence = {"version": STRUCTURAL_STOP_POLICY, "legacy_stop": legacy.get("stop_price"),
                "source_strategy_id": legacy.get("source_strategy_id"), "signal_date": legacy.get("signal_date"), "buffer": 0.0}
    result = {"version": STRUCTURAL_STOP_POLICY, "as_of_date": as_of_date.isoformat(),
              "availability": "unavailable", "unavailable_reason": None,
              "trade_plan": None, "trade_readiness": "avoid" if row.get("trade_readiness") == "avoid" else "leader",
              "pullback_quality": {}, "adr14_pct": row.get("adr14_pct"), "stop_to_adr": None,
              "adr_suitability": "unavailable", "adr_reason": "Structural stop evidence unavailable; no ATR fallback.",
              "evidence": evidence}

    def unavailable(reason: str) -> dict[str, Any]:
        return {**result, "unavailable_reason": reason}

    source = plan.get("source_strategy_id")
    if source not in {"ma_pullback_retest", "ema21_pullback_buy", "sma200_pullback_buy"}:
        return unavailable("no_supported_plan")
    if str(plan.get("signal_date")) != as_of_date.isoformat():
        return unavailable("structural_confirmation_stale")
    quality = copy.deepcopy(row.get("pullback_quality") or {})
    if source == "ma_pullback_retest":
        structural = structural_retest_plan(frame, as_of_date=as_of_date)
        if structural is None:
            return unavailable("structural_evidence_missing")
        quality = structural["pullback_quality"]
        plan.update({key: value for key, value in structural.items() if key != "pullback_quality"})
        plan.update(trigger_label=structural["support_label"] + " reclaim", stop_label="Daily reclaim candle low")
        evidence.update({key: value for key, value in structural.items() if key != "pullback_quality"})
    else:
        completed = [index.date() for index in frame.index if index.date() <= as_of_date] if frame is not None and not frame.empty else []
        if not completed or max(completed) != as_of_date:
            return unavailable("structural_evidence_missing")
        evidence.update(stop_basis="saved_support_test_low", invalidation_low=plan.get("stop_price"), invalidation_date=None)
    entry, stop = number(plan.get("trigger_price")), number(plan.get("stop_price"))
    risk = (entry - stop) / entry * 100 if entry is not None and stop is not None and entry > stop > 0 else None
    adr = number(row.get("adr14_pct"))
    ratio = risk / adr if risk is not None and adr is not None and adr > 0 else None
    valid = risk is not None and risk <= 5
    plan.update(reference_risk_pct=risk, max_entry_price=stop / .95 if stop is not None and stop > 0 else None,
                risk_valid=valid, risk_status="invalid_levels" if risk is None else "within_limit" if valid else "over_limit")
    avoid = row.get("trade_readiness") == "avoid" or (row.get("strike_zone") or {}).get("state") == "avoid" or quality.get("state") == "failed"
    readiness = "avoid" if avoid else "defined_risk" if plan.get("state") == "active" and valid else "reclaim_trigger" if plan.get("state") == "active" else "pullback_zone"
    status = ("risk_failure" if risk is not None and risk > 5 else "unavailable" if adr is None or adr <= 0 else
              "low" if adr < 3 else "wide_stop" if ratio is not None and ratio > .5 else "caution" if adr < 5 else "suitable")
    reason = (f"Structural stop {risk:.2f}% exceeds the 5.00% maximum risk limit" if status == "risk_failure" else
              "ADR14 unavailable; volatility suitability cannot be evaluated" if status == "unavailable" else
              f"Structural stop is {ratio:.2f}× ADR14; half ADR is preferred" if ratio is not None else "Define valid structural levels before entry")
    if status == "low":
        reason = f"Low ADR caution: ADR14 {adr:.2f}% is below the preferred 3% range; " + reason
    evidence.update(stop=stop, reference_entry=entry, reference_risk_pct=risk, stop_to_adr=ratio,
                    adr_fit="unavailable" if ratio is None else "within_half_adr" if ratio <= .5 else "wide_stop_caution",
                    provisional=plan.get("state") != "active")
    return {**result, "availability": "available", "trade_plan": plan, "trade_readiness": readiness,
            "pullback_quality": quality, "stop_to_adr": ratio, "adr_suitability": status, "adr_reason": reason}
