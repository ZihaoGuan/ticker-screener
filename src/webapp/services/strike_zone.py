from __future__ import annotations

import datetime as dt
from typing import Any


MAX_ATR_TO_SMA50_EXTENSION = 5.0
FRESH_SIGNAL_CALENDAR_DAYS = 5

_TRIGGER_SIGNALS: dict[str, tuple[str, int]] = {
    "darvas_box_breakout": ("Darvas breakout", 40),
    "high_volume_close": ("high-volume close", 38),
    "weekly_tight_close_breakout": ("weekly tight-close breakout", 35),
    "sean_breakout": ("breakout", 35),
    "wyckoff_buy_signal": ("Wyckoff buy signal", 35),
    "ftd_sweep": ("follow-through day", 30),
    "ema21_pullback_buy": ("EMA21 reclaim", 30),
    "sma200_pullback_buy": ("SMA200 reclaim", 30),
}

_SETUP_SIGNALS: dict[str, tuple[str, int]] = {
    "qullamaggie": ("Qullamaggie setup", 20),
    "vcp": ("VCP setup", 20),
    "weekly_vcp": ("weekly VCP", 20),
    "vcp_v3": ("VCP setup", 20),
    "weekly_vcp_v3": ("weekly VCP", 20),
    "vcp_scored": ("VCP contraction", 18),
    "weekly_vcp_scored": ("weekly VCP contraction", 18),
    "vcp_spec": ("VCP specification", 18),
    "weekly_vcp_spec": ("weekly VCP specification", 18),
    "minervini_vcp_detector": ("Minervini VCP", 20),
    "three_weeks_tight": ("three weeks tight", 20),
    "high_tight_flag": ("high tight flag", 20),
    "high_tight_flag_setup": ("high tight flag setup", 20),
    "rmv_tightness": ("RMV tightness", 18),
    "finviz_pattern_tlsupport": ("trendline support", 15),
    "finviz_pattern_wedgeresistance": ("wedge resistance", 15),
    "fearzone": ("Fearzone context", 8),
    "fearzone_zeiierman": ("Fearzone context", 8),
}

STRIKE_ZONE_SCANNER_IDS = frozenset({"ma_pullback_retest", *_TRIGGER_SIGNALS, *_SETUP_SIGNALS})


def strike_zone_scanner_label(scanner_id: str) -> str:
    if scanner_id == "ma_pullback_retest":
        return "MA Pullback & Retest"
    if scanner_id in _TRIGGER_SIGNALS:
        return _TRIGGER_SIGNALS[scanner_id][0]
    if scanner_id in _SETUP_SIGNALS:
        return _SETUP_SIGNALS[scanner_id][0]
    return scanner_id.replace("_", " ").title()


def _as_float(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _as_date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return dt.date.fromisoformat(value[:10])
    except ValueError:
        return None


def _signal_is_fresh(signal_date: dt.date | None, as_of_date: dt.date | None) -> bool:
    if as_of_date is None:
        return True
    if signal_date is None:
        return False
    age_days = (as_of_date - signal_date).days
    return 0 <= age_days <= FRESH_SIGNAL_CALENDAR_DAYS


def _signal_entry(*, id: str, label: str, points: int, kind: str, signal_date: dt.date | None, as_of_date: dt.date | None) -> dict[str, object]:
    age_days = (as_of_date - signal_date).days if signal_date and as_of_date else None
    return {
        "id": id,
        "label": label,
        "points": points,
        "kind": kind,
        "signal_date": signal_date.isoformat() if signal_date else None,
        "age_days": age_days,
        "fresh": _signal_is_fresh(signal_date, as_of_date),
    }


def build_strike_zone(row: dict[str, Any], *, as_of_date: dt.date | None) -> dict[str, object]:
    """Return explainable entry-readiness guidance, never an automatic buy verdict."""
    warnings: list[str] = []
    action_payload = row.get("position_action") if isinstance(row.get("position_action"), dict) else {}
    position_action = str(action_payload.get("action") or "")
    earnings_days = _as_int(row.get("earnings_days"))
    extension = _as_float(row.get("atr_to_sma50"))
    stage = row.get("stage_analysis") if isinstance(row.get("stage_analysis"), dict) else {}
    stage_alias = str(stage.get("alias") or "")

    hard_risks: list[str] = []
    if position_action == "avoid_new":
        hard_risks.append("Current position model advises against new exposure.")
    if earnings_days is not None and earnings_days <= 7:
        hard_risks.append(f"Earnings are in {earnings_days} days.")
    if extension is not None and extension >= MAX_ATR_TO_SMA50_EXTENSION:
        hard_risks.append(f"Price is {extension:.1f} ATR above SMA50.")
    if stage_alias.startswith(("3", "4")):
        hard_risks.append(f"Weinstein Stage {stage_alias} is not a preferred entry regime.")
    if hard_risks:
        return {
            "state": "avoid",
            "label": "Avoid",
            "score": 0,
            "reason": hard_risks[0],
            "primary_signal": None,
            "trigger_date": None,
            "signal_age_days": None,
            "supporting_signals": [],
            "warnings": hard_risks,
        }

    scanner_entries = [item for item in row.get("scanners", []) if isinstance(item, dict)]
    scanner_ids = {
        str(item.get("strategy_id") or item.get("id") or "").strip()
        for item in scanner_entries
    }
    scanner_dates = {
        str(item.get("strategy_id") or item.get("id") or "").strip(): _as_date(item.get("sort_date"))
        for item in scanner_entries
    }
    triggers: list[dict[str, object]] = []
    setups: list[dict[str, object]] = []
    confirmations: list[dict[str, object]] = []

    ma_state = str(row.get("signal_state") or "")
    ma_date = scanner_dates.get("ma_pullback_retest")
    if "ma_pullback_retest" in scanner_ids:
        profiles = row.get("active_profiles") if ma_state == "active" else row.get("ready_profiles")
        profile_label = ", ".join(str(item) for item in profiles if str(item).strip()) if isinstance(profiles, list) else "moving-average"
        if ma_state == "active":
            triggers.append(_signal_entry(id="ma_pullback_retest", label=f"{profile_label} reclaim", points=40, kind="trigger", signal_date=ma_date, as_of_date=as_of_date))
        else:
            setups.append(_signal_entry(id="ma_pullback_retest", label=f"{profile_label} pullback", points=22, kind="setup", signal_date=ma_date, as_of_date=as_of_date))

    for scanner_id in scanner_ids:
        signal_date = scanner_dates.get(scanner_id)
        if scanner_id in _TRIGGER_SIGNALS:
            label, points = _TRIGGER_SIGNALS[scanner_id]
            triggers.append(_signal_entry(id=scanner_id, label=label, points=points, kind="trigger", signal_date=signal_date, as_of_date=as_of_date))
        elif scanner_id in _SETUP_SIGNALS:
            label, points = _SETUP_SIGNALS[scanner_id]
            setups.append(_signal_entry(id=scanner_id, label=label, points=points, kind="setup", signal_date=signal_date, as_of_date=as_of_date))

    rmv = row.get("rmv") if isinstance(row.get("rmv"), dict) else {}
    if _as_int(rmv.get("rank")) in {1, 2} and "rmv_tightness" not in scanner_ids:
        setups.append(_signal_entry(id="rmv", label="RMV A+ compression", points=18, kind="setup", signal_date=_as_date(rmv.get("as_of_date")), as_of_date=as_of_date))

    if stage_alias.startswith("2"):
        confirmations.append({"label": f"Weinstein Stage {stage_alias}", "points": 10, "kind": "trend"})
    if "trend_template" in scanner_ids:
        confirmations.append({"label": "Minervini Trend Template", "points": 5, "kind": "trend"})
    if position_action == "add_position":
        confirmations.append({"label": "Position model permits adds", "points": 5, "kind": "trend"})
    rs_rating = _as_float(row.get("daily_rs_rating") or row.get("rs_rating"))
    if rs_rating is not None:
        if rs_rating >= 90:
            confirmations.append({"label": f"RS {rs_rating:.0f}", "points": 10, "kind": "strength"})
        elif rs_rating >= 80:
            confirmations.append({"label": f"RS {rs_rating:.0f}", "points": 6, "kind": "strength"})

    fresh_triggers = [item for item in triggers if bool(item["fresh"])]
    fresh_setups = [item for item in setups if bool(item["fresh"])]
    trigger_points = max((int(item["points"]) for item in fresh_triggers), default=0)
    setup_points = min(25, max((int(item["points"]) for item in fresh_setups), default=0) + (5 if len(fresh_setups) >= 2 else 0))
    confirmation_points = min(25, sum(int(item["points"]) for item in confirmations))
    score = min(100, trigger_points + setup_points + confirmation_points)
    primary_trigger = max(fresh_triggers, key=lambda item: int(item["points"]), default=None)
    primary_signal = primary_trigger or max(fresh_setups, key=lambda item: int(item["points"]), default=None)
    if primary_trigger is not None and score >= 65:
        state, label = "active", "Active"
        reason = f"Fresh {primary_trigger['label']} with {score} Strike Zone points."
    elif score >= 45 and (fresh_setups or fresh_triggers):
        state, label = "ready", "Ready"
        assert primary_signal is not None
        reason = f"{primary_signal['label']} is constructive; wait for a confirmed trigger."
    else:
        state, label = "context", "Context"
        reason = "Leadership or setup evidence is present, but there is no fresh qualified entry trigger."
    if extension is None:
        warnings.append("ATR-to-SMA50 extension is unavailable.")
    if earnings_days is None:
        warnings.append("Earnings date is unavailable.")

    all_signals = [*fresh_triggers, *fresh_setups, *confirmations]
    return {
        "state": state,
        "label": label,
        "score": score,
        "reason": reason,
        "primary_signal": str(primary_signal["label"]) if primary_signal else None,
        "trigger_date": primary_signal.get("signal_date") if primary_signal else None,
        "signal_age_days": primary_signal.get("age_days") if primary_signal else None,
        "supporting_signals": all_signals,
        "warnings": warnings,
    }
