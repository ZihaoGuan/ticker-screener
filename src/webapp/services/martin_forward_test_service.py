from __future__ import annotations

import math
import logging
from contextvars import ContextVar
from contextlib import nullcontext
from datetime import date
from typing import Any

from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.market_data_access import load_many_ticker_windows
from src.ticker_filters import is_leveraged_or_inverse_ticker
from src.trade_plan_policies import POLICY_FIELDS, STRUCTURAL_STOP_POLICY
from src.webapp.services.martin_rotation import (ROTATION_STRATEGY_ID, ROTATION_POLICY_VERSION, ROTATION_LIMITS,
                                                session_dates, valid_history, size_order, rotation_decision)
from src.webapp.services.martin_rotation import replacement_metrics, require_completed_session, metrics


STRATEGY_ID = "martin_daily_v1"
SECTOR_STRATEGY_ID = "martin_sector_book_v1"
DAILY_TOP5_STRATEGY_ID = "martin_daily_top5_v1"
RS_EXPANDED_STRATEGY_ID = "martin_rs_expanded_v1"
RS_ACCOUNT_STRATEGIES = {RS_EXPANDED_STRATEGY_ID, ROTATION_STRATEGY_ID}
ENTRY_POLICY_VERSION = "top_hits_max_entry_5pct_v1"
SECTOR_ORDER = {"leading": 0, "improving": 1, "weakening": 2, "lagging": 3, "unknown": 4}
FOCUS_SCANNERS = {"weekly_candidate_pool", "best_winners", "qullamaggie", "one_year_winners", "trend_template", "daily_rs_new_high", "rs"}
_BOOK_ID: ContextVar[str | None] = ContextVar("martin_forward_book_id", default=None)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _scanner_ids(row: dict[str, Any]) -> set[str]:
    scanner_ids = {str(value).strip().lower() for value in row.get("scanner_ids", []) if value}
    scanner_ids.update(
        str(item.get(key)).strip().lower()
        for item in row.get("scanners", [])
        if isinstance(item, dict)
        for key in ("id", "strategy_id")
        if item.get(key)
    )
    return scanner_ids


def classify_candidate(row: dict[str, Any], rank: int) -> dict[str, Any]:
    """Apply the persisted Martin Focus row gates without consulting live scanner state."""
    ticker = str(row.get("ticker") or "").strip().upper()
    labels = _scanner_ids(row)
    market_cap = _number(row.get("market_cap"))
    close = _number(row.get("day_close", row.get("close", row.get("close_price"))))
    rs = _number(row.get("daily_rs_rating"))
    reasons: list[str] = []
    if not ticker:
        reasons.append("missing_ticker")
    elif is_leveraged_or_inverse_ticker(ticker):
        reasons.append("leveraged_or_inverse_fund")
    if market_cap is None or market_cap < 1_000_000_000:
        reasons.append("market_cap_below_1b_or_missing")
    if close is None or close < 5:
        reasons.append("close_below_5_or_missing")
    if rs is None or not 90 <= rs <= 99:
        reasons.append("rs_outside_90_99_or_missing")
    if not labels.intersection(FOCUS_SCANNERS):
        reasons.append("no_martin_focus_scanner")
    plan = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
    strike_zone = row.get("strike_zone") if isinstance(row.get("strike_zone"), dict) else {}
    pullback_quality = row.get("pullback_quality") if isinstance(row.get("pullback_quality"), dict) else {}
    readiness = str(row.get("trade_readiness") or "")
    orderable = readiness == "defined_risk" and not reasons
    status = "blocked" if reasons else ("pending" if orderable else ("avoid" if readiness == "avoid" else "watch"))
    adr = _number(row.get("adr14_pct"))
    stop = _number(plan.get("stop_price"))
    max_entry = _number(plan.get("max_entry_price"))
    entry = _number(plan.get("trigger_price"))
    if not orderable and not reasons:
        if readiness == "avoid":
            reasons.append("marked_avoid")
        elif not plan:
            reasons.append("no_supported_plan")
        elif entry is None or stop is None or stop <= 0 or entry <= stop:
            reasons.append("plan_levels_missing")
        else:
            if plan.get("state") != "active":
                reasons.append("awaiting_confirmation")
            risk = (entry - stop) / entry * 100
            if risk > 5:
                reasons.append("risk_exceeds_5pct")
            if not reasons:
                reasons.append("awaiting_confirmation")
    scanner_count = _number(row.get("scanner_count"))
    result = {
        "ticker": ticker, "rank": rank, "source_snapshot_rank": row.get("rank", rank),
        "entry_policy_version": ENTRY_POLICY_VERSION, "status": status, "reasons": reasons,
        "scanner_labels": sorted(labels), "trade_readiness": readiness,
        "close": close, "market_cap": market_cap, "rs_rating": rs,
        "entry": entry, "stop": stop, "max_entry": max_entry, "adr14_pct": adr,
        "plan_state": plan.get("state"), "risk_status": plan.get("risk_status"),
        "readiness_details": _snapshot_readiness_explanation(readiness, strike_zone, pullback_quality),
        "confirmation_rules": _confirmation_rules(plan.get("source_strategy_id")),
        "reference_risk_pct": _number(plan.get("reference_risk_pct")),
        "source_strategy_id": plan.get("source_strategy_id"), "signal_date": plan.get("signal_date"),
        "sector": row.get("sector") or row.get("sector_name"),
        "sector_momentum": row.get("sector_momentum") if isinstance(row.get("sector_momentum"), dict) else {},
        "adr_suitability": row.get("adr_suitability"), "stop_to_adr": row.get("stop_to_adr"),
        "strike_score": row.get("strike_score", (row.get("strike_zone") or {}).get("score") if isinstance(row.get("strike_zone"), dict) else None),
        "scanner_count": int(scanner_count) if scanner_count is not None else len(labels),
        "selection_metadata": row.get("selection_metadata", {}),
    }
    if orderable:
        if adr is None or adr <= 0 or stop is None or stop <= 0:
            result.update(status="blocked_data", reasons=["missing_adr_or_stop"])
        else:
            cap = 0.05
            adjusted = stop / (1 - cap)
            limit = min(max_entry, adjusted) if max_entry is not None and max_entry > 0 else adjusted
            if limit <= stop:
                result.update(status="blocked_data", reasons=["invalid_entry_stop_levels"])
            else:
                result.update(limit_price=limit, risk_cap_pct=cap)
    result["readiness_explanation"] = (result["readiness_details"] or [None])[0]
    return result


def _structural_rs_row(row: dict[str, Any], as_of_date: date) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    """Consume the published policy, never recalculate stops from newer bars."""
    policies = row.get("trade_plans")
    saved = policies.get(STRUCTURAL_STOP_POLICY) if isinstance(policies, dict) else None
    if not isinstance(saved, dict) or saved.get("version") != STRUCTURAL_STOP_POLICY:
        return row, {"version": STRUCTURAL_STOP_POLICY}, "structural_snapshot_plan_missing"
    evidence = saved.get("evidence") if isinstance(saved.get("evidence"), dict) else {"version": STRUCTURAL_STOP_POLICY}
    if saved.get("as_of_date") != as_of_date.isoformat():
        return row, evidence, "structural_confirmation_stale"
    if saved.get("availability") != "available":
        return row, evidence, saved.get("unavailable_reason") or "structural_evidence_missing"
    plan = saved.get("trade_plan")
    if not isinstance(plan, dict):
        return row, evidence, "structural_evidence_missing"
    if plan.get("signal_date") != as_of_date.isoformat():
        return row, evidence, "structural_confirmation_stale"
    if plan.get("source_strategy_id") not in {"ma_pullback_retest", "ema21_pullback_buy", "sma200_pullback_buy"}:
        return row, evidence, "no_supported_plan"
    selected = {**row, **{key: saved.get(key) for key in POLICY_FIELDS}}
    entry, stop, maximum = (_number(plan.get(key)) for key in ("trigger_price", "stop_price", "max_entry_price"))
    # Validate the trust boundary without replacing the snapshot's evidence or plan.
    if any(isinstance(plan.get(key), bool) for key in ("trigger_price", "stop_price", "max_entry_price")) or entry is None or stop is None or not entry > stop > 0 or maximum is None or maximum <= stop:
        return selected, evidence, "plan_levels_missing"
    if selected.get("trade_readiness") == "defined_risk" and (plan.get("state") != "active" or (entry - stop) / entry * 100 > 5):
        return selected, evidence, "structural_snapshot_plan_invalid"
    source = plan["source_strategy_id"]
    expected_basis = "daily_reclaim_low" if source == "ma_pullback_retest" else "saved_support_test_low"
    if (evidence.get("version") != STRUCTURAL_STOP_POLICY or evidence.get("source_strategy_id") != source
            or evidence.get("signal_date") != as_of_date.isoformat() or evidence.get("stop_basis") != expected_basis
            or _number(evidence.get("stop")) != stop or _number(evidence.get("invalidation_low")) != stop
            or _number(evidence.get("reference_entry")) != entry or _number(evidence.get("buffer")) != 0
            or (source == "ma_pullback_retest" and (evidence.get("invalidation_date") != as_of_date.isoformat()
                or not evidence.get("profile_id") or evidence.get("profile_id") != plan.get("profile_id")))):
        return selected, evidence, "structural_evidence_missing"
    if (row.get("strike_zone") or {}).get("state") == "avoid" or (selected.get("pullback_quality") or {}).get("state") == "failed":
        selected = {**selected, "trade_readiness": "avoid"}
    return selected, evidence, None


def classify_rs_expanded_candidate(row: dict[str, Any], rank: int, as_of_date: date,
                                   frames: dict[str, Any]) -> dict[str, Any]:
    """Isolate the emerging-leader experiment from the unchanged baseline gates."""
    row, stop_policy, stop_error = _structural_rs_row(row, as_of_date)
    rs = _number(row.get("daily_rs_rating"))
    tier = ("missing" if row.get("daily_rs_rating") is None else "invalid" if isinstance(row.get("daily_rs_rating"), bool) or rs is None or not 0 <= rs <= 99
            else "preferred" if rs >= 90 else "emerging" if rs >= 80 else "research")
    item = classify_candidate({**row, "daily_rs_rating": 90} if tier == "emerging" else row, rank)
    item.update(rs_rating=rs, rs_tier=tier, rs_exception_checks={}, rs_exception_metrics={},
                stop_policy_version=STRUCTURAL_STOP_POLICY, stop_policy=stop_policy)
    validation = row.get("replay_data_validation")
    if isinstance(validation, dict) and validation.get("origin") == "historical_replay" and validation.get("blocked"):
        reasons = [issue["reason"] for issue in validation.get("issues", []) if isinstance(issue, dict) and issue.get("reason")]
        item.update(status="blocked_data", selected_for_book=False,
                    reasons=list(dict.fromkeys([*reasons, *item["reasons"]])) or ["replay_candidate_data_invalid"],
                    replay_data_validation=validation,
                    readiness_explanation="Historical replay inputs are incomplete or invalid for this ticker/session; no new order is submitted.")
        item["readiness_details"] = [item["readiness_explanation"], *reasons]
        item.pop("limit_price", None)
        return item
    if stop_error:
        item.update(status="avoid" if row.get("trade_readiness") == "avoid" else "blocked_data",
                    reasons=list(dict.fromkeys([*item["reasons"], stop_error])), stop=None, max_entry=None,
                    reference_risk_pct=None, risk_status="structural_unavailable", stop_to_adr=None)
        item.pop("limit_price", None)
        item["readiness_details"] = [*item["readiness_details"], RS_EXCEPTION_EXPLANATIONS.get(stop_error, "No supported structural plan is available.")]
    item["readiness_explanation"] = (item["readiness_details"] or [None])[0]
    if tier in {"missing", "invalid", "research"}:
        code = {"missing": "rs_rating_missing", "invalid": "rs_rating_invalid", "research": "rs_below_80_research"}[tier]
        reasons = [reason for reason in item["reasons"] if reason != "rs_outside_90_99_or_missing"]
        item.update(status="watch" if tier == "research" and not reasons else "blocked_data" if tier != "research" else "blocked",
                    reasons=[*reasons, code])
        item["readiness_details"] = [*item["readiness_details"], RS_EXCEPTION_EXPLANATIONS[code]]
        item["readiness_explanation"] = item["readiness_details"][0]
        return item
    if tier == "preferred":
        return item
    checks: dict[str, bool | None] = {}
    metrics: dict[str, Any] = {"as_of_date": as_of_date.isoformat(), "minimum_adv20_dollars": 20_000_000,
                               "maximum_order_adv_fraction": .001}
    item.update(rs_exception_checks=checks, rs_exception_metrics=metrics)
    reasons = list(item["reasons"])

    def gate(name: str, passed: bool | None, code: str) -> None:
        checks[name] = passed
        if passed is not True and code not in reasons:
            reasons.append(code)

    stock = _completed_observations(frames.get(item["ticker"]), as_of_date)
    spy = _completed_observations(frames.get("SPY"), as_of_date)
    fresh = bool(stock and spy and max(stock) == as_of_date and max(spy) == as_of_date)
    gate("fresh_completed_session", fresh, "rs_history_missing_or_stale")
    dates = sorted(spy)[-205:]
    aligned = fresh and len(dates) == 205 and sorted(stock)[-205:] == dates
    closes = [stock[day].get("close") for day in dates] if aligned else []
    valid_closes = len(closes) == 205 and all(value is not None and value > 0 for value in closes)
    gate("history_complete", valid_closes, "rs_exception_data_missing")
    trend = strength = liquid = None
    if valid_closes:
        sma50, sma200 = sum(closes[-50:]) / 50, sum(closes[-200:]) / 200
        old50, old200 = sum(closes[-55:-5]) / 50, sum(closes[-205:-5]) / 200
        ema21 = closes[0]
        for close in closes[1:]:
            ema21 += (close - ema21) * 2 / 22
        trend = closes[-1] > sma50 and closes[-1] > sma200 and sma50 > old50 and sma200 > old200 and ema21 > sma50
        metrics.update(close=closes[-1], sma50=sma50, sma200=sma200, sma50_5_sessions_ago=old50,
                       sma200_5_sessions_ago=old200, ema21=ema21)
        spy_now, spy_then = spy[dates[-1]].get("close"), spy[dates[-21]].get("close")
        if spy_now is not None and spy_then is not None and spy_now > 0 and spy_then > 0:
            now, then = closes[-1] / spy_now, closes[-21] / spy_then
            strength = now > then
            metrics.update(stock_spy_ratio=now, stock_spy_ratio_20_sessions_ago=then)
        dollars = [stock[day].get("volume") for day in dates[-20:]]
        if all(value is not None and value > 0 for value in dollars):
            adv = sum(close * volume for close, volume in zip(closes[-20:], dollars)) / 20
            metrics["adv20_dollars"] = adv
            liquid = adv >= 20_000_000
    gate("trend", trend, "rs_trend_not_qualified" if trend is not None else "rs_exception_data_missing")
    gate("relative_strength_improving", strength, "rs_relative_strength_not_improving" if strength is not None else "rs_exception_data_missing")
    gate("liquidity", liquid, "rs_liquidity_below_minimum" if liquid is not None else "rs_exception_data_missing")
    plan = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
    quality = row.get("pullback_quality") if isinstance(row.get("pullback_quality"), dict) else {}
    gate("supported_plan", plan.get("source_strategy_id") in {"ma_pullback_retest", "ema21_pullback_buy", "sma200_pullback_buy"}, "rs_supported_plan_required")
    confirmed = plan.get("state") == "active" and str(plan.get("signal_date")) == as_of_date.isoformat()
    gate("confirmed", confirmed, "rs_confirmation_required")
    if confirmed and "awaiting_confirmation" in reasons:
        reasons.remove("awaiting_confirmation")
    gate("defined_risk_readiness", row.get("trade_readiness") == "defined_risk", "rs_defined_risk_required")
    known_quality = quality.get("state") in {"constructive", "neutral", "warning", "failed"}
    gate("pullback_quality", quality.get("state") in {"constructive", "neutral"} if known_quality else None,
         "rs_pullback_quality_required" if known_quality else "rs_pullback_quality_missing")
    entry, stop = item["entry"], item["stop"]
    levels = entry is not None and stop is not None and entry > stop > 0
    gate("valid_plan_levels", levels, "plan_levels_missing")
    risk = (entry - stop) / entry * 100 if levels else None
    metrics["reference_risk_pct"] = risk
    gate("risk_within_5pct", risk <= 5 if risk is not None else None, "risk_exceeds_5pct" if risk is not None else "plan_levels_missing")
    if reasons:
        data_issue = any(reason in {"rs_history_missing_or_stale", "rs_exception_data_missing", "rs_pullback_quality_missing", "plan_levels_missing", "missing_adr_or_stop", "structural_evidence_missing", "structural_confirmation_stale", "structural_snapshot_plan_missing", "structural_snapshot_plan_invalid"} for reason in reasons)
        item.update(status="avoid" if row.get("trade_readiness") == "avoid" or quality.get("state") == "failed" else "blocked_data" if data_issue else "blocked" if item["status"] == "blocked" else "watch", reasons=reasons)
    item["readiness_details"] = [*item["readiness_details"], *[RS_EXCEPTION_EXPLANATIONS[reason] for reason in reasons if reason in RS_EXCEPTION_EXPLANATIONS]]
    item["readiness_explanation"] = (item["readiness_details"] or [None])[0]
    return item


RS_EXCEPTION_EXPLANATIONS = {
    "structural_snapshot_plan_missing": "This snapshot has no saved Structural v1 plan; refresh the snapshot before selection.",
    "structural_snapshot_plan_invalid": "The saved Structural v1 plan has inconsistent confirmation or risk levels; no order is submitted.",
    "structural_confirmation_stale": "Structural v1 requires a source signal dated this exact completed session.",
    "structural_evidence_missing": "Structural stop evidence is missing or stale; no ATR fallback is used.",
    "rs_rating_missing": "RS rating is missing; this is a data issue, not weak relative strength.",
    "rs_rating_invalid": "RS rating is invalid or outside the supported 0–99 range.",
    "rs_below_80_research": "RS below 80 remains research-only and cannot submit an order.",
    "rs_history_missing_or_stale": "The stock and SPY need completed bars for this exact selection session.",
    "rs_exception_data_missing": "Emerging-leader history or required price/volume observations are incomplete.",
    "rs_trend_not_qualified": "Emerging leaders must be above rising SMA50/SMA200, with EMA21 above SMA50.",
    "rs_relative_strength_not_improving": "The stock/SPY relative-strength ratio must improve over 20 market sessions.",
    "rs_liquidity_below_minimum": "Twenty-session average dollar volume must be at least $20 million.",
    "rs_supported_plan_required": "Emerging leaders require a supported MA pullback/retest plan.",
    "rs_confirmation_required": "A fresh active daily confirmation for the exact selection session is required.",
    "rs_defined_risk_required": "Defined-risk readiness is required; confirmation alone does not make the plan eligible.",
    "rs_pullback_quality_missing": "Pullback quality is unavailable; emerging-leader eligibility cannot be established.",
    "rs_pullback_quality_required": "Pullback quality must be constructive or neutral; warning or missing quality stays Watch.",
    "rs_order_notional_exceeds_liquidity": "The actual planned order exceeds 0.1% of twenty-session average dollar volume.",
}


def _completed_observations(frame: Any, as_of_date: date) -> dict[date, dict[str, float | None]]:
    if frame is None or getattr(frame, "empty", True):
        return {}
    observations = {}
    for index, row in frame.iterrows():
        day = index.date() if hasattr(index, "date") else index
        if day > as_of_date:
            continue
        keys = {str(key).lower(): key for key in row.index}
        observations[day] = {name: _number(row[keys[name]]) if name in keys else None for name in ("close", "volume")}
    return observations


def _snapshot_readiness_explanation(readiness: str, strike_zone: dict[str, Any], quality: dict[str, Any]) -> list[str]:
    if readiness == "avoid":
        reasons = strike_zone.get("warnings") if strike_zone.get("state") == "avoid" and isinstance(strike_zone.get("warnings"), list) else []
        details = [item.strip() for item in reasons if isinstance(item, str) and item.strip()]
        reason = str(strike_zone.get("reason") or "").strip() if strike_zone.get("state") == "avoid" else ""
        if reason and reason not in details:
            details.insert(0, reason)
        if quality.get("state") == "failed":
            if not details:
                details.append("Pullback quality failed.")
            if isinstance(quality.get("reasons"), list):
                details.extend(item.strip() for item in quality["reasons"] if isinstance(item, str) and item.strip())
        return list(dict.fromkeys(details))
    return []


def _confirmation_rules(source_strategy_id: Any) -> str | None:
    rules = {
        "ma_pullback_retest": "The MA Retest scanner requires a rising support average, a support touch and hold within its ATR tolerance during the last three daily sessions, and a latest bar with low near support, close above support, close above open, and close at or above the prior close. The saved row does not report which individual checks passed.",
        "ema21_pullback_buy": "The EMA21 scanner requires its strict uptrend and a bullish breakout bar whose intraday high exceeds the saved test high; the test is separate from the breakout and a close above the test high is not required. The saved row does not report which individual checks passed.",
        "sma200_pullback_buy": "The SMA200 scanner requires its strict long trend and a bullish breakout bar whose intraday high exceeds the saved test high; the test is separate from the breakout and a close above the test high is not required. The saved row does not report which individual checks passed.",
    }
    return rules.get(str(source_strategy_id or ""))


def select_sector_candidates(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    """Choose one primary per sector, then eligible Leading/Improving alternates."""
    candidates = []
    for rank, row in enumerate(rows, start=1):
        item = classify_candidate(row, rank)
        sector = str(item.get("sector") or "").strip()
        momentum = item.get("sector_momentum") or {}
        quadrant = str(momentum.get("quadrant") or "Unknown").strip().title()
        item.update(sector=sector or None, sector_quadrant=quadrant,
                    sector_rs_ratio=momentum.get("rs_ratio"), sector_momentum_value=momentum.get("momentum"),
                    sector_as_of_date=momentum.get("as_of_date"), selection_tier=None)
        if not sector:
            item.update(status="not_selected", reasons=["missing_sector"])
        candidates.append(item)
    sector_key = lambda item: (SECTOR_ORDER.get(str(item["sector_quadrant"]).lower(), 4), item["rank"])
    eligible = sorted((item for item in candidates if item["status"] == "pending"), key=sector_key)
    selected: list[dict[str, Any]] = []
    used: set[int] = set()
    sectors: set[str] = set()
    for item in eligible:
        if item["sector"] not in sectors and len(selected) < limit:
            item["selection_tier"] = "Primary"
            selected.append(item)
            used.add(id(item))
            sectors.add(item["sector"])
    for item in eligible:
        if len(selected) >= limit:
            break
        if id(item) in used:
            continue
        if item["sector_quadrant"] in {"Leading", "Improving"} and sum(x["sector"] == item["sector"] for x in selected) < 2:
            item["selection_tier"] = "Alternate"
            selected.append(item)
            used.add(id(item))
    for item in candidates:
        if item["status"] == "pending" and id(item) not in used:
            item.update(status="not_selected", reasons=["sector_quota_or_session_limit"])
    for item in candidates:
        metadata = dict(item.get("selection_metadata") or {}) if isinstance(item.get("selection_metadata"), dict) else {}
        metadata.update(snapshot_rank=item["rank"], sector_quadrant=item["sector_quadrant"],
                        selection_tier=item.get("selection_tier"),
                        selection_reason=("primary_for_sector" if item.get("selection_tier") == "Primary" else
                                          "alternate_for_leading_or_improving_sector" if item.get("selection_tier") == "Alternate" else
                                          (item.get("reasons") or ["not_selected"])[0]))
        item["selection_metadata"] = metadata
    chosen_ids = {id(item): index for index, item in enumerate(selected)}
    return sorted(candidates, key=lambda item: (chosen_ids.get(id(item), limit + item["rank"]), item["rank"]))


def order_outcome(order: dict[str, Any], bar: dict[str, Any]) -> tuple[str, float | None, str | None]:
    """Resolve one-session buy limit; same-day stop is handled by the position engine."""
    opening, low = _number(bar.get("open")), _number(bar.get("low"))
    stop, limit = _number(order.get("stop")), _number(order.get("limit_price"))
    if None in (opening, low, stop, limit):
        return "pending", None, "missing_bar_data"
    if opening <= stop:
        return "cancelled", None, "opened_at_or_below_stop"
    if opening <= limit:
        return "filled", opening, None
    if low <= limit:
        return "filled", limit, None
    return "expired", None, "limit_not_reached"


def position_exit(position: dict[str, Any], bar: dict[str, Any]) -> tuple[str, float | None] | None:
    """Apply exit precedence for one daily bar; 10 bps adverse on stops/market opens."""
    opening, low = _number(bar.get("open")), _number(bar.get("low"))
    stop, entry = _number(position.get("stop")), _number(position.get("entry_price"))
    if None in (opening, low, stop, entry):
        return None
    slip = 0.001
    if position.get("exit_next_open"):
        return "ema9_exit", opening * (1 - slip)
    if opening <= stop:
        return "gap_stop", opening * (1 - slip)
    if low <= stop:
        return "stop", stop * (1 - slip)
    high = _number(bar.get("high"))
    r = _number(position.get("initial_r")) or (entry - stop)
    if high is not None and r > 0 and high >= entry + 3 * r and not position.get("trimmed"):
        return "trim_3r", entry + 3 * r
    return None


class MartinForwardTestService:
    def __init__(self, repository: MartinForwardTestRepository, *, database_url: str = "", bar_loader=None) -> None:
        self.repository = repository
        self.database_url = database_url
        self.bar_loader = bar_loader or load_many_ticker_windows

    @property
    def _book_id(self) -> str:
        return _BOOK_ID.get() or STRATEGY_ID

    def run_session(self, *, as_of_date: date, snapshot: dict[str, Any] | None, strategy_id: str = STRATEGY_ID,
                    book_id: str | None = None, policy_id: str | None = None) -> dict[str, Any]:
        if policy_id is not None:
            if strategy_id != STRATEGY_ID and strategy_id != policy_id:
                raise ValueError("Conflicting Martin policy identities")
            strategy_id = policy_id
        token = _BOOK_ID.set(book_id or strategy_id)
        try:
            return self._run_session_with_book(as_of_date=as_of_date, snapshot=snapshot, strategy_id=strategy_id)
        finally:
            _BOOK_ID.reset(token)

    def _run_session_with_book(self, *, as_of_date: date, snapshot: dict[str, Any] | None,
                               strategy_id: str) -> dict[str, Any]:
        # strategy_id selects policy; the context-local book ID selects persistence.
        if strategy_id == ROTATION_STRATEGY_ID:
            require_completed_session(as_of_date)
            with self.repository.atomic_session(self._book_id):
                latest = self.repository.latest_session_date(self._book_id)
                if latest is not None and latest >= as_of_date:
                    return {"strategy_id": strategy_id, "session_date": as_of_date.isoformat(), "skipped": True, "reason": "session_already_processed"}
                self._rotation_dates = session_dates(as_of_date)
                if not self._rotation_dates or self._rotation_dates[-1] != as_of_date:
                    raise ValueError("Rotation strategy requires an exchange session")
                if latest is not None and latest != self._rotation_dates[-2]:
                    raise ValueError("Rotation book has missed intermediate sessions; explicit recovery is required")
                if not snapshot or str(snapshot.get("target_trading_date")) != as_of_date.isoformat():
                    raise ValueError("Rotation strategy requires the exact session's persisted snapshot")
                result = self._run_session(as_of_date=as_of_date, snapshot=snapshot, strategy_id=strategy_id)
                if result.get("run_id") is None or result.get("partial"):
                    raise ValueError("Incomplete rotation session; all changes rolled back")
                return result
        lock = getattr(self.repository, "execution_lock", None)
        with lock(self._book_id) if lock and strategy_id == RS_EXPANDED_STRATEGY_ID else nullcontext():
            return self._run_session(as_of_date=as_of_date, snapshot=snapshot, strategy_id=strategy_id)

    def _run_session(self, *, as_of_date: date, snapshot: dict[str, Any] | None, strategy_id: str) -> dict[str, Any]:
        """Process one exact session only; missed intermediate sessions require explicit replay support."""
        replay_covers = getattr(self.repository, "replay_covers_date", None)
        if strategy_id == RS_EXPANDED_STRATEGY_ID and replay_covers and replay_covers(as_of_date, self._book_id):
            return {"strategy_id": strategy_id, "session_date": as_of_date.isoformat(), "skipped": True,
                    "reason": "historical_replay_already_covers_session"}
        # shortcut: settles orders and positions only on this exact session, when multi-session replay is added.
        book_id = getattr(self, "_book_id", strategy_id)
        open_positions = self.repository.open_positions(strategy_id=book_id)
        pending = self.repository.pending_candidates(before=as_of_date, strategy_id=book_id)
        tickers = {str(item.get("ticker") or "").upper() for item in pending + open_positions}
        board_rows: list[dict[str, Any]] = []
        if snapshot and str(snapshot.get("target_trading_date")) == as_of_date.isoformat():
            board = snapshot.get("guru_board") if isinstance(snapshot.get("guru_board"), dict) else {}
            board_rows = [
                row
                for row in board.get("rows", [])
                if isinstance(row, dict) and _scanner_ids(row).intersection(FOCUS_SCANNERS)
            ]
            tickers.update(str(row.get("ticker") or "").upper() for row in board_rows)
        frames = self.bar_loader(sorted(tickers), as_of_date, 35, database_url=self.database_url) if tickers else {}
        if strategy_id == ROTATION_STRATEGY_ID:
            for item in open_positions + pending:
                if not valid_history(frames.get(str(item["ticker"]).upper()), self._rotation_dates):
                    raise ValueError("Missing or invalid rotation execution history; all changes rolled back")
        if strategy_id in RS_ACCOUNT_STRATEGIES:
            emerging = {str(row.get("ticker") or "").upper() for row in board_rows
                        if (rs := _number(row.get("daily_rs_rating"))) is not None and 80 <= rs < 90}
            try:
                qualification_tickers = (tickers | {"SPY", "QQQ"}) if strategy_id == ROTATION_STRATEGY_ID else emerging | {"SPY"}
                qualification_frames = self.bar_loader(sorted(qualification_tickers), as_of_date, 260, database_url=self.database_url) if emerging or strategy_id == ROTATION_STRATEGY_ID else {}
            except Exception:
                logging.getLogger(__name__).warning("RS-expanded qualification history unavailable; emerging candidates blocked.")
                qualification_frames = {}
            # Never replace the engine's 35-bar exit frames with longer EMA history.
            selection_frames = {**frames, **qualification_frames}
            board_rows = [{**row, "_rs_candidate": classify_rs_expanded_candidate(row, rank, as_of_date, selection_frames)}
                          for rank, row in enumerate(board_rows, start=1)]
            board_rows.sort(key=lambda row: (row["_rs_candidate"]["rs_tier"] != "preferred", row["_rs_candidate"]["rank"]))
            for rank, row in enumerate(board_rows, start=1):
                row["_rs_candidate"]["rank"] = rank
                if strategy_id == ROTATION_STRATEGY_ID and not valid_history(selection_frames.get(str(row["ticker"]).upper()), self._rotation_dates):
                    row["_rs_candidate"].update(status="blocked_data", reasons=["rotation_candidate_history_missing"], selected_for_book=False)
            if strategy_id == ROTATION_STRATEGY_ID:
                self._rotation_frames = selection_frames
        counts = {"filled": 0, "expired": 0, "cancelled": 0, "exited": 0, "trimmed": 0, "missing_bars": 0}
        run_id = self._save_snapshot(as_of_date, snapshot, board_rows, strategy_id)
        if strategy_id == ROTATION_STRATEGY_ID:
            counts.update(rotation_exited=0, rotation_scheduled=0)
            self._rotation_opening_exits(open_positions, frames, as_of_date, run_id, counts)
            open_positions = self.repository.open_positions(strategy_id=self._book_id)

        # Pending orders are one-session orders, so the target session's exact bar settles them.
        filled_positions: list[dict[str, Any]] = []
        for order in pending:
            if is_leveraged_or_inverse_ticker(str(order.get("ticker") or "")):
                changed = self.repository.apply_event(run_id=run_id, position_id=None, ticker=order["ticker"],
                    event_type="order_cancelled", event_date=as_of_date,
                    payload={"reason": "leveraged_or_inverse_fund", "status": "cancelled"},
                    candidate_id=order["id"], candidate_status="cancelled", strategy_id=self._book_id)
                counts["cancelled"] += int(changed)
                continue
            awaited_session = (order.get("payload_json") or {}).get("awaiting_session_date")
            if awaited_session and as_of_date.isoformat() > str(awaited_session):
                counts["missing_bars"] += 1
                continue
            order_date = order.get("session_date")
            frame = frames.get(str(order["ticker"]).upper())
            next_bar_date, bar = _bar_after_date(frame, order_date, as_of_date)
            if bar is None or next_bar_date is None:
                counts["missing_bars"] += 1
                self._mark_order_awaiting_data(order, "awaiting_session_bar", as_of_date)
                continue
            if next_bar_date != as_of_date:
                # The order's first bar was missed; do not apply a later bar out of sequence.
                counts["missing_bars"] += 1
                self._mark_order_awaiting_data(order, "first_session_bar_missed", next_bar_date)
                continue
            event_date = next_bar_date
            payload = order.get("payload_json") or {}
            if strategy_id == ROTATION_STRATEGY_ID:
                payload = self._rotation_execution_order(order, payload, bar, as_of_date, counts)
                if payload is None:
                    continue
            state, fill, reason = order_outcome(payload, bar)
            if state == "pending":
                continue
            if state == "filled":
                shares = int(payload.get("shares") or 0)
                position = {**payload, "entry_price": fill, "original_stop": payload.get("stop"),
                            "initial_r": float(fill) - float(payload["stop"]), "shares": shares,
                            "initial_r_per_share": float(fill) - float(payload["stop"]),
                            "remaining_shares": shares, "initial_shares": shares, "realized_r": 0.0, "realized_pnl": 0.0,
                            "holding_sessions": 0, "trimmed": False, "exit_next_open": False,
                            "opened_on": event_date.isoformat()}
                if strategy_id == ROTATION_STRATEGY_ID:
                    position.update(rotation_policy_version=ROTATION_POLICY_VERSION, last_close=float(bar["close"]),
                                    max_favorable_r=max(0.0, (float(bar["high"]) - float(fill)) / (float(fill) - float(payload["stop"]))))
                    if position.get("rotation_intent"):
                        position["rotation_entry_intent"] = position.pop("rotation_intent")
                position_id = self.repository.fill_order(candidate_id=order["id"], run_id=order["run_id"], ticker=order["ticker"],
                                                         event_date=event_date, position_payload=position,
                                                         event_payload={"price": fill, "shares": shares}, strategy_id=self._book_id)
                if position_id is None:
                    state, reason = "blocked", "open_position_already_exists"
                else:
                    filled_positions.append({"id": position_id, "ticker": order["ticker"], "payload_json": position})
                    counts["filled"] += 1
                    if strategy_id == ROTATION_STRATEGY_ID:
                        continue  # Settle all entries before any intraday stop can release cash for another opening entry.
                    # Same-day stop is active, but a 3R trim is explicitly deferred.
                    stop_hit = order_outcome({"stop": position["stop"], "limit_price": fill}, bar)[0] == "cancelled"
                    if stop_hit or float(bar["low"]) <= float(position["stop"]):
                        exit_type, exit_price = ("gap_stop", float(bar["open"]) * .999) if float(bar["open"]) <= float(position["stop"]) else ("stop", float(position["stop"]) * .999)
                        pnl = (exit_price - float(fill)) * shares
                        initial_risk = float(position["initial_r_per_share"]) * shares
                        closed = {**position, "remaining_shares": 0, "exit_next_open": False, "closed_on": event_date.isoformat(), "exit_price": exit_price,
                                  "exit_type": exit_type, "realized_pnl": pnl, "realized_r": pnl / initial_risk if initial_risk else 0,
                                  "holding_sessions": 1}
                        self.repository.apply_event(run_id=run_id, position_id=position_id, ticker=order["ticker"], event_type=exit_type,
                            event_date=event_date, payload={"price": exit_price, "reason": "same_day_stop", "realized_pnl": pnl}, position_payload=closed, close_position=True, strategy_id=self._book_id)
                        counts["exited"] += 1
                    else:
                        self._schedule_ema_exit(order["ticker"], position_id, position, frame, event_date, strategy_id)
            if state != "filled":
                event_type = "order_expired" if state == "expired" else ("order_cancelled" if state == "cancelled" else "order_blocked")
                changed = self.repository.apply_event(run_id=run_id, position_id=None, ticker=order["ticker"], event_type=event_type,
                    event_date=event_date, payload={"reason": reason, "status": state}, candidate_id=order["id"], candidate_status=state, strategy_id=self._book_id)
                counts[state if state in counts else "expired"] += int(changed)

        current_position_ids = {int(item["id"]) for item in filled_positions}
        if strategy_id == ROTATION_STRATEGY_ID:
            for position in filled_positions:
                frame = frames.get(position["ticker"])
                self._manage_position(position, _bar_for_date(frame, as_of_date), frame, as_of_date, run_id, counts, strategy_id, allow_trim=False)
        for position_row in open_positions:
            if int(position_row["id"]) in current_position_ids:
                continue
            ticker = str(position_row["ticker"]).upper()
            bar = _bar_for_date(frames.get(ticker), as_of_date)
            if bar is None:
                counts["missing_bars"] += 1
                continue
            self._manage_position(position_row, bar, frames.get(ticker), as_of_date, run_id, counts, strategy_id)

        if snapshot and run_id is not None:
            self._reserve_new_candidates(board_rows, as_of_date, run_id, frames, counts, strategy_id)
            if strategy_id == ROTATION_STRATEGY_ID:
                self._schedule_rotation(board_rows, as_of_date, run_id, counts)
            focus_tickers = {str(row.get("ticker") or "").upper() for row in board_rows}
            for position in self.repository.open_positions(strategy_id=self._book_id):
                payload = position.get("payload_json") or {}
                if str(position["ticker"]).upper() not in focus_tickers and not payload.get("dropped_from_focus"):
                    updated = {**payload, "dropped_from_focus": True, "dropped_from_focus_date": as_of_date.isoformat()}
                    self.repository.apply_event(run_id=run_id, position_id=int(position["id"]), ticker=position["ticker"],
                        event_type="dropped_from_focus", event_date=as_of_date,
                        payload={"reason": "not_in_complete_snapshot"}, position_payload=updated, strategy_id=self._book_id)
        mark_prices = {ticker: bar["close"] for ticker in tickers if (bar := _bar_for_date(frames.get(ticker), as_of_date)) is not None}
        finalize = getattr(self.repository, "update_run_summary", None)
        if run_id is not None and finalize:
            kwargs = {"run_id": run_id, "strategy_id": self._book_id, "mark_prices": mark_prices, "action_counts": counts}
            if self._book_id != strategy_id:
                kwargs["policy_id"] = strategy_id
            finalize(**kwargs)
        return {"strategy_id": strategy_id, "session_date": as_of_date.isoformat(), "run_id": run_id, **counts, "partial": counts["missing_bars"] > 0,
                "snapshot_available": bool(snapshot and str(snapshot.get("target_trading_date")) == as_of_date.isoformat())}

    def _save_snapshot(self, as_of_date: date, snapshot: dict[str, Any] | None, rows: list[dict[str, Any]], strategy_id: str) -> int | None:
        if not snapshot or str(snapshot.get("target_trading_date")) != as_of_date.isoformat():
            return None
        source_run_id = int(snapshot.get("_snapshot_run_id") or 0)
        fingerprint = str(snapshot.get("source_fingerprint") or "")
        replay_provenance = snapshot.get("replay_provenance") if isinstance(snapshot.get("replay_provenance"), dict) else {}
        reconstructed = replay_provenance.get("source_mode") == "martin_reconstructed_inputs_v1"
        if (source_run_id <= 0 and not reconstructed) or not fingerprint:
            return None
        candidates = (select_sector_candidates(rows) if strategy_id == SECTOR_STRATEGY_ID else
                      [dict(row["_rs_candidate"]) if strategy_id in RS_ACCOUNT_STRATEGIES else classify_candidate(row, rank)
                       for rank, row in enumerate(rows, start=1)])
        for item in candidates:
            item["selection_date"] = as_of_date.isoformat()
            if strategy_id == ROTATION_STRATEGY_ID:
                item["rotation_policy_version"] = ROTATION_POLICY_VERSION
            if replay_provenance:
                item["replay_provenance"] = replay_provenance
            if item["status"] == "pending":
                item["queue_rank"] = item["rank"]
                item["selection_reason"] = "snapshot_rank"
        if strategy_id == DAILY_TOP5_STRATEGY_ID:
            for item in candidates:
                item["selected_for_book"] = False
                item["queue_rank"] = item["rank"]
        for item in candidates:
            if item["status"] == "pending":
                item["selected_for_book"] = strategy_id != DAILY_TOP5_STRATEGY_ID
                item["status"] = "eligible"
            else:
                item["selected_for_book"] = bool(item.get("selection_tier"))
        summary = {"strategy_id": strategy_id, "target_trading_date": as_of_date.isoformat(), "source_run_id": source_run_id,
                   "source_fingerprint": fingerprint, "candidate_count": len(candidates)}
        if strategy_id in RS_ACCOUNT_STRATEGIES:
            summary["stop_policy_version"] = STRUCTURAL_STOP_POLICY
        if strategy_id == ROTATION_STRATEGY_ID:
            summary.update(rotation_policy_version=ROTATION_POLICY_VERSION, rotation_limits=ROTATION_LIMITS)
        if replay_provenance:
            summary["replay_provenance"] = replay_provenance
            summary["replay_validation"] = snapshot.get("replay_validation", {})
        return self.repository.save_session(session_date=as_of_date, source_run_id=source_run_id, fingerprint=fingerprint,
                                            candidates=candidates, summary=summary, strategy_id=self._book_id)

    def _reserve_new_candidates(self, rows: list[dict[str, Any]], as_of_date: date, run_id: int,
                                frames: dict[str, Any], counts: dict[str, int], strategy_id: str) -> None:
        if strategy_id == ROTATION_STRATEGY_ID:
            self._reserve_rotation_candidates(rows, as_of_date, run_id)
            return
        open_positions = self.repository.open_positions(strategy_id=self._book_id)
        pending_orders = self.repository.pending_orders(strategy_id=self._book_id)
        open_tickers = {str(row["ticker"]).upper() for row in open_positions}
        pending_tickers = {str(row["ticker"]).upper() for row in pending_orders}
        equity = 100_000.0
        risk_budget = equity * 0.005
        per_position_allocation = equity * 0.20
        allocated = sum(float((row.get("payload_json") or {}).get("entry_price") or 0) * int((row.get("payload_json") or {}).get("remaining_shares") or 0) for row in open_positions)
        allocated += sum(float((row.get("payload_json") or {}).get("limit_price") or 0) * int((row.get("payload_json") or {}).get("shares") or 0) for row in pending_orders)
        slots = (10 if strategy_id == SECTOR_STRATEGY_ID else
                 5 if strategy_id == DAILY_TOP5_STRATEGY_ID else max(0, 5 - len(open_positions) - len(pending_orders)))
        submitted_today = self.repository.submitted_order_count(session_date=as_of_date, strategy_id=self._book_id) if strategy_id == DAILY_TOP5_STRATEGY_ID else 0
        daily_orders_remaining = max(0, 5 - submitted_today) if strategy_id == DAILY_TOP5_STRATEGY_ID else None
        if strategy_id == DAILY_TOP5_STRATEGY_ID:
            selected = (self.repository.daily_top5_selected_tickers(session_date=as_of_date)
                        if self._book_id == DAILY_TOP5_STRATEGY_ID else
                        self.repository.daily_top5_selected_tickers(session_date=as_of_date, book_id=self._book_id))
            prior_selection = set(selected)
        else:
            prior_selection = set()
        daily_pool: set[str] = set()
        if strategy_id == DAILY_TOP5_STRATEGY_ID:
            if prior_selection:
                daily_pool = prior_selection
            else:
                actionable = []
                for rank, row in enumerate(rows, start=1):
                    item = classify_candidate(row, rank)
                    ticker = item["ticker"]
                    if item["status"] == "pending" and ticker and ticker not in open_tickers and ticker not in pending_tickers:
                        actionable.append(ticker)
                daily_pool = set(actionable[:5])
        for rank, row in enumerate(rows, start=1):
            candidate = dict(row["_rs_candidate"]) if strategy_id in RS_ACCOUNT_STRATEGIES else classify_candidate(row, rank)
            if candidate["status"] != "pending":
                continue
            ticker = candidate["ticker"]
            existing_status = self.repository.candidate_status(run_id, ticker)
            if existing_status != "eligible":
                continue
            candidate.update({key: value for key, value in self.repository.candidate_payload(run_id, ticker).items()
                              if key in {"selected_for_book", "selection_date", "selection_tier", "sector_quadrant", "sector_rs_ratio", "sector_momentum_value", "sector_as_of_date", "queue_rank", "replay_provenance"}})
            if not ticker or ticker in open_tickers:
                candidate.update(status="blocked_open_position", selected_for_book=False, reasons=["open_position_exists"])
                self.repository.update_candidate(run_id, ticker, candidate)
                self.repository.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type="candidate_blocked_open_position",
                    event_date=as_of_date, payload={"reason": "open_position_exists"}, candidate_id=None, strategy_id=self._book_id)
                continue
            if self.repository.has_pending_order(ticker, strategy_id=self._book_id):
                candidate.update(status="blocked_existing_order", selected_for_book=False, reasons=["pending_order_exists"])
                self.repository.update_candidate(run_id, ticker, candidate)
                continue
            if strategy_id == DAILY_TOP5_STRATEGY_ID and ticker not in daily_pool:
                candidate.update(status="not_selected", reasons=["daily_top5_order_limit"])
                self.repository.update_candidate(run_id, ticker, candidate)
                continue
            if strategy_id == DAILY_TOP5_STRATEGY_ID:
                candidate["selected_for_book"] = True
            if strategy_id == DAILY_TOP5_STRATEGY_ID and daily_orders_remaining <= 0:
                candidate.update(status="not_selected", reasons=["daily_order_limit_reached"])
                self.repository.update_candidate(run_id, ticker, candidate)
                continue
            limit, stop = float(candidate["limit_price"]), float(candidate["stop"])
            normalized = strategy_id == SECTOR_STRATEGY_ID
            daily_top5 = strategy_id == DAILY_TOP5_STRATEGY_ID
            shares = (100 if normalized or daily_top5 else min(int(risk_budget / (limit - stop)), int(per_position_allocation / limit), int((equity - allocated) / limit)))
            if (slots <= 0 and strategy_id != SECTOR_STRATEGY_ID) or shares <= 0:
                candidate.update(status="waiting_capacity", reasons=["position_or_allocation_capacity"])
                candidate["queue_rank"] = rank
                self.repository.update_candidate(run_id, ticker, candidate)
                continue
            if strategy_id in RS_ACCOUNT_STRATEGIES and candidate["rs_tier"] == "emerging":
                candidate["rs_exception_metrics"] = {**candidate["rs_exception_metrics"], "actual_order_notional": shares * limit}
                within_liquidity = shares * limit <= .001 * candidate["rs_exception_metrics"]["adv20_dollars"]
                candidate["rs_exception_checks"] = {**candidate["rs_exception_checks"], "order_notional_within_liquidity": within_liquidity}
                if not within_liquidity:
                    candidate.update(status="watch", selected_for_book=False, reasons=["rs_order_notional_exceeds_liquidity"],
                                     readiness_explanation=RS_EXCEPTION_EXPLANATIONS["rs_order_notional_exceeds_liquidity"],
                                     readiness_details=[RS_EXCEPTION_EXPLANATIONS["rs_order_notional_exceeds_liquidity"]])
                    self.repository.update_candidate(run_id, ticker, candidate)
                    continue
            candidate.update(shares=shares, equity=None if normalized or daily_top5 else equity,
                             sizing_mode="normalized_r" if normalized else "fixed_100_shares" if daily_top5 else "account_risk",
                             risk_dollars=shares * (limit - stop), allocation_dollars=shares * limit, status="pending", order_date=as_of_date.isoformat())
            candidate["queue_rank"] = rank
            candidate["selection_reason"] = candidate.get("selection_reason") or "snapshot_rank"
            self.repository.update_candidate(run_id, ticker, candidate)
            allocated += shares * limit
            if strategy_id == DAILY_TOP5_STRATEGY_ID:
                daily_orders_remaining -= 1
            if strategy_id not in {SECTOR_STRATEGY_ID, DAILY_TOP5_STRATEGY_ID}:
                slots -= 1

    def _rotation_opening_exits(self, positions, frames, day, run_id, counts):
        for row in positions:
            ticker, p = row["ticker"], dict(row.get("payload_json") or {})
            bar = _bar_for_date(frames.get(ticker), day)
            opening, stop = float(bar["open"]), float(p["stop"])
            event = "gap_stop" if opening <= stop else "ema9_exit" if p.get("exit_next_open") else None
            intent = p.get("rotation_intent")
            if intent and intent["outgoing"]["position_id"] != row["id"]:
                intent = None
            if intent and intent["decision_date"] != self._rotation_dates[-2].isoformat():
                raise ValueError("Rotation exit missed its exact execution session")
            if event is None and intent:
                event = "rotation_exit"
            if event:
                shares, entry = int(p["remaining_shares"]), float(p["entry_price"])
                price = opening * .999
                pnl = (price - entry) * shares
                original_risk = float(p["initial_r_per_share"]) * int(p["initial_shares"])
                closed = {**p, "remaining_shares": 0, "closed_on": day.isoformat(), "exit_price": price,
                          "exit_type": event, "exit_next_open": False,
                          "realized_pnl": float(p.get("realized_pnl") or 0) + pnl,
                          "realized_r": float(p.get("realized_r") or 0) + pnl / original_risk,
                          "holding_sessions": int(p.get("holding_sessions") or 0) + 1}
                changed = self.repository.apply_event(run_id=run_id, position_id=row["id"], ticker=ticker, event_type=event,
                    event_date=day, payload={"price": price, "shares": shares, "realized_pnl": pnl,
                                            "rotation_intent": intent, "rotation_policy_version": ROTATION_POLICY_VERSION},
                    position_payload=closed, close_position=True, strategy_id=self._book_id)
                if not changed:
                    raise ValueError("Rotation opening exit could not be recorded atomically")
                counts["exited"] += 1
                counts["rotation_exited"] += int(event == "rotation_exit")
            else:
                r = float(p["initial_r_per_share"])
                p.update(last_close=float(bar["close"]), max_favorable_r=max(float(p.get("max_favorable_r") or 0),
                    (float(bar["high"]) - float(p["entry_price"])) / r))
                self.repository.set_position_payload(row["id"], p)

    def _rotation_execution_order(self, order, payload, bar, day, counts):
        intent = payload.get("rotation_intent")
        reason = None
        if order["session_date"] != self._rotation_dates[-2]:
            raise ValueError("Rotation book order missed its exact execution session")
        opened = self.repository.open_positions(strategy_id=self._book_id)
        if float(bar["open"]) <= float(payload["stop"]):
            return payload  # Preserve the inherited gap-below-stop cancellation reason and status.
        if intent and any(row["id"] == intent["outgoing"]["position_id"] for row in opened):
            reason = "rotation_exit_not_filled"
        elif intent and float(bar["open"]) > float(payload["limit_price"]):
            reason = "rotation_replacement_gap_above_max_entry"
        if reason is None:
            budget = self.repository.account_state(self._book_id, exclude_order_ticker=order["ticker"])
            price = min(float(bar["open"]), float(payload["limit_price"]))
            shares = min(int(payload["shares"]), size_order(payload, budget, price=price))
            if len(opened) >= 8 or shares <= 0:
                reason = "rotation_execution_capacity_unavailable"
            else:
                payload = {**payload, "shares": shares, "allocation_dollars": shares * float(payload["limit_price"]),
                           "risk_dollars": shares * (price - float(payload["stop"]))}
                self.repository.update_candidate(order["run_id"], order["ticker"], payload)
                return payload
        self.repository.apply_event(run_id=order["run_id"], position_id=None, ticker=order["ticker"],
            event_type="rotation_entry_expired" if intent else "order_expired", event_date=day,
            payload={"reason": reason, "rotation_intent": intent}, candidate_id=order["id"], candidate_status="expired",
            strategy_id=self._book_id)
        counts["expired"] += 1
        return None

    def _reserve_rotation_candidates(self, rows, day, run_id):
        for row in rows:
            candidate = dict(row["_rs_candidate"])
            ticker = candidate["ticker"]
            if candidate["status"] != "pending" or self.repository.candidate_status(run_id, ticker) != "eligible":
                continue
            candidate.update({"rotation_policy_version": ROTATION_POLICY_VERSION, "selection_date": day.isoformat(),
                              "queue_rank": candidate["rank"], "selected_for_book": False})
            opened = self.repository.open_positions(strategy_id=self._book_id)
            pending = self.repository.pending_orders(strategy_id=self._book_id)
            if ticker in {p["ticker"] for p in opened}:
                candidate.update(status="blocked_open_position", reasons=["open_position_exists"])
            elif ticker in {p["ticker"] for p in pending}:
                candidate.update(status="blocked_existing_order", reasons=["pending_order_exists"])
            else:
                budget = self.repository.account_state(self._book_id)
                shares = size_order(candidate, budget)
                if len(opened) + len(pending) >= 8 or shares <= 0:
                    candidate.update(status="waiting_capacity", reasons=["position_cash_or_risk_capacity"])
                else:
                    limit, stop = float(candidate["limit_price"]), float(candidate["stop"])
                    candidate.update(shares=shares, equity=100_000.0, sizing_mode="account_risk", risk_dollars=shares * (limit - stop),
                        allocation_dollars=shares * limit, status="pending", order_date=day.isoformat(), selected_for_book=True)
            self.repository.update_candidate(run_id, ticker, candidate)

    def _schedule_rotation(self, rows, day, run_id, counts):
        candidates = [self.repository.candidate_payload(run_id, row["ticker"]) for row in rows]
        waiting = [item for item in candidates if item.get("status") == "waiting_capacity"]
        last = self.repository.last_rotation_exit(self._book_id)
        decision = rotation_decision(day=day, positions=self.repository.open_positions(strategy_id=self._book_id),
            candidates=waiting, rows=rows, frames=self._rotation_frames, last_rotation=last, dates=self._rotation_dates)
        # Prefer replacing a weaker unfilled reservation before displacing an actual holding.
        if waiting and decision.get("reason") not in {"rotation_cooldown", "rotation_benchmark_data_missing", "rotation_market_regime"}:
            replacement = self._replace_rotation_pending(waiting, rows, day, run_id)
            if replacement:
                replacement.update(decision_date=day.isoformat(), cooldown_sessions_remaining=decision["cooldown_sessions_remaining"],
                                   last_rotation_exit=decision["last_rotation_exit"])
                self.repository.set_run_metadata(run_id, {"rotation": replacement})
                return
            waiting = [self.repository.candidate_payload(run_id, item["ticker"]) for item in waiting
                       if self.repository.candidate_status(run_id, item["ticker"]) == "waiting_capacity"]
            decision = rotation_decision(day=day, positions=self.repository.open_positions(strategy_id=self._book_id),
                candidates=waiting, rows=rows, frames=self._rotation_frames, last_rotation=last, dates=self._rotation_dates)
        if decision["status"] == "scheduled":
            outgoing = next(row for row in self.repository.open_positions(strategy_id=self._book_id)
                            if row["id"] == decision["outgoing"]["position_id"])
            ticker = decision["incoming"]["ticker"]
            candidate = dict(self.repository.candidate_payload(run_id, ticker))
            p = outgoing["payload_json"]
            budget = self.repository.account_state(self._book_id)
            budget["available_cash"] += int(p["remaining_shares"]) * float(self._rotation_frames[outgoing["ticker"]]["Close"].iloc[-1]) * .999
            released_risk = int(p["remaining_shares"]) * max(float(p["entry_price"]) - float(p["stop"]), 0)
            budget["available_risk"] = max(0.0, 2_500 - budget["committed_stop_risk"] + released_risk)
            shares = size_order(candidate, budget)
            if shares <= 0:
                decision.update(status="not_scheduled", reason="rotation_projected_capacity_unavailable")
            else:
                candidate.update(status="pending", selected_for_book=True, shares=shares, order_date=day.isoformat(),
                    equity=100_000.0, sizing_mode="account_risk", rotation_intent=decision, rotation_policy_version=ROTATION_POLICY_VERSION,
                    risk_dollars=shares * (float(candidate["limit_price"]) - float(candidate["stop"])),
                    allocation_dollars=shares * float(candidate["limit_price"]), reasons=["rotation_waiting_exit"])
                self.repository.update_candidate(run_id, ticker, candidate)
                self.repository.set_position_payload(outgoing["id"], {**p, "rotation_intent": decision})
                if not self.repository.apply_event(run_id=run_id, position_id=outgoing["id"], ticker=outgoing["ticker"],
                        event_type="rotation_scheduled", event_date=day, payload=decision, strategy_id=self._book_id):
                    raise ValueError("Duplicate rotation intent; session rolled back")
                counts["rotation_scheduled"] += 1
        self.repository.set_run_metadata(run_id, {"rotation": decision})

    def _replace_rotation_pending(self, waiting, rows, day, run_id):
        row_map = {row["ticker"]: row for row in rows}
        qualified = [(candidate, replacement_metrics(candidate, row_map[candidate["ticker"]],
                        self._rotation_frames.get(candidate["ticker"]), day, self._rotation_dates)) for candidate in waiting]
        qualified = [(candidate, m) for candidate, m in qualified if m is not None]
        qualified.sort(key=lambda pair: (-pair[0]["rs_rating"], -pair[1]["return5_pct"], -pair[1]["adv20_dollars"], pair[0]["ticker"]))
        for candidate, m in qualified:
            new_priority = (-candidate["rs_rating"], -m["return5_pct"], -m["adv20_dollars"], candidate["ticker"])
            pending = []
            for old in self.repository.pending_orders(strategy_id=self._book_id):
                old_metrics = metrics(self._rotation_frames.get(old["ticker"]), day, self._rotation_dates)
                if old["payload_json"].get("rotation_intent") or old_metrics is None:
                    continue
                priority = (-float(old["payload_json"].get("rs_rating") or 0), -old_metrics["return5_pct"],
                            -old_metrics["adv20_dollars"], old["ticker"])
                if priority > new_priority:
                    pending.append((priority, old))
            for _, old in sorted(pending, key=lambda item: item[0], reverse=True):
                p = old["payload_json"]
                budget = self.repository.account_state(self._book_id)
                budget["available_cash"] += float(p["limit_price"]) * int(p["shares"])
                released_risk = max(float(p["limit_price"]) - float(p["stop"]), 0) * int(p["shares"])
                budget["available_risk"] = max(0.0, 2_500 - budget["committed_stop_risk"] + released_risk)
                shares = size_order(candidate, budget)
                if shares <= 0:
                    continue
                self.repository.update_candidate(run_id, old["ticker"], {**p, "status": "cancelled", "selected_for_book": False,
                    "reasons": ["replaced_by_higher_priority_pending"], "replacement_ticker": candidate["ticker"]})
                limit, stop = float(candidate["limit_price"]), float(candidate["stop"])
                self.repository.update_candidate(run_id, candidate["ticker"], {**candidate, "status": "pending", "selected_for_book": True,
                    "shares": shares, "order_date": day.isoformat(), "risk_dollars": shares * (limit - stop), "allocation_dollars": shares * limit,
                    "equity": 100_000.0, "sizing_mode": "account_risk", "reasons": ["replaced_lower_priority_pending"]})
                self.repository.apply_event(run_id=run_id, position_id=None, ticker=old["ticker"], event_type="pending_replaced",
                    event_date=day, payload={"incoming_ticker": candidate["ticker"], "reason": "higher_priority_unfilled_order"}, strategy_id=self._book_id)
                return {"status": "pending_replaced", "reason": "higher_priority_unfilled_order", "outgoing_ticker": old["ticker"],
                        "incoming_ticker": candidate["ticker"], "rotation_policy_version": ROTATION_POLICY_VERSION}

    def _mark_order_awaiting_data(self, order: dict[str, Any], reason: str, session_date: date) -> None:
        payload = dict(order.get("payload_json") or {})
        payload["market_data_state"] = "awaiting_data"
        payload["market_data_reason"] = reason
        payload["awaiting_session_date"] = session_date.isoformat()
        payload["reasons"] = [reason]
        self.repository.update_candidate(order["run_id"], order["ticker"], payload)

    def _manage_position(self, row: dict[str, Any], bar: dict[str, Any], frame: Any, as_of_date: date,
                         run_id: int | None, counts: dict[str, int], strategy_id: str, *, allow_trim: bool = True) -> None:
        position = row.get("payload_json") or {}
        ticker, position_id = str(row["ticker"]).upper(), int(row["id"])
        outcome = position_exit(position, bar)
        if outcome and outcome[0] == "trim_3r" and not allow_trim:
            outcome = None
        if outcome and outcome[0] != "trim_3r":
            event_type, price = outcome
            remaining = int(position.get("remaining_shares") or position.get("shares") or 0)
            pnl = (price - float(position["entry_price"])) * remaining
            risk = float(position.get("initial_r_per_share") or position.get("initial_r") or 0)
            closed = {**position, "remaining_shares": 0, "closed_on": as_of_date.isoformat(), "exit_price": price, "exit_next_open": False,
                      "exit_type": event_type,
                      "realized_pnl": float(position.get("realized_pnl") or 0) + pnl,
                      "realized_r": float(position.get("realized_r") or 0) + (pnl / (risk * int(position.get("initial_shares") or position.get("shares") or 1)) if risk > 0 else 0),
                      "holding_sessions": int(position.get("holding_sessions") or 0) + 1}
            if self.repository.apply_event(run_id=run_id, position_id=position_id, ticker=ticker, event_type=event_type,
                    event_date=as_of_date, payload={"price": price, "realized_pnl": pnl}, position_payload=closed, close_position=True, strategy_id=self._book_id):
                counts["exited"] += 1
            return
        if outcome and outcome[0] == "trim_3r":
            shares = int(position.get("remaining_shares") or 0)
            trim_shares = int(shares * .10)
            if trim_shares > 0:
                trim_price = float(outcome[1])
                pnl = (trim_price - float(position["entry_price"])) * trim_shares
                risk = float(position.get("initial_r_per_share") or position.get("initial_r") or 0)
                updated = {**position, "remaining_shares": shares - trim_shares, "trimmed": True, "trimmed_on": as_of_date.isoformat(),
                           "realized_pnl": float(position.get("realized_pnl") or 0) + pnl,
                           "realized_r": float(position.get("realized_r") or 0) + (pnl / (risk * int(position.get("initial_shares") or position.get("shares") or 1)) if risk > 0 else 0),
                           "holding_sessions": int(position.get("holding_sessions") or 0) + int(position.get("last_managed_date") != as_of_date.isoformat()),
                           "last_managed_date": as_of_date.isoformat()}
                if self.repository.apply_event(run_id=run_id, position_id=position_id, ticker=ticker, event_type="trim_3r",
                        event_date=as_of_date, payload={"price": outcome[1], "shares": trim_shares, "realized_pnl": pnl}, position_payload=updated, strategy_id=self._book_id):
                    counts["trimmed"] += 1
                position = updated
        else:
            if position.get("last_managed_date") != as_of_date.isoformat():
                position = {**position, "holding_sessions": int(position.get("holding_sessions") or 0) + 1,
                            "last_managed_date": as_of_date.isoformat()}
                self.repository.set_position_payload(position_id, position)
        self._schedule_ema_exit(ticker, position_id, position, frame, as_of_date, strategy_id)

    def _schedule_ema_exit(self, ticker: str, position_id: int, position: dict[str, Any], frame: Any, as_of_date: date, strategy_id: str) -> None:
        if frame is None or len(frame) < 9:
            return
        closes = frame["Close"]
        ema9 = float(closes.ewm(span=9, adjust=False).mean().iloc[-1])
        close = float(closes.iloc[-1])
        if close < ema9 and not position.get("exit_next_open"):
            updated = {**position, "exit_next_open": True, "ema9_breach_date": as_of_date.isoformat()}
            self.repository.apply_event(run_id=None, position_id=position_id, ticker=ticker, event_type="ema9_exit_scheduled",
                event_date=as_of_date, payload={"close": close, "ema9": ema9}, position_payload=updated, strategy_id=self._book_id)


def _bar_for_date(frame: Any, as_of_date: date) -> dict[str, float] | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    for index, row in frame.iterrows():
        value = index.date() if hasattr(index, "date") else index
        if value != as_of_date:
            continue
        keys = {str(key).lower(): key for key in row.index}
        try:
            return {name: float(row[keys[name]]) for name in ("open", "high", "low", "close") if name in keys}
        except (TypeError, ValueError):
            return None
    return None


def _bar_after_date(frame: Any, after_date: Any, through_date: date) -> tuple[date | None, dict[str, float] | None]:
    if frame is None or getattr(frame, "empty", True) or after_date is None:
        return None, None
    start = after_date.date() if hasattr(after_date, "date") else after_date
    for index, row in frame.iterrows():
        value = index.date() if hasattr(index, "date") else index
        if value <= start or value > through_date:
            continue
        keys = {str(key).lower(): key for key in row.index}
        try:
            return value, {name: float(row[keys[name]]) for name in ("open", "high", "low", "close") if name in keys}
        except (TypeError, ValueError):
            return None, None
    return None, None
