"""Power Gap & Volume Watch v1, adapted from TXInvestor's Pine v6 script.

Source: TXInvestor, "Power Earnings Gap (PEG) & HVE", MPL-2.0
https://mozilla.org/MPL/2.0/ . Earnings qualification/reset logic is intentionally
omitted. HVE uses completed weekly totals available before each daily event bar.
"""
from __future__ import annotations

import datetime as dt
import math
from collections import defaultdict
from typing import Any, Iterable

RULES_VERSION = "power_gap_volume_watch_v1"
SCANNER_ID = "power_gap_volume_watch"


def _date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def _number(row: dict[str, Any], key: str) -> float:
    value = float(row[key])
    if not math.isfinite(value):
        raise ValueError(f"{key} must be finite")
    return value


def _validate(rows: Iterable[dict[str, Any]], as_of_date: dt.date) -> tuple[list[dict[str, Any]], list[str]]:
    valid, issues, seen, bases = [], [], set(), set()
    for raw in rows:
        try:
            row = dict(raw)
            row["date"] = _date(row["date"])
            if row["date"] > as_of_date:
                continue
            if row["date"] in seen:
                raise ValueError("duplicate session date")
            seen.add(row["date"])
            for field in ("open", "high", "low", "close", "volume"):
                row[field] = _number(row, field)
            if min(row["open"], row["high"], row["low"], row["close"]) <= 0 or row["volume"] < 0:
                raise ValueError("prices must be positive and volume nonnegative")
            if row["high"] < max(row["open"], row["close"], row["low"]) or row["low"] > min(row["open"], row["close"], row["high"]):
                raise ValueError("inconsistent OHLC range")
            basis = row.get("adjustment_basis")
            if basis is not None:
                bases.add(str(basis))
            valid.append(row)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            issues.append(str(exc))
    if len(bases) > 1:
        issues.append("mixed adjustment bases")
    if issues:
        return [], list(dict.fromkeys(issues))
    return sorted(valid, key=lambda row: row["date"]), []


def detect_power_gap_volume(ticker: str, daily_bars: Iterable[dict[str, Any]], as_of_date: dt.date | str,
                           expected_sessions: Iterable[dt.date | str] | None = None) -> dict[str, Any]:
    """Return one ticker's event/current context, or explicit component coverage issues."""
    cutoff = _date(as_of_date)
    bars, problems = _validate(daily_bars, cutoff)
    expected = sorted({_date(value) for value in expected_sessions or [] if _date(value) <= cutoff})
    observed = {bar["date"] for bar in bars}
    missing_dates = [value for value in expected if bars and bars[0]["date"] <= value <= cutoff and value not in observed]
    missing = [value.isoformat() for value in missing_dates]
    if not bars:
        problems.append("no daily history returned")
    coverage: dict[str, Any] = {
        "history_start": bars[0]["date"].isoformat() if bars else None,
        "history_end": bars[-1]["date"].isoformat() if bars else None,
        "daily_bars": len(bars),
        "issues": problems,
        "hvy_complete": bool(expected and len([value for value in expected if value in observed]) >= 252 and not missing),
        "hvy_history": "complete_252_expected_sessions" if expected and len([value for value in expected if value in observed]) >= 252 and not missing else "incomplete_or_calendar_unavailable",
        "listing_status": "unknown_without_listing_metadata",
        "available_history_short": bool(bars and len(bars) < 200),
        "latest_data_stale": bool(bars and bars[-1]["date"] < cutoff),
        "latest_data_days_before_as_of": (cutoff - bars[-1]["date"]).days if bars else None,
    }
    if problems or not bars:
        return {"ticker": ticker.upper(), "signals": [], "coverage": coverage, "power_gap_volume_context": None}

    component_issues: dict[str, str] = {}
    if expected:
        expected_through_asof = [value for value in expected if value <= cutoff]
        recent_gap_sessions = set(expected_through_asof[-260:])
        recent_hvy_sessions = set(expected_through_asof[-252:])
        gap_missing = [value for value in missing_dates if value in recent_gap_sessions]
        hvy_missing = [value for value in missing_dates if value in recent_hvy_sessions]
        if gap_missing:
            component_issues["gap"] = f"missing required sessions in recent gap window: {', '.join(value.isoformat() for value in gap_missing[:5])}"
        if hvy_missing:
            component_issues["hvy"] = f"missing required sessions in recent HVY window: {', '.join(value.isoformat() for value in hvy_missing[:5])}"
        if missing_dates:
            component_issues["hve"] = "incomplete daily history prevents reliable running-record HVE"
        coverage["hvy_complete"] = (len(recent_hvy_sessions) == 252 and len(recent_hvy_sessions & observed) == 252
                                     and not any(value in recent_hvy_sessions for value in missing_dates))
        coverage["hvy_history"] = "complete_252_expected_sessions" if coverage["hvy_complete"] else "incomplete_or_calendar_unavailable"
    elif coverage["latest_data_stale"]:
        component_issues["gap"] = component_issues["hvy"] = "exchange-session coverage unavailable for stale history"
        component_issues["hve"] = "exchange-session coverage unavailable for HVE history"
    coverage["component_issues"] = component_issues
    coverage["missing_sessions"] = missing

    vols = [row["volume"] for row in bars]
    closes = [row["close"] for row in bars]
    gap_event = None
    last_trigger = -10**9
    for i, row in enumerate(bars):
        if i < 199 or i < 19 or i == 0:
            continue
        sma200 = sum(closes[i - 199:i + 1]) / 200
        baseline = sum(vols[i - 19:i + 1]) / 20
        gap = (row["open"] - bars[i - 1]["close"]) / bars[i - 1]["close"] * 100
        span = row["high"] - row["low"]
        strength = (row["close"] - row["low"]) / span * 100 if span > 0 else 0
        ratio = row["volume"] / baseline if baseline else 0
        if gap >= 10 and ratio >= 3 and row["close"] > sma200 and strength >= 50 and i - last_trigger > 1:
            gap_event = {"date": row["date"], "index": i, "gap_pct": gap, "volume": row["volume"],
                         "volume_ratio": ratio, "volume_baseline": baseline, "close_strength_pct": strength,
                         "open": row["open"], "high": row["high"], "low": row["low"], "close": row["close"], "sma200": sma200}
            last_trigger = i

    # Daily volume grouped into calendar weeks. At a daily event, only weeks
    # ending before that session may guard the HVE record.
    weekly: dict[tuple[int, int], float] = defaultdict(float)
    for bar in bars:
        iso = bar["date"].isocalendar()
        weekly[(iso.year, iso.week)] += bar["volume"]
    week_keys = sorted(weekly)
    prior_week_max: dict[tuple[int, int], float] = {}
    for index, key in enumerate(week_keys):
        prior_week_max[key] = max((weekly[previous] for previous in week_keys[max(0, index - 250):index]), default=0.0)
    hve_events, running_max = [], 0.0
    for row in bars:
        iso = row["date"].isocalendar()
        prior_max = prior_week_max.get((iso.year, iso.week), 0.0)
        if row["volume"] > running_max and row["volume"] > prior_max:
            hve_events.append({"date": row["date"], "volume": row["volume"], "high": row["high"]})
            running_max = row["volume"]
    window = bars[-252:]
    max_vol = max((r["volume"] for r in window), default=-1)
    # Pine highestbars selects the most recent tied maximum (offset 0 first).
    hvy = next(r for r in reversed(window) if r["volume"] == max_vol) if window else None
    hve = hve_events[-1] if hve_events else None
    age_days = lambda event: (cutoff - event["date"]).days if event else None
    recent_hve = hve if hve and age_days(hve) <= 30 and "hve" not in component_issues else None
    recent_hvy = {"date": hvy["date"], "volume": hvy["volume"], "high": hvy["high"]} if hvy and age_days({"date": hvy["date"]}) <= 30 and "hvy" not in component_issues else None
    if gap_event:
        gap_event["age_sessions"] = len(bars) - 1 - gap_event["index"]
    active_gap = gap_event if gap_event and gap_event["age_sessions"] <= 60 and "gap" not in component_issues else None
    signals = []
    if active_gap: signals.append("gap")
    if recent_hve: signals.append("hve")
    if recent_hvy: signals.append("hvy")
    if not signals and component_issues:
        coverage["issues"].extend(component_issues.values())
    current = bars[-1]
    event = active_gap or gap_event
    context = {
        "rules_version": RULES_VERSION, "as_of_date": cutoff.isoformat(), "active_signals": signals,
        "coverage": coverage,
        "gap_event": ({**event, "date": event["date"].isoformat(), "age_trading_sessions": event["age_sessions"]} if event else None),
        "volume_events": {
            "hve": ({**recent_hve, "date": recent_hve["date"].isoformat(), "age_calendar_days": age_days(recent_hve), "algorithm": "daily_running_record_and_prior_250_completed_weekly_totals"} if recent_hve else None),
            "hvy": ({**recent_hvy, "date": recent_hvy["date"].isoformat(), "age_calendar_days": age_days(recent_hvy), "algorithm": "max_volume_trailing_252_daily_bars"} if recent_hvy else None),
        },
        "current_context": {"latest_close": current["close"], "gap_day_low_breached": bool(event and len(bars) - 1 > event["index"] and min(r["low"] for r in bars[event["index"] + 1:]) < event["low"]),
                            "distance_from_gap_low_pct": ((current["close"] / event["low"] - 1) * 100 if event else None)},
        "earnings_context": None,
    }
    return {"ticker": ticker.upper(), "signals": signals, "coverage": coverage, "power_gap_volume_context": context}


def detect_many(ticker_bars: dict[str, Iterable[dict[str, Any]]], as_of_date: dt.date | str,
                expected_sessions: Iterable[dt.date | str] | None = None) -> list[dict[str, Any]]:
    """Isolate input failures per ticker so one bad history cannot stop a batch."""
    results = []
    expected = list(expected_sessions) if expected_sessions is not None else None
    for ticker, bars in ticker_bars.items():
        try:
            results.append(detect_power_gap_volume(ticker, bars, as_of_date, expected))
        except Exception as exc:
            results.append({"ticker": ticker.upper(), "signals": [], "power_gap_volume_context": None,
                            "coverage": {"issues": [f"history evaluation failed: {exc}"], "history_start": None,
                                         "history_end": None, "daily_bars": 0, "hvy_complete": False,
                            "hvy_history": "unknown", "listing_status": "unknown_without_listing_metadata",
                            "available_history_short": False,
                                         "latest_data_stale": None}})
    return results


def enrich_shortlisted_gaps(rows: list[dict[str, Any]], config: Any, as_of_date: dt.date | str, *, provider: str | None = None) -> list[dict[str, Any]]:
    """Enrich only distinct ticker/event-date pairs, preserving identity per row."""
    from .earnings_enrichment import build_earnings_event_contexts

    cutoff = _date(as_of_date)
    refs = []
    for row in rows:
        context = row.get("power_gap_volume_context") or row.get("context") or {}
        gap = context.get("gap_event")
        if "gap" in context.get("active_signals", []) and gap and gap.get("date"):
            ref = {"ticker": str(row.get("ticker", "")).upper(), "signal_date": str(gap["date"])[:10]}
            refs.append(ref)
    unique = list({(r["ticker"], r["signal_date"]): r for r in refs}.values())
    enriched = build_earnings_event_contexts(unique, config, as_of_date=cutoff, provider=provider) if unique else []
    by_key = {(r["ticker"], r["signal_date"]): r.get("earnings_context") for r in enriched}
    out = []
    for row in rows:
        clone = dict(row)
        context = dict(clone.get("power_gap_volume_context") or clone.get("context") or {})
        gap = context.get("gap_event")
        key = (str(clone.get("ticker", "")).upper(), str(gap.get("date", ""))[:10]) if "gap" in context.get("active_signals", []) and gap else None
        if key and key in by_key:
            context["earnings_context"] = by_key[key]
        clone["power_gap_volume_context"] = context
        out.append(clone)
    return out
