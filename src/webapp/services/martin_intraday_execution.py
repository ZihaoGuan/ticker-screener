"""Frozen regular-session intraday input validation and sequential limit execution."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from typing import Any


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def evaluate_sequence(bars: list[dict[str, Any]], *, entry: float, stop: float) -> dict[str, Any]:
    """Walk bars chronologically; after a fill, continue looking for a later stop."""
    filled = False
    fill_price = None
    fill_index = None
    for index, bar in enumerate(bars):
        opening, low = float(bar["open"]), float(bar["low"])
        if not filled:
            if opening <= stop:
                return {"status": "stop_before_entry", "fill_price": None, "stop_price": None,
                        "fill_bar_index": None, "stop_bar_index": index, "bar_index": None}
            if opening <= entry:
                filled, fill_price, fill_index = True, opening, index
                if low <= stop:
                    return {"status": "stop_after_entry", "fill_price": opening, "stop_price": stop * .999,
                            "fill_bar_index": index, "stop_bar_index": index, "bar_index": index}
            elif low <= entry:
                if low <= stop:
                    return {"status": "same_bar_ambiguous", "fill_price": entry, "stop_price": stop * .999,
                            "fill_bar_index": index, "stop_bar_index": index, "bar_index": index}
                filled, fill_price, fill_index = True, entry, index
        elif opening <= stop or low <= stop:
            return {"status": "stop_after_entry", "fill_price": fill_price,
                    "stop_price": (opening if opening <= stop else stop) * .999,
                    "fill_bar_index": fill_index, "stop_bar_index": index, "bar_index": fill_index}
    return {"status": "entry_before_stop" if filled else "no_fill", "fill_price": fill_price,
            "stop_price": None, "fill_bar_index": fill_index, "stop_bar_index": None, "bar_index": fill_index}


def evaluate_frozen_order(bundle: dict[str, Any], *, day: dt.date, requested_symbol: str,
                          entry: float, stop: float, daily: dict[str, float]) -> dict[str, Any]:
    data = bundle.get("data") if isinstance(bundle.get("data"), dict) else bundle
    candles = data.get("candles") or []
    metadata = data.get("metadata") or {}
    query = data.get("query") or {}
    actual = metadata.get("full_name") or metadata.get("pro_name")
    starts = bundle.get("expected_starts")
    if starts is None:
        from scripts.audit_martin_intraday_execution import session_starts
        starts = session_starts(day.isoformat(), int(bundle.get("interval_minutes") or 15))
    starts = [int(value) for value in starts]
    by_time = {int(row["time"]): row for row in candles}
    bars = [by_time[t] for t in starts if t in by_time]
    complete = bool(starts) and len(bars) == len(starts) and len(candles) == len(starts) and set(by_time) == set(starts)
    aggregate = (None if not candles else {"open": candles[0]["open"],
        "high": max(row["high"] for row in candles), "low": min(row["low"] for row in candles),
        "close": candles[-1]["close"]})
    mismatch = actual != requested_symbol
    alternate_feed_supported = False
    if not mismatch and aggregate:
        tolerance = max(.02, abs(float(daily["close"])) * .0005)
        mismatch = any(abs(float(aggregate[key]) - float(daily[key])) > tolerance
                       for key in ("open", "high", "low", "close"))
    if mismatch and actual and actual.startswith("BATS:") and complete and aggregate:
        tolerance = max(.02, abs(float(daily["close"])) * .0005)
        range_ok = aggregate["high"] <= float(daily["high"]) + tolerance and aggregate["low"] >= float(daily["low"]) - tolerance
        open_fill_same = (float(aggregate["open"]) <= entry) == (float(daily["open"]) <= entry)
        mismatch = not (range_ok and open_fill_same)
        alternate_feed_supported = not mismatch
    settings_ok = (query.get("session") == "regular" and query.get("adjustment") == "splits" and
        query.get("timezone") == "America/New_York" and metadata.get("subsession_id") == "regular" and
        metadata.get("timezone") == "America/New_York" and metadata.get("currency_code") == "USD" and
        str(query.get("timeframe")) == str(bundle.get("interval_minutes") or 15))
    result: dict[str, Any] = {"status": "missing_bars", "reason": None, "fill_price": None,
        "stop_price": None, "actual_feed": actual, "requested_feed": requested_symbol,
        "expected_bar_count": len(starts), "received_bar_count": len(candles), "feed_mismatch": bool(mismatch),
        "bundle_fingerprint": fingerprint(bundle), "timeframe": "15m", "session": "regular",
        "adjustment": "splits", "query_timezone": query.get("timezone"),
        "audit_fingerprint": bundle.get("audit_fingerprint"),
        "candle_fingerprint": bundle.get("candle_fingerprint"),
        "source_date": bundle.get("source_date"),
        "feed_basis": "alternate_bats_supported" if alternate_feed_supported else "requested_feed_exact" if actual == requested_symbol and not mismatch else "unverified",
        "evidence_status": "alternate_feed_supported" if alternate_feed_supported else "exact_requested_feed" if actual == requested_symbol and not mismatch else "feed_unverified",
        "evidence_tier": "guarded_alternate" if alternate_feed_supported else "primary_exact" if actual == requested_symbol and not mismatch else "unverified",
        "alternate_feed_supported": alternate_feed_supported}
    if mismatch or not settings_ok:
        result.update(status="blocked_data", reason="intraday_feed_mismatch",
                      evidence_status="feed_mismatch" if mismatch else "intraday_settings_mismatch",
                      evidence_tier="rejected")
        return result
    if not complete:
        result.update(status="blocked_data", reason="intraday_coverage_incomplete")
        return result
    for bar in bars:
        try:
            values = [float(bar[key]) for key in ("open", "high", "low", "close", "volume")]
        except (KeyError, TypeError, ValueError, OverflowError):
            values = []
        if (not values or any(not math.isfinite(value) for value in values) or
                min(values[:4]) <= 0 or values[4] < 0 or
                not values[2] <= min(values[0], values[3]) <= max(values[0], values[3]) <= values[1]):
            result.update(status="blocked_data", reason="intraday_bar_invalid")
            return result
    sequence = evaluate_sequence(bars, entry=entry, stop=stop)
    if sequence["status"] == "stop_before_entry":
        result.update(status="cancelled", reason="opened_at_or_below_stop")
    elif sequence["status"] == "same_bar_ambiguous":
        fine = data.get("one_minute")
        refined = evaluate_frozen_order({"data": fine, "expected_starts": bundle.get("expected_one_minute_starts"), "interval_minutes": 1},
            day=day, requested_symbol=requested_symbol, entry=entry, stop=stop, daily=daily) if fine else data.get("one_minute_refinement")
        result["one_minute_refinement"] = refined
        if refined and refined.get("status") in {"filled", "filled_stopped"}:
            provenance = {key: result[key] for key in ("audit_fingerprint", "candle_fingerprint", "source_date")}
            result.update(refined)
            result.update(provenance)
            result["timeframe"] = "1m_refined"
            result["bundle_fingerprint"] = fingerprint(bundle)
        elif refined and refined.get("status") == "cancelled":
            result.update(refined)
            result["timeframe"] = "1m_refined"
        elif refined and refined.get("status") == "blocked_data":
            result.update(status="blocked_data", reason="ambiguous_bar_without_valid_one_minute")
        else:
            result.update(status="filled_stopped", reason="same_bar_stop_conservative", stop_kind="stop",
                          fill_price=entry, stop_price=stop * .999, fill_bar_index=sequence["fill_bar_index"],
                          stop_bar_index=sequence["stop_bar_index"])
    elif sequence["status"] == "stop_after_entry":
        result.update(status="filled_stopped", reason="stop_after_entry",
            stop_kind="gap_stop" if float(bars[sequence["stop_bar_index"]]["open"]) <= stop else "stop",
            fill_price=sequence["fill_price"], stop_price=sequence["stop_price"],
            fill_bar_index=sequence["fill_bar_index"], stop_bar_index=sequence["stop_bar_index"])
    elif sequence["status"] in {"entry_before_stop", "opening_fill"}:
        result.update(status="filled", fill_price=sequence["fill_price"], fill_bar_index=sequence["fill_bar_index"])
    else:
        result.update(status="expired", reason="limit_not_reached")
    return result
