"""RS Leader 30-minute pivot entry rules for the isolated v1 paper book."""
from __future__ import annotations

import math
import json
import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any

POLICY_ID = "rs_leader_30m_pivot_v1"
EXECUTION_VERSION = "rs_leader_30m_pivot_execution_v1"
MANAGEMENT_VERSION = "rs_leader_30m_pivot_management_v1"
STOP_BUFFER_PCT = 0.001


def _valid_bar(bar: dict[str, Any]) -> bool:
    try:
        values = {key: float(bar[key]) for key in ("open", "high", "low", "close", "volume")}
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    return (all(math.isfinite(value) for value in values.values()) and
            min(values[key] for key in ("open", "high", "low", "close")) > 0 and
            values["volume"] >= 0 and
            values["low"] <= min(values["open"], values["close"]) <=
            max(values["open"], values["close"]) <= values["high"])


def _json_safe(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def support_decline_candidate(frame: Any, discovery_date: Any,
                              saved_support: float | None = None) -> dict[str, Any]:
    """Daily EMA21 or weekly EMA8 support with an inferred 3-12% decline.

    A persisted gap/base level may join the support set when the source scan saved one.
    Distance and pullback thresholds are experimental adaptations.
    """
    import pandas as pd
    day = pd.Timestamp(discovery_date).date()
    if frame is None or getattr(frame, "empty", True):
        return {"status": "blocked_data", "reason": "daily_history_missing"}
    history = frame.loc[frame.index.date <= day].tail(260)
    if len(history) < 40:
        return {"status": "blocked_data", "reason": "daily_or_weekly_support_history_missing"}
    try:
        closes = [float(x) for x in history["Close"]]
        highs = [float(x) for x in history["High"]]
        if not all(math.isfinite(x) and x > 0 for x in closes + highs):
            raise ValueError
        close, peak = closes[-1], max(highs[-5:])
        daily_ema21 = float(pd.Series(closes).ewm(span=21, adjust=False).mean().iloc[-1])
        weekly = history[["Open", "High", "Low", "Close", "Volume"]].resample("W-FRI").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna()
        if len(weekly) < 8:
            return {"status": "blocked_data", "reason": "8_week_history_missing"}
        weekly_ema8 = float(weekly["Close"].ewm(span=8, adjust=False).mean().iloc[-1])
        supports = [("daily_ema21", daily_ema21), ("weekly_ema8", weekly_ema8)]
        if saved_support is not None and math.isfinite(float(saved_support)) and float(saved_support) > 0:
            supports.append(("saved_gap_or_base", float(saved_support)))
        in_zone = [(name, level) for name, level in supports if 0 <= (close - level) / level <= 0.02]
    except (KeyError, TypeError, ValueError, OverflowError):
        return {"status": "blocked_data", "reason": "daily_history_invalid"}
    pullback = (peak - close) / peak
    support = min(in_zone, key=lambda item: (close - item[1]) / item[1]) if in_zone else None
    if not 0.03 <= pullback <= 0.12:
        return {"status": "rejected", "reason": "pullback_outside_inferred_3_to_12_pct"}
    if support is None:
        return {"status": "rejected", "reason": "no_daily_ema21_weekly_ema8_or_saved_gap_base_support"}
    support_distance = (close - support[1]) / support[1]
    return {"status": "eligible", "support": support[1], "support_type": support[0], "five_session_high": peak,
            "pullback_pct": pullback * 100, "support_distance_pct": support_distance * 100,
            "thresholds_inferred": True, "candidate_date": day.isoformat(),
            "earliest_trade_date": "next_XNYS_session"}


def evaluate_pivot_sequence(bars: list[dict[str, Any]]) -> dict[str, Any]:
    """Use the first green 30m bar after a red bar as pivot; later bars may break its high.

    Same-bar break/stop ambiguity is conservatively recorded and not filled.
    """
    if not bars or any(not _valid_bar(bar) for bar in bars):
        return {"status": "blocked_data", "reason": "intraday_bars_missing_or_invalid"}
    pivot_index = next((i for i, b in enumerate(bars)
                        if i > 0 and float(b["close"]) > float(b["open"]) and
                        float(bars[i - 1]["close"]) < float(bars[i - 1]["open"])), None)
    if pivot_index is None:
        return {"status": "expired", "reason": "no_green_pivot_after_red_decline"}
    pivot = bars[pivot_index]
    entry, pivot_low = float(pivot["high"]), float(pivot["low"])
    stop = pivot_low * (1 - STOP_BUFFER_PCT)
    fill_index, fill_price = None, None
    for index in range(pivot_index + 1, len(bars)):
        bar = bars[index]
        if fill_index is not None:
            if float(bar["open"]) <= stop or float(bar["low"]) <= stop:
                return {"status": "filled_stopped", "reason": "stop_after_pivot_break",
                        "pivot_index": pivot_index, "bar_index": fill_index,
                        "stop_index": index, "entry": fill_price, "stop": stop,
                        "exit_price": (float(bar["open"]) if float(bar["open"]) <= stop else stop) * 0.999}
            continue
        opening = float(bar["open"])
        if opening <= stop:
            return {"status": "cancelled", "reason": "opened_at_or_below_pivot_stop",
                    "pivot_index": pivot_index, "bar_index": index, "entry": entry, "stop": stop}
        if opening >= entry:
            fill_index, fill_price = index, opening
            if float(bar["low"]) <= stop:
                return {"status": "filled_stopped", "reason": "opening_break_then_stop",
                        "pivot_index": pivot_index, "bar_index": index, "stop_index": index,
                        "entry": fill_price, "stop": stop, "exit_price": stop * 0.999}
            continue
        # A later bar that spans both levels cannot prove their order.
        if float(bar["high"]) >= entry and float(bar["low"]) <= stop:
            return {"status": "ambiguous", "reason": "same_bar_entry_stop_order_unknown",
                    "pivot_index": pivot_index, "bar_index": index, "entry": entry, "stop": stop}
        if float(bar["low"]) <= stop:
            return {"status": "cancelled", "reason": "stop_before_pivot_break",
                    "pivot_index": pivot_index, "bar_index": index, "entry": entry, "stop": stop}
        if float(bar["high"]) >= entry:
            fill_index, fill_price = index, entry
    if fill_index is not None:
        return {"status": "filled", "pivot_index": pivot_index, "bar_index": fill_index,
                "pivot_low": pivot_low, "stop_buffer_pct": STOP_BUFFER_PCT,
                "pivot_entry": entry, "entry": fill_price, "stop": stop, "risk_per_share": fill_price - stop}
    return {"status": "expired", "reason": "pivot_not_broken_by_close",
            "pivot_index": pivot_index, "pivot_low": pivot_low, "stop_buffer_pct": STOP_BUFFER_PCT,
            "entry": entry, "stop": stop}


def frozen_30m_bars(bundle: dict[str, Any], day: Any) -> list[dict[str, Any]]:
    """Validate exact XNYS 30m coverage and freeze/query provenance before evaluation."""
    import datetime as dt
    from scripts.audit_martin_intraday_execution import digest, session_starts
    session = day if isinstance(day, dt.date) else dt.date.fromisoformat(str(day)[:10])
    manifest = bundle.get("manifest") or {}
    record = bundle.get("record") or {}
    data = bundle.get("data") or {}
    if manifest.get("fingerprint") != digest({key: value for key, value in manifest.items() if key != "fingerprint"}):
        raise ValueError("frozen_30m_manifest_fingerprint_mismatch")
    if (manifest.get("schema") != "rs_leader_30m_pivot_audit_v1" or manifest.get("provider") != "TradingView" or
            manifest.get("timeframe") != "30m" or manifest.get("session") != "regular" or
            manifest.get("adjustment") != "splits"):
        raise ValueError("frozen_30m_manifest_settings_invalid")
    if record.get("timeframe") != "30m" or record.get("session") != "regular" or record.get("adjustment") != "splits":
        raise ValueError("frozen_30m_manifest_settings_invalid")
    expected = session_starts(session.isoformat(), 30)
    query, metadata = data.get("query") or {}, data.get("metadata") or {}
    if (query.get("session") != "regular" or query.get("adjustment") != "splits" or
            query.get("timezone") != "America/New_York" or str(query.get("timeframe")) != "30" or
            metadata.get("subsession_id") != "regular" or metadata.get("timezone") != "America/New_York" or
            metadata.get("currency_code") != "USD"):
        raise ValueError("frozen_30m_query_settings_invalid")
    requested = data.get("resolved_symbol") or data.get("requested_symbol") or query.get("symbol")
    actual = metadata.get("full_name") or metadata.get("pro_name")
    if not requested or actual != requested:
        raise ValueError("frozen_30m_symbol_mismatch")
    if str(actual).rsplit(":", 1)[-1].upper() != str(record.get("ticker") or "").upper():
        raise ValueError("frozen_30m_ticker_mismatch")
    if record.get("resolved_symbol") and record["resolved_symbol"] != actual:
        raise ValueError("frozen_30m_resolved_symbol_provenance_mismatch")
    return frozen_intraday_bars(data, session, 30)


def frozen_intraday_bars(data: dict[str, Any], session: Any, interval: int) -> list[dict[str, Any]]:
    import datetime as dt
    from scripts.audit_martin_intraday_execution import session_starts
    session = session if isinstance(session, dt.date) else dt.date.fromisoformat(str(session)[:10])
    query, metadata = data.get("query") or {}, data.get("metadata") or {}
    candles = data.get("candles") or []
    expected = session_starts(session.isoformat(), interval)
    by_time = {}
    for candle in candles:
        if not _valid_bar(candle):
            raise ValueError(f"frozen_{interval}m_bar_invalid")
        stamp = int(candle.get("time"))
        if stamp in by_time:
            raise ValueError(f"frozen_{interval}m_duplicate_timestamp")
        by_time[stamp] = candle
    if not expected or set(by_time) != set(expected):
        raise ValueError(f"frozen_{interval}m_coverage_incomplete")
    return [by_time[stamp] for stamp in expected]


def frozen_one_minute_bars(data: dict[str, Any], session: Any, expected_symbol: str | None = None) -> list[dict[str, Any]]:
    query, metadata = data.get("query") or {}, data.get("metadata") or {}
    if (query.get("session") != "regular" or query.get("adjustment") != "splits" or
            query.get("timezone") != "America/New_York" or str(query.get("timeframe")) != "1" or
            metadata.get("subsession_id") != "regular" or metadata.get("timezone") != "America/New_York" or
            metadata.get("currency_code") != "USD"):
        raise ValueError("frozen_1m_query_settings_invalid")
    requested = data.get("resolved_symbol") or data.get("requested_symbol") or query.get("symbol")
    actual = metadata.get("full_name") or metadata.get("pro_name")
    if not requested or actual != requested or (expected_symbol and actual != expected_symbol):
        raise ValueError("frozen_1m_symbol_mismatch")
    return frozen_intraday_bars(data, session, 1)


def refine_pivot_ambiguity(bars_30m: list[dict[str, Any]], bars_1m: list[dict[str, Any]],
                           ambiguous: dict[str, Any]) -> dict[str, Any]:
    """Resolve a 30m both-level touch only when the intervening 1m order is provable."""
    index, pivot = int(ambiguous["bar_index"]), int(ambiguous["pivot_index"])
    entry, stop = float(ambiguous["entry"]), float(ambiguous["stop"])
    start = int(bars_30m[index]["time"])
    fine = [bar for bar in bars_1m if start <= int(bar["time"]) < start + 30 * 60]
    if len(fine) != 30 or [int(bar["time"]) for bar in fine] != list(range(start, start + 30 * 60, 60)):
        return ambiguous
    filled = False
    fill_price = None
    for bar in fine:
        opening, high, low = float(bar["open"]), float(bar["high"]), float(bar["low"])
        if not filled:
            if opening <= stop:
                return {"status": "cancelled", "reason": "one_minute_stop_before_entry",
                        "pivot_index": pivot, "bar_index": index, "entry": entry, "stop": stop}
            if opening >= entry:
                filled, fill_price = True, opening
                if low <= stop:
                    return {"status": "filled_stopped", "reason": "one_minute_open_break_then_stop",
                            "pivot_index": pivot, "bar_index": index, "entry": fill_price, "stop": stop,
                            "exit_price": stop * 0.999}
                continue
            if high >= entry and low <= stop:
                return ambiguous
            if low <= stop:
                return {"status": "cancelled", "reason": "one_minute_stop_before_entry",
                        "pivot_index": pivot, "bar_index": index, "entry": entry, "stop": stop}
            if high >= entry:
                filled, fill_price = True, entry
        elif opening <= stop or low <= stop:
            return {"status": "filled_stopped", "reason": "one_minute_stop_after_entry",
                    "pivot_index": pivot, "bar_index": index, "entry": fill_price, "stop": stop,
                    "exit_price": (opening if opening <= stop else stop) * 0.999}
    if filled:
        return {"status": "filled", "reason": "one_minute_entry_order_resolved",
                "pivot_index": pivot, "bar_index": index, "entry": fill_price, "stop": stop,
                "risk_per_share": fill_price - stop, "refined_with": "1m"}
    return ambiguous


def load_frozen_30m_directory(directory: str | Path, day: Any, book_id: str | None = None,
                             simulate_backdated_buys: bool | None = None) -> dict[str, dict[str, Any]]:
    """Read only manifest-listed bundles and reject tampered, missing or wrong-timeframe evidence."""
    root = Path(directory)
    manifest_path = root / "audit.json"
    document = json.loads(manifest_path.read_text())
    supplied = document.pop("fingerprint", None)
    from scripts.audit_martin_intraday_execution import digest
    if not supplied or digest(document) != supplied:
        raise ValueError("frozen_30m_manifest_fingerprint_mismatch")
    if book_id and document.get("book_id") != book_id:
        raise ValueError("frozen_30m_book_id_mismatch")
    if simulate_backdated_buys is not None and bool(document.get("simulate_backdated_buys", False)) != simulate_backdated_buys:
        raise ValueError("frozen_30m_backdated_mode_mismatch")
    result = {}
    for item in document.get("records", []):
        if str(item.get("date"))[:10] != str(day)[:10]:
            continue
        ticker = str(item.get("ticker") or "").upper()
        filename = item.get("candle_file")
        if not ticker or ticker in result:
            raise ValueError("frozen_30m_bundle_missing_or_duplicate")
        if not filename:
            result[ticker] = {"freeze_error": item.get("classification") or "provider_unavailable",
                              "record": item, "manifest": {**document, "fingerprint": supplied}}
            continue
        if Path(filename).name != filename or not filename.endswith(".json"):
            raise ValueError("frozen_30m_bundle_path_invalid")
        data = json.loads((root / filename).read_text())
        if digest(data) != item.get("candle_fingerprint"):
            raise ValueError(f"frozen_30m_bundle_fingerprint_mismatch:{ticker}")
        bundle = {"manifest": {**document, "fingerprint": supplied}, "record": item, "data": data}
        frozen_30m_bars(bundle, day)
        result[ticker] = bundle
    return result


def management_metadata() -> dict[str, Any]:
    return {"execution_management_version": MANAGEMENT_VERSION, "trim_fraction": "1/3",
            "trim_share_rounding": "floor", "trim_target_r": 3,
            "additional_partial_trims": False,
            "stop_under_pivot_low_buffer_pct": STOP_BUFFER_PCT,
            "runner_management": "existing_stop_and_ema9_logic"}


def candidates_from_saved_run(source: dict[str, Any], frames: dict[str, Any], day: Any,
                              simulate_backdated_buys: bool = False) -> list[dict[str, Any]]:
    """Build candidates directly from saved watchlist hits, without Bar Retest labels."""
    import datetime as dt
    session = day if isinstance(day, dt.date) else dt.date.fromisoformat(str(day)[:10])
    if (source.get("strategy_id") != "martin_bar_watchlist_v1" or
            source.get("rules_version") != "martin_bar_watchlist_v1" or
            str(source.get("target_trading_date") or source.get("run_date"))[:10] != session.isoformat()):
        raise ValueError("Saved Martin Bar Watchlist source provenance is invalid.")
    result = []
    for hit in source.get("hits") or []:
        ticker = str(hit.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        categories = set(hit.get("discovery_categories") or [])
        if not categories.intersection({"leader_30_session", "potent_mover", "gap_mover"}):
            continue
        saved = (hit.get("prior_gap_low") or hit.get("base_support") or hit.get("support_level") or
                 (hit.get("low") if "gap_mover" in categories else None))
        try:
            saved = float(saved) if saved is not None else None
        except (TypeError, ValueError, OverflowError):
            saved = None
        setup = support_decline_candidate(frames.get(ticker), session, saved_support=saved)
        result.append({"ticker": ticker, "status": "pending" if setup["status"] == "eligible" else setup["status"],
                       "reasons": [setup.get("reason")] if setup.get("reason") else [],
                       "setup": setup, "event_category": "rs_leader_30m_pivot",
                       "discovery_categories": sorted(categories), "event_date": session.isoformat(),
                       "source_run_id": source.get("source_run_id"), "source_published_at": source.get("created_at"),
                       "source_fingerprint": source.get("source_fingerprint"),
                       "earliest_trade_date": "next_XNYS_session",
                       "retrospective_source_scan": (source.get("replay_provenance") or {}).get("retrospective") is True,
                       "backdated_buy_simulation": bool(simulate_backdated_buys and
                           (source.get("replay_provenance") or {}).get("retrospective") is True),
                       "fill_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and
                           (source.get("replay_provenance") or {}).get("retrospective") is True else "forward_safe"})
        if (source.get("replay_provenance") or {}).get("retrospective") is True and result[-1]["status"] == "pending":
            if not simulate_backdated_buys:
                result[-1]["status"] = "blocked_source"
                result[-1]["reasons"] = ["saved_scan_published_after_next_session_open"]
    return result


def execute_pivot_range(connection: Any, *, sessions: list[Any], sources: dict[Any, dict[str, Any]],
                        book_id: str, database_url: str, intraday_dir: str | Path,
                        simulate_backdated_buys: bool = False) -> list[dict[str, Any]]:
    """Run a complete fresh-book replay; caller owns the transaction and rollback/commit."""
    import datetime as dt
    import hashlib
    import json
    from src.market_data_access import load_many_ticker_windows
    from src.ticker_filters import is_leveraged_or_inverse_ticker, leveraged_or_inverse_provenance
    from src.webapp.services.martin_bar_retest_execution import (bar_for, manage_position, MANAGEMENT_V2, MAX_POSITIONS)
    from src.webapp.services.martin_rotation import require_completed_session, size_order
    from src.webapp.services.martin_rs_replay import ReplayRepository
    from scripts.replay_martin_bar_retest_range import continuation_mark
    from scripts.audit_martin_intraday_execution import digest

    repo = ReplayRepository(connection)
    exclusion = leveraged_or_inverse_provenance()
    results = []
    for day in sessions:
        require_completed_session(day)
        source = sources.get(day)
        if source is None:
            raise ValueError(f"Missing persisted source run for {day}; replay is all-or-nothing.")
        active_orders = repo.pending_candidates(before=day, strategy_id=book_id)
        positions = repo.open_positions(strategy_id=book_id)
        candidate_tickers = [str(x.get("ticker") or "").upper() for x in source.get("hits", [])]
        tickers = sorted({str(x["ticker"]).upper() for x in active_orders + positions} | set(candidate_tickers))
        frames = load_many_ticker_windows(tickers, day, 320, database_url=database_url) if tickers else {}
        new_candidates = candidates_from_saved_run(source, frames, day,
            simulate_backdated_buys=simulate_backdated_buys)
        for rank, candidate in enumerate(new_candidates, 1):
            candidate["rank"] = rank
            candidate["setup"]["daily_bar_provenance"] = _json_safe(repo.daily_bar_replay_provenance(
                candidate["ticker"], sessions[0], day))
        fingerprint = hashlib.sha256(json.dumps({"source": source.get("source_fingerprint"), "date": day.isoformat(),
            "policy": POLICY_ID, "candidates": new_candidates}, sort_keys=True, default=str).encode()).hexdigest()
        summary = {"strategy_id": POLICY_ID, "book_id": book_id, "rule_version": POLICY_ID,
            "execution_book_version": EXECUTION_VERSION, "session_date": day.isoformat(),
            "source_run_id": source.get("source_run_id"), "source_fingerprint": source.get("source_fingerprint"),
            "source_mode": (source.get("replay_provenance") or {}).get("source_mode"),
            "replay_provenance": source.get("replay_provenance"),
            "simulate_backdated_buys": simulate_backdated_buys,
            "backdated_buy_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and
                (source.get("replay_provenance") or {}).get("retrospective") else "forward_safe",
            "retrospective_source_scan": bool((source.get("replay_provenance") or {}).get("retrospective")),
            "fund_exclusions": {
                "version": exclusion["version"], "sha256": exclusion["sha256"]},
            "setup_rule": "daily_ema21_or_weekly_ema8_or_saved_gap_base; 3-12pct pullback; within 2pct above support",
            "thresholds_inferred": True}
        run_id = repo.save_session(session_date=day, source_run_id=int(source.get("source_run_id") or 0),
            fingerprint=fingerprint, candidates=new_candidates, summary=summary, strategy_id=book_id)
        actions = {"filled": 0, "cancelled": 0, "expired": 0, "blocked_data": 0,
                   "ambiguous": 0, "trimmed": 0, "exited": 0}
        try:
            frozen = load_frozen_30m_directory(intraday_dir, day, book_id=book_id,
                simulate_backdated_buys=simulate_backdated_buys)
            frozen_error = None
        except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            frozen, frozen_error = {}, str(exc)
        for order in active_orders:
            ticker = str(order["ticker"]).upper()
            if is_leveraged_or_inverse_ticker(ticker):
                result = {"status": "cancelled", "reason": "leveraged_or_inverse_fund"}
            else:
                bundle = frozen.get(ticker)
                try:
                    if bundle is None:
                        raise ValueError(frozen_error or "frozen_30m_bundle_missing")
                    if bundle.get("freeze_error"):
                        raise ValueError(str(bundle["freeze_error"]))
                    source_payload = order.get("payload_json") or {}
                    record = bundle.get("record") or {}
                    if (str(record.get("source_run_id")) != str(source_payload.get("source_run_id")) or
                            record.get("source_fingerprint") != source_payload.get("source_fingerprint")):
                        raise ValueError("frozen_30m_source_provenance_mismatch")
                    bars = frozen_30m_bars(bundle, day)
                    result = evaluate_pivot_sequence(bars)
                    fine = (bundle.get("data") or {}).get("one_minute")
                    if result.get("status") == "ambiguous" and fine:
                        try:
                            metadata = (bundle.get("data") or {}).get("metadata") or {}
                            expected_symbol = metadata.get("full_name") or metadata.get("pro_name")
                            result = refine_pivot_ambiguity(bars, frozen_one_minute_bars(fine, day, expected_symbol), result)
                        except (ValueError, KeyError, TypeError, OverflowError):
                            result["one_minute_refinement"] = "unavailable_or_invalid"
                    result["intraday_provenance"] = {
                        "manifest_fingerprint": (bundle.get("manifest") or {}).get("fingerprint"),
                        "candle_fingerprint": record.get("candle_fingerprint"),
                        "source_file": record.get("candle_file"), "source_date": record.get("date"),
                        "timeframe": "30m", "session": "regular", "adjustment": "splits",
                        "requested_symbol": record.get("requested_symbol"),
                        "resolved_symbol": record.get("resolved_symbol"),
                        "exchange_lookup": record.get("exchange_lookup"),
                        "one_minute_fingerprint": digest(fine) if fine else None}
                except (ValueError, OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
                    result = {"status": "blocked_data", "reason": str(exc)}
            status = result["status"]
            if status not in {"filled", "filled_stopped"}:
                event_type = "pivot_entry_" + status
                repo.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type=event_type,
                    event_date=day, payload=result, candidate_id=int(order["id"]), candidate_status=status,
                    strategy_id=book_id)
                actions[status] = actions.get(status, 0) + 1
                continue
            payload = order.get("payload_json") or {}
            plan = {**payload, "max_entry": result["entry"], "limit_price": result["entry"], "stop": result["stop"]}
            if len(repo.open_positions(strategy_id=book_id)) >= MAX_POSITIONS:
                repo.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type="pivot_entry_blocked",
                    event_date=day, payload={**result, "reason": "eight_open_positions_limit"},
                    candidate_id=int(order["id"]), candidate_status="blocked_capacity", strategy_id=book_id)
                actions["blocked_capacity"] = actions.get("blocked_capacity", 0) + 1
                continue
            budget = repo.account_state(book_id)
            shares = size_order(plan, budget)
            if not shares:
                repo.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type="pivot_entry_blocked",
                    event_date=day, payload={**result, "reason": "account_capacity_or_risk_limit"},
                    candidate_id=int(order["id"]), candidate_status="blocked_capacity", strategy_id=book_id)
                actions["blocked_capacity"] = actions.get("blocked_capacity", 0) + 1
                continue
            position = {**plan, "status": "open", "entry_price": result["entry"],
                "initial_stop": result["stop"], "original_stop": result["stop"],
                "initial_r_per_share": result["entry"] - result["stop"],
                "initial_r": result["entry"] - result["stop"], "shares": shares,
                "initial_shares": shares, "remaining_shares": shares, "realized_pnl": 0.0,
                "realized_r": 0.0, "holding_sessions": 0, "trimmed": False,
                "opened_on": day.isoformat(), "execution_status": status,
                "intraday_execution": result, "execution_management_version": MANAGEMENT_VERSION}
            position_id = repo.fill_order(candidate_id=int(order["id"]), run_id=int(order["run_id"]),
                ticker=ticker, event_date=day, position_payload=position,
                event_payload={**result, "shares": shares, **management_metadata()}, strategy_id=book_id)
            if position_id is None:
                continue
            actions["filled"] += 1
            if status == "filled_stopped":
                exit_price = result.get("exit_price") or result["stop"] * 0.999
                pnl = (exit_price - result["entry"]) * shares
                closed = {**position, "status": "closed", "remaining_shares": 0,
                    "closed_on": day.isoformat(), "exit_price": exit_price, "exit_type": "stop",
                    "realized_pnl": pnl, "realized_r": pnl / ((result["entry"] - result["stop"]) * shares)}
                repo.apply_event(run_id=run_id, position_id=position_id, ticker=ticker,
                    event_type="stop", event_date=day, payload={"price": exit_price, "reason": "post_entry_30m_stop"},
                    position_payload=closed, close_position=True, strategy_id=book_id)
                actions["exited"] += 1
        # Existing daily runner management applies after entry-day pivot execution.
        for position_row in repo.open_positions(strategy_id=book_id):
            ticker = str(position_row["ticker"]).upper()
            bar = bar_for(frames.get(ticker), day)
            if bar is None or position_row.get("opened_on") == day:
                continue
            outcome = manage_position(position_row.get("payload_json") or {}, bar, day,
                frames.get(ticker), management_version=MANAGEMENT_V2)
            if outcome:
                event_type, updated, event = outcome
                closed = event_type not in {"trim_3r", "trim_3r_skipped", "ema9_exit_scheduled"}
                repo.apply_event(run_id=run_id, position_id=int(position_row["id"]), ticker=ticker,
                    event_type=event_type, event_date=day, payload={**event, **management_metadata()},
                    position_payload=updated, close_position=closed, strategy_id=book_id)
                actions["trimmed" if event_type == "trim_3r" else "exited"] += int(closed or event_type == "trim_3r")
        marks = continuation_mark(repo, book_id, day, database_url)
        repo.set_run_metadata(run_id, {**summary, "execution_actions": actions,
            **management_metadata(), "partial": marks["mark_coverage"]["status"] != "complete",
            "intraday_evidence_partial": bool(actions["blocked_data"]),
            "intraday_blocked_count": actions["blocked_data"],
            "execution_account_limits": {"base_equity": 100000, "risk_per_trade": 500,
                "max_name_value": 20000, "max_committed_risk": 2500, "max_positions": 8}, **marks})
        results.append({"date": day.isoformat(), "source_run_id": source.get("source_run_id"),
            "source_hit_count": len(source.get("hits") or []), "candidate_count": len(new_candidates),
            "candidate_status_counts": {state: sum(x["status"] == state for x in new_candidates)
                for state in sorted({x["status"] for x in new_candidates})}, "execution_actions": actions})
    return results
