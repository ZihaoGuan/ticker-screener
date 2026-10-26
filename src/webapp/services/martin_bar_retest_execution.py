"""Paper execution rules for the isolated Martin Bar Retest account book."""
from __future__ import annotations

import datetime as dt
import math
from typing import Any

from src.ticker_filters import is_leveraged_or_inverse_ticker

from src.webapp.services.martin_forward_test_service import order_outcome, position_exit
from src.webapp.services.martin_rotation import size_order

EXECUTION_BOOK_PREFIX = "martin_range_bar_retest_"
BASE_EQUITY = 100_000.0
RISK_PER_TRADE = 500.0
MAX_NAME_VALUE = 20_000.0
MAX_COMMITTED_RISK = 2_500.0
MAX_POSITIONS = 8
MANAGEMENT_V1 = "martin_bar_retest_management_v1"
MANAGEMENT_V2 = "martin_bar_retest_management_v2"


def bar_for(frame: Any, day: dt.date) -> dict[str, float] | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    matches = frame.loc[frame.index.date == day]
    if matches.empty:
        return None
    row = matches.iloc[-1]
    try:
        bar = {key: float(row[column]) for key, column in (("open", "Open"), ("high", "High"),
                    ("low", "Low"), ("close", "Close"), ("volume", "Volume"))}
    except (TypeError, ValueError, KeyError, OverflowError):
        return None
    if (any(not math.isfinite(v) for v in bar.values()) or min(bar[k] for k in ("open", "high", "low", "close")) <= 0
            or bar["volume"] < 0 or not bar["low"] <= min(bar["open"], bar["close"]) <= max(bar["open"], bar["close"]) <= bar["high"]):
        return None
    return bar


def schedule_payload(candidate: dict[str, Any], *, day: dt.date, shares: int) -> dict[str, Any]:
    stop = float(candidate["stop"])
    limit = float(candidate["max_entry"])
    return {**candidate, "status": "pending", "order_date": day.isoformat(),
            "awaiting_session_date": None, "limit_price": limit, "stop": stop,
            "shares": shares, "risk_dollars": shares * (limit - stop),
            "reserved_value": shares * limit, "execution_status": "pending"}


def settle_order(payload: dict[str, Any], bar: dict[str, float], day: dt.date) -> dict[str, Any]:
    state, price, reason = order_outcome({**payload, "limit_price": payload.get("max_entry") or payload.get("limit_price")}, bar)
    if state != "filled":
        return {"status": state, "reason": reason, "event_date": day.isoformat()}
    stop = float(payload["stop"])
    position = {**payload, "status": "open", "entry_price": float(price), "initial_stop": stop,
                "original_stop": stop, "initial_r": float(price) - stop,
                "initial_r_per_share": float(price) - stop, "remaining_shares": int(payload["shares"]),
                "initial_shares": int(payload["shares"]), "realized_pnl": 0.0, "realized_r": 0.0,
                "holding_sessions": 0, "trimmed": False, "exit_next_open": False,
                "opened_on": day.isoformat(), "execution_status": "filled"}
    position["risk_dollars"] = max(0.0, (float(price) - stop) * int(payload["shares"]))
    position["reserved_value"] = float(price) * int(payload["shares"])
    # Daily OHLC cannot establish whether target or stop happened first. Stop wins.
    if float(bar["open"]) <= stop or float(bar["low"]) <= stop:
        exit_price = float(bar["open"]) * .999 if float(bar["open"]) <= stop else stop * .999
        pnl = (exit_price - float(price)) * int(payload["shares"])
        position.update(status="closed", remaining_shares=0, closed_on=day.isoformat(),
                        exit_price=exit_price, exit_type="gap_stop" if float(bar["open"]) <= stop else "stop",
                        realized_pnl=pnl, realized_r=pnl / (float(position["initial_r_per_share"]) * int(payload["shares"])),
                        holding_sessions=1, execution_status="filled_then_stopped")
    return {"status": "filled", "price": float(price), "position": position,
            "same_day_stop": position.get("status") == "closed", "event_date": day.isoformat()}


def settle_intraday_order(payload: dict[str, Any], evidence: dict[str, Any], day: dt.date) -> dict[str, Any]:
    price, stop, shares = float(evidence["fill_price"]), float(payload["stop"]), int(payload["shares"])
    position = {**payload, "status": "open", "entry_price": price, "initial_stop": stop,
        "original_stop": stop, "initial_r": price - stop, "initial_r_per_share": price - stop,
        "remaining_shares": shares, "initial_shares": shares, "realized_pnl": 0.0, "realized_r": 0.0,
        "holding_sessions": 0, "trimmed": False, "exit_next_open": False,
        "opened_on": day.isoformat(), "execution_status": "filled",
        "risk_dollars": max(0.0, (price - stop) * shares), "reserved_value": price * shares,
        "intraday_execution": evidence}
    stopped = evidence.get("status") == "filled_stopped"
    if stopped:
        exit_price = float(evidence.get("stop_price") or stop * .999)
        pnl = (exit_price - price) * shares
        position.update(status="closed", remaining_shares=0, closed_on=day.isoformat(),
            exit_price=exit_price, exit_type=evidence.get("stop_kind") or "stop", realized_pnl=pnl,
            realized_r=pnl / ((price - stop) * shares), holding_sessions=1,
            execution_status="filled_then_stopped")
    return {"status": "filled", "price": price, "position": position,
            "same_day_stop": stopped, "event_date": day.isoformat()}


def schedule_ema9_exit(position: dict[str, Any], bar: dict[str, float], day: dt.date,
                       frame: Any) -> tuple[dict[str, Any], dict[str, Any]] | None:
    if frame is None:
        return None
    history = frame.loc[frame.index.date <= day]
    if len(history) < 9 or position.get("exit_next_open"):
        return None
    closes = history["Close"]
    ema9 = float(closes.ewm(span=9, adjust=False).mean().iloc[-1])
    if float(bar["close"]) >= ema9:
        return None
    return ({**position, "exit_next_open": True, "ema9_breach_date": day.isoformat(),
             "last_managed_date": day.isoformat(),
             "holding_sessions": int(position.get("holding_sessions") or 0) +
                 int(position.get("last_managed_date") != day.isoformat())},
            {"close": float(bar["close"]), "ema9": ema9})


def manage_position(position: dict[str, Any], bar: dict[str, float], day: dt.date,
                    frame: Any = None, *, allow_trim: bool = True,
                    management_version: str = MANAGEMENT_V1) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    outcome = position_exit(position, bar)
    if outcome is None:
        scheduled = schedule_ema9_exit(position, bar, day, frame)
        return ("ema9_exit_scheduled", *scheduled) if scheduled else None
    kind, price = outcome
    if kind == "trim_3r":
        if allow_trim:
            shares = int(position.get("remaining_shares") or 0)
            v2_trim = management_version == MANAGEMENT_V2
            trim = shares // 3 if v2_trim else int(shares * .10)
            if trim:
                pnl = (float(price) - float(position["entry_price"])) * trim
                risk = float(position.get("initial_r_per_share") or 0) * int(position.get("initial_shares") or shares or 1)
                updated = {**position, "remaining_shares": shares - trim, "trimmed": True,
                           "realized_pnl": float(position.get("realized_pnl") or 0) + pnl,
                           "realized_r": float(position.get("realized_r") or 0) + (pnl / risk if risk else 0),
                           "trimmed_on": day.isoformat(), "last_managed_date": day.isoformat(),
                           "holding_sessions": int(position.get("holding_sessions") or 0) + int(position.get("last_managed_date") != day.isoformat())}
                event = {"price": float(price), "shares": trim, "realized_pnl": pnl}
                if v2_trim:
                    updated.update(management_version=MANAGEMENT_V2, trim_fraction="1/3", trim_share_rounding="floor")
                    event.update(trim_fraction="1/3", trim_share_rounding="floor", management_version=MANAGEMENT_V2)
                return kind, updated, event
            if v2_trim:
                updated = {**position, "trim_3r_skipped": True, "trim_3r_skipped_on": day.isoformat(),
                    "management_version": MANAGEMENT_V2, "trim_fraction": "1/3", "trim_share_rounding": "floor",
                    "last_managed_date": day.isoformat()}
                return "trim_3r_skipped", updated, {"price": float(price), "shares": 0,
                    "reason": "floor_rounding_below_one_share", "trim_fraction": "1/3",
                    "trim_share_rounding": "floor", "management_version": MANAGEMENT_V2}
        scheduled = schedule_ema9_exit(position, bar, day, frame)
        return ("ema9_exit_scheduled", *scheduled) if scheduled else None
    shares = int(position.get("remaining_shares") or 0)
    pnl = (float(price) - float(position["entry_price"])) * shares
    initial = float(position.get("initial_r_per_share") or 0) * int(position.get("initial_shares") or shares or 1)
    updated = {**position, "remaining_shares": 0, "closed_on": day.isoformat(), "exit_price": float(price),
               "exit_type": kind, "realized_pnl": float(position.get("realized_pnl") or 0) + pnl,
               "realized_r": (float(position.get("realized_pnl") or 0) + pnl) / initial if initial else 0}
    return kind, updated, {"price": float(price), "realized_pnl": pnl}


class MartinBarRetestExecutionService:
    """Apply one session to the same DB primitives as other Martin paper books."""

    def __init__(self, repository: Any, *, database_url: str = "", bar_loader=None):
        from src.market_data_access import load_many_ticker_windows
        self.repository = repository
        self.database_url = database_url
        self.bar_loader = bar_loader or load_many_ticker_windows

    def run_session(self, *, day: dt.date, run_id: int | None, candidates: list[dict[str, Any]],
                    source_on_time: bool, simulate_backdated_buys: bool = False,
                    frozen_intraday: dict[str, dict[str, Any]] | None = None) -> dict[str, int]:
        book = getattr(self, "book_id", None) or "martin_range_bar_retest_v1"
        pending = self.repository.pending_candidates(before=day, strategy_id=book)
        positions = self.repository.open_positions(strategy_id=book)
        orders = self.repository.pending_orders(strategy_id=book)
        ordered = sorted(candidates, key=lambda x: (int(x.get("rank") or 10**9), x["ticker"]))
        tickers = sorted({str(x["ticker"]).upper() for x in pending + positions + ordered})
        frames = self.bar_loader(tickers, day, 320, database_url=self.database_url) if tickers else {}
        counts = {"filled": 0, "expired": 0, "cancelled": 0, "exited": 0, "trimmed": 0,
                  "missing_bars": 0, "scheduled": 0, "intraday_fallbacks": 0}
        management_version = (MANAGEMENT_V2 if getattr(self, "execution_version", None) == "martin_bar_retest_execution_v2"
                              else MANAGEMENT_V1)
        closed_today: set[str] = set()
        for order in pending:
            ticker = str(order["ticker"]).upper()
            if is_leveraged_or_inverse_ticker(ticker):
                changed = self.repository.apply_event(run_id=run_id, position_id=None, ticker=ticker,
                    event_type="order_cancelled", event_date=day,
                    payload={"reason": "leveraged_or_inverse_fund", "status": "cancelled"},
                    candidate_id=order["id"], candidate_status="cancelled", strategy_id=book)
                counts["cancelled"] += int(changed)
                continue
            bar = bar_for(frames.get(ticker), day)
            if bar is None:
                counts["missing_bars"] += 1
                self.repository.apply_event(run_id=run_id, position_id=None, ticker=ticker,
                    event_type="order_blocked_missing_bar", event_date=day,
                    payload={"reason": "one_session_order_bar_missing"}, candidate_id=order["id"],
                    candidate_status="blocked_data", strategy_id=book)
                continue
            payload = order.get("payload_json") or {}
            intraday_evidence = None
            if getattr(self, "execution_version", "martin_bar_retest_execution_v1") == "martin_bar_retest_execution_v2":
                from src.webapp.services.martin_intraday_execution import evaluate_frozen_order
                bundle = (frozen_intraday or {}).get(ticker)
                if bundle is None or bundle.get("freeze_error"):
                    intraday_evidence = {"status": "fallback_daily",
                        "evidence_status": (bundle or {}).get("freeze_error") or "frozen_intraday_bundle_missing",
                        "execution_version": "martin_bar_retest_execution_v2",
                        "audit_fingerprint": (bundle or {}).get("audit_fingerprint"),
                        "source_date": (bundle or {}).get("source_date")}
                else:
                    intraday_evidence = evaluate_frozen_order(bundle, day=day,
                        requested_symbol=str(bundle.get("requested_symbol") or f"NASDAQ:{ticker}"),
                        entry=float(payload.get("limit_price") or payload.get("max_entry")),
                        stop=float(payload["stop"]), daily=bar)
                    if intraday_evidence["status"] == "blocked_data":
                        intraday_evidence["evidence_status"] = intraday_evidence.get("reason") or "frozen_intraday_invalid"
                        intraday_evidence["status"] = "fallback_daily"
                if intraday_evidence["status"] == "fallback_daily":
                    counts["intraday_fallbacks"] += 1
            if intraday_evidence and intraday_evidence["status"] in {"filled", "filled_stopped"}:
                outcome = settle_intraday_order(payload, intraday_evidence, day)
            else:
                outcome = settle_order(payload, bar, day)
            state = outcome["status"]
            if state != "filled":
                changed = self.repository.apply_event(run_id=run_id, position_id=None, ticker=ticker,
                    event_type="order_" + state, event_date=day,
                    payload={"reason": outcome.get("reason"), "status": state,
                        **({"intraday_execution": intraday_evidence} if intraday_evidence else {})},
                    candidate_id=order["id"], candidate_status=state, strategy_id=book)
                counts[state] += int(changed)
                continue
            position = outcome["position"]
            position_id = self.repository.fill_order(candidate_id=order["id"], run_id=order["run_id"], ticker=ticker,
                event_date=day, position_payload=position, event_payload={"price": outcome["price"],
                "shares": position["shares"], "same_day_stop": outcome["same_day_stop"],
                **({"intraday_execution": intraday_evidence} if intraday_evidence else {}),
                **({"management_version": MANAGEMENT_V2, "trim_fraction": "1/3", "trim_share_rounding": "floor"}
                   if management_version == MANAGEMENT_V2 else {}),
                **({"backdated_simulation": True, "source_reconstructed_later": True,
                   "fill_basis": "hypothetical_backdated_simulation"}
                  if position.get("backdated_simulation") else {})}, strategy_id=book)
            if position_id is None:
                continue
            counts["filled"] += 1
            if outcome["same_day_stop"]:
                kind = position["exit_type"]
                self.repository.apply_event(run_id=run_id, position_id=position_id, ticker=ticker, event_type=kind,
                    event_date=day, payload={"price": position["exit_price"], "reason": "same_day_stop",
                    "realized_pnl": position["realized_pnl"],
                    **({"intraday_execution": intraday_evidence} if intraday_evidence else {}),
                    **({"management_version": MANAGEMENT_V2} if management_version == MANAGEMENT_V2 else {})},
                    position_payload=position, close_position=True, strategy_id=book)
                counts["exited"] += 1
                closed_today.add(ticker)
            else:
                scheduled = manage_position(position, bar, day, frames.get(ticker), allow_trim=False,
                    management_version=management_version)
                if scheduled:
                    kind, updated, event = scheduled
                    if management_version == MANAGEMENT_V2:
                        updated.setdefault("management_version", MANAGEMENT_V2)
                        event.setdefault("management_version", MANAGEMENT_V2)
                    self.repository.apply_event(run_id=run_id, position_id=position_id, ticker=ticker,
                        event_type=kind, event_date=day, payload=event, position_payload=updated, strategy_id=book)
        for row in positions:
            ticker = str(row["ticker"]).upper()
            if ticker in closed_today:
                continue
            bar = bar_for(frames.get(ticker), day)
            if bar is None:
                counts["missing_bars"] += 1
                continue
            position = row.get("payload_json") or {}
            outcome = manage_position(position, bar, day, frames.get(ticker), management_version=management_version)
            if outcome:
                kind, updated, event = outcome
                if management_version == MANAGEMENT_V2:
                    updated.setdefault("management_version", MANAGEMENT_V2)
                    event.setdefault("management_version", MANAGEMENT_V2)
                closed = kind not in {"trim_3r", "trim_3r_skipped", "ema9_exit_scheduled"}
                self.repository.apply_event(run_id=run_id, position_id=int(row["id"]), ticker=ticker,
                    event_type=kind, event_date=day, payload=event, position_payload=updated,
                    close_position=closed, strategy_id=book)
                if kind == "trim_3r": counts["trimmed"] += 1
                elif kind not in {"ema9_exit_scheduled", "trim_3r_skipped"}: counts["exited"] += 1
                if closed:
                    closed_today.add(ticker)
                if kind in {"trim_3r", "trim_3r_skipped"}:
                    scheduled = schedule_ema9_exit(updated, bar, day, frames.get(ticker))
                    if scheduled:
                        exit_payload, exit_event = scheduled
                        if management_version == MANAGEMENT_V2:
                            exit_payload.setdefault("management_version", MANAGEMENT_V2)
                            exit_event.setdefault("management_version", MANAGEMENT_V2)
                        self.repository.apply_event(run_id=run_id, position_id=int(row["id"]), ticker=ticker,
                            event_type="ema9_exit_scheduled", event_date=day, payload=exit_event,
                            position_payload=exit_payload, strategy_id=book)
            elif position.get("last_managed_date") != day.isoformat():
                self.repository.set_position_payload(int(row["id"]), {**position,
                    "holding_sessions": int(position.get("holding_sessions") or 0) + 1,
                    "last_managed_date": day.isoformat()})
        open_now = self.repository.open_positions(strategy_id=book)
        orders_now = self.repository.pending_orders(strategy_id=book)
        held_names = {str(x["ticker"]).upper() for x in open_now + orders_now}
        for candidate in ordered:
            ticker = str(candidate["ticker"]).upper()
            backdated = simulate_backdated_buys and (candidate.get("retrospective_source_scan") is True
                or candidate.get("original_source_retrospective") is True)
            original_source_retrospective = candidate.get("original_source_retrospective") is True
            if (candidate.get("status") != "confirmed" or not (source_on_time or backdated)
                    or (original_source_retrospective and not backdated)
                    or ticker in held_names or run_id is None):
                continue
            try:
                limit, stop = float(candidate["max_entry"]), float(candidate["stop"])
            except (TypeError, ValueError):
                continue
            if len(open_now) + len(orders_now) >= MAX_POSITIONS:
                continue
            budget = self.repository.account_state(book)
            shares = size_order({**candidate, "limit_price": limit}, budget)
            if not shares:
                continue
            payload = schedule_payload({**candidate, **({"backdated_simulation": True,
                "source_reconstructed_later": True,
                "fill_basis": "hypothetical_backdated_simulation"} if backdated else {})}, day=day, shares=shares)
            if management_version == MANAGEMENT_V2:
                payload.update(management_version=MANAGEMENT_V2, trim_fraction="1/3", trim_share_rounding="floor")
            self.repository.update_candidate(run_id, ticker, payload)
            held_names.add(ticker)
            orders_now.append({"ticker": ticker, "payload_json": payload})
            counts["scheduled"] += 1
        return counts
