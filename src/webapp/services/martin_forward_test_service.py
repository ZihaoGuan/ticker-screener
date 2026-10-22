from __future__ import annotations

import math
from datetime import date
from typing import Any

from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.market_data_access import load_many_ticker_windows


STRATEGY_ID = "martin_daily_v1"
SECTOR_STRATEGY_ID = "martin_sector_book_v1"
DAILY_TOP5_STRATEGY_ID = "martin_daily_top5_v1"
ENTRY_POLICY_VERSION = "top_hits_max_entry_5pct_v1"
SECTOR_ORDER = {"leading": 0, "improving": 1, "weakening": 2, "lagging": 3, "unknown": 4}
FOCUS_SCANNERS = {"weekly_candidate_pool", "best_winners", "qullamaggie", "one_year_winners", "trend_template", "daily_rs_new_high", "rs"}


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
    if market_cap is None or market_cap < 1_000_000_000:
        reasons.append("market_cap_below_1b_or_missing")
    if close is None or close < 5:
        reasons.append("close_below_5_or_missing")
    if rs is None or not 90 <= rs <= 99:
        reasons.append("rs_outside_90_99_or_missing")
    if not labels.intersection(FOCUS_SCANNERS):
        reasons.append("no_martin_focus_scanner")
    plan = row.get("trade_plan") if isinstance(row.get("trade_plan"), dict) else {}
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
    return result


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

    def run_session(self, *, as_of_date: date, snapshot: dict[str, Any] | None, strategy_id: str = STRATEGY_ID) -> dict[str, Any]:
        """Process one exact session only; missed intermediate sessions require explicit replay support."""
        # shortcut: settles orders and positions only on this exact session, when multi-session replay is added.
        open_positions = self.repository.open_positions(strategy_id=strategy_id)
        pending = self.repository.pending_candidates(before=as_of_date, strategy_id=strategy_id)
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
        counts = {"filled": 0, "expired": 0, "cancelled": 0, "exited": 0, "trimmed": 0, "missing_bars": 0}
        run_id = self._save_snapshot(as_of_date, snapshot, board_rows, strategy_id)

        # Pending orders are one-session orders, so the target session's exact bar settles them.
        filled_positions: list[dict[str, Any]] = []
        for order in pending:
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
                position_id = self.repository.fill_order(candidate_id=order["id"], run_id=order["run_id"], ticker=order["ticker"],
                                                         event_date=event_date, position_payload=position,
                                                         event_payload={"price": fill, "shares": shares}, strategy_id=strategy_id)
                if position_id is None:
                    state, reason = "blocked", "open_position_already_exists"
                else:
                    filled_positions.append({"id": position_id, "ticker": order["ticker"], "payload_json": position})
                    counts["filled"] += 1
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
                            event_date=event_date, payload={"price": exit_price, "reason": "same_day_stop", "realized_pnl": pnl}, position_payload=closed, close_position=True, strategy_id=strategy_id)
                        counts["exited"] += 1
                    else:
                        self._schedule_ema_exit(order["ticker"], position_id, position, frame, event_date, strategy_id)
            if state != "filled":
                event_type = "order_expired" if state == "expired" else ("order_cancelled" if state == "cancelled" else "order_blocked")
                changed = self.repository.apply_event(run_id=run_id, position_id=None, ticker=order["ticker"], event_type=event_type,
                    event_date=event_date, payload={"reason": reason, "status": state}, candidate_id=order["id"], candidate_status=state, strategy_id=strategy_id)
                counts[state if state in counts else "expired"] += int(changed)

        current_position_ids = {int(item["id"]) for item in filled_positions}
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
            focus_tickers = {str(row.get("ticker") or "").upper() for row in board_rows}
            for position in self.repository.open_positions(strategy_id=strategy_id):
                payload = position.get("payload_json") or {}
                if str(position["ticker"]).upper() not in focus_tickers and not payload.get("dropped_from_focus"):
                    updated = {**payload, "dropped_from_focus": True, "dropped_from_focus_date": as_of_date.isoformat()}
                    self.repository.apply_event(run_id=run_id, position_id=int(position["id"]), ticker=position["ticker"],
                        event_type="dropped_from_focus", event_date=as_of_date,
                        payload={"reason": "not_in_complete_snapshot"}, position_payload=updated, strategy_id=strategy_id)
        mark_prices = {ticker: bar["close"] for ticker in tickers if (bar := _bar_for_date(frames.get(ticker), as_of_date)) is not None}
        finalize = getattr(self.repository, "update_run_summary", None)
        if run_id is not None and finalize:
            finalize(run_id=run_id, strategy_id=strategy_id, mark_prices=mark_prices, action_counts=counts)
        return {"strategy_id": strategy_id, "session_date": as_of_date.isoformat(), "run_id": run_id, **counts, "partial": counts["missing_bars"] > 0,
                "snapshot_available": bool(snapshot and str(snapshot.get("target_trading_date")) == as_of_date.isoformat())}

    def _save_snapshot(self, as_of_date: date, snapshot: dict[str, Any] | None, rows: list[dict[str, Any]], strategy_id: str) -> int | None:
        if not snapshot or str(snapshot.get("target_trading_date")) != as_of_date.isoformat():
            return None
        source_run_id = int(snapshot.get("_snapshot_run_id") or 0)
        fingerprint = str(snapshot.get("source_fingerprint") or "")
        if source_run_id <= 0 or not fingerprint:
            return None
        candidates = select_sector_candidates(rows) if strategy_id == SECTOR_STRATEGY_ID else [classify_candidate(row, rank) for rank, row in enumerate(rows, start=1)]
        for item in candidates:
            item["selection_date"] = as_of_date.isoformat()
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
        return self.repository.save_session(session_date=as_of_date, source_run_id=source_run_id, fingerprint=fingerprint,
                                            candidates=candidates, summary=summary, strategy_id=strategy_id)

    def _reserve_new_candidates(self, rows: list[dict[str, Any]], as_of_date: date, run_id: int,
                                frames: dict[str, Any], counts: dict[str, int], strategy_id: str) -> None:
        open_positions = self.repository.open_positions(strategy_id=strategy_id)
        pending_orders = self.repository.pending_orders(strategy_id=strategy_id)
        open_tickers = {str(row["ticker"]).upper() for row in open_positions}
        pending_tickers = {str(row["ticker"]).upper() for row in pending_orders}
        equity = 100_000.0
        risk_budget = equity * 0.005
        per_position_allocation = equity * 0.20
        allocated = sum(float((row.get("payload_json") or {}).get("entry_price") or 0) * int((row.get("payload_json") or {}).get("remaining_shares") or 0) for row in open_positions)
        allocated += sum(float((row.get("payload_json") or {}).get("limit_price") or 0) * int((row.get("payload_json") or {}).get("shares") or 0) for row in pending_orders)
        slots = (10 if strategy_id == SECTOR_STRATEGY_ID else
                 5 if strategy_id == DAILY_TOP5_STRATEGY_ID else max(0, 5 - len(open_positions) - len(pending_orders)))
        submitted_today = self.repository.submitted_order_count(session_date=as_of_date, strategy_id=strategy_id) if strategy_id == DAILY_TOP5_STRATEGY_ID else 0
        daily_orders_remaining = max(0, 5 - submitted_today) if strategy_id == DAILY_TOP5_STRATEGY_ID else None
        prior_selection = set(self.repository.daily_top5_selected_tickers(session_date=as_of_date)) if strategy_id == DAILY_TOP5_STRATEGY_ID else set()
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
            candidate = classify_candidate(row, rank)
            if candidate["status"] != "pending":
                continue
            ticker = candidate["ticker"]
            existing_status = self.repository.candidate_status(run_id, ticker)
            if existing_status != "eligible":
                continue
            candidate.update({key: value for key, value in self.repository.candidate_payload(run_id, ticker).items()
                              if key in {"selected_for_book", "selection_date", "selection_tier", "sector_quadrant", "sector_rs_ratio", "sector_momentum_value", "sector_as_of_date", "queue_rank"}})
            if not ticker or ticker in open_tickers:
                candidate.update(status="blocked_open_position", selected_for_book=False, reasons=["open_position_exists"])
                self.repository.update_candidate(run_id, ticker, candidate)
                self.repository.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type="candidate_blocked_open_position",
                    event_date=as_of_date, payload={"reason": "open_position_exists"}, candidate_id=None, strategy_id=strategy_id)
                continue
            if self.repository.has_pending_order(ticker, strategy_id=strategy_id):
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

    def _mark_order_awaiting_data(self, order: dict[str, Any], reason: str, session_date: date) -> None:
        payload = dict(order.get("payload_json") or {})
        payload["market_data_state"] = "awaiting_data"
        payload["market_data_reason"] = reason
        payload["awaiting_session_date"] = session_date.isoformat()
        payload["reasons"] = [reason]
        self.repository.update_candidate(order["run_id"], order["ticker"], payload)

    def _manage_position(self, row: dict[str, Any], bar: dict[str, Any], frame: Any, as_of_date: date,
                         run_id: int | None, counts: dict[str, int], strategy_id: str) -> None:
        position = row.get("payload_json") or {}
        ticker, position_id = str(row["ticker"]).upper(), int(row["id"])
        outcome = position_exit(position, bar)
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
                    event_date=as_of_date, payload={"price": price, "realized_pnl": pnl}, position_payload=closed, close_position=True, strategy_id=strategy_id):
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
                        event_date=as_of_date, payload={"price": outcome[1], "shares": trim_shares, "realized_pnl": pnl}, position_payload=updated, strategy_id=strategy_id):
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
                event_date=as_of_date, payload={"close": close, "ema9": ema9}, position_payload=updated, strategy_id=strategy_id)


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
