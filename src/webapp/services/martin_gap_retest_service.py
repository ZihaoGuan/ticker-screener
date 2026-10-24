"""Paper execution service for the independent daily gap retest book."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from zoneinfo import ZoneInfo

from src.market_data_access import load_many_ticker_windows
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_rotation import require_completed_session, session_dates, valid_history
from src.martin_daily_gap_retest import (CONFIG, CONFIG_HASH, DISCOVERY_RULES_VERSION, REPLAY_STRATEGY_ID,
    REPLAY_V2_STRATEGY_ID, REPLAY_V3_STRATEGY_ID, REPLAY_V4_STRATEGY_ID, STRATEGY_ID,
    daily_indicators, evaluate_gap_retest, rank_confirmations, resolve_gap_next_open, size_gap_order)


def _d(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _bar(frame: Any, day: date) -> dict[str, float] | None:
    if frame is None or getattr(frame, "empty", True):
        return None
    for idx, row in frame.iterrows():
        dt = idx.date() if hasattr(idx, "date") else idx
        if dt == day:
            values = {str(k).lower(): v for k, v in row.items()}
            try:
                return {name: float(values[name]) for name in ("open", "high", "low", "close", "volume")}
            except (KeyError, TypeError, ValueError):
                return None
    return None


def _asof_frame(frame: Any, day: date) -> Any:
    if frame is None or getattr(frame, "empty", True):
        return frame
    dates = [idx.date() if hasattr(idx, "date") else idx for idx in frame.index]
    clipped = frame.loc[[value <= day for value in dates]].copy()
    if hasattr(frame, "attrs"):
        clipped.attrs.update(frame.attrs)
    return clipped


_TV_SOURCES = {"tradingview", "tradingview_screener"}
_MASSIVE_SOURCES = {"massive", "massive_api"}
_YAHOO_SOURCES = {"yahoo", "yahoo_finance", "yfinance", "yfinance+nasdaq", "yfinance_historical_repair"}
_SOURCE_PRIORITY = {source: 0 for source in _TV_SOURCES} | {source: 1 for source in _MASSIVE_SOURCES} | {source: 2 for source in _YAHOO_SOURCES}


def _source_basis(frame: Any, rows: list[dict[str, Any]], start: date, end: date, *, historical_replay: bool) -> dict[str, Any] | None:
    """Certify complete per-session provenance and fail closed on unknown action history."""
    dates = [idx.date() if hasattr(idx, "date") else idx for idx in frame.index]
    if not rows or len(rows) != len(dates) or any(row.get("trade_date") is None for row in rows):
        return None
    by_date = {row["trade_date"]: row for row in rows}
    if len(by_date) != len(rows) or set(by_date) != set(dates) or min(dates) < start or max(dates) != end:
        return None
    if any(_number(row.get("split_factor")) is None or _number(row.get("split_factor")) <= 0 for row in rows):
        return None
    sources = {str(row.get("source") or "").strip().lower() for row in rows}
    if not sources or not sources.issubset(_SOURCE_PRIORITY):
        return None
    if not historical_replay and not sources.issubset(_TV_SOURCES):
        return None
    latest_split = next((row.get("latest_split_date") for row in rows if row.get("latest_split_date") is not None), None)
    history_start = next((row.get("history_first_date") for row in rows if row.get("history_first_date") is not None), None)
    unknown_action_rows = max((int(row.get("unknown_split_rows") or 0) for row in rows), default=0)
    unknown_source_rows = max((int(row.get("unknown_source_rows") or 0) for row in rows), default=0)
    if history_start is None or history_start >= min(dates) or unknown_action_rows or unknown_source_rows:
        return None
    split_day = latest_split.date() if isinstance(latest_split, datetime) else latest_split
    # Raw providers match a split-adjusted series only in the latest known share regime.
    for day, row in by_date.items():
        source = str(row.get("source") or "").strip().lower()
        if source not in _TV_SOURCES and (not historical_replay or (split_day is not None and day < split_day)):
            return None
    # Catch near-exact common split ratios at provider handoffs without rejecting ordinary large gaps.
    ordered = sorted(by_date)
    close_by_date = {}
    for idx, bar in frame.iterrows():
        bar_day = idx.date() if hasattr(idx, "date") else idx
        values = [_number(bar.get(column)) for column in ("Open", "High", "Low", "Close", "Volume")]
        if any(value is None for value in values):
            return None
        opening, high, low, close, volume = values
        if min(opening, high, low, close) <= 0 or volume < 0 or high < max(opening, close, low) or low > min(opening, close, high):
            return None
        close_by_date[bar_day] = close
    for prior_day, day in zip(ordered, ordered[1:]):
        prior_source = str(by_date[prior_day].get("source") or "").strip().lower()
        source = str(by_date[day].get("source") or "").strip().lower()
        if source != prior_source:
            prior_close, close = close_by_date[prior_day], close_by_date[day]
            ratio = close / prior_close if prior_close and close else None
            split_ratios = (0.1, 0.2, 0.25, 1 / 3, 0.5, 2 / 3, 0.8, 1.25, 1.5, 2, 3, 4, 5, 10)
            if ratio is None or any(abs(ratio / split_ratio - 1) <= 0.015 for split_ratio in split_ratios):
                return None
    ordered_sources = sorted(sources, key=lambda source: (_SOURCE_PRIORITY[source], source))
    return {"sources": ordered_sources, "latest_split_date": split_day.isoformat() if split_day else None,
            "history_first_date": str(history_start)[:10], "stored_split_factor_rows_complete": True,
            "basis_confidence": "assumption_from_complete_stored_action_rows",
            "basis_assumption": "Split factors are populated for every stored bar from the recorded history start through the replay date; absence of a split event means no split is recorded in that stored history.",
            "basis": "split_adjusted_regular_session_assumed"}


def _source_hit_payload(hit: dict[str, Any]) -> dict[str, Any]:
    for key in ("hit_payload_json", "payload_json"):
        if isinstance(hit.get(key), dict):
            return hit[key]
    return hit


def active_gap_events(source_record: dict[str, Any] | None, day: date) -> list[dict[str, Any]]:
    if not isinstance(source_record, dict) or str(source_record.get("target_trading_date") or source_record.get("run_date") or source_record.get("session_date") or "")[:10] != day.isoformat():
        return []
    run_id = source_record.get("source_run_id", source_record.get("id", source_record.get("run_id")))
    result = []
    for hit in source_record.get("hits", source_record.get("rows", [])) or []:
        payload = _source_hit_payload(hit) if isinstance(hit, dict) else {}
        context = payload.get("power_gap_volume_context")
        signals = context.get("active_signals", []) if isinstance(context, dict) else []
        if "gap" not in {str(x).lower() for x in signals}:
            continue
        rules_version = context.get("rules_version") if isinstance(context, dict) else None
        if rules_version != DISCOVERY_RULES_VERSION:
            continue
        ticker = str(payload.get("ticker") or hit.get("ticker") or "").strip().upper()
        gap = context.get("gap_event") if isinstance(context.get("gap_event"), dict) else {}
        gap_date = gap.get("date", context.get("gap_date", context.get("original_gap_date")))
        if not ticker or not gap_date:
            continue
        event = {"ticker": ticker, "original_gap_date": str(gap_date)[:10], "source_run_id": run_id,
                 "source_published_at": source_record.get("published_at", source_record.get("created_at")),
                 "discovery_rules_version": rules_version,
                 "original_gap_event": gap,
                 "gap_day_low": gap.get("low", context.get("gap_day_low")), "first_observed_date": day.isoformat(),
                 "event_identity": {"ticker": ticker, "original_gap_date": str(gap_date)[:10],
                                    "discovery_rules_version": rules_version}}
        result.append(event)
    # A source run can contain repeated rows; keep the newest event per name.
    newest = {}
    for event in result:
        if event["ticker"] not in newest or event["original_gap_date"] > newest[event["ticker"]]["original_gap_date"]:
            newest[event["ticker"]] = event
    return list(newest.values())


def _xnys_session_bounds(day: date, next_session: date) -> tuple[datetime, datetime]:
    try:
        import exchange_calendars as xcals
        calendar = xcals.get_calendar("XNYS", start=day.isoformat(), end=next_session.isoformat())
        close = calendar.session_close(day.isoformat()).to_pydatetime()
        opened = calendar.session_open(next_session.isoformat()).to_pydatetime()
        return close.astimezone(timezone.utc), opened.astimezone(timezone.utc)
    except (ImportError, ValueError, KeyError):
        # Local unit environments may omit the exchange calendar package.
        close = datetime.combine(day, time(16), ZoneInfo("America/New_York")).astimezone(timezone.utc)
        opened = datetime.combine(next_session, time(9, 30), ZoneInfo("America/New_York")).astimezone(timezone.utc)
        return close, opened


def _published_before_session(source_record: dict[str, Any] | None, day: date, next_session: date,
                              session_bounds=None) -> bool:
    value = (source_record or {}).get("published_at", (source_record or {}).get("created_at"))
    if not value:
        return False
    try:
        published = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if published.tzinfo is None:
            return False
        close, opened = session_bounds(day, next_session) if session_bounds else _xnys_session_bounds(day, next_session)
        published = published.astimezone(timezone.utc)
        return close <= published < opened and published <= datetime.now(timezone.utc)
    except (TypeError, ValueError):
        return False


def manage_position(position: dict[str, Any], bar: dict[str, Any], *, day: date, ema9: float | None = None) -> list[dict[str, Any]]:
    """Pure daily management decisions; stop precedence matches the saved policy."""
    p = dict(position)
    opening, low, high, close = (float(bar[k]) for k in ("open", "low", "high", "close"))
    stop, entry = float(p["stop"]), float(p["entry_price"])
    shares = int(p.get("remaining_shares", 0))
    if p.get("exit_next_open"):
        return [{"type": "ema9_exit", "shares": shares, "price": float(_d(Decimal(str(opening)) * Decimal(".999"))),
                 "close": True, "gap_through_stop_diagnostic": opening <= stop}]
    if opening <= stop:
        return [{"type": "gap_stop", "shares": shares, "price": float(_d(Decimal(str(opening)) * Decimal(".999"))), "close": True}]
    if low <= stop:
        return [{"type": "stop", "shares": shares, "price": float(_d(Decimal(str(stop)) * Decimal(".999"))), "close": True}]
    risk = float(p.get("initial_r_per_share") or (entry - stop))
    target = float(_d(Decimal(str(entry)) + Decimal("3") * Decimal(str(risk))))
    if day.isoformat() != str(p.get("opened_on")) and not p.get("trimmed"):
        if opening >= target:
            trim_price = float(_d(Decimal(str(opening)) * Decimal(".999")))
        elif high >= target:
            trim_price = float(_d(Decimal(str(target)) * Decimal(".999")))
        else:
            trim_price = None
        trim_shares = int(p.get("initial_shares", p.get("shares", 0)) * .10)
        if trim_price is not None:
            if trim_shares <= 0:
                p["trim_unavailable"] = True
            else:
                return [{"type": "trim_3r", "shares": min(trim_shares, shares), "price": trim_price, "close": False,
                         "schedule_exit": ema9 is not None and close < ema9}]
    if ema9 is not None and close < ema9:
        p["exit_next_open"] = True
    return [{"type": "mark", "close": close, "position": p}]


class MartinGapRetestService:
    def __init__(self, repository: MartinForwardTestRepository | None = None, *, database_url: str = "", bar_loader=None,
                 calendar=None, session_bounds=None) -> None:
        self.repository = repository or MartinForwardTestRepository(database_url=database_url)
        self.database_url = database_url
        self.bar_loader = bar_loader or load_many_ticker_windows
        self._uses_default_bar_loader = bar_loader is None
        self.calendar = calendar
        self.session_bounds = session_bounds

    def _dates(self, day: date) -> list[date]:
        if self.calendar is not None:
            return list(self.calendar(day))
        return session_dates(day, 326)

    def _next_session(self, day: date) -> date:
        values = list(self.calendar(day + timedelta(days=14))) if self.calendar is not None else session_dates(day + timedelta(days=14), 400)
        try:
            return next(value for value in values if value > day)
        except StopIteration as error:
            raise ValueError("Could not resolve next XNYS session") from error

    def _certify_frame(self, ticker: str, frame: Any, start: date, end: date, *, historical_replay: bool = False,
                       mixed_source_replay: bool = False) -> bool:
        if frame is None or getattr(frame, "empty", True):
            return False
        attrs = getattr(frame, "attrs", {})
        if attrs.get("adjustment_basis") == "split_adjusted_regular_session" and not mixed_source_replay:
            return True
        provenance = getattr(self.repository, "daily_bar_replay_provenance" if mixed_source_replay else "daily_bar_provenance", None)
        if not provenance:
            return False
        groups = provenance(ticker, start, end)
        if mixed_source_replay:
            basis = _source_basis(frame, groups or [], start, end, historical_replay=True)
            if basis is None:
                return False
            attrs["adjustment_basis"] = "split_adjusted_regular_session"
            attrs["provenance_source"] = "+".join(basis["sources"])
            attrs["data_provenance"] = basis
            return True
        sources = {str(row.get("source") or "").lower() for row in groups or []}
        allowed_sources = {"tradingview", "tradingview_screener"} if historical_replay else {"tradingview"}
        if not groups or not sources or not sources.issubset(allowed_sources):
            return False
        dates = [idx.date() if hasattr(idx, "date") else idx for idx in frame.index]
        count = sum(int(row.get("rows") or 0) for row in groups)
        if count != len(frame) or min(dates) < start or max(dates) != end:
            return False
        for row in groups:
            factor = _number(row.get("split_factor"))
            if factor is None or factor <= 0:
                return False
        # The refresh source validates TradingView's split-adjusted, regular-session query.
        attrs["adjustment_basis"] = "split_adjusted_regular_session"
        attrs["provenance_source"] = "+".join(sorted(sources))
        return True

    def run_session(self, *, as_of_date: date, source_record: dict[str, Any] | None, dry_run: bool = True,
                    strategy_id: str = STRATEGY_ID, historical_replay: bool = False) -> dict[str, Any]:
        if (historical_replay and strategy_id not in {REPLAY_STRATEGY_ID, REPLAY_V2_STRATEGY_ID, REPLAY_V3_STRATEGY_ID, REPLAY_V4_STRATEGY_ID}) or (not historical_replay and strategy_id not in {STRATEGY_ID, REPLAY_V4_STRATEGY_ID}):
            raise ValueError("Unsupported gap retest book identity")
        if strategy_id == REPLAY_V4_STRATEGY_ID and not historical_replay and self.repository.latest_session_date(strategy_id) is None:
            raise ValueError("Replay v4 must be seeded through the previous market session before daily continuation")
        dates = self._dates(as_of_date)
        if not dates or dates[-1] != as_of_date:
            raise ValueError("Daily Gap Retest requires an XNYS session")
        require_completed_session(as_of_date)
        if not dry_run:
            with self.repository.atomic_session(strategy_id):
                if self.repository.session_run_id(strategy_id, as_of_date) is not None:
                    return {"strategy_id": strategy_id, "session_date": as_of_date.isoformat(), "skipped": True,
                            "reason": "session_already_processed"}
                latest = self.repository.latest_session_date(strategy_id)
                prior = dates[-2] if len(dates) > 1 else None
                if latest is not None and latest != prior:
                    raise ValueError("Gap retest book has missed intermediate XNYS sessions; explicit sequential recovery is required")
                return self._run(as_of_date, source_record, dry_run=False, dates=dates, strategy_id=strategy_id, historical_replay=historical_replay)
        return self._run(as_of_date, source_record, dry_run=True, dates=dates, strategy_id=strategy_id, historical_replay=historical_replay)

    def _run(self, day: date, source_record: dict[str, Any] | None, *, dry_run: bool, dates: list[date],
             strategy_id: str = STRATEGY_ID, historical_replay: bool = False) -> dict[str, Any]:
        repo, sid = self.repository, strategy_id
        if not dry_run and repo.latest_session_date(sid) is not None and repo.latest_session_date(sid) >= day:
            return {"strategy_id": sid, "session_date": day.isoformat(), "skipped": True, "reason": "session_already_processed"}
        watches = repo.latest_gap_watch_states(sid) if hasattr(repo, "latest_gap_watch_states") else {}
        positions = repo.open_positions(strategy_id=sid) if hasattr(repo, "open_positions") else []
        pending = repo.pending_candidates(before=day, strategy_id=sid) if hasattr(repo, "pending_candidates") else []
        source_session_matches = bool(source_record and str(source_record.get("target_trading_date") or source_record.get("run_date") or source_record.get("session_date"))[:10] == day.isoformat())
        next_session = self._next_session(day)
        reconstructed = bool(historical_replay and source_record and source_record.get("source_mode") == "historical_reconstruction"
                             and source_record.get("rules_version") == DISCOVERY_RULES_VERSION)
        mixed_source_replay = strategy_id == REPLAY_V4_STRATEGY_ID
        source_valid = source_session_matches and (reconstructed or _published_before_session(source_record, day, next_session, self.session_bounds))
        events = active_gap_events(source_record if source_valid else None, day)
        prior_ids = {str(p.get("ticker", "")).upper(): p for p in positions}
        tickers = {str(x).upper() for x in watches} | {str(x["ticker"]).upper() for x in positions + pending} | {x["ticker"] for x in events}
        if source_valid:
            tickers.add("SPY")
        try:
            if not tickers:
                frames = {}
            elif self._uses_default_bar_loader:
                frames = self.bar_loader(sorted(tickers), day, 326, database_url=self.database_url, start_date=dates[0])
            else:
                frames = self.bar_loader(sorted(tickers), day, 326, database_url=self.database_url)
        except Exception:
            frames = {}
        prior_session = dates[-2] if len(dates) > 1 else None
        frames = {ticker: _asof_frame(frame, day) for ticker, frame in frames.items()}
        for ticker, frame in frames.items():
            self._certify_frame(ticker, frame, dates[0], day, historical_replay=historical_replay,
                                mixed_source_replay=mixed_source_replay)
        for row in positions + pending:
            ticker = str(row["ticker"]).upper()
            frame = frames.get(ticker)
            if _bar(frame, day) is None or not valid_history(frame, dates[-260:]) or not self._certify_frame(
                    ticker, frame, dates[0], day, historical_replay=historical_replay, mixed_source_replay=mixed_source_replay):
                if not dry_run:
                    raise ValueError("Missing or unverified execution history for existing gap retest order or position; session rolled back")
            if row in positions and hasattr(repo, "daily_bar_provenance"):
                opened = (row.get("opened_on") or (row.get("payload_json") or {}).get("opened_on"))
                if opened:
                    history_basis = repo.daily_bar_provenance(ticker, opened, day)
                    factors = {str(item.get("split_factor")) for item in history_basis}
                    if len(factors) > 1:
                        raise ValueError("corporate_action_requires_review; gap retest session rolled back")
        report: dict[str, Any] = {"strategy_id": sid, "session_date": day.isoformat(), "dry_run": dry_run,
            "source_run_id": ((source_record or {}).get("source_run_id", (source_record or {}).get("id"))
                              if source_valid and not historical_replay else None),
            "source_fingerprint": (source_record or {}).get("source_fingerprint") if source_valid else None,
            "discovery_complete": source_valid, "config_version": sid, "config_hash": CONFIG_HASH,
            "entry_policy_version": "gap_daily_reclaim_next_open_v1", "stop_policy_version": "gap_reclaim_candle_low_v1",
            "management_policy_version": "gap_trim10_3r_ema9_v1", "source_hit_count": len((source_record or {}).get("hits", [])),
            "active_gap_event_count": len(events), "candidate_count": 0, "confirmed_count": 0, "orders": [], "candidates": [],
            "execution_preview": [],
            "data_provenance_by_ticker": {ticker: frame.attrs["data_provenance"]
                for ticker, frame in frames.items() if getattr(frame, "attrs", {}).get("data_provenance")}}
        if historical_replay:
            report["replay_provenance"] = {"origin": "historical_replay", "source_mode": "historical_reconstruction",
                "from_date": (source_record or {}).get("replay_from_date"), "to_date": (source_record or {}).get("replay_to_date"),
                "source_generated_at": (source_record or {}).get("published_at"),
                "limitations": "Reconstructed Power Gap universe from current configured symbols and stored OHLCV; historical revisions may apply."}
        for order in pending:
            payload = order.get("payload_json") or {}
            bar = _bar(frames.get(str(order["ticker"]).upper()), day)
            preview = resolve_gap_next_open(payload, bar) if bar and payload.get("awaiting_session_date") == day.isoformat() else {
                "status": "blocked_data", "reason": "missing_execution_bar"}
            report["execution_preview"].append({"ticker": order["ticker"], "kind": "pending_order", **preview})
        for held in positions:
            ticker = str(held["ticker"]).upper()
            bar = _bar(frames.get(ticker), day)
            if bar:
                metric = daily_indicators(frames.get(ticker))
                action = manage_position(held.get("payload_json") or {}, bar, day=day, ema9=metric.get("ema9"))[0]
                report["execution_preview"].append({"ticker": ticker, "kind": "position", **action})
        # Apply the preview to a temporary budget ledger so dry-run and apply
        # allocate against the same post-session book state.
        budget_state = dict(repo.account_state(sid)) if hasattr(repo, "account_state") else {"available_cash": 100000.0, "committed_stop_risk": 0.0}
        virtual_cash = float(budget_state.get("available_cash", 100000))
        virtual_risk = float(budget_state.get("committed_stop_risk", 0))
        virtual_slots = max(0, 8 - len(positions) - len(pending))
        open_tickers = {str(p["ticker"]).upper() for p in positions}
        pending_tickers = {str(p["ticker"]).upper() for p in pending}
        filled_event_ids = set()
        for preview in report["execution_preview"]:
            if preview["kind"] == "pending_order":
                order = next((x.get("payload_json", {}) for x in pending if str(x["ticker"]).upper() == preview["ticker"]), {})
                reserve = float(order.get("reservation_cash", float(order.get("shares", 0)) * float(order.get("max_entry", order.get("limit_price", 0)))))
                risk_reserve = float(order.get("planned_stop_risk", int(order.get("shares", 0)) * (float(order.get("max_entry", 0)) - float(order.get("stop", 0)))))
                virtual_cash += reserve; virtual_risk -= risk_reserve
                pending_tickers.discard(preview["ticker"])
                if preview.get("status") == "filled":
                    filled_event_ids.add(json.dumps(order.get("event_identity"), sort_keys=True))
                    shares, fill, stop = int(order.get("shares", 0)), float(preview["fill_price"]), float(order["stop"])
                    execution_bar = _bar(frames.get(preview["ticker"]), day) or {}
                    if execution_bar and (execution_bar["open"] <= stop or execution_bar["low"] <= stop):
                        exit_price = float(_d((execution_bar["open"] if execution_bar["open"] <= stop else stop) * .999))
                        virtual_cash += shares * (exit_price - fill)
                        virtual_risk += shares * max(0, fill - stop)
                        virtual_risk -= shares * max(fill - stop, 0)
                        virtual_slots += 1
                    else:
                        virtual_cash -= shares * fill; virtual_risk += shares * max(0, fill - stop)
                        open_tickers.add(preview["ticker"])
                else:
                    pending_tickers.discard(preview["ticker"]); virtual_slots += 1
            else:
                held = next((x.get("payload_json", {}) for x in positions if str(x["ticker"]).upper() == preview["ticker"]), {})
                kind = preview.get("type")
                if kind == "mark":
                    continue
                shares, entry, stop = int(preview.get("shares", 0)), float(held.get("entry_price", 0)), float(held.get("stop", 0))
                pnl = (float(preview.get("price", entry)) - entry) * shares
                virtual_cash += entry * shares + pnl
                virtual_risk -= shares * max(entry - stop, 0)
                if preview.get("close"):
                    open_tickers.discard(preview["ticker"]); virtual_slots += 1
        candidates: dict[str, dict[str, Any]] = {}
        # Existing retained watches are always reassessed, even when the source run omits them.
        event_map = {event["ticker"]: event for event in events}
        for ticker, saved in watches.items():
            payload = saved if isinstance(saved, dict) else {}
            identity = payload.get("event_identity") or {"ticker": ticker, "original_gap_date": payload.get("original_gap_date"),
                "discovery_rules_version": payload.get("discovery_rules_version", DISCOVERY_RULES_VERSION)}
            current = event_map.get(ticker)
            same_event = bool(current and current.get("event_identity") == identity)
            if payload.get("gap_state") in {"closed", "retired"} and (current is None or same_event):
                candidates[ticker] = {**payload, "ticker": ticker, "status": payload.get("status", payload.get("gap_state")),
                                      "decision_date": day.isoformat()}
                event_map.pop(ticker, None)
                continue
            if current is None:
                event_map[ticker] = {**payload, "ticker": ticker, "original_gap_date": identity.get("original_gap_date"),
                                     "event_identity": identity, "source_run_id": payload.get("source_run_id")}
            elif current.get("original_gap_date", "") <= identity.get("original_gap_date", ""):
                event_map[ticker] = {**payload, "ticker": ticker, "original_gap_date": identity.get("original_gap_date"),
                                     "event_identity": identity, "source_run_id": payload.get("source_run_id")}
            if same_event and payload.get("gap_state") in {"retired", "closed"}:
                event_map.pop(ticker, None)
        for event in event_map.values():
            ticker = event["ticker"]
            if ticker in prior_ids:
                candidates[ticker] = {"ticker": ticker, "status": "open", "gap_state": "open",
                    "reasons": ["open_position_exists"], "event_identity": event.get("event_identity"),
                    "original_gap_date": event.get("original_gap_date"), "source_run_id": event.get("source_run_id"),
                    "decision_date": day.isoformat(), "first_observed_date": event.get("first_observed_date")}
                continue
            frame = frames.get(ticker)
            if frame is None or getattr(frame, "empty", True):
                result = {"ticker": ticker, "status": "blocked_data", "gap_state": "blocked_data", "reasons": ["history_missing_sessions"]}
            else:
                prior = watches.get(ticker)
                if prior and prior.get("event_identity") != event.get("event_identity"):
                    prior = None
                result = evaluate_gap_retest(frame, event, prior, ticker=ticker, calendar_sessions=dates)
            prior_observation = watches.get(ticker) or {}
            first_observed = (prior_observation.get("first_observed_date")
                              if prior_observation.get("event_identity") == event.get("event_identity") else None)
            result.update(event_identity=event.get("event_identity"), source_run_id=event.get("source_run_id"),
                          original_gap_event=event.get("original_gap_event"),
                          gap_day_low=event.get("gap_day_low"), source_published_at=event.get("source_published_at"),
                          data_provenance_details=getattr(frame, "attrs", {}).get("data_provenance"),
                          first_observed_date=first_observed or event.get("first_observed_date"),
                          decision_date=day.isoformat(), source_missing=not source_valid,
                          management_policy_version="gap_trim10_3r_ema9_v1",
                          data_provenance="unknown", digest=hashlib.sha256(json.dumps({
                              "adjustment_basis": getattr(frame, "attrs", {}).get("adjustment_basis"),
                              "bars": [[str(idx), *[str(value) for value in row]] for idx, row in frame.iterrows()]},
                              separators=(",", ":")).encode()).hexdigest() if frame is not None else None)
            if frame is not None and hasattr(frame, "attrs") and frame.attrs.get("adjustment_basis") == "split_adjusted_regular_session":
                result["data_provenance"] = (getattr(frame, "attrs", {}).get("data_provenance") or {}).get(
                    "basis", "split_adjusted_regular_session")
            else:
                result.update(status="blocked_data", gap_state="blocked_data", reasons=["adjustment_basis_unknown"])
            if hasattr(repo, "daily_bar_provenance") and event.get("original_gap_date"):
                event_basis = repo.daily_bar_provenance(ticker, date.fromisoformat(str(event["original_gap_date"])[:10]), day)
                if len({str(item.get("split_factor")) for item in event_basis}) > 1:
                    result.update(status="blocked_data", gap_state="blocked_data", reasons=["adjustment_basis_unknown"])
            if not source_valid and ticker not in watches:
                continue
            candidates[ticker] = result
        eligible = [item for item in candidates.values() if item.get("status") == "confirmed"]
        spy = frames.get("SPY")
        if spy is not None and not getattr(spy, "empty", True):
            spy_close = {idx.date() if hasattr(idx, "date") else idx: float(row["Close"]) for idx, row in spy.iterrows()}
            for candidate in eligible:
                stock = frames.get(candidate["ticker"])
                if stock is None or getattr(stock, "empty", True):
                    continue
                stock_rows = [(idx.date() if hasattr(idx, "date") else idx, float(row["Close"])) for idx, row in stock.iterrows()]
                aligned = [(d, c, spy_close[d]) for d, c in stock_rows if d in spy_close and d <= day]
                if len(aligned) >= 21 and aligned[-1][0] == day:
                    candidate["relative_return20_pct"] = (aligned[-1][1] / aligned[-21][1] - aligned[-1][2] / aligned[-21][2]) * 100
        for candidate in eligible:
            if candidate.get("relative_return20_pct") is None:
                candidate.update(status="blocked_data", gap_state="blocked_data", reasons=["history_missing_sessions"])
        eligible = [item for item in eligible if item.get("status") == "confirmed"]
        # Rank is persisted evidence; allocation can only consume today's fresh confirmation.
        ranked = rank_confirmations(eligible)
        report["confirmed_count"] = len(ranked)
        for rank, candidate in enumerate(ranked, 1):
            candidate["rank"] = rank
        if source_valid:
            slots = virtual_slots
            free_risk = max(0.0, 2500 - max(0.0, virtual_risk))
            cash = max(0.0, virtual_cash)
            for candidate in ranked:
                ticker = candidate["ticker"]
                identity = candidate.get("event_identity")
                if ticker in open_tickers:
                    candidate.update(status="open", gap_state="open", reasons=["open_position_exists"])
                    continue
                if ticker in pending_tickers:
                    candidate.update(status="waiting_capacity", gap_state="waiting_capacity", reasons=["capacity_full"])
                    continue
                if identity and (json.dumps(identity, sort_keys=True) in filled_event_ids or repo.has_gap_event_fill(sid, identity)):
                    candidate.update(status="retired", gap_state="retired", reasons=["event_already_traded"])
                    continue
                budget = size_gap_order(candidate, available_cash=cash, available_risk=free_risk, slots_available=slots)
                candidate.update(budget)
                if candidate["status"] == "pending":
                    candidate.update(order_date=day.isoformat(), awaiting_session_date=next_session.isoformat(),
                                     max_entry=candidate["max_entry"], event_identity=identity, selected_for_book=True)
                    candidate["limit_price"] = candidate["max_entry"]
                    cash -= float(candidate["reservation_cash"]); free_risk -= float(candidate["planned_stop_risk"]); slots -= 1
        confirmed_ranks = {item["ticker"]: item["rank"] for item in ranked}
        ordered_candidates = sorted(candidates.values(), key=lambda item: (confirmed_ranks.get(item["ticker"], 10**9), item["ticker"]))
        for index, candidate in enumerate(ordered_candidates, 1):
            candidate["rank"] = confirmed_ranks.get(candidate["ticker"], len(ranked) + index)
        report["candidate_count"] = len(candidates)
        report["candidates"] = sorted(candidates.values(), key=lambda x: (x.get("rank", 999999), x["ticker"]))
        if dry_run:
            report["status"] = "dry_run_complete"
            return report
        source_id = int((source_record or {}).get("source_run_id", (source_record or {}).get("id")) or 0)
        fingerprint = str((source_record or {}).get("source_fingerprint") or "")
        if source_valid and not historical_replay and (not source_id or not fingerprint):
            raise ValueError("Persisted source run ID and fingerprint are required")
        if historical_replay and not fingerprint:
            raise ValueError("Reconstructed source fingerprint is required")
        if not source_valid and not watches and not positions and not pending:
            return {**report, "status": "missing_prerequisite", "reason": "source_missing_or_stale"}
        if not source_valid:
            source_lookup = getattr(repo, "latest_source_run_id", None)
            source_id = int(source_lookup(sid) or 0) if source_lookup else 0
            if not source_id and sid != REPLAY_V4_STRATEGY_ID:
                context = repo.gap_retest_context() if hasattr(repo, "gap_retest_context") else {}
                prior_runs = context.get("runs", []) if isinstance(context, dict) else []
                previous = next((run for run in prior_runs if run.get("source_run_id")), None)
                source_id = int(previous.get("source_run_id")) if previous else 0
            if not source_id and sid != REPLAY_V4_STRATEGY_ID:
                raise ValueError("Established gap retest book has no prior valid source run ID")
            # A replay-seeded v4 book can manage existing risk with source_run_id=0.
        fingerprint = fingerprint or f"source-missing:{day.isoformat()}"
        summary = {**{k: v for k, v in report.items() if k not in {"candidates", "orders"}},
                   "execution_complete": True, "discovery_complete": source_valid,
                   "source_missing_reason": None if source_valid else "source_missing_or_stale"}
        run_id = repo.save_session(session_date=day, source_run_id=source_id, fingerprint=fingerprint,
                                   candidates=report["candidates"], summary=summary, strategy_id=sid)
        if run_id is None:
            raise RuntimeError("Gap retest session could not be persisted")
        counts = {"fills": 0, "expiries": 0, "cancellations": 0, "stops": 0, "trims": 0, "trail_exits": 0}
        # Settle previously executable opening orders at this exact session only.
        for order in pending:
            payload = dict(order.get("payload_json") or {})
            ticker = str(order["ticker"]).upper()
            if payload.get("awaiting_session_date") != day.isoformat():
                raise ValueError("Pending gap retest order missed its exact execution session; session rolled back")
            bar = _bar(frames.get(ticker), day)
            if bar is None:
                raise ValueError("Missing pending-order execution bar; session rolled back")
            outcome = resolve_gap_next_open(payload, bar)
            if outcome["status"] == "filled":
                shares, fill = int(payload["shares"]), float(outcome["fill_price"])
                position = {**payload, "entry_price": fill, "initial_stop": payload["stop"],
                    "initial_r_per_share": float(_d(fill - float(payload["stop"]))), "initial_shares": shares,
                    "remaining_shares": shares, "shares": shares, "realized_pnl": 0.0, "realized_r": 0.0,
                    "trimmed": False, "exit_next_open": False, "opened_on": day.isoformat(),
                    "last_close": bar["close"], "management_policy_version": "gap_trim10_3r_ema9_v1"}
                position_id = repo.fill_order(candidate_id=order["id"], run_id=order["run_id"], ticker=ticker,
                    event_date=day, position_payload=position,
                    event_payload={"price": fill, "shares": shares, "event_identity": payload.get("event_identity")}, strategy_id=sid)
                if position_id is None:
                    raise ValueError("Gap retest fill could not be recorded atomically")
                counts["fills"] += 1
                # The hard stop is live on entry day. A daily bar cannot order a stop and target, so stop wins.
                if bar["open"] <= float(position["stop"]) or bar["low"] <= float(position["stop"]):
                    exit_price = float(_d((bar["open"] if bar["open"] <= float(position["stop"]) else float(position["stop"])) * .999))
                    pnl = (exit_price - fill) * shares
                    closed = {**position, "remaining_shares": 0, "closed_on": day.isoformat(), "exit_price": exit_price,
                              "exit_type": "gap_stop" if bar["open"] <= float(position["stop"]) else "stop",
                              "realized_pnl": pnl, "realized_r": pnl / (position["initial_r_per_share"] * shares)}
                    repo.apply_event(run_id=run_id, position_id=position_id, ticker=ticker, event_type=closed["exit_type"],
                        event_date=day, payload={"price": exit_price, "shares": shares}, position_payload=closed,
                        close_position=True, strategy_id=sid)
                    counts["stops"] += 1
                else:
                    ema9 = daily_indicators(frames.get(ticker)).get("ema9")
                    position["exit_next_open"] = ema9 is not None and bar["close"] < ema9
                    repo.set_position_payload(position_id, position)
            else:
                event_type = "order_expired" if outcome["status"] == "expired" else "order_cancelled"
                if not repo.apply_event(run_id=run_id, position_id=None, ticker=ticker, event_type=event_type,
                    event_date=day, payload=outcome, candidate_id=order["id"], candidate_status=outcome["status"], strategy_id=sid):
                    raise ValueError("Gap retest order outcome could not be recorded atomically")
                counts["expiries" if outcome["status"] == "expired" else "cancellations"] += 1
        # Manage positions with the same 260-session as-of EMA policy used for entry calculations.
        for position_row in positions:
            ticker = str(position_row["ticker"]).upper()
            p = dict(position_row.get("payload_json") or {})
            bar = _bar(frames.get(ticker), day)
            if bar is None:
                raise ValueError("Missing position management bar; session rolled back")
            metrics = daily_indicators(frames.get(ticker))
            actions = manage_position(p, bar, day=day, ema9=metrics.get("ema9"))
            action = actions[0]
            if action["type"] == "mark":
                payload = dict(action["position"])
                payload.update(last_close=bar["close"], holding_sessions=int(payload.get("holding_sessions", 0)) + 1)
                repo.set_position_payload(position_row["id"], payload)
                continue
            shares, exit_price = int(action["shares"]), float(action["price"])
            pnl = (exit_price - float(p["entry_price"])) * shares
            risk_total = float(p["initial_r_per_share"]) * int(p.get("initial_shares") or p.get("shares") or shares)
            remaining = int(p.get("remaining_shares", 0)) - shares
            updated = {**p, "remaining_shares": remaining, "realized_pnl": float(p.get("realized_pnl", 0)) + pnl,
                       "realized_r": float(p.get("realized_r", 0)) + (pnl / risk_total if risk_total else 0),
                       "last_close": bar["close"], "trimmed": p.get("trimmed", False) or action["type"] == "trim_3r",
                       "exit_next_open": False if action["close"] else bool(action.get("schedule_exit", p.get("exit_next_open", False)))}
            close_position = bool(action["close"] or remaining <= 0)
            updated.update({"closed_on": day.isoformat(), "exit_type": action["type"], "exit_price": exit_price} if close_position else {})
            if not repo.apply_event(run_id=run_id, position_id=position_row["id"], ticker=ticker,
                event_type=action["type"], event_date=day,
                payload={"price": exit_price, "shares": shares, "realized_pnl": pnl,
                         "gap_through_stop_diagnostic": bool(action.get("gap_through_stop_diagnostic"))},
                position_payload=updated, close_position=close_position, strategy_id=sid):
                raise ValueError("Gap retest management event could not be recorded atomically")
            counts["stops"] += int(action["type"] in {"stop", "gap_stop"})
            counts["trims"] += int(action["type"] == "trim_3r")
            counts["trail_exits"] += int(action["type"] == "ema9_exit")
        # Orders created today reserve exactly these fixed shares for the next exchange open.
        report.update(run_id=run_id, status="applied", **counts)
        if hasattr(repo, "set_run_metadata"):
            repo.set_run_metadata(run_id, {**counts, "execution_complete": True, "discovery_complete": source_valid,
                                           "new_source_run_id": source_id if source_valid else None})
        if hasattr(repo, "update_run_summary"):
            current_positions = repo.open_positions(strategy_id=sid)
            marks = {str(p["ticker"]).upper(): float((p.get("payload_json") or {}).get("last_close") or 0)
                     for p in current_positions if (p.get("payload_json") or {}).get("last_close")}
            repo.update_run_summary(run_id=run_id, strategy_id=sid, mark_prices=marks, action_counts=counts)
        return report
