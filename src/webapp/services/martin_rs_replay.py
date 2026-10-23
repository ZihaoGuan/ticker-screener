"""Explicit, all-or-nothing RS Expanded replay from point-in-time saved boards."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
from contextlib import nullcontext
from typing import Any

import pandas as pd

from src.trade_plan_policies import CURRENT_STOP_POLICY, STRUCTURAL_STOP_POLICY, current_plan_payload, structural_plan_payload
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_forward_test_service import FOCUS_SCANNERS, RS_EXPANDED_STRATEGY_ID, MartinForwardTestService, _scanner_ids, classify_rs_expanded_candidate

REPLAY_VERSION = "rs_expanded_replay_v2"


def timestamp(value: Any) -> dt.datetime:
    parsed = pd.Timestamp(value)
    if pd.isna(parsed) or parsed.tzinfo is None:
        raise ValueError("A timezone-aware publication timestamp is required")
    return parsed.to_pydatetime().astimezone(dt.timezone.utc)


def choose_snapshot(records: list[dict[str, Any]], day: dt.date, close: dt.datetime,
                    next_open: dt.datetime, now: dt.datetime) -> dict[str, Any] | None:
    eligible = []
    for record in records:
        summary = record.get("result_summary_json")
        if not isinstance(summary, dict) or str(summary.get("target_trading_date")) != day.isoformat():
            continue
        board = summary.get("guru_board")
        if not isinstance(board, dict) or not isinstance(board.get("rows"), list):
            continue
        if str(record.get("run_date", day)) != day.isoformat() or int(record.get("id") or 0) <= 0:
            continue
        try:
            generated = timestamp(summary.get("generated_at"))
            created = timestamp(record.get("created_at"))
            if not close <= generated < next_open or generated > now or not close <= created < next_open:
                continue
            reference = summary.get("reference_now_new_york")
            if reference and not close <= timestamp(reference) < next_open:
                continue
            if any(card.get("captured_at") and timestamp(card["captured_at"]) > generated
                   for card in summary.get("cards", []) if isinstance(card, dict)):
                continue
        except (ValueError, TypeError, OverflowError):
            continue
        eligible.append((generated, int(record["id"]), summary))
    if not eligible:
        return None
    generated, run_id, summary = max(eligible, key=lambda value: value[:2])
    return {**copy.deepcopy(summary), "_snapshot_run_id": run_id,
            "_original_generated_at": generated.isoformat()}


def valid_bar(row: pd.Series) -> bool:
    try:
        opening, high, low, close, volume = (float(row[key]) for key in ("Open", "High", "Low", "Close", "Volume"))
        return all(math.isfinite(value) and value > 0 for value in (opening, high, low, close, volume)) and low <= min(opening, close) <= max(opening, close) <= high
    except (KeyError, ValueError, TypeError):
        return False


def prepare_replay(start: dt.date, end: dt.date, records: list[dict[str, Any]], frames: dict[str, pd.DataFrame],
                   calendar: Any, now: dt.datetime) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if end < start:
        raise ValueError("--to-date must be on or after --from-date")
    sessions = list(calendar.sessions_in_range(start.isoformat(), end.isoformat()))
    errors, prepared, blocked_candidates, candidate_counts = [], [], [], {}
    valid_dates = {ticker: {index.date() for index, bar in frame.iterrows() if valid_bar(bar)}
                   for ticker, frame in frames.items()}
    if not sessions:
        errors.append({"reason": "no_exchange_sessions_in_range"})
    for session in sessions:
        day = session.date()
        close = timestamp(calendar.session_close(session))
        next_open = timestamp(calendar.session_open(calendar.next_session(session)))
        if close > now:
            errors.append({"date": day.isoformat(), "reason": "session_not_completed"})
            continue
        snapshot = choose_snapshot(records, day, close, next_open, now)
        if snapshot is None:
            errors.append({"date": day.isoformat(), "reason": "point_in_time_snapshot_missing_or_overwritten"})
            continue
        rows = snapshot["guru_board"]["rows"]
        focus = [row for row in rows if isinstance(row, dict) and _scanner_ids(row).intersection(FOCUS_SCANNERS)]
        history_dates = list(calendar.sessions_in_range((day - dt.timedelta(days=500)).isoformat(), day.isoformat()))
        expected35 = {label.date() for label in history_dates[-35:]}
        expected205 = {label.date() for label in history_dates[-205:]}
        missing_spy = expected35 - valid_dates.get("SPY", set())
        if missing_spy:
            errors.append({"date": day.isoformat(), "ticker": "SPY", "reason": "missing_or_invalid_35_session_history",
                           "missing_dates": sorted(value.isoformat() for value in missing_spy)})
        for row in focus:
            ticker = str(row.get("ticker") or "").strip().upper()
            blockers = []
            missing = expected35 - valid_dates.get(ticker, set())
            if missing:
                blockers.append({"reason": "missing_or_invalid_35_session_history",
                                 "missing_dates": sorted(value.isoformat() for value in missing)})
            frame = frames.get(ticker, pd.DataFrame())
            past = frame.loc[frame.index.date <= day].tail(1300) if not frame.empty else frame
            if isinstance(row.get("daily_rs_rating"), bool):
                rating = None
            else:
                try:
                    rating = float(row.get("daily_rs_rating"))
                except (ValueError, TypeError):
                    rating = None
            if rating is not None and 80 <= rating < 90:
                for symbol in (ticker, "SPY"):
                    available = valid_dates.get(symbol, set())
                    if not expected205.issubset(available):
                        issue = {"date": day.isoformat(), "ticker": symbol, "reason": "emerging_205_session_history_missing",
                                 "missing_dates": sorted(value.isoformat() for value in expected205 - available)}
                        if symbol == "SPY":
                            if issue not in errors:
                                errors.append(issue)
                        else:
                            blockers.append({key: value for key, value in issue.items() if key not in {"date", "ticker"}})
            # Preserve the original universe, ratings, quality and Avoid gates; only reconstruct policy fields.
            structural_row = dict(row)
            adr_source = "saved_snapshot"
            try:
                adr_valid = math.isfinite(float(row.get("adr14_pct"))) and float(row["adr14_pct"]) > 0
            except (ValueError, TypeError):
                adr_valid = False
            if not adr_valid and len(past) >= 14 and all(valid_bar(bar) for _, bar in past.tail(14).iterrows()):
                structural_row["adr14_pct"] = float(((past["High"] - past["Low"]) / past["Close"] * 100).tail(14).mean())
                adr_source = "historical_14_completed_bars"
            row["trade_plans"] = {CURRENT_STOP_POLICY: current_plan_payload(row, day),
                                  STRUCTURAL_STOP_POLICY: structural_plan_payload(structural_row, day, past)}
            policy = row["trade_plans"][STRUCTURAL_STOP_POLICY]
            policy["evidence"]["adr_source"] = adr_source
            if policy["availability"] == "unavailable" and policy["unavailable_reason"] == "structural_evidence_missing":
                blockers.append({"reason": "structural_evidence_missing"})
            row["replay_data_validation"] = {"origin": "historical_replay", "as_of_date": day.isoformat(), "version": REPLAY_VERSION,
                                             "blocked": bool(blockers), "issues": blockers}
            if blockers:
                blocked_candidates.append({"date": day.isoformat(), "ticker": ticker, "issues": blockers})
        candidate_counts[day.isoformat()] = {}
        for rank, row in enumerate(focus, start=1):
            state = classify_rs_expanded_candidate(row, rank, day, frames)["status"]
            candidate_counts[day.isoformat()][state] = candidate_counts[day.isoformat()].get(state, 0) + 1
        provenance = {"origin": "historical_replay", "version": REPLAY_VERSION,
                      "source_run_id": snapshot["_snapshot_run_id"], "source_generated_at": snapshot["_original_generated_at"],
                      "original_fingerprint": snapshot.get("source_fingerprint"), "stop_policy_version": STRUCTURAL_STOP_POLICY,
                      "from_date": start.isoformat(), "to_date": end.isoformat(),
                      "limitations": "Saved universe and ratings; reconstructed stops; current stored OHLCV may include later corrections."}
        snapshot["replay_provenance"] = provenance
        snapshot["replay_validation"] = {"required_execution_dates": sorted(value.isoformat() for value in expected35),
                                         "candidate_status_counts": candidate_counts[day.isoformat()],
                                         "blocked_candidates": [item for item in blocked_candidates if item["date"] == day.isoformat()]}
        digest = json.dumps(snapshot, sort_keys=True, default=str, allow_nan=False)
        snapshot["source_fingerprint"] = hashlib.sha256(digest.encode()).hexdigest()[:32]
        prepared.append(snapshot)
    report = {"strategy_id": RS_EXPANDED_STRATEGY_ID, "origin": "historical_replay", "version": REPLAY_VERSION,
              "from_date": start.isoformat(), "to_date": end.isoformat(), "sessions": [session.date().isoformat() for session in sessions],
              "prepared_sessions": len(prepared), "errors": errors, "ready": not errors,
              "blocked_candidates": blocked_candidates,
              "candidate_status_counts": candidate_counts,
              "dry_run_scope": "Read-only input preflight; does not simulate fills or calculate performance.",
              "starting_state": "Empty RS Expanded account; no earlier positions or pending orders imported."}
    return prepared, report


class _BorrowedConnection:
    """Let existing repository methods share the CLI's single outer transaction."""
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self.connection.cursor()


class ReplayRepository(MartinForwardTestRepository):
    def __init__(self, connection):
        self.connection = _BorrowedConnection(connection)

    def _connect(self):
        return self.connection

    def execution_lock(self, strategy_id):
        # The outer transaction already holds the same book's advisory lock.
        return nullcontext()


def ensure_empty_book(connection) -> None:
    with connection.cursor() as cursor:
        for table in ("martin_forward_runs", "martin_forward_positions", "martin_forward_events"):
            cursor.execute(f"SELECT count(*) FROM {table} WHERE strategy_id=%s", (RS_EXPANDED_STRATEGY_ID,))
            if cursor.fetchone()[0]:
                raise ValueError("RS Expanded already has history. Replay refuses to merge, overwrite or back-insert. No records changed.")


def apply_replay(connection, snapshots: list[dict[str, Any]], frames: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    ensure_empty_book(connection)
    def loader(tickers, day, days, **_kwargs):
        return {ticker: frames[ticker].loc[frames[ticker].index.date <= day].tail(days)
                for ticker in tickers if ticker in frames}
    repository = ReplayRepository(connection)
    service = MartinForwardTestService(repository, bar_loader=loader)
    results = []
    for snapshot in snapshots:
        day = dt.date.fromisoformat(snapshot["target_trading_date"])
        required = {dt.date.fromisoformat(value) for value in snapshot["replay_validation"]["required_execution_dates"]}
        exposed = repository.open_positions(strategy_id=RS_EXPANDED_STRATEGY_ID) + repository.pending_candidates(before=day, strategy_id=RS_EXPANDED_STRATEGY_ID)
        for ticker in sorted({str(item["ticker"]).strip().upper() for item in exposed}):
            frame = frames.get(ticker, pd.DataFrame())
            available = {index.date() for index, bar in frame.iterrows() if valid_bar(bar)}
            if not required.issubset(available):
                raise ValueError(f"Replay execution history missing or invalid for exposed ticker {ticker} on {day}; the entire range must be rolled back.")
        result = service.run_session(as_of_date=day,
                                     snapshot=snapshot, strategy_id=RS_EXPANDED_STRATEGY_ID)
        if result["run_id"] is None or result["partial"]:
            raise ValueError("Replay produced an incomplete session; the entire range must be rolled back.")
        results.append(result)
    return results


def run_database_replay(database_url: str, start: dt.date, end: dt.date, *, apply: bool = False) -> dict[str, Any]:
    import exchange_calendars as xcals
    import psycopg
    calendar = xcals.get_calendar("XNYS", start=start - dt.timedelta(days=2700), end=end + dt.timedelta(days=15))
    now = dt.datetime.now(dt.timezone.utc)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" + (", READ ONLY" if not apply else ""))
            if apply:
                cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{RS_EXPANDED_STRATEGY_ID}",))
                if not cursor.fetchone()[0]:
                    raise ValueError("RS Expanded daily job or another replay is running; retry later.")
            ensure_empty_book(connection)
            records = MartinForwardTestRepository._fetch(cursor, """SELECT id,run_date,created_at,result_summary_json
                FROM screen_runs WHERE strategy_id='scanner_top_hits_snapshot' AND deleted_at IS NULL
                AND run_date >= %s AND run_date <= %s ORDER BY run_date,id""", (start, end))
            tickers = {"SPY"}
            for record in records:
                summary = record.get("result_summary_json")
                board = summary.get("guru_board") if isinstance(summary, dict) else None
                for row in board.get("rows", []) if isinstance(board, dict) and isinstance(board.get("rows"), list) else []:
                    if isinstance(row, dict) and _scanner_ids(row).intersection(FOCUS_SCANNERS):
                        tickers.add(str(row.get("ticker") or "").strip().upper())
            bars = MartinForwardTestRepository._fetch(cursor, """SELECT ticker,trade_date,open,high,low,close,volume
                FROM daily_bars WHERE ticker=ANY(%s) AND trade_date >= %s AND trade_date <= %s ORDER BY ticker,trade_date""",
                (sorted(tickers), start - dt.timedelta(days=2600), end))
        frames = {}
        for ticker in sorted(tickers):
            rows = [bar for bar in bars if bar["ticker"] == ticker]
            if rows:
                frame = pd.DataFrame(rows).rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
                frame.index = pd.to_datetime(frame.pop("trade_date"))
                frames[ticker] = frame[["Open", "High", "Low", "Close", "Volume"]].astype(float)
        snapshots, report = prepare_replay(start, end, records, frames, calendar, now)
        report["mode"] = "apply" if apply else "dry_run"
        if apply and report["ready"]:
            report["results"] = apply_replay(connection, snapshots, frames)
            report["saved"] = True
        else:
            report["saved"] = False
        return report
