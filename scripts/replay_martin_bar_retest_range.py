#!/usr/bin/env python3
"""Replay saved Martin Bar Watchlist scans across a completed XNYS date range."""
from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

POLICY_ID = "martin_bar_retest_v1"
SOURCE_ID = "martin_bar_watchlist_v1"


def xnys_sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=start - dt.timedelta(days=10), end=end + dt.timedelta(days=10))
    return [label.date() for label in calendar.sessions_in_range(start.isoformat(), end.isoformat())]


def publication_timing(day: dt.date, published_at: Any) -> tuple[bool, str]:
    import exchange_calendars as xcals
    import pandas as pd
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=5), end=day + dt.timedelta(days=20))
    following = calendar.next_session(day.isoformat()).date()
    published = pd.Timestamp(published_at)
    if published.tzinfo is None:
        published = published.tz_localize("UTC")
    retrospective = published >= calendar.session_open(following.isoformat())
    return retrospective, ("published_after_next_open" if retrospective else "saved_before_next_open")


def load_range_sources(history: Any, sessions: list[dt.date]) -> dict[dt.date, dict[str, Any]]:
    from scripts.run_martin_bar_retest import _load_source

    sources = {}
    for day in sessions:
        runs = history.list_runs(strategy_id=SOURCE_ID, start_date=day, end_date=day, limit=100)
        matching = [run for run in runs if str(run.get("run_date") or "")[:10] == day.isoformat()]
        if len(matching) > 1:
            raise ValueError(f"Multiple persisted {SOURCE_ID} source runs exist for {day}; source selection is ambiguous.")
        if not matching:
            raise ValueError(f"Missing persisted source run for {day}.")
        source = _load_source(history, day)
        if source is None:
            raise ValueError(f"Missing persisted source run for {day}.")
        if int(source.get("source_run_id") or 0) != int(matching[0].get("id") or 0):
            raise ValueError(f"Loaded source run for {day} does not match the listed persisted run.")
        if source.get("strategy_id") != SOURCE_ID or source.get("rules_version") != SOURCE_ID:
            raise ValueError(f"Source run {source.get('source_run_id')} has invalid strategy or rules provenance.")
        if source.get("hit_count") is None or int(source["hit_count"]) != len(source.get("hits") or []):
            raise ValueError(f"Source run {source.get('source_run_id')} hit count does not match loaded hits.")
        if not source.get("created_at") or not source.get("source_fingerprint"):
            raise ValueError(f"Source run {source.get('source_run_id')} is missing publication or fingerprint provenance.")
        retrospective, timing = publication_timing(day, source["created_at"])
        source["replay_provenance"] = {
            "origin": "historical_replay",
            "source_mode": timing,
            "retrospective": retrospective,
            "source_run_id": source.get("source_run_id"),
            "created_after_session_close": True,
            "limitation": "Saved screen run used as historical input; the creation timestamp does not prove point-in-time universe or data availability.",
        }
        sources[day] = source
    return sources


def check_empty_book(connection: Any, book_id: str) -> None:
    with connection.cursor() as cursor:
        for table in ("martin_forward_runs", "martin_forward_positions", "martin_forward_events"):
            cursor.execute(f"SELECT count(*) FROM {table} WHERE strategy_id=%s", (book_id,))
            if cursor.fetchone()[0]:
                raise ValueError(f"Book {book_id} already has history; replay refuses to modify it.")


def frozen_intraday_for(directory: str | Path | None, day: dt.date) -> dict[str, dict[str, Any]]:
    if directory is None:
        return {}
    root = Path(directory)
    audit_path = root / "audit.json"
    if not audit_path.exists():
        return {}
    try:
        document = json.loads(audit_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("Frozen intraday directory requires a valid audit.json manifest.") from exc
    from scripts.audit_martin_intraday_execution import digest
    supplied_fingerprint = document.pop("fingerprint", None)
    if not supplied_fingerprint or digest(document) != supplied_fingerprint:
        raise ValueError("Frozen intraday audit manifest fingerprint mismatch.")
    manifests = {item.get("candle_file"): item for item in document.get("records", [])
                 if item.get("candle_file") and item.get("date") == day.isoformat()}
    result = {}
    for path in sorted(root.glob(f"*_{day.isoformat()}.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            # Missing or unreadable bars are surfaced by the executor's marked daily fallback.
            continue
        ticker = path.name[:-(len(day.isoformat()) + 6)].upper()
        if not ticker or ticker in result:
            raise ValueError(f"Duplicate or invalid frozen intraday ticker: {path.name}")
        record = manifests.get(path.name)
        if (record is None or str(record.get("ticker") or "").upper() != ticker or
                digest(data) != record.get("candle_fingerprint")):
            raise ValueError(f"Frozen intraday bundle fingerprint mismatch: {path.name}")
        result[ticker] = {"data": data, "requested_symbol": (data.get("query") or {}).get("symbol") or
            (data.get("metadata") or {}).get("full_name") or f"NASDAQ:{ticker}",
            "audit_fingerprint": supplied_fingerprint, "candle_fingerprint": record["candle_fingerprint"],
            "source_date": record.get("date"), "source_file": path.name}
    for record in document.get("records", []):
        ticker = str(record.get("ticker") or "").upper()
        if record.get("date") == day.isoformat() and ticker and ticker not in result and not record.get("candle_file"):
            result[ticker] = {"freeze_error": record.get("classification") or "provider_unavailable",
                "requested_symbol": record.get("requested_symbol"), "audit_fingerprint": supplied_fingerprint,
                "source_date": record.get("date")}
    return result


def auto_freeze_book_intraday(*, book_id: str, day: dt.date, pending_orders: list[dict[str, Any]],
                              database_url: str, artifacts_dir: str | Path,
                              client_dir: str | Path | None = None, daily_loader=None, fetcher=None) -> tuple[Path, dict[str, dict[str, Any]]]:
    """Freeze the next session's pending-order inputs before opening the write transaction."""
    from scripts.audit_martin_intraday_execution import classify, digest, fetch_safely, load_daily
    tickers: dict[str, list[dict[str, Any]]] = {}
    for row in pending_orders:
        ticker = str(row.get("ticker") or "").upper()
        if ticker:
            payload = row.get("payload_json") or {}
            try:
                levels = {"entry": float(payload.get("limit_price") or payload.get("max_entry")),
                          "stop": float(payload["stop"])}
            except (KeyError, TypeError, ValueError, OverflowError):
                levels = None
            tickers.setdefault(ticker, []).append(levels or {})
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = Path(artifacts_dir) / "martin_intraday_execution_v2" / book_id / f"{day.isoformat()}_{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    client_dir = str(client_dir or os.environ.get("TRADINGVIEW_CLIENT_DIR") or
                     "/opt/tradingview/node_modules/@mathieuc/tradingview")
    fetch_data = fetcher or fetch_safely
    connection = None
    if daily_loader is None:
        try:
            import psycopg
            connection = psycopg.connect(database_url,
                options="-c default_transaction_read_only=on -c statement_timeout=60000")
        except Exception:
            connection = None
    records = []
    try:
        for ticker, orders in sorted(tickers.items()):
            requested = f"NASDAQ:{ticker}"
            data = fetch_data(client_dir, requested, day.isoformat(), "15")
            try:
                daily = load_daily(connection, ticker, day) if connection is not None else (daily_loader(ticker, day) if daily_loader else None)
            except Exception:
                daily = None
            if data is None:
                records.append({"ticker": ticker, "date": day.isoformat(), "requested_symbol": requested,
                    "classification": "provider_unavailable", "candle_file": None, "candle_fingerprint": None,
                    "order_levels": orders, "daily_source": (daily or {}).get("source")})
                continue
            candles = data.get("candles") or []
            try:
                evaluations = [classify(candles=candles, metadata=data.get("metadata"), requested_symbol=requested,
                    day=day.isoformat(), entry=levels["entry"], stop=levels["stop"], daily=daily)
                    for levels in orders if levels]
            except Exception:
                evaluations = [{"classification": "provider_data_invalid"}]
            needs_one_minute = any(item.get("classification") == "same_bar_ambiguous" for item in evaluations)
            if needs_one_minute:
                fine = fetch_data(client_dir, requested, day.isoformat(), "1")
                if fine is not None:
                    data["one_minute"] = fine
                else:
                    data["one_minute_refinement"] = {"classification": "provider_unavailable"}
            path = output / f"{ticker}_{day.isoformat()}.json"
            path.write_text(json.dumps(data, sort_keys=True, indent=2, default=str) + "\n")
            records.append({"ticker": ticker, "date": day.isoformat(), "requested_symbol": requested,
                "classification": "frozen", "order_evaluations": evaluations, "order_levels": orders,
                "timeframe": "15m", "session": "regular", "adjustment": "splits",
                "candle_file": path.name, "candle_fingerprint": digest(data),
                "daily_source": (daily or {}).get("source"),
                "one_minute_status": "frozen" if "one_minute" in data else ("provider_unavailable" if needs_one_minute else None)})
    finally:
        if connection is not None:
            connection.close()
    manifest = {"schema": "martin_intraday_execution_audit_v2", "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "read_only": True, "book_id": book_id, "from_date": day.isoformat(), "to_date": day.isoformat(),
        "provider": "TradingView", "records": records}
    manifest["fingerprint"] = digest(manifest)
    (output / "audit.json").write_text(json.dumps(manifest, sort_keys=True, indent=2, default=str) + "\n")
    return output, frozen_intraday_for(output, day)


def save_execution_only_run(repository: Any, *, book_id: str, day: dt.date,
                            exclusion_provenance: dict[str, Any] | None = None,
                            execution_version: str = "martin_bar_retest_execution_v1") -> int | None:
    """Keep zero-scan execution sessions in the book's sequential session history."""
    from src.ticker_filters import leveraged_or_inverse_provenance
    exclusion = exclusion_provenance or leveraged_or_inverse_provenance()
    return repository.save_session(session_date=day, source_run_id=0,
        fingerprint=hashlib.sha256(f"{book_id}:{day}:execution-only".encode()).hexdigest(),
        candidates=[], summary={"strategy_id": POLICY_ID, "book_id": book_id,
            "rule_version": execution_version, "session_date": day.isoformat(),
            "source_run_id": None, "source_mode": "execution_only_no_saved_scan", "zero_hit_day": None,
            "execution_mode": "historical_replay", "replay_provenance": {"fund_exclusions": {
                "version": exclusion["version"], "sha256": exclusion["sha256"]}},
            **({"execution_management_version": "martin_bar_retest_management_v2", "trim_fraction": "1/3",
                "trim_share_rounding": "floor", "trim_target_r": 3, "additional_partial_trims": False}
               if execution_version == "martin_bar_retest_execution_v2" else {})}, strategy_id=book_id)


def book_performance(connection: Any, book_id: str) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT status,ticker,payload_json FROM martin_forward_positions WHERE strategy_id=%s", (book_id,))
        rows = cursor.fetchall()
    positions = [{**(row[2] or {}), "status": row[0], "ticker": row[1]} for row in rows]
    fills_by_category: dict[str, int] = {}
    for position in positions:
        category = str(position.get("event_category") or position.get("category") or "unknown")
        fills_by_category[category] = fills_by_category.get(category, 0) + 1
    return {"open_positions": sum(row["status"] == "open" for row in positions),
            "open_tickers": sorted(str(row.get("ticker") or "").upper() for row in positions if row["status"] == "open"),
            "closed_positions": sum(row["status"] == "closed" for row in positions),
            "realized_pnl": sum(float(row.get("realized_pnl") or 0) for row in positions),
            "fills_by_discovery_category": fills_by_category}


def run_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
              book_id: str, database_url: str, simulate_backdated_buys: bool = False,
              exclusion_provenance: dict[str, Any] | None = None,
              execution_version: str = "martin_bar_retest_execution_v1",
              intraday_dir: str | Path | None = None) -> list[dict[str, Any]]:
    from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
    from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService
    from src.webapp.services.martin_rs_replay import ReplayRepository

    repository = ReplayRepository(connection)
    service = MartinBarRetestService(repository, database_url=database_url)
    execution = MartinBarRetestExecutionService(repository, database_url=database_url)
    execution.book_id = book_id
    execution.execution_version = execution_version
    if execution_version == "martin_bar_retest_execution_v2" and intraday_dir is None:
        raise ValueError("Version 2 replay requires an --intraday-dir; individual missing bundles use daily fallback.")
    results = []
    for day in sessions:
        source = sources.get(day)
        if source is None:
            from src.webapp.services.martin_rotation import require_completed_session
            require_completed_session(day)
            run_id = save_execution_only_run(repository, book_id=book_id, day=day,
                                             exclusion_provenance=exclusion_provenance, execution_version=execution_version)
            actions = execution.run_session(day=day, run_id=run_id, candidates=[], source_on_time=False,
                frozen_intraday=frozen_intraday_for(intraday_dir, day))
            if actions["missing_bars"]:
                raise ValueError(f"Execution replay is missing required bars on {day}; the full range was rolled back.")
            marks = continuation_mark(repository, book_id, day, database_url)
            repository.set_run_metadata(run_id, {"execution_book_version": execution_version,
                **({"execution_management_version": "martin_bar_retest_management_v2", "trim_fraction": "1/3",
                    "trim_share_rounding": "floor", "trim_target_r": 3, "additional_partial_trims": False}
                   if execution_version == "martin_bar_retest_execution_v2" else {}),
                "execution_only": True, "execution_actions": actions,
                "simulate_backdated_buys": simulate_backdated_buys,
                "backdated_buy_basis": None, "partial": marks["mark_coverage"]["status"] != "complete" or actions.get("intraday_fallbacks", 0) > 0,
                "intraday_evidence_partial": actions.get("intraday_fallbacks", 0) > 0,
                "intraday_fallback_count": actions.get("intraday_fallbacks", 0), **marks})
            results.append({"date": day.isoformat(), "source_run_id": None, "source_hit_count": 0,
                "new_event_count": 0, "active_event_count": 0, "zero_hit_day": None,
                "retrospective_source_scan": None, "source_publication_timing": "no_scan_execution_only",
                "execution_actions": actions, "category_cohorts": {}, "candidate_status_counts": {},
                "reason_counts": {}, "candidate_count": 0, "status": "execution_only"})
            continue
        result = service.run_session(as_of_date=day, source_record=source, dry_run=False,
                                     policy_id=POLICY_ID, book_id=book_id)
        if result.get("status") in {"missing_source", "blocked_source"} or result.get("skipped"):
            raise ValueError(f"Replay blocked or duplicated on {day}: {result.get('reason') or result.get('status')}.")
        actions = execution.run_session(day=day, run_id=result.get("run_id"), candidates=result.get("candidates", []),
                                        source_on_time=result.get("retrospective_source_scan") is False,
                                        simulate_backdated_buys=simulate_backdated_buys,
                                        frozen_intraday=frozen_intraday_for(intraday_dir, day))
        if actions["missing_bars"]:
            raise ValueError(f"Execution replay is missing required bars on {day}; the full range was rolled back.")
        cohorts = {category: sum(item.get("event_category") == category for item in result.get("candidates", []))
                   for category in sorted({str(item.get("event_category") or "unknown") for item in result.get("candidates", [])})}
        if result.get("run_id") is not None:
            marks = continuation_mark(repository, book_id, day, database_url)
            repository.set_run_metadata(result["run_id"], {"execution_book_version": execution_version,
                **({"execution_management_version": "martin_bar_retest_management_v2", "trim_fraction": "1/3",
                    "trim_share_rounding": "floor", "trim_target_r": 3, "additional_partial_trims": False}
                   if execution_version == "martin_bar_retest_execution_v2" else {}),
                "execution_actions": actions, "execution_source_on_time": result.get("retrospective_source_scan") is False,
                "simulate_backdated_buys": simulate_backdated_buys,
                "backdated_buy_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and result.get("retrospective_source_scan") else "forward_safe",
                "source_reconstructed_later": bool(result.get("retrospective_source_scan")),
                "partial": marks["mark_coverage"]["status"] != "complete" or actions.get("intraday_fallbacks", 0) > 0,
                "intraday_evidence_partial": actions.get("intraday_fallbacks", 0) > 0,
                "intraday_fallback_count": actions.get("intraday_fallbacks", 0),
                **marks,
                "category_cohorts": cohorts, "execution_account_limits": {"base_equity": 100000,
                "risk_per_trade": 500, "max_name_value": 20000, "max_committed_risk": 2500, "max_positions": 8}})
        statuses = Counter(str(item.get("status") or "unknown") for item in result.get("candidates", []))
        reasons = Counter(str(reason) for item in result.get("candidates", [])
                          for reason in (item.get("reasons") or []))
        results.append({"date": day.isoformat(), "source_run_id": source["source_run_id"],
                        "source_hit_count": result["source_hit_count"],
                        "new_event_count": result["new_event_count"],
                        "active_event_count": result["active_event_count"],
                        "zero_hit_day": result["zero_hit_day"],
                        "retrospective_source_scan": result["retrospective_source_scan"],
                        "backdated_buy_simulation": bool(simulate_backdated_buys and result["retrospective_source_scan"]),
                        "source_reconstructed_later": bool(result["retrospective_source_scan"]),
                        "fill_basis": "hypothetical_backdated_simulation" if simulate_backdated_buys and result["retrospective_source_scan"] else "forward_safe",
                        "source_publication_timing": source["replay_provenance"]["source_mode"],
                        "candidate_status_counts": dict(statuses), "reason_counts": dict(reasons),
                        "execution_actions": actions,
                        "category_cohorts": cohorts,
                        "candidate_count": len(result.get("candidates", [])),
                        "status": "complete"})
    if results:
        results[-1]["book_performance"] = book_performance(connection, book_id)
    return results


def continuation_mark(repository: Any, book_id: str, day: dt.date, database_url: str) -> dict[str, Any]:
    """Mark only positions actually open on this session, using that session's Close."""
    from src.market_data_access import load_many_ticker_windows
    from src.webapp.services.martin_bar_retest_execution import bar_for
    rows = [(row["status"], row["ticker"], row["payload_json"]) for row in repository._rows(
        "SELECT status,ticker,payload_json FROM martin_forward_positions WHERE strategy_id=%s", (book_id,))]
    previous_rows = repository._rows("SELECT summary_json FROM martin_forward_runs WHERE strategy_id=%s AND session_date<%s AND summary_json->>'account_equity' IS NOT NULL AND summary_json->>'cumulative_r' IS NOT NULL ORDER BY session_date DESC,id DESC LIMIT 1", (book_id, day))
    opened = [row for row in rows if row[0] == "open"]
    tickers = sorted({str(row[1]).upper() for row in opened})
    frames = load_many_ticker_windows(tickers, day, 320, database_url=database_url) if tickers else {}
    realized_pnl = sum(float((row[2] or {}).get("realized_pnl") or 0) for row in rows)
    realized_r = sum(float((row[2] or {}).get("realized_r") or 0) for row in rows)
    unrealized_pnl = unrealized_r = 0.0
    covered = []
    for _, ticker, payload in opened:
        bar = bar_for(frames.get(str(ticker).upper()), day)
        if bar is None:
            continue
        payload = payload or {}
        shares = int(payload.get("remaining_shares") or 0)
        pnl = (bar["close"] - float(payload.get("entry_price") or 0)) * shares
        denominator = float(payload.get("initial_r_per_share") or 0) * int(payload.get("initial_shares") or payload.get("shares") or 1)
        unrealized_pnl += pnl
        unrealized_r += pnl / denominator if denominator else 0.0
        covered.append(str(ticker).upper())
    cumulative_r = realized_r + unrealized_r
    equity = 100000.0 + realized_pnl + unrealized_pnl
    prev = (previous_rows[0].get("summary_json") or {}) if previous_rows else {}
    peak_equity = max(100000.0, float(prev.get("equity_peak") or prev.get("account_equity") or 100000.0), equity)
    peak_r = max(0.0, float(prev.get("r_peak") or prev.get("cumulative_r") or 0.0), cumulative_r)
    complete = len(covered) == len(opened)
    return {"mark_date": day.isoformat(), "realized_pnl": realized_pnl, "unrealized_pnl": unrealized_pnl if complete else None,
        "realized_r": realized_r, "unrealized_r": unrealized_r if complete else None, "cumulative_r": cumulative_r if complete else None,
        "account_equity": equity if complete else None, "equity_peak": peak_equity if complete else None, "r_peak": peak_r if complete else None,
        "equity_drawdown_pct": (equity / peak_equity - 1) * 100 if complete else None,
        "r_drawdown": cumulative_r - peak_r if complete else None, "mark_coverage": {"open_positions": len(opened), "marked_positions": len(covered),
        "missing_tickers": sorted(set(tickers) - set(covered)), "status": "complete" if complete else "partial"}}


def continue_book(*, book_id: str, database_url: str, history: Any,
                  intraday_dir: str | Path | None = None) -> dict[str, Any]:
    """Apply exactly the next completed XNYS session to an existing execution book."""
    from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
    from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
    from src.webapp.services.martin_bar_retest_execution import MartinBarRetestExecutionService
    from src.webapp.services.martin_rotation import require_completed_session
    repository = MartinForwardTestRepository(database_url=database_url)
    latest = repository.latest_session_date(book_id)
    if latest is None:
        raise ValueError("Continuation requires an existing execution book; historical replay must create a fresh book first.")
    version_rows = repository._rows("SELECT summary_json FROM martin_forward_runs WHERE strategy_id=%s ORDER BY session_date DESC,id DESC LIMIT 1", (book_id,))
    version = (version_rows[0].get("summary_json") or {}).get("execution_book_version") if version_rows else None
    if version not in {"martin_bar_retest_execution_v1", "martin_bar_retest_execution_v2"}:
        raise ValueError(f"Book {book_id} is not a versioned Martin Bar Retest execution book.")
    day = xnys_sessions(latest + dt.timedelta(days=1), latest + dt.timedelta(days=30))[0]
    prior_run = repository.session_run_id(book_id, day)
    if prior_run is not None:
        return {"status": "already_processed", "book_id": book_id, "session_date": day.isoformat(), "run_id": prior_run}
    try:
        require_completed_session(day)
    except ValueError as exc:
        if "completed exchange session" not in str(exc):
            raise
        return {"status": "not_ready", "book_id": book_id,
                "session_date": latest.isoformat(), "next_session_date": day.isoformat(),
                "reason": "awaiting_next_completed_session"}
    intraday_source = None
    frozen_intraday: dict[str, dict[str, Any]] = {}
    if version == "martin_bar_retest_execution_v2":
        if intraday_dir is None:
            from src.webapp.config import load_webapp_config
            intraday_dir, frozen_intraday = auto_freeze_book_intraday(book_id=book_id, day=day,
                pending_orders=repository.pending_orders(strategy_id=book_id, before=day), database_url=database_url,
                artifacts_dir=load_webapp_config().artifacts_dir)
            intraday_source = "automatic_post_close_freeze"
        else:
            frozen_intraday = frozen_intraday_for(intraday_dir, day)
            intraday_source = "provided_frozen_bundle"
    source = None
    from scripts.run_martin_bar_retest import _load_source
    source = _load_source(history, day)
    with repository.atomic_session(book_id):
        latest = repository.latest_session_date(book_id)
        if latest is None or xnys_sessions(latest + dt.timedelta(days=1), latest + dt.timedelta(days=30))[0] != day:
            raise ValueError("Execution book advanced concurrently; retry to process its next session.")
        locked_version_rows = repository._rows("SELECT summary_json FROM martin_forward_runs WHERE strategy_id=%s ORDER BY session_date DESC,id DESC LIMIT 1", (book_id,))
        locked_version = (locked_version_rows[0].get("summary_json") or {}).get("execution_book_version") if locked_version_rows else None
        if locked_version != version:
            raise ValueError(f"Book {book_id} is not a versioned Martin Bar Retest execution book.")
        if source is None:
            run_id = save_execution_only_run(repository, book_id=book_id, day=day)
            actions = MartinBarRetestExecutionService(repository, database_url=database_url)
            actions.book_id = book_id
            actions.execution_version = version
            action_counts = actions.run_session(day=day, run_id=run_id, candidates=[], source_on_time=False,
                frozen_intraday=frozen_intraday)
            mode = "execution_only_no_saved_scan"
            result = None
        else:
            result = MartinBarRetestService(repository, database_url=database_url).run_session(
                as_of_date=day, source_record=source, dry_run=False, policy_id=POLICY_ID, book_id=book_id)
            if result.get("status") in {"missing_source", "blocked_source"}:
                raise ValueError(f"Persisted watchlist source failed validation for {day}: {result.get('reason') or result.get('status')}")
            run_id = result.get("run_id")
            if result.get("skipped"):
                return {"status": "already_processed", "book_id": book_id, "session_date": day.isoformat(), "run_id": run_id}
            executor = MartinBarRetestExecutionService(repository, database_url=database_url)
            executor.book_id = book_id
            executor.execution_version = version
            action_counts = executor.run_session(day=day, run_id=run_id, candidates=result.get("candidates", []),
                source_on_time=result.get("retrospective_source_scan") is False,
                frozen_intraday=frozen_intraday)
            mode = "persisted_watchlist"
        if action_counts.get("missing_bars"):
            mark_status = "partial"
        else:
            mark_status = "complete"
        marks = continuation_mark(repository, book_id, day, database_url)
        if mark_status == "partial":
            marks["mark_coverage"]["status"] = "partial"
            for key in ("unrealized_pnl", "unrealized_r", "cumulative_r", "account_equity", "equity_peak", "r_peak", "equity_drawdown_pct", "r_drawdown"):
                marks[key] = None
        repository.set_run_metadata(run_id, {"execution_book_version": version,
            "execution_mode": "forward_continuation", "source_mode": mode,
            **({"execution_management_version": "martin_bar_retest_management_v2", "trim_fraction": "1/3",
                "trim_share_rounding": "floor", "trim_target_r": 3, "additional_partial_trims": False}
               if version == "martin_bar_retest_execution_v2" else {}),
            "intraday_source": intraday_source, "intraday_bundle_dir": str(intraday_dir) if version == "martin_bar_retest_execution_v2" else None,
            "source_run_id": (source or {}).get("source_run_id"), "source_published_at": (source or {}).get("published_at"),
            "source_on_time": bool(result and result.get("retrospective_source_scan") is False),
            "execution_actions": action_counts, "partial": marks["mark_coverage"]["status"] != "complete" or action_counts.get("intraday_fallbacks", 0) > 0,
            "intraday_evidence_partial": action_counts.get("intraday_fallbacks", 0) > 0,
            "intraday_fallback_count": action_counts.get("intraday_fallbacks", 0), **marks})
    return {"status": "complete", "book_id": book_id, "session_date": day.isoformat(), "source_mode": mode,
            "execution_actions": action_counts, "intraday_source": intraday_source,
            "intraday_bundle_dir": str(intraday_dir) if version == "martin_bar_retest_execution_v2" else None, **marks}


def continue_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Continue an existing Martin Bar Retest execution book by one completed XNYS session.")
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--apply", action="store_true", help="Execute and persist one continuation session.")
    parser.add_argument("--intraday-dir", help="Directory of immutable audit JSON bundles required by v2.")
    args = parser.parse_args(argv)
    if not args.book_id.startswith("martin_range_bar_retest_") or args.book_id == POLICY_ID:
        parser.error("--book-id must name an isolated martin_range_bar_retest_ execution book")
    if not args.apply:
        print(json.dumps({"ready": True, "book_id": args.book_id, "mode": "preflight_only_no_writes",
                          "executed": False, "saved": False,
                          "detail": "Continuation was not evaluated; pass --apply to run it."}))
        return 0
    from src.webapp.config import load_webapp_config
    from src.webapp.repositories.history_repository import HistoryRepository
    from src.webapp.services.screener_history_service import ScreenerHistoryService
    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=True))
    result = continue_book(book_id=args.book_id, database_url=config.database_url, history=history,
                           intraday_dir=args.intraday_dir)
    print(json.dumps(result, sort_keys=True, default=str))
    return 0 if result["status"] in {"complete", "already_processed", "not_ready"} else 2


def execute_range(connection: Any, *, sessions: list[dt.date], sources: dict[dt.date, dict[str, Any]],
                  book_id: str, database_url: str, apply: bool, simulate_backdated_buys: bool = False,
                  execution_version: str = "martin_bar_retest_execution_v1",
                  intraday_dir: str | Path | None = None) -> list[dict[str, Any]]:
    from src.ticker_filters import leveraged_or_inverse_provenance, set_replay_fund_exclusions, reset_replay_fund_exclusions
    exclusion_provenance = leveraged_or_inverse_provenance()
    for source in sources.values():
        source.setdefault("replay_provenance", {})["fund_exclusions"] = {
            "version": exclusion_provenance["version"], "sha256": exclusion_provenance["sha256"]}
    token = set_replay_fund_exclusions(exclusion_provenance["symbols"])
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"martin-forward:{book_id}",))
            if not cursor.fetchone()[0]:
                raise RuntimeError(f"Replay book {book_id} is currently locked.")
        check_empty_book(connection, book_id)
        results = run_range(connection, sessions=sessions, sources=sources, book_id=book_id,
                            database_url=database_url, simulate_backdated_buys=simulate_backdated_buys,
                            exclusion_provenance=exclusion_provenance, execution_version=execution_version,
                            intraday_dir=intraday_dir)
        if apply:
            connection.commit()
        else:
            connection.rollback()
        return results
    except Exception:
        connection.rollback()
        raise
    finally:
        reset_replay_fund_exclusions(token)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay saved Martin Bar Retest scans and subsequent execution sessions into a fresh isolated book.")
    parser.add_argument("--from-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--to-date", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--book-id", required=True)
    parser.add_argument("--apply", action="store_true", help="Commit the full session range atomically; default simulates then rolls back.")
    parser.add_argument("--simulate-backdated-buys", action="store_true",
        help="Allow confirmed signals from scans saved after next-session open to schedule hypothetical historical fills.")
    parser.add_argument("--execution-version", choices=("v1", "v2"), default="v1",
        help="Isolated execution policy; v1 remains the default.")
    parser.add_argument("--intraday-dir", help="Directory of immutable audit JSON bundles required by v2.")
    args = parser.parse_args(argv)
    if args.to_date < args.from_date:
        parser.error("--to-date must be on or after --from-date")
    if not args.book_id.startswith("martin_range_bar_retest_") or args.book_id == POLICY_ID:
        parser.error("--book-id must start with martin_range_bar_retest_ and be separate from the policy ID")
    if args.execution_version == "v2" and not args.intraday_dir:
        parser.error("--intraday-dir is required for v2 frozen-bar execution")

    from src.webapp.config import load_webapp_config
    from src.webapp.repositories.history_repository import HistoryRepository
    from src.webapp.services.screener_history_service import ScreenerHistoryService
    import psycopg

    config = load_webapp_config()
    if not config.database_url:
        parser.error("TICKER_SCREENER_DATABASE_URL is required")
    sessions = xnys_sessions(args.from_date, args.to_date)
    history = ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir,
        repository=HistoryRepository(database_url=config.database_url, artifacts_dir=config.artifacts_dir, read_only=True))
    try:
        sources = load_range_sources(history, sessions)
    except Exception as exc:
        print(json.dumps({"ready": False, "book_id": args.book_id, "reason": "source_preflight_failed", "detail": str(exc)}))
        return 2

    with psycopg.connect(config.database_url) as connection:
        results = execute_range(connection, sessions=sessions, sources=sources, book_id=args.book_id,
                                database_url=config.database_url, apply=args.apply,
                                simulate_backdated_buys=args.simulate_backdated_buys,
                                execution_version=f"martin_bar_retest_execution_{args.execution_version}",
                                intraday_dir=args.intraday_dir)
    print(json.dumps({"ready": True, "policy_id": POLICY_ID, "execution_policy_id": f"martin_bar_retest_execution_{args.execution_version}", "book_id": args.book_id,
        "from_date": args.from_date.isoformat(), "to_date": args.to_date.isoformat(),
        "mode": "apply" if args.apply else "dry_run_rolled_back", "saved": bool(args.apply),
        "simulate_backdated_buys": args.simulate_backdated_buys,
        "backdated_buy_basis": "hypothetical_backdated_simulation" if args.simulate_backdated_buys else "forward_safe",
        "intraday_source": "frozen_tradingview_bundle" if args.execution_version == "v2" else None,
        "source_mode": "saved_screen_runs_with_execution_only_sessions", "sessions": results}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
