#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import uuid
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.sync_postgres_market_data import _connect
from src.config import load_app_config
from src.split_refresh_reporting import RESULT_PREFIX, persistence_fields
from src.ticker_filters import is_excluded_ticker, load_excluded_tickers
from src.webapp.config import load_webapp_config


DEFAULT_STATE_PATH = PROJECT_ROOT / "artifacts" / "raw" / "nasdaq_split_events.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover Nasdaq split announcements and replace affected tracked ticker history on the effective date."
    )
    parser.add_argument("--as-of-date", default="", help="Date to evaluate in America/New_York (YYYY-MM-DD).")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Include announcements with effective dates from this many calendar days before --as-of-date.",
    )
    parser.add_argument("--start-date", default="2020-01-01", help="Full-history refresh start date (YYYY-MM-DD).")
    parser.add_argument("--database-url", default="", help="Optional Postgres connection string override.")
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="Persistent split announcement state JSON.")
    return parser.parse_args()


def _parse_calendar_rows(payload: dict[str, object]) -> list[dict[str, str]]:
    data = payload.get("data")
    rows = data.get("rows") if isinstance(data, dict) else None
    if rows is None:
        raise ValueError("Nasdaq split calendar response is missing data.rows")
    events: list[dict[str, str]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("symbol") or "").strip().upper()
        ratio = str(row.get("ratio") or "").strip()
        raw_date = str(row.get("executionDate") or "").strip()
        if not ticker or not ratio or not raw_date:
            continue
        try:
            execution_date = dt.datetime.strptime(raw_date, "%m/%d/%Y").date().isoformat()
        except ValueError:
            continue
        events.append(
            {
                "ticker": ticker,
                "name": str(row.get("name") or "").strip(),
                "ratio": ratio,
                "execution_date": execution_date,
            }
        )
    return events


def _fetch_calendar(as_of_date: dt.date) -> list[dict[str, str]]:
    query = urllib_parse.urlencode({"date": as_of_date.isoformat()})
    request = urllib_request.Request(
        f"https://api.nasdaq.com/api/calendar/splits?{query}",
        headers={
            "accept": "application/json, text/plain, */*",
            "referer": "https://www.nasdaq.com/market-activity/stock-splits",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/109 Safari/537.36",
        },
    )
    with urllib_request.urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return _parse_calendar_rows(payload)


def _calendar_start(as_of_date: dt.date, lookback_days: int) -> dt.date:
    if lookback_days < 0:
        raise ValueError("lookback days must not be negative")
    return as_of_date - dt.timedelta(days=lookback_days)


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"events": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"events": []}
    return payload if isinstance(payload, dict) else {"events": []}


def _merge_events(state: dict[str, object], announcements: list[dict[str, str]], discovered_at: str) -> list[dict[str, object]]:
    existing_rows = state.get("events")
    if not isinstance(existing_rows, list):
        existing_rows = []
    existing = {
        (str(row.get("ticker")), str(row.get("execution_date"))): dict(row)
        for row in existing_rows if isinstance(row, dict)
    }
    for announcement in announcements:
        key = (announcement["ticker"], announcement["execution_date"])
        event = existing.get(key, {})
        if event.get("ratio") and event.get("ratio") != announcement["ratio"]:
            event["status"] = "pending"
            event.pop("processed_at", None)
        event.update(announcement)
        event.setdefault("status", "pending")
        event.setdefault("discovered_at", discovered_at)
        event["last_seen_at"] = discovered_at
        existing[key] = event
    return sorted(existing.values(), key=lambda row: (str(row.get("execution_date")), str(row.get("ticker"))))


def _write_state(path: Path, events: list[dict[str, object]], updated_at: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps({"updated_at": updated_at, "events": events}, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _known_tickers(database_url: str, tickers: list[str]) -> set[str]:
    if not tickers:
        return set()
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT ticker FROM ticker_metadata WHERE ticker = ANY(%s)", (tickers,))
            return {str(row[0]).upper() for row in cursor.fetchall()}


def _refresh_ticker(ticker: str, start_date: str, end_date: str, state_path: Path) -> dict[str, object]:
    manifest_path = state_path.parent / "split_refresh" / f"{uuid.uuid4().hex}.json"
    command = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "sync_postgres_market_data.py"),
        "--tickers",
        ticker,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--chunk-size",
        "1",
        "--replace-existing-history",
        "--manifest-path",
        str(manifest_path),
    ]
    unknown = {"ticker": ticker, "status": "failed_unconfirmed", "rows_persisted": None,
               "reason": "Child refresh did not provide valid persistence evidence.", "history_retained": None}
    try:
        result = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows = manifest.get("ticker_results", [])
        row = next((row for row in rows if row.get("ticker") == ticker), None)
        if row is None or type(row.get("rows_persisted")) is not int or row["rows_persisted"] < 0:
            return unknown
        if result.returncode != 0 and str(row.get("status", "")).startswith("synced"):
            row = {**row, "status": "failed_child_exit", "reason": "Child exited after committing price rows."}
        return row
    except (OSError, ValueError, TypeError, AttributeError):
        return unknown


def main() -> int:
    args = parse_args()
    as_of_date = dt.date.fromisoformat(args.as_of_date) if args.as_of_date else dt.datetime.now(ZoneInfo("America/New_York")).date()
    dt.date.fromisoformat(args.start_date)
    calendar_start = _calendar_start(as_of_date, args.lookback_days)
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    state_path = Path(args.state_path)
    state = _load_state(state_path)
    announcements = _fetch_calendar(calendar_start)
    events = _merge_events(state, announcements, now)
    _write_state(state_path, events, now)
    print(
        f"split_calendar_start={calendar_start.isoformat()} split_announcements={len(announcements)} tracked_events={len(events)}",
        flush=True,
    )

    due = [
        event for event in events
        if str(event.get("status") or "pending") in {"pending", "failed"}
        and dt.date.fromisoformat(str(event["execution_date"])) <= as_of_date
    ]
    database_url = (args.database_url or load_webapp_config().database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured.")
    known = _known_tickers(database_url, sorted({str(event["ticker"]) for event in due}))
    excluded = load_excluded_tickers(load_app_config())

    failures = 0
    refreshed = 0
    ignored = 0
    results: list[dict[str, object]] = []
    for event in due:
        ticker = str(event["ticker"])
        if is_excluded_ticker(ticker, excluded):
            event["status"] = "ignored_excluded"
            event["processed_at"] = now
            event["message"] = "Ticker is excluded from screener and market-data workflows."
            print(f"split_ignored ticker={ticker} reason=excluded", flush=True)
            ignored += 1
            results.append({"ticker": ticker, "status": "ignored_excluded", "rows_persisted": 0,
                            "reason": event["message"]})
            continue
        if ticker not in known:
            event["status"] = "ignored_not_tracked"
            event["processed_at"] = now
            event["message"] = "Ticker is not present in ticker_metadata."
            print(f"split_ignored ticker={ticker} reason=not_tracked", flush=True)
            ignored += 1
            results.append({"ticker": ticker, "status": "ignored_not_tracked", "rows_persisted": 0,
                            "reason": event["message"]})
            continue
        print(
            f"split_refresh ticker={ticker} effective={event['execution_date']} ratio={event['ratio']}",
            flush=True,
        )
        outcome = _refresh_ticker(ticker, args.start_date, as_of_date.isoformat(), state_path)
        results.append(outcome)
        if str(outcome["status"]).startswith("synced") and int(outcome.get("rows_persisted") or 0) > 0:
            event["status"] = "completed"
            event["processed_at"] = now
            event["message"] = "Existing daily bars replaced after a successful full-history download."
            refreshed += 1
        else:
            event["status"] = "failed"
            event["message"] = str(outcome.get("reason") or "Refresh failed; persistence is unconfirmed.")
            failures += 1
    _write_state(state_path, events, now)
    print(f"split_due={len(due)} split_refreshed={refreshed} split_ignored={ignored} split_failed={failures}", flush=True)
    print(RESULT_PREFIX + json.dumps(persistence_fields(results)["split_refresh_summary"]), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
