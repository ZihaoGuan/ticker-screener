#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.refresh_tradingview_history import calendar_sessions, fingerprint, validate_provider, write_json
from scripts.sync_postgres_market_data import _connect
from src.config import load_app_config
from src.split_refresh_reporting import RESULT_PREFIX, persistence_fields
from src.ticker_filters import is_excluded_ticker, load_excluded_tickers
from src.webapp.config import load_webapp_config


DEFAULT_STATE_PATH = PROJECT_ROOT / "artifacts" / "raw" / "nasdaq_split_events.json"
DEFAULT_TRADINGVIEW_CLIENT_DIR = Path(
    os.environ.get("TRADINGVIEW_CLIENT_DIR", "/opt/tradingview/node_modules/@mathieuc/tradingview")
)
BAR_COLUMNS = "ticker,trade_date,open,high,low,close,adj_close,volume,dividend,split_factor,source,updated_at"
OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


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
    parser.add_argument("--start-date", default="2020-01-01", help="Legacy option; rebase starts at the earliest stored daily bar.")
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


def _known_tickers(database_url: str, tickers: list[str]) -> dict[str, str | None]:
    if not tickers:
        return {}
    with _connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT ticker,exchange FROM ticker_metadata WHERE ticker = ANY(%s)", (tickers,))
            return {str(row[0]).upper(): row[1] for row in cursor.fetchall()}


def _latest_completed_session(as_of_date: dt.date, now: dt.datetime | None = None) -> dt.date:
    import exchange_calendars as xcals

    now = now or dt.datetime.now(dt.timezone.utc)
    calendar = xcals.get_calendar("XNYS", start=str(as_of_date - dt.timedelta(days=10)), end=str(as_of_date))
    sessions = calendar.sessions_in_range(str(as_of_date - dt.timedelta(days=10)), str(as_of_date))
    completed = [session.date() for session in sessions if calendar.session_close(session).to_pydatetime() <= now]
    if not completed:
        raise ValueError("no_completed_session")
    return max(completed)


def _exchange_symbol(ticker: str, exchange: str | None) -> str:
    exchanges = {"NYSE": "NYSE", "NASDAQ": "NASDAQ", "AMEX": "AMEX", "NYSEAMERICAN": "AMEX"}
    prefix = exchanges.get(str(exchange or "").upper())
    if not prefix:
        raise ValueError("unsupported_exchange")
    return f"{prefix}:{ticker}"


def _plan_split_rebase(
    rows: list[dict[str, object]], candles: dict[str, dict[str, object]], effective_date: str,
    end_date: str,
) -> list[str]:
    if not rows:
        raise ValueError("no_stored_history")
    dates = {str(row["trade_date"]) for row in rows}
    missing = sorted(dates - candles.keys())
    if missing:
        raise ValueError("history_coverage_loss")
    if max(candles) != end_date:
        raise ValueError("latest_completed_session_missing")
    old_by_date = {str(row["trade_date"]): row for row in rows}
    post_split = sorted(day for day in dates if day >= effective_date and day in candles)
    if not post_split:
        raise ValueError("post_split_price_basis_unverified")
    # Prices after the effective date must already share the current split basis.
    comparison_day = post_split[-1]
    old_close = old_by_date[comparison_day].get("close")
    new_close = candles[comparison_day].get("close")
    if old_close is None or new_close is None or Decimal(str(old_close)) <= 0:
        raise ValueError("post_split_price_basis_unverified")
    if abs(Decimal(str(new_close)) / Decimal(str(old_close)) - 1) > Decimal("0.02"):
        raise ValueError("post_split_price_basis_mismatch")
    return sorted(candles)


def _load_daily_rows(connection, ticker: str, *, lock: bool = False) -> list[dict[str, object]]:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {BAR_COLUMNS} FROM daily_bars WHERE ticker=%s ORDER BY trade_date" + (" FOR UPDATE" if lock else ""), (ticker,))
        return cursor.fetchall()


def _connect_rows(database_url: str):
    import psycopg
    from psycopg.rows import dict_row

    return psycopg.connect(database_url, row_factory=dict_row, options="-c statement_timeout=60000 -c lock_timeout=5000")


def _apply_split_rebase(
    database_url: str, ticker: str, baseline: list[dict[str, object]],
    candles: dict[str, dict[str, object]], config, effective_date: str, end_date: str,
) -> tuple[int, list[dict[str, object]]]:
    with _connect_rows(database_url) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("split-adjusted-history:" + ticker,))
                cursor.execute("SELECT is_active FROM ticker_metadata WHERE ticker=%s FOR SHARE", (ticker,))
                metadata = cursor.fetchone()
            if not metadata or metadata["is_active"] is False or is_excluded_ticker(ticker, load_excluded_tickers(config)):
                raise ValueError("no_longer_scan_eligible")
            current = _load_daily_rows(connection, ticker, lock=True)
            if fingerprint(current) != fingerprint(baseline):
                raise ValueError("concurrent_history_change")
            _plan_split_rebase(current, candles, effective_date, end_date)
            before_by_date = {str(row["trade_date"]): row for row in current}
            updated_at = dt.datetime.now(dt.timezone.utc)
            upserts = []
            for day in sorted(candles):
                bar = candles[day]
                old = before_by_date.get(day)
                upserts.append((
                    ticker, dt.date.fromisoformat(day), *(bar[column] for column in OHLCV_COLUMNS),
                    # TradingView returns split-adjusted OHLC, so stale dividend-adjusted close cannot be retained.
                    bar["close"], old.get("dividend", 0) if old else 0,
                    old.get("split_factor", 1) if old else 1, "tradingview", updated_at,
                ))
            with connection.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO daily_bars (
                         ticker,trade_date,open,high,low,close,volume,adj_close,dividend,split_factor,source,updated_at
                       ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (ticker,trade_date) DO UPDATE SET
                         open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,
                         volume=EXCLUDED.volume,adj_close=EXCLUDED.adj_close,
                         source=EXCLUDED.source,updated_at=EXCLUDED.updated_at""",
                    upserts,
                )
                persisted = cursor.rowcount
            after = _load_daily_rows(connection, ticker)
            if len(after) < len(current) or any(day not in {str(row["trade_date"]) for row in after} for day in before_by_date):
                raise RuntimeError("coverage_verification_failed")
            after_by_date = {str(row["trade_date"]): row for row in after}
            for day, bar in candles.items():
                row = after_by_date.get(day)
                if not row or row["source"] != "tradingview" or any(row[column] != bar[column] for column in OHLCV_COLUMNS):
                    raise RuntimeError("persisted_candle_mismatch")
                if row["adj_close"] != bar["close"]:
                    raise RuntimeError("persisted_adjusted_close_mismatch")
                old = before_by_date.get(day)
                if old and any(row[field] != old[field] for field in ("dividend", "split_factor")):
                    raise RuntimeError("corporate_actions_changed")
            if persisted != len(candles):
                raise RuntimeError("persisted_rowcount_mismatch")
        return persisted, after


def _refresh_ticker(
    ticker: str, effective_date: str, end_date: str, database_url: str, state_path: Path,
    *, exchange: str, config, client_dir: Path = DEFAULT_TRADINGVIEW_CLIENT_DIR,
) -> dict[str, object]:
    evidence_dir = state_path.parent / "split_refresh" / ticker / effective_date
    try:
        symbol = _exchange_symbol(ticker, exchange)
        with _connect_rows(database_url) as connection:
            baseline = _load_daily_rows(connection, ticker)
        if not baseline:
            raise ValueError("no_stored_history")
        start_date = min(str(row["trade_date"]) for row in baseline)
        node = os.environ.get("NODE_BINARY", "node")
        fetched = subprocess.run(
            [node, str(PROJECT_ROOT / "scripts" / "fetch_tradingview_history.mjs"), str(client_dir), symbol, start_date, end_date],
            cwd=PROJECT_ROOT, check=False, capture_output=True, text=True, timeout=75,
        )
        if fetched.returncode != 0:
            raise ValueError(f"tradingview_client_exit_{fetched.returncode}")
        payload = json.loads(fetched.stdout)
        sessions = calendar_sessions(start_date, end_date)
        candles = validate_provider(payload, symbol, start_date, end_date, sessions)
        ordered_dates = _plan_split_rebase(baseline, candles, effective_date, end_date)
        write_json(evidence_dir / "provider.json.gz", payload)
        write_json(evidence_dir / "before.json.gz", baseline)
        persisted, after = _apply_split_rebase(database_url, ticker, baseline, candles, config, effective_date, end_date)
        try:
            write_json(evidence_dir / "after.json.gz", after)
        except OSError:
            pass  # The transaction already verified and committed the exact persisted rows.
        return {
            "ticker": ticker, "status": "synced_split_adjusted", "source": "tradingview",
            "rows_persisted": persisted, "history_retained": False,
            "first_trade_date": ordered_dates[0], "last_trade_date": ordered_dates[-1],
            "baseline_sha256": fingerprint(baseline), "persisted_sha256": fingerprint(after),
            "resolved_symbol": (payload.get("metadata") or {}).get("full_name"),
            "retrieved_at": payload.get("retrieved_at"),
        }
    except Exception as exc:
        reason = str(exc) if isinstance(exc, ValueError) and str(exc).replace("_", "").isalnum() else type(exc).__name__
        return {"ticker": ticker, "status": "failed", "source": "tradingview", "rows_persisted": 0,
                "history_retained": True, "reason": reason}


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
    config = load_app_config()
    excluded = load_excluded_tickers(config)

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
        completed_session = _latest_completed_session(as_of_date)
        outcome = _refresh_ticker(
            ticker, str(event["execution_date"]), completed_session.isoformat(), database_url, state_path,
            exchange=known[ticker], config=config,
        )
        results.append(outcome)
        if str(outcome["status"]).startswith("synced") and int(outcome.get("rows_persisted") or 0) > 0:
            event["status"] = "completed"
            event["processed_at"] = now
            event["message"] = "Daily bars rebased to TradingView split-adjusted history and verified after commit."
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
