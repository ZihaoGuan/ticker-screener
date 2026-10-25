#!/usr/bin/env python3
"""Fill specific missing daily sessions from a validated TradingView history cache.

The existing history refresh replaces Yahoo-labelled rows but deliberately never
inserts dates. This companion uses its provider validator and applies only dates
named on the command line. Dry-run is the default; apply requires its frozen
dry-run artifacts. Existing candles are never updated.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
from decimal import Decimal
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import refresh_tradingview_history as tv
from src.config import load_app_config
from src.ticker_filters import is_excluded_ticker
from src.universe import load_universe

PRICE_FIELDS = ("open", "high", "low", "close")
MAX_BASIS_DIFFERENCE = Decimal("0.005")


def _completed_sessions(dates: list[str], end_date: str) -> None:
    import exchange_calendars as xcals

    end = dt.date.fromisoformat(end_date)
    calendar = xcals.get_calendar("XNYS")
    now = dt.datetime.now(dt.timezone.utc)
    for value in [end_date, *dates]:
        day = dt.date.fromisoformat(value)
        if day > end or not calendar.is_session(value) or calendar.session_close(value).to_pydatetime() > now:
            raise ValueError(f"{value} is not a completed XNYS session through {end_date}")


@lru_cache(maxsize=32)
def _sessions(start: str, end: str) -> frozenset[str]:
    return frozenset(tv.calendar_sessions(start, end))


def _provider_candles(path: Path, symbol: str, end_date: str) -> dict[str, dict[str, object]]:
    payload = tv.read_json(path)
    query = payload.get("query") or {}
    start = str(query.get("from") or "")[:10]
    end = str(query.get("to") or "")[:10]
    if not start or end != end_date:
        raise ValueError("provider_cache_range_mismatch")
    return tv.validate_provider(payload, symbol, start, end, _sessions(start, end))


def plan_inserts(
    rows: list[dict[str, object]], candles: dict[str, dict[str, object]], dates: list[str],
    *, known_split_dates: set[str] | None = None,
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """Require two matching TradingView anchors on each side of every new bar."""
    by_day = {str(row["trade_date"]): row for row in rows}
    if len(by_day) != len(rows):
        raise ValueError("duplicate_stored_session")
    first, last = min(by_day, default=""), max(by_day, default="")
    planned: dict[str, dict[str, object]] = {}
    blockers: dict[str, str] = {}
    for day in dates:
        if day in (known_split_dates or set()):
            blockers[day] = "known_split_event"
            continue
        if day in by_day:
            blockers[day] = "already_present"
            continue
        if day not in candles:
            blockers[day] = "provider_missing_session"
            continue
        if not first or day <= first or day >= last:
            blockers[day] = "outside_stored_history"
            continue
        nearby = [
            row for row in rows
            if row["source"] == "tradingview"
            and str(row["trade_date"]) in candles
            and abs((dt.date.fromisoformat(str(row["trade_date"])) - dt.date.fromisoformat(day)).days) <= 30
        ]
        before = sorted((row for row in nearby if str(row["trade_date"]) < day),
                        key=lambda row: str(row["trade_date"]), reverse=True)[:2]
        after = sorted((row for row in nearby if str(row["trade_date"]) > day),
                       key=lambda row: str(row["trade_date"]))[:2]
        if len(before) < 2 or len(after) < 2:
            blockers[day] = "insufficient_tradingview_basis_anchors"
            continue
        mismatch = False
        for row in [*before, *after]:
            provider = candles[str(row["trade_date"])]
            for field in PRICE_FIELDS:
                stored_price = Decimal(str(row[field]))
                provider_price = Decimal(str(provider[field]))
                if stored_price <= 0 or abs(stored_price / provider_price - 1) > MAX_BASIS_DIFFERENCE:
                    mismatch = True
                    break
            if mismatch:
                break
        if mismatch:
            blockers[day] = "adjustment_basis_mismatch"
            continue
        planned[day] = candles[day]
    return planned, blockers


def _apply_one(ticker: str, end_date: str, baseline_hash: str, provider_hash: str,
               dates: list[str], symbol: str, cache_path: Path, config, backup_path: Path,
               known_split_dates: set[str]) -> int:
    if hashlib.sha256(cache_path.read_bytes()).hexdigest() != provider_hash:
        raise ValueError("provider_cache_changed")
    candles = _provider_candles(cache_path, symbol, end_date)
    with tv.connect() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("tradingview-history:" + ticker,))
                cursor.execute("SELECT is_active FROM ticker_metadata WHERE ticker=%s FOR SHARE", (ticker,))
                metadata = cursor.fetchone()
            if not metadata or metadata["is_active"] is False or is_excluded_ticker(ticker, tv.exclusions(config)):
                raise ValueError("no_longer_scan_eligible")
            current = tv.load_rows(connection, ticker, end_date, lock=True)
            if tv.fingerprint(current) != baseline_hash:
                raise ValueError("changed_since_dry_run")
            planned, _ = plan_inserts(current, candles, dates, known_split_dates=known_split_dates)
            if not planned:
                raise ValueError("no_missing_validated_sessions")
            tv.write_json(backup_path, {"ticker": ticker, "before_sha256": baseline_hash,
                                        "provider_sha256": provider_hash, "after_sha256": None,
                                        "inserted_dates": sorted(planned)})
            now = dt.datetime.now(dt.timezone.utc)
            with connection.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO daily_bars
                       (ticker,trade_date,open,high,low,close,volume,adj_close,dividend,split_factor,source,updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,1,'tradingview',%s)
                       ON CONFLICT (ticker,trade_date) DO NOTHING""",
                    [(ticker, dt.date.fromisoformat(day), *(bar[field] for field in PRICE_FIELDS),
                      bar["volume"], bar["close"], now) for day, bar in sorted(planned.items())],
                )
                inserted = cursor.rowcount
            if inserted != len(planned):
                raise RuntimeError("inserted_rowcount_mismatch")
            after = tv.load_rows(connection, ticker, end_date)
            after_by_day = {str(row["trade_date"]): row for row in after}
            if len(after) != len(current) + inserted or any(after_by_day[str(row["trade_date"])] != row for row in current):
                raise RuntimeError("existing_history_changed")
            for day, bar in planned.items():
                row = after_by_day[day]
                if (row["source"] != "tradingview" or row["adj_close"] != bar["close"]
                        or any(row[field] != bar[field] for field in (*PRICE_FIELDS, "volume"))):
                    raise RuntimeError("persisted_candle_mismatch")
            tv.write_json(backup_path, {"ticker": ticker, "before_sha256": baseline_hash,
                                        "provider_sha256": provider_hash,
                                        "after_sha256": tv.fingerprint(after),
                                        "inserted_dates": sorted(planned),
                                        "inserted_rows": [after_by_day[day] for day in sorted(planned)]})
    return inserted


def _rollback_one(ticker: str, end_date: str, backup_path: Path) -> int:
    backup = tv.read_json(backup_path)
    if not backup.get("after_sha256"):
        raise ValueError("missing_applied_backup")
    with tv.connect() as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("tradingview-history:" + ticker,))
            current = tv.load_rows(connection, ticker, end_date, lock=True)
            if tv.fingerprint(current) != backup["after_sha256"]:
                raise ValueError("changed_since_apply")
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM daily_bars WHERE ticker=%s AND trade_date=ANY(%s)",
                               (ticker, [dt.date.fromisoformat(day) for day in backup["inserted_dates"]]))
                removed = cursor.rowcount
            if removed != len(backup["inserted_dates"]) or tv.fingerprint(tv.load_rows(connection, ticker, end_date)) != backup["before_sha256"]:
                raise RuntimeError("rollback_verification_failed")
    return removed


def _process(item: dict[str, str], args, config, split_events: set[tuple[str, str]]) -> dict[str, object]:
    ticker = item["ticker"]
    folder = Path(args.artifacts_dir) / ticker
    outcome_path = folder / ("rollback.json" if args.rollback else "apply.json" if args.apply else "dry_run.json")
    if outcome_path.exists():
        return tv.read_json(outcome_path)
    result: dict[str, object] = {"ticker": ticker, "mode": "rollback" if args.rollback else "apply" if args.apply else "dry-run"}
    known_split_dates = {day for symbol, day in split_events if symbol == ticker}
    try:
        backup_path = folder / "backup.json.gz"
        if args.rollback:
            result.update(status="rolled_back", rows_removed=_rollback_one(ticker, args.end_date, backup_path))
        else:
            cache_path = Path(args.provider_cache_root) / ticker / "provider.json.gz"
            if not cache_path.is_file():
                raise ValueError("provider_cache_missing")
            provider_hash = hashlib.sha256(cache_path.read_bytes()).hexdigest()
            if args.apply:
                frozen = tv.read_json(folder / "dry_run.json")
                if frozen.get("status") != "ready":
                    result.update(status="skipped", reason="dry_run_not_ready")
                elif frozen["provider_sha256"] != provider_hash:
                    raise ValueError("provider_cache_changed")
                else:
                    inserted = _apply_one(ticker, args.end_date, frozen["baseline_sha256"], provider_hash,
                                          args.dates, item["symbol"], cache_path, config, backup_path,
                                          known_split_dates)
                    result.update(status="applied", rows_persisted=inserted,
                                  inserted_dates=frozen["planned_dates"])
            else:
                with tv.connect(read_only=True) as connection:
                    baseline = tv.load_rows(connection, ticker, args.end_date)
                candles = _provider_candles(cache_path, item["symbol"], args.end_date)
                planned, blockers = plan_inserts(baseline, candles, args.dates,
                                                 known_split_dates=known_split_dates)
                result.update(status="ready" if planned else "skipped",
                              baseline_sha256=tv.fingerprint(baseline), provider_sha256=provider_hash,
                              planned_dates=sorted(planned), blocked_dates=blockers,
                              reason=None if planned else "no_missing_validated_sessions")
    except Exception as exc:
        result.update(status="skipped", reason=str(exc) if isinstance(exc, ValueError) else type(exc).__name__)
    tv.write_json(outcome_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-results-file", required=True)
    parser.add_argument("--provider-cache-root", required=True)
    parser.add_argument("--split-events-file", required=True,
                        help="Dated split-event artifact; a matching ticker/date is left for split repair.")
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--tickers", nargs="*")
    parser.add_argument("--workers", type=int, choices=(1, 2, 3, 4), default=2)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    if args.apply and args.rollback:
        parser.error("--apply and --rollback are mutually exclusive")
    args.dates = sorted(set(args.dates))
    _completed_sessions(args.dates, args.end_date)
    raw = tv.read_json(Path(args.raw_results_file))
    if raw.get("as_of_date") != args.end_date:
        parser.error("raw result date does not match --end-date")
    blocked = {str(row["ticker"]).upper() for row in raw.get("failed_tickers", [])
               if row.get("error") == "missing_required_market_session"}
    cache_scope = tv.read_json(Path(args.provider_cache_root) / "scope.json")
    if cache_scope.get("end_date") != args.end_date:
        parser.error("provider cache end date does not match --end-date")
    split_path = Path(args.split_events_file)
    split_raw = split_path.read_bytes()
    split_events = {(str(row.get("ticker") or "").upper(), str(row.get("execution_date") or ""))
                    for row in json.loads(split_raw).get("events", [])}
    config = load_app_config()
    with tv.connect(read_only=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT ticker,COALESCE(is_active,TRUE) AS active FROM ticker_metadata")
            metadata = {row["ticker"]: row["active"] for row in cursor.fetchall()}
    allowed = {row["ticker"]: row for row in tv.select_universe(load_universe(config), metadata, tv.exclusions(config))}
    requested = {ticker.upper() for ticker in args.tickers} if args.tickers else blocked
    items = sorted((item for item in cache_scope["tickers"]
                    if item["ticker"] in blocked & requested & allowed.keys()), key=lambda item: item["ticker"])
    scope = {"raw_results_file": args.raw_results_file, "provider_cache_root": args.provider_cache_root,
             "split_events_file": args.split_events_file,
             "split_events_sha256": hashlib.sha256(split_raw).hexdigest(),
             "end_date": args.end_date, "dates": args.dates, "tickers": args.tickers, "items": items}
    root = Path(args.artifacts_dir)
    root.mkdir(parents=True, exist_ok=True)
    scope_path = root / "scope.json"
    if scope_path.exists() and tv.read_json(scope_path) != scope:
        parser.error("artifact directory belongs to a different repair scope")
    if not scope_path.exists():
        tv.write_json(scope_path, scope)
    if args.apply:
        dry_summary_path = root / "dry_run_summary.json"
        if not dry_summary_path.exists() or tv.read_json(dry_summary_path).get("processed") != len(items):
            parser.error("a completed dry run for this exact scope is required before --apply")
    print(f"eligible_tickers={len(items)} outside_eligible_scope={len(blocked & requested) - len(items)} "
          f"mode={'rollback' if args.rollback else 'apply' if args.apply else 'dry-run'}", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_process, item, args, config, split_events) for item in items]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            counts = Counter(row["status"] for row in results)
            if len(results) % 50 == 0 or len(results) == len(items):
                print(f"processed={len(results)}/{len(items)} statuses={dict(counts)}", flush=True)
            if len(results) % 25 == 0 or len(results) == len(items):
                tv.write_json(root / ("rollback_summary.json" if args.rollback else "apply_summary.json" if args.apply else "dry_run_summary.json"),
                              {"processed": len(results), "universe": len(items), "results": results})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
