#!/usr/bin/env python3
"""Prepare and explicitly apply missing historical earnings event rows."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import time
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


START_DATE = dt.date(2015, 1, 1)
CSV_URL = "https://raw.githubusercontent.com/vivek-v-rao/Earnings-Dates/main/earnings_dates_all.csv"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default="")
    p.add_argument("--database-url", default="")
    p.add_argument("--csv-file", help="Use a local copy of Vivek V Rao's earnings_dates_all.csv")
    p.add_argument("--proposal", help="Proposal path. Dry runs default to a unique timestamped path; required with --apply.")
    p.add_argument("--apply", action="store_true", help="Insert the already-created proposal; default is dry run")
    p.add_argument("--allow-incomplete-apply", action="store_true", help="Explicitly allow applying a proposal with unresolved provider failures")
    p.add_argument("--limit", type=int, help="Limit active universe for a small run")
    p.add_argument("--tickers", nargs="+", help="Optional subset of active, non-excluded tickers for a pilot")
    return p.parse_args()


def _date(value: object) -> dt.date | None:
    try:
        return dt.date.fromisoformat(str(value).strip()[:10])
    except (ValueError, TypeError):
        return None


def _event(value: object) -> tuple[dt.date | None, str | None]:
    raw = str(value or "").strip()
    try:
        stamp = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return stamp.date(), None
        local = stamp.astimezone(ZoneInfo("America/New_York"))
        local_time = local.timetz().replace(tzinfo=None)
        session = "before_market" if local_time < dt.time(9, 30) else "after_market" if local_time >= dt.time(16) else "during_market"
        return local.date(), session
    except ValueError:
        return _date(raw), None


def _num(value: object) -> float | None:
    try:
        parsed = float(str(value).replace(",", "").strip())
        return parsed if parsed == parsed and abs(parsed) != float("inf") else None
    except (TypeError, ValueError):
        return None


def _first(row: dict[str, str], names: tuple[str, ...]) -> str:
    for name in names:
        for key, value in row.items():
            if key and key.strip().lower() == name.strip().lower() and value:
                return value.strip()
    lower = {k.strip().lower().replace(" ", "_"): v for k, v in row.items() if k}
    for name in names:
        normalized = name.strip().lower().replace(" ", "_")
        if lower.get(normalized):
            return lower[normalized].strip()
    return ""


def read_seed(path: Path, symbols: set[str]) -> list[dict[str, Any]]:
    rows = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ticker = _first(row, ("ticker", "symbol", "stock" )).upper().replace("-", ".")
            day, session = _event(_first(row, ("Earnings Date", "earnings_date_localized", "date", "report_date", "reported_date", "earnings_date")))
            eps_actual = _num(_first(row, ("reported_eps", "eps_actual")))
            if ticker in symbols and day and eps_actual is not None and START_DATE <= day <= dt.date.today():
                rows.append({"ticker": ticker, "earnings_date": day.isoformat(), "session_timing": session,
                             "source": "retrospective_backfill:vivek_v_rao_earnings_dates_csv", "eps_actual": eps_actual,
                             "eps_estimate": _num(_first(row, ("eps_estimate", "estimated_eps"))), "revenue_actual": None, "revenue_estimate": None})
    return rows


def _load_csv(path: Path | None) -> Path:
    if path:
        return path
    import requests
    response = requests.get(CSV_URL, timeout=60)
    response.raise_for_status()
    target = Path("/tmp/vivek_v_rao_earnings_dates_all.csv")
    target.write_bytes(response.content)
    return target


def fetch_yahoo(ticker: str) -> list[dict[str, Any]]:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed; install requirements.txt") from exc
    from src.earnings_enrichment import _parse_yahoo_timestamp
    frame = yf.Ticker(ticker).get_earnings_dates(limit=100)
    if frame is None or frame.empty:
        raise RuntimeError("Yahoo returned no earnings rows; treated as unknown (may be throttled or unavailable)")
    out = []
    for index, data in frame.iterrows():
        parsed = _parse_yahoo_timestamp(index)
        if not parsed:
            continue
        day, session = parsed
        actual_eps = _num(data.get("Reported EPS"))
        if not START_DATE <= day <= dt.date.today() or actual_eps is None:
            continue
        def num(key: str) -> float | None:
            try:
                value = float(data.get(key))
                return value if value == value and abs(value) != float("inf") else None
            except (TypeError, ValueError):
                return None
        out.append({"ticker": ticker, "earnings_date": day.isoformat(), "session_timing": session,
                    "source": "retrospective_backfill:yfinance_get_earnings_dates", "eps_actual": actual_eps,
                    "eps_estimate": num("EPS Estimate"), "revenue_actual": None, "revenue_estimate": None})
    return out


def fetch_with_retries(ticker: str, fetcher=fetch_yahoo, sleeper=time.sleep) -> tuple[list[dict[str, Any]], str | None, bool]:
    """Three total attempts; a final rate-limit response stops all further Yahoo requests."""
    for attempt in range(3):
        try:
            rows = fetcher(ticker)
            if not rows:
                return [], "Yahoo returned no usable rows; coverage is unknown", False
            return rows, None, False
        except Exception as exc:
            rate_limited = (getattr(getattr(exc, "response", None), "status_code", None) == 429
                            or "429" in str(exc) or "ratelimit" in type(exc).__name__.lower())
            if not rate_limited:
                return [], f"{type(exc).__name__}: {exc}", False
            if attempt == 2:
                return [], "Yahoo 429 after 3 total attempts; stopped Yahoo globally", True
            sleeper(2 ** attempt)
    raise AssertionError("unreachable")


def check_apply_allowed(payload: dict[str, Any], allow_incomplete: bool) -> None:
    if not payload.get("complete", False) and not allow_incomplete:
        raise ValueError("Proposal is incomplete; resolve failures and regenerate, or pass --allow-incomplete-apply explicitly.")


def _existing(connection, symbols: list[str]) -> set[tuple[str, str]]:
    with connection.cursor() as cur:
        cur.execute("SELECT ticker, earnings_date::text FROM earnings_events WHERE ticker = ANY(%s) AND earnings_date >= %s", (symbols, START_DATE))
        return {(str(t).upper(), str(d)[:10]) for t, d in cur.fetchall()}


def main() -> int:
    args = parse_args()
    from src.config import load_app_config
    from src.market_data_access import load_active_universe_from_db, resolve_database_url
    from src.ticker_filters import filter_universe_tickers, load_excluded_tickers
    if args.apply:
        if not args.proposal:
            raise SystemExit("--apply requires the exact --proposal path created by a dry run.")
        proposal = Path(args.proposal)
        if not proposal.exists():
            raise SystemExit(f"Frozen proposal not found: {proposal}; run without --apply first.")
        payload = json.loads(proposal.read_text())
        try:
            check_apply_allowed(payload, args.allow_incomplete_apply)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        result_path = proposal.with_suffix(proposal.suffix + ".applied.json")
        if result_path.exists():
            raise SystemExit(f"Apply result already exists; refusing to replay proposal: {result_path}")
        db = resolve_database_url(args.database_url)
        import psycopg
        inserted = 0
        with psycopg.connect(db) as conn, conn.cursor() as cur:
            for row in payload["proposed_rows"]:
                cur.execute("""INSERT INTO earnings_events (ticker, earnings_date, session_timing, source, eps_actual, eps_estimate, revenue_actual, revenue_estimate)
                    VALUES (%(ticker)s,%(earnings_date)s,%(session_timing)s,%(source)s,%(eps_actual)s,%(eps_estimate)s,%(revenue_actual)s,%(revenue_estimate)s)
                    ON CONFLICT (ticker, earnings_date) DO NOTHING""", row)
                inserted += max(cur.rowcount, 0)
            applied_payload = {"applied_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                               "inserted_rows": inserted, "proposed_rows": len(payload["proposed_rows"]),
                               "proposal": str(proposal.resolve())}
        result_path.write_text(json.dumps(applied_payload, indent=2) + "\n")
        print(f"Applied frozen proposal: inserted={inserted}, proposed={len(payload['proposed_rows'])}")
        return 0

    config = load_app_config(args.config or None)
    db = resolve_database_url(args.database_url)
    import psycopg
    with psycopg.connect(db) as conn:
        universe = load_active_universe_from_db(database_url=db)
        universe = filter_universe_tickers(universe, load_excluded_tickers(config))
        symbols = sorted({x.symbol.upper() for x in universe})
        if args.tickers:
            requested = {str(t).upper().replace("-", ".") for t in args.tickers}
            symbols = [symbol for symbol in symbols if symbol in requested]
        if args.limit is not None:
            symbols = symbols[:max(0, args.limit)]
        existing = _existing(conn, symbols)
    source_path = _load_csv(Path(args.csv_file) if args.csv_file else None)
    records = read_seed(source_path, set(symbols))
    seed_row_count = len(records)
    seed_last_date = {}
    for row in records:
        seed_last_date[row["ticker"]] = max(seed_last_date.get(row["ticker"], ""), row["earnings_date"])
    yahoo_stopped = False
    failures = []
    for idx, ticker in enumerate(symbols, 1):
        if yahoo_stopped or (ticker in seed_last_date and seed_last_date[ticker] >= (dt.date.today() - dt.timedelta(days=180)).isoformat()):
            continue
        rows, error, rate_limited = fetch_with_retries(ticker)
        records.extend(rows)
        if error:
            failures.append({"ticker": ticker, "error": error})
        if rate_limited:
            yahoo_stopped = True
        if idx % 100 == 0:
            print(f"Yahoo fallback progress {idx}/{len(symbols)}")
    unique = {}
    for row in records:
        key = (row["ticker"], row["earnings_date"])
        if key not in existing:
            if key not in unique:
                unique[key] = row
            else:
                prior = unique[key]
                for field, value in row.items():
                    if value is not None and value != "" and field != "source":
                        prior[field] = value
                if row["source"] not in prior["source"].split("+"):
                    prior["source"] += "+" + row["source"]
    unresolved = {failure["ticker"] for failure in failures}
    if yahoo_stopped:
        attempted = {failure["ticker"] for failure in failures}
        unresolved.update(t for t in symbols if t not in seed_last_date and t not in attempted)
    complete = not failures and not yahoo_stopped
    payload = {"created_at": dt.datetime.now(dt.timezone.utc).isoformat(), "start_date": START_DATE.isoformat(),
               "end_date": dt.date.today().isoformat(), "universe_count": len(symbols),
               "universe_source": "active ticker_metadata with daily_bars; configured exclusions applied",
        "seed_source": CSV_URL if not args.csv_file else str(Path(args.csv_file).resolve()),
               "yahoo_stopped_after_429": yahoo_stopped, "failures": failures, "unresolved_tickers": sorted(unresolved), "complete": complete,
               "counts": {"seed_rows": seed_row_count, "fetched_yahoo_rows": len(records) - seed_row_count,
                          "proposed_missing_rows": len(unique),
                          "existing_skipped": sum((r["ticker"], r["earnings_date"]) in existing for r in records),
                          "tickers_without_seed": len(set(symbols) - set(seed_last_date)),
                          "csv_only_missing_tickers": len(set(symbols) - set(seed_last_date)),
                          "yahoo_target_tickers": sum(t not in seed_last_date or seed_last_date[t] < (dt.date.today() - dt.timedelta(days=180)).isoformat() for t in symbols)},
               "caveat": "Fetched historical dates were not necessarily known at historical decision time; provider timestamps do not establish an exact release minute.",
               "proposed_rows": list(unique.values())}
    proposal = Path(args.proposal) if args.proposal else Path("artifacts/raw") / f"earnings_history_proposal_{dt.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
    if proposal.exists():
        raise SystemExit(f"Proposal already exists; refusing to overwrite: {proposal}")
    proposal.parent.mkdir(parents=True, exist_ok=True)
    proposal.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"Dry run: universe={len(symbols)} proposed={len(unique)} failures={len(failures)} proposal={proposal}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
