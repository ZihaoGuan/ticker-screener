#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.market_data_access import load_daily_bars_frame_from_db, resolve_database_url
from src.ticker_filters import load_excluded_tickers
from src.trendline_snapshots import build_snapshot_rows_for_range, upsert_trendline_snapshot_rows
from src.universe import UniverseTicker, load_universe


HISTORY_FLOOR_DATE = dt.date(1990, 1, 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill or refresh key trendline snapshots in Postgres.")
    parser.add_argument("--config", default="", help="Optional market config path.")
    parser.add_argument("--start-date", default="2020-01-01", help="Inclusive snapshot start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=today_label(), help="Inclusive snapshot end date (YYYY-MM-DD).")
    parser.add_argument(
        "--incremental-days",
        type=int,
        default=0,
        help="If set, ignore --start-date and refresh only the last N calendar days through --end-date.",
    )
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list instead of configured universe.")
    parser.add_argument(
        "--include-excluded-tickers",
        action="store_true",
        help="When used with --tickers, allow explicit repair/backfill of tickers that are currently excluded.",
    )
    parser.add_argument("--database-url", default="", help="Optional Postgres connection string override.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Rows per Postgres executemany batch.")
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Apply sql/postgres_app_schema.sql before backfilling.",
    )
    parser.add_argument("--manifest-path", default="", help="Optional explicit path for a JSON summary manifest.")
    return parser.parse_args()


def _resolve_date_window(args: argparse.Namespace) -> tuple[dt.date, dt.date]:
    end_date = dt.date.fromisoformat(str(args.end_date))
    if int(args.incremental_days) > 0:
        start_date = end_date - dt.timedelta(days=int(args.incremental_days))
        return start_date, end_date
    return dt.date.fromisoformat(str(args.start_date)), end_date


def _manual_tickers(symbols: list[str], excluded: set[str], *, include_excluded: bool) -> list[UniverseTicker]:
    seen: set[str] = set()
    tickers: list[UniverseTicker] = []
    for raw in symbols:
        symbol = str(raw).strip().upper()
        if not symbol or symbol in seen:
            continue
        if not include_excluded and symbol in excluded:
            continue
        seen.add(symbol)
        tickers.append(UniverseTicker(symbol=symbol))
    return tickers


def _ensure_schema(database_url: str) -> None:
    if not database_url:
        return
    try:
        import psycopg
    except ImportError:
        return
    schema_path = PROJECT_ROOT / "sql" / "postgres_app_schema.sql"
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema_path.read_text(encoding="utf-8"))
        connection.commit()


def _manifest_default_path() -> Path:
    return PROJECT_ROOT / "artifacts" / "raw" / f"trendline_snapshot_backfill_{today_label()}.json"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    database_url = resolve_database_url(args.database_url)
    if not database_url:
        raise SystemExit("TICKER_SCREENER_DATABASE_URL is required for trendline snapshot backfill.")

    if args.ensure_schema:
        _ensure_schema(database_url)

    config = load_app_config(args.config or None)
    excluded = load_excluded_tickers(config)
    start_date, end_date = _resolve_date_window(args)
    tickers = (
        _manual_tickers(args.tickers, excluded, include_excluded=bool(args.include_excluded_tickers))
        if args.tickers
        else load_universe(config, limit=args.limit)
    )
    total_tickers = len(tickers)
    total_rows = 0
    processed_tickers = 0
    skipped_tickers = 0
    failures: list[dict[str, str]] = []

    print(
        "starting trendline snapshot backfill: "
        f"total={total_tickers}, "
        f"start={start_date.isoformat()}, "
        f"end={end_date.isoformat()}"
    )

    for position, ticker in enumerate(tickers, start=1):
        print(f"[{position}/{total_tickers}] computing {ticker.symbol} | stored_rows={total_rows}")
        try:
            frame = load_daily_bars_frame_from_db(
                ticker.symbol,
                HISTORY_FLOOR_DATE,
                end_date,
                database_url=database_url,
            )
            if frame is None or getattr(frame, "empty", False):
                skipped_tickers += 1
                print(f"[{position}/{total_tickers}] {ticker.symbol} skipped: missing daily_bars | stored_rows={total_rows}")
                continue

            rows = build_snapshot_rows_for_range(
                ticker.symbol,
                frame,
                start_date=start_date,
                end_date=end_date,
            )
            if not rows:
                skipped_tickers += 1
                print(f"[{position}/{total_tickers}] {ticker.symbol} skipped: no rows in requested range | stored_rows={total_rows}")
                continue

            stored = upsert_trendline_snapshot_rows(
                rows,
                database_url=database_url,
                batch_size=args.batch_size,
            )
            total_rows += stored
            processed_tickers += 1
            print(f"[{position}/{total_tickers}] {ticker.symbol} stored: {stored} rows | stored_rows={total_rows}")
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | stored_rows={total_rows}")

    manifest = {
        "job": "trendline_snapshot_backfill",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_tickers": total_tickers,
        "processed_tickers": processed_tickers,
        "skipped_tickers": skipped_tickers,
        "failed_tickers": failures,
        "stored_rows": total_rows,
    }
    manifest_path = Path(args.manifest_path) if args.manifest_path else _manifest_default_path()
    _write_json(manifest_path, manifest)
    print(f"Wrote run summary to {manifest_path}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
