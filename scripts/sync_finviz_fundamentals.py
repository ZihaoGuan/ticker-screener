#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import random
import sys
import time
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.ratings.finviz_api import FinvizApiError, fetch_finviz_api_snapshot, snapshot_needs_fallback
from src.ratings.constants import RATING_STATUS_SCRAPE_FAILED
from src.ratings.finviz_parser import parse_finviz_probe
from src.ratings.finviz_probe import FinvizProbeError, looks_blocked, looks_retryable_failure, probe_finviz_ticker
from src.ratings.models import FundamentalsSnapshot
from src.ratings.repository import RatingsRepository
from src.universe import UniverseTicker, load_universe
from src.webapp.config import load_webapp_config


def _normalize_sector_values(values: list[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(normalized for normalized in (str(item).strip().lower() for item in values) if normalized)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape Finviz fundamentals into Postgres snapshots.")
    parser.add_argument("--config", default="", help="Optional app config path.")
    parser.add_argument("--as-of-date", default=today_label(), help="Snapshot date label YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, help="Optional universe limit for smoke runs.")
    parser.add_argument("--tickers", nargs="+", help="Optional explicit ticker list.")
    parser.add_argument("--resume-from", default="", help="Resume from ticker symbol.")
    parser.add_argument("--delay-min-seconds", type=float, default=0.15)
    parser.add_argument("--delay-max-seconds", type=float, default=0.4)
    parser.add_argument("--batch-size-before-rest", type=int, default=500)
    parser.add_argument("--rest-seconds", type=float, default=5.0)
    parser.add_argument("--overwrite-policy", default="skip-existing", choices=("latest-date", "replace-date", "skip-existing"))
    parser.add_argument("--include-sectors", nargs="+", help="Only sync tickers from the selected sectors.")
    parser.add_argument("--database-url", default="", help="Optional Postgres connection string.")
    parser.add_argument("--manifest-path", default="", help="Optional explicit manifest path.")
    parser.add_argument("--retry-failed-from-manifest", action="store_true", help="Retry only failed or blocked tickers from the manifest.")
    parser.add_argument(
        "--circuit-breaker-consecutive-503",
        type=int,
        default=25,
        help="Stop early after this many consecutive HTTP 503 scrape failures. Set 0 to disable.",
    )
    return parser.parse_args()


def _manual_tickers(symbols: list[str]) -> list[UniverseTicker]:
    seen: set[str] = set()
    result: list[UniverseTicker] = []
    for item in symbols:
        normalized = str(item).strip().upper()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(UniverseTicker(symbol=normalized))
    return result


def _load_target_universe(args: argparse.Namespace) -> list[UniverseTicker]:
    if args.tickers:
        return _manual_tickers(args.tickers)
    config = load_app_config(args.config or None)
    universe = load_universe(config, limit=None)
    include_sectors = set(_normalize_sector_values(args.include_sectors))
    if include_sectors:
        universe = [item for item in universe if str(item.sector or "").strip().lower() in include_sectors]
    if args.limit is not None:
        return universe[: args.limit]
    return universe


def _manifest_path(args: argparse.Namespace) -> Path:
    if args.manifest_path:
        return Path(args.manifest_path)
    return PROJECT_ROOT / "artifacts" / "raw" / f"finviz_fundamentals_manifest_{str(args.as_of_date).strip()}.json"


def _load_retry_manifest_tickers(manifest_path: Path) -> list[str]:
    if not manifest_path.exists():
        raise RuntimeError(f"Retry manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    failed_rows = payload.get("failed_tickers") if isinstance(payload, dict) else []
    blocked_rows = payload.get("blocked_tickers") if isinstance(payload, dict) else []
    ordered: list[str] = []
    seen: set[str] = set()
    if isinstance(failed_rows, list):
        for item in failed_rows:
            ticker = str(item.get("ticker") or "").strip().upper() if isinstance(item, dict) else ""
            if ticker and ticker not in seen:
                seen.add(ticker)
                ordered.append(ticker)
    if isinstance(blocked_rows, list):
        for item in blocked_rows:
            ticker = str(item or "").strip().upper()
            if ticker and ticker not in seen:
                seen.add(ticker)
                ordered.append(ticker)
    return ordered


def _write_manifest(
    manifest_path: Path,
    *,
    args: argparse.Namespace,
    completed: list[str],
    failed: list[dict[str, str]],
    blocked: list[str],
    next_resume_ticker: str | None,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "args": {
            "as_of_date": args.as_of_date,
            "limit": args.limit,
            "tickers": args.tickers or [],
            "resume_from": args.resume_from,
            "delay_min_seconds": args.delay_min_seconds,
            "delay_max_seconds": args.delay_max_seconds,
            "batch_size_before_rest": args.batch_size_before_rest,
            "rest_seconds": args.rest_seconds,
            "overwrite_policy": args.overwrite_policy,
            "include_sectors": args.include_sectors or [],
        },
        "completed_tickers": completed,
        "failed_tickers": failed,
        "blocked_tickers": blocked,
        "next_resume_ticker": next_resume_ticker,
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _sleep_with_jitter(min_seconds: float, max_seconds: float) -> None:
    upper = max(min_seconds, max_seconds)
    lower = min(min_seconds, max_seconds)
    time.sleep(random.uniform(lower, upper))


def _should_write_manifest(index: int, total: int, *, force: bool = False) -> bool:
    if force or index >= total:
        return True
    return index % 25 == 0


def _build_failed_snapshot(ticker: str, as_of_date: dt.date, message: str) -> FundamentalsSnapshot:
    return FundamentalsSnapshot(
        ticker=ticker,
        as_of_date=as_of_date,
        sector=None,
        industry=None,
        parse_status=RATING_STATUS_SCRAPE_FAILED,
        parse_error=message,
        source_url=f"https://finviz.com/quote.ashx?t={ticker}&p=d",
    )


def _should_count_as_503_failure(snapshot: FundamentalsSnapshot) -> bool:
    if snapshot.parse_status != RATING_STATUS_SCRAPE_FAILED:
        return False
    error_text = str(snapshot.parse_error or "").lower()
    return "unexpected http status: 503" in error_text


def main() -> int:
    args = parse_args()
    database_url = (args.database_url or load_webapp_config().database_url).strip()
    if not database_url:
        raise RuntimeError("No Postgres connection string configured. Pass --database-url or set TICKER_SCREENER_DATABASE_URL.")
    as_of_date = dt.date.fromisoformat(str(args.as_of_date))
    manifest_path = _manifest_path(args)
    if args.retry_failed_from_manifest:
        retry_tickers = _load_retry_manifest_tickers(manifest_path)
        if not retry_tickers:
            print(f"retry_manifest_empty path={manifest_path}", flush=True)
            return 0
        explicit_filter = {str(item).strip().upper() for item in (args.tickers or []) if str(item).strip()}
        if explicit_filter:
            retry_tickers = [ticker for ticker in retry_tickers if ticker in explicit_filter]
        universe = _manual_tickers(retry_tickers)
    else:
        universe = _load_target_universe(args)
    if args.resume_from:
        resume_key = str(args.resume_from).strip().upper()
        start_index = next((index for index, item in enumerate(universe) if item.symbol == resume_key), None)
        if start_index is not None:
            universe = universe[start_index:]
    repository = RatingsRepository(database_url)
    latest_states = repository.load_latest_fundamentals_statuses([item.symbol for item in universe])
    print(
        "sync_config "
        f"tickers={len(universe)} "
        f"delay_min_seconds={args.delay_min_seconds} "
        f"delay_max_seconds={args.delay_max_seconds} "
        f"batch_size_before_rest={args.batch_size_before_rest} "
        f"rest_seconds={args.rest_seconds} "
        f"overwrite_policy={args.overwrite_policy} "
        f"include_sectors={','.join(args.include_sectors or []) or '-'} "
        f"retry_failed_from_manifest={bool(args.retry_failed_from_manifest)} "
        f"circuit_breaker_consecutive_503={args.circuit_breaker_consecutive_503}",
        flush=True,
    )

    completed: list[str] = []
    failed: list[dict[str, str]] = []
    blocked: list[str] = []
    consecutive_blocked = 0
    consecutive_503 = 0

    for index, ticker_meta in enumerate(universe, start=1):
        ticker = ticker_meta.symbol.strip().upper()
        latest_state = latest_states.get(ticker) or {}
        existing_date = latest_state.get("as_of_date")
        existing_status = str(latest_state.get("parse_status") or "").strip().lower()
        existing_is_failed = existing_status == RATING_STATUS_SCRAPE_FAILED
        if args.overwrite_policy == "skip-existing" and existing_date == as_of_date and not existing_is_failed:
            completed.append(ticker)
            print(f"[{index}/{len(universe)}] {ticker} skipped_existing as_of_date={as_of_date.isoformat()}", flush=True)
            next_resume = universe[index].symbol if index < len(universe) else None
            if _should_write_manifest(index, len(universe)):
                _write_manifest(
                    manifest_path,
                    args=args,
                    completed=completed,
                    failed=failed,
                    blocked=blocked,
                    next_resume_ticker=next_resume,
                )
            continue
        if args.overwrite_policy == "latest-date" and existing_date is not None and existing_date >= as_of_date and not existing_is_failed:
            completed.append(ticker)
            print(f"[{index}/{len(universe)}] {ticker} skipped_latest existing_date={existing_date.isoformat()}", flush=True)
            next_resume = universe[index].symbol if index < len(universe) else None
            if _should_write_manifest(index, len(universe)):
                _write_manifest(
                    manifest_path,
                    args=args,
                    completed=completed,
                    failed=failed,
                    blocked=blocked,
                    next_resume_ticker=next_resume,
                )
            continue
        snapshot: FundamentalsSnapshot | None = None
        failure_message: str | None = None
        blocked_this_ticker = False

        for attempt in range(1, 4):
            try:
                snapshot = fetch_finviz_api_snapshot(
                    ticker,
                    as_of_date=as_of_date,
                    fallback_sector=ticker_meta.sector,
                    fallback_industry=ticker_meta.industry,
                )
                if snapshot_needs_fallback(snapshot):
                    probe = probe_finviz_ticker(ticker)
                    if looks_blocked(probe):
                        blocked_this_ticker = True
                        blocked.append(ticker)
                        consecutive_blocked += 1
                        if attempt < 3:
                            time.sleep(300.0)
                            continue
                        failure_message = "Finviz block/captcha detected."
                        snapshot = _build_failed_snapshot(ticker, as_of_date, failure_message)
                        break
                    snapshot = parse_finviz_probe(
                        probe,
                        as_of_date=as_of_date,
                        fallback_sector=ticker_meta.sector,
                        fallback_industry=ticker_meta.industry,
                    )
                break
            except FinvizApiError as exc:
                failure_message = str(exc)
            try:
                probe = probe_finviz_ticker(ticker)
                if looks_blocked(probe):
                    blocked_this_ticker = True
                    blocked.append(ticker)
                    consecutive_blocked += 1
                    if attempt < 3:
                        time.sleep(300.0)
                        continue
                    failure_message = "Finviz block/captcha detected."
                    snapshot = _build_failed_snapshot(ticker, as_of_date, failure_message)
                    break
                snapshot = parse_finviz_probe(
                    probe,
                    as_of_date=as_of_date,
                    fallback_sector=ticker_meta.sector,
                    fallback_industry=ticker_meta.industry,
                )
                break
            except FinvizProbeError as exc:
                if failure_message:
                    failure_message = f"{failure_message}; fallback_probe={exc}"
                else:
                    failure_message = str(exc)
                if looks_retryable_failure(failure_message) and attempt < 3:
                    time.sleep(20.0 if attempt == 1 else 60.0)
                    continue
                snapshot = _build_failed_snapshot(ticker, as_of_date, failure_message)
                break

        if snapshot is None:
            snapshot = _build_failed_snapshot(ticker, as_of_date, failure_message or "Unknown scrape failure.")

        repository.ensure_ticker_metadata_stub(
            ticker,
            sector=snapshot.sector or ticker_meta.sector,
            industry=snapshot.industry or ticker_meta.industry,
        )
        repository.upsert_fundamentals_snapshots([snapshot])
        latest_states[ticker] = {"as_of_date": snapshot.as_of_date, "parse_status": snapshot.parse_status}

        if snapshot.parse_status == RATING_STATUS_SCRAPE_FAILED:
            failed.append({"ticker": ticker, "reason": snapshot.parse_error or "scrape_failed"})
            print(f"[{index}/{len(universe)}] {ticker} scrape_failed reason={snapshot.parse_error or 'unknown'}", flush=True)
            if _should_count_as_503_failure(snapshot):
                consecutive_503 += 1
            else:
                consecutive_503 = 0
        else:
            completed.append(ticker)
            consecutive_blocked = 0
            consecutive_503 = 0
            print(f"[{index}/{len(universe)}] {ticker} fundamentals_ok sector={snapshot.sector or '-'}", flush=True)

        next_resume = universe[index].symbol if index < len(universe) else None
        if _should_write_manifest(index, len(universe), force=blocked_this_ticker or snapshot.parse_status == RATING_STATUS_SCRAPE_FAILED):
            _write_manifest(
                manifest_path,
                args=args,
                completed=completed,
                failed=failed,
                blocked=blocked,
                next_resume_ticker=next_resume,
            )

        if blocked_this_ticker and consecutive_blocked >= 3:
            print("finviz_blocked=stop consecutive=3", flush=True)
            _write_manifest(
                manifest_path,
                args=args,
                completed=completed,
                failed=failed,
                blocked=blocked,
                next_resume_ticker=next_resume,
            )
            return 1

        if args.circuit_breaker_consecutive_503 > 0 and consecutive_503 >= int(args.circuit_breaker_consecutive_503):
            print(f"finviz_503_circuit_breaker=stop consecutive={consecutive_503}", flush=True)
            _write_manifest(
                manifest_path,
                args=args,
                completed=completed,
                failed=failed,
                blocked=blocked,
                next_resume_ticker=next_resume,
            )
            return 1

        if index < len(universe):
            _sleep_with_jitter(args.delay_min_seconds, args.delay_max_seconds)
            if args.batch_size_before_rest > 0 and index % args.batch_size_before_rest == 0:
                time.sleep(max(0.0, float(args.rest_seconds)))

    print(f"fundamentals_completed={len(completed)}", flush=True)
    print(f"fundamentals_failed={len(failed)}", flush=True)
    _write_manifest(
        manifest_path,
        args=args,
        completed=completed,
        failed=failed,
        blocked=blocked,
        next_resume_ticker=None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
