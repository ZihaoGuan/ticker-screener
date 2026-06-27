#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._earnings_post_event_support import load_weekly_earnings_events, resolve_selected_week
from scripts._screen_run_persistence import persist_screen_run_artifacts_if_configured
from src.artifact_paths import build_screener_artifact_paths
from src.config import today_label


SKILL_SCRIPTS_DIR = (
    PROJECT_ROOT / "trading-skills" / "skills" / "earnings-trade-analyzer" / "scripts"
)
if str(SKILL_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SKILL_SCRIPTS_DIR))

from analyze_earnings_trades import analyze_stock, apply_entry_filter, normalize_timing  # type: ignore[import-not-found]
from fmp_client import ApiCallBudgetExceeded, FMPClient  # type: ignore[import-not-found]


STRATEGY_ID = "earnings_trade_analyzer"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run post-earnings earnings-trade-analyzer for current weekly earnings names."
    )
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--api-key", help="FMP API key (defaults to FMP_API_KEY environment variable)")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD)")
    parser.add_argument("--reference-date", help="Reference date for selected earnings week (YYYY-MM-DD)")
    parser.add_argument("--week-offset", type=int, default=0, help="0=this week, 1=next week, 2=week after")
    parser.add_argument("--limit", type=int, help="Optional limit on weekly earnings events")
    parser.add_argument(
        "--min-market-cap",
        type=float,
        default=500_000_000,
        help="Minimum market cap in dollars (default: 500000000)",
    )
    parser.add_argument("--min-gap", type=float, default=0.0, help="Minimum absolute earnings gap percent")
    parser.add_argument(
        "--max-api-calls",
        type=int,
        default=400,
        help="FMP API call budget for this run (default: 400)",
    )
    parser.add_argument(
        "--apply-entry-filter",
        action="store_true",
        help="Apply analyzer entry-quality filter after scoring",
    )
    parser.add_argument(
        "--ignore-exclusions",
        action="store_true",
        help="Ignore configured ticker exclusions when building weekly earnings universe",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _event_lookup(events: list[Any]) -> dict[str, Any]:
    return {event.ticker.upper(): event for event in events}


def _build_watchlist_entry(hit: dict[str, Any]) -> dict[str, object]:
    return {
        "ticker": hit["ticker"],
        "symbol": hit["ticker"],
        "summary": (
            f"Grade {hit.get('grade', '?')} score {float(hit.get('composite_score') or 0):.1f}. "
            f"Gap {float(hit.get('gap_pct') or 0):+.1f}%. "
            f"{str(hit.get('guidance') or '').strip()}".strip()
        ),
        "event_date": hit.get("earnings_date"),
        "event_label": "Earnings",
        "setup_label": "Post-earnings analyzer",
        "entry_style": "earnings_trade_analyzer",
        "sector": hit.get("sector"),
        "exchange": hit.get("exchange"),
        "grade": hit.get("grade"),
        "score": hit.get("composite_score"),
        "guidance": hit.get("guidance"),
    }


def main() -> int:
    args = parse_args()
    reference_date = dt.date.fromisoformat(args.reference_date) if args.reference_date else None
    run_date, week_start, week_end = resolve_selected_week(reference_date, week_offset=args.week_offset)
    date_label = args.date_label or today_label()

    weekly_events = load_weekly_earnings_events(
        config_path=args.config,
        reference_date=reference_date,
        week_offset=args.week_offset,
        ignore_exclusions=args.ignore_exclusions,
        limit=args.limit,
    )
    event_lookup = _event_lookup(weekly_events)
    eligible_events = [event for event in weekly_events if event.eligible_on <= run_date]
    eligible_lookup = _event_lookup(eligible_events)

    print(
        f"Weekly earnings events: {len(weekly_events)} | eligible after T+1: {len(eligible_events)}",
        file=sys.stderr,
    )

    artifact_paths = build_screener_artifact_paths(
        PROJECT_ROOT / "artifacts",
        strategy_id=STRATEGY_ID,
        date_label=date_label,
    )
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    if not eligible_events:
        raw_payload = {
            "run_date": run_date.isoformat(),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "strategy_id": STRATEGY_ID,
            "hits": [],
            "failed_tickers": [],
            "eligible_events": [event.to_dict() for event in weekly_events],
        }
        summary_payload = {
            "strategy_id": STRATEGY_ID,
            "date_label": date_label,
            "as_of_date": run_date.isoformat(),
            "reference_date": run_date.isoformat(),
            "source": "weekly-earnings-post-event",
            "week_offset": args.week_offset,
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "eligible_tickers": 0,
            "total_tickers": len(weekly_events),
            "passed_tickers": 0,
            "failed_tickers": 0,
            "raw_results_file": str(raw_path),
            "watchlist_file": str(watchlist_path),
        }
        _write_json(raw_path, raw_payload)
        _write_json(watchlist_path, [])
        _write_json(summary_path, summary_payload)
        persist_screen_run_artifacts_if_configured(args=args, summary_path=summary_path)
        print("No T+1-eligible earnings events for selected week.", file=sys.stderr)
        return 0

    client = FMPClient(api_key=args.api_key, max_api_calls=args.max_api_calls)
    earnings_rows = client.get_earnings_calendar(week_start.isoformat(), run_date.isoformat()) or []
    eligible_symbols = set(eligible_lookup.keys())
    earnings_by_symbol: dict[str, dict[str, Any]] = {}
    for row in earnings_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol or symbol not in eligible_symbols or symbol in earnings_by_symbol:
            continue
        earnings_by_symbol[symbol] = row

    profiles = client.get_company_profiles(sorted(eligible_symbols)) if eligible_symbols else {}
    failures: list[dict[str, object]] = []
    candidates: list[dict[str, Any]] = []
    for symbol in sorted(eligible_symbols):
        event = eligible_lookup[symbol]
        earning = earnings_by_symbol.get(symbol)
        profile = profiles.get(symbol) if isinstance(profiles, dict) else None
        if not isinstance(earning, dict):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "earnings_row_missing",
                    "earnings_date": event.event_date.isoformat(),
                    "eligible_on": event.eligible_on.isoformat(),
                }
            )
            continue
        if not isinstance(profile, dict):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "company_profile_missing",
                    "earnings_date": event.event_date.isoformat(),
                    "eligible_on": event.eligible_on.isoformat(),
                }
            )
            continue
        market_cap = float(profile.get("mktCap") or 0)
        exchange = str(profile.get("exchangeShortName") or "")
        if market_cap < float(args.min_market_cap):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "market_cap_below_min",
                    "market_cap": market_cap,
                    "earnings_date": event.event_date.isoformat(),
                    "eligible_on": event.eligible_on.isoformat(),
                }
            )
            continue
        if exchange not in FMPClient.US_EXCHANGES:
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "non_us_exchange",
                    "exchange": exchange,
                    "earnings_date": event.event_date.isoformat(),
                    "eligible_on": event.eligible_on.isoformat(),
                }
            )
            continue
        candidates.append(
            {
                "symbol": symbol,
                "company_name": profile.get("companyName", symbol),
                "earnings_date": str(earning.get("date") or event.event_date.isoformat()),
                "earnings_timing": normalize_timing(earning.get("time")),
                "market_cap": market_cap,
                "sector": profile.get("sector") or event.sector or "N/A",
                "industry": profile.get("industry") or "N/A",
                "exchange": exchange or event.exchange,
                "price": profile.get("price", 0),
                "earnings_summary": event.summary,
                "eligible_on": event.eligible_on.isoformat(),
            }
        )

    results: list[dict[str, Any]] = []
    filtered_out_candidates: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        symbol = candidate["symbol"]
        print(f"[{index}/{len(candidates)}] analyzing {symbol}", file=sys.stderr)
        try:
            daily_prices = client.get_historical_prices(symbol, days=250)
        except ApiCallBudgetExceeded:
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "api_budget_exceeded",
                    "earnings_date": candidate["earnings_date"],
                    "eligible_on": candidate["eligible_on"],
                }
            )
            break
        if not daily_prices or len(daily_prices) < 50:
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "insufficient_price_history",
                    "history_days": len(daily_prices) if daily_prices else 0,
                    "earnings_date": candidate["earnings_date"],
                    "eligible_on": candidate["eligible_on"],
                }
            )
            continue
        analysis = analyze_stock(
            daily_prices,
            candidate["earnings_date"],
            candidate["earnings_timing"],
        )
        composite = analysis["composite"]
        gap_pct = float(analysis["gap"]["gap_pct"])
        if abs(gap_pct) < float(args.min_gap):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "gap_below_min",
                    "gap_pct": gap_pct,
                    "min_gap": float(args.min_gap),
                    "earnings_date": candidate["earnings_date"],
                    "eligible_on": candidate["eligible_on"],
                }
            )
            continue
        current_price = daily_prices[0]["close"] if daily_prices else candidate["price"]
        results.append(
            {
                "ticker": symbol,
                "symbol": symbol,
                "company_name": candidate["company_name"],
                "earnings_date": candidate["earnings_date"],
                "earnings_timing": candidate["earnings_timing"],
                "earnings_summary": candidate["earnings_summary"],
                "eligible_on": candidate["eligible_on"],
                "gap_pct": gap_pct,
                "composite_score": composite["composite_score"],
                "grade": composite["grade"],
                "grade_description": composite["grade_description"],
                "guidance": composite["guidance"],
                "weakest_component": composite["weakest_component"],
                "strongest_component": composite["strongest_component"],
                "component_breakdown": composite["component_breakdown"],
                "current_price": round(float(current_price), 2),
                "market_cap": candidate["market_cap"],
                "sector": candidate["sector"],
                "industry": candidate["industry"],
                "exchange": candidate["exchange"],
                "components": {
                    "gap_size": analysis["gap"],
                    "pre_earnings_trend": analysis["pre_earnings_trend"],
                    "volume_trend": analysis["volume_trend"],
                    "ma200_position": analysis["ma200_position"],
                    "ma50_position": analysis["ma50_position"],
                },
                "score": composite["composite_score"],
                "reasons": [composite["guidance"]],
            }
        )

    unfiltered_results = list(results)
    if args.apply_entry_filter:
        kept_symbols = {
            str(item.get("symbol") or "").strip().upper()
            for item in apply_entry_filter(results)
        }
        filtered_out_candidates = [item for item in results if item["symbol"] not in kept_symbols]
        results = [item for item in results if item["symbol"] in kept_symbols]
        for item in filtered_out_candidates:
            failures.append(
                {
                    "ticker": item["ticker"],
                    "symbol": item["symbol"],
                    "error": "entry_filter_rejected",
                    "grade": item.get("grade"),
                    "composite_score": item.get("composite_score"),
                    "earnings_date": item.get("earnings_date"),
                    "eligible_on": item.get("eligible_on"),
                }
            )

    results.sort(key=lambda item: float(item.get("composite_score") or 0), reverse=True)
    watchlist = [_build_watchlist_entry(item) for item in results]
    raw_payload = {
        "run_date": run_date.isoformat(),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "strategy_id": STRATEGY_ID,
        "generator": "earnings-trade-analyzer-wrapper",
        "eligible_events": [event.to_dict() for event in eligible_events],
        "hits": results,
        "failed_tickers": failures,
        "metadata": {
            "apply_entry_filter": bool(args.apply_entry_filter),
            "min_market_cap": float(args.min_market_cap),
            "min_gap": float(args.min_gap),
            "api_calls_made": client.api_calls_made,
            "max_api_calls": client.max_api_calls,
            "unfiltered_result_count": len(unfiltered_results),
        },
    }
    summary_payload = {
        "strategy_id": STRATEGY_ID,
        "date_label": date_label,
        "as_of_date": run_date.isoformat(),
        "reference_date": run_date.isoformat(),
        "source": "weekly-earnings-post-event",
        "week_offset": args.week_offset,
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "eligible_tickers": len(eligible_events),
        "total_tickers": len(weekly_events),
        "passed_tickers": len(results),
        "failed_tickers": len(failures),
        "raw_results_file": str(raw_path),
        "watchlist_file": str(watchlist_path),
    }

    _write_json(raw_path, raw_payload)
    _write_json(watchlist_path, watchlist)
    _write_json(summary_path, summary_payload)

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")
    persisted_run_id = persist_screen_run_artifacts_if_configured(
        args=args,
        summary_path=summary_path,
    )
    if persisted_run_id is not None:
        print(f"Persisted screen run id={persisted_run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
