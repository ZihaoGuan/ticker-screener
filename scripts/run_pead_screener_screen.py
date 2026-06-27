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
from src.ibd_distribution_day_monitor.fmp_client import ApiCallBudgetExceeded, FMPClient
from src.pead_screener_runtime import analyze_stock


STRATEGY_ID = "pead_screener"
ANALYZER_STRATEGY_ID = "earnings_trade_analyzer"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PEAD screener for weekly earnings tickers using analyzer output as candidate source."
    )
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--api-key", help="FMP API key (defaults to FMP_API_KEY environment variable)")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD)")
    parser.add_argument("--reference-date", help="Reference date for selected earnings week (YYYY-MM-DD)")
    parser.add_argument("--week-offset", type=int, default=0, help="0=this week, 1=next week, 2=week after")
    parser.add_argument("--limit", type=int, help="Optional limit on weekly earnings events")
    parser.add_argument("--watch-weeks", type=int, default=5, help="PEAD monitoring window in weeks")
    parser.add_argument("--min-grade", choices=("A", "B", "C", "D"), default="B", help="Minimum analyzer grade to feed into PEAD")
    parser.add_argument("--max-api-calls", type=int, default=300, help="FMP API call budget for this run")
    parser.add_argument(
        "--price-provider",
        choices=("auto", "fmp", "yfinance"),
        default="auto",
        help="Price history provider for PEAD analysis (default: auto)",
    )
    parser.add_argument(
        "--analyzer-raw",
        help="Optional path to analyzer raw_results.json. Defaults to same date-label earnings_trade_analyzer artifact.",
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


def _build_watchlist_entry(hit: dict[str, Any]) -> dict[str, object]:
    return {
        "ticker": hit["ticker"],
        "symbol": hit["ticker"],
        "summary": (
            f"{str(hit.get('stage') or 'UNKNOWN')} | Score {float(hit.get('composite_score') or 0):.1f}. "
            f"Gap {float(hit.get('gap_pct') or 0):+.1f}%. "
            f"{str(hit.get('guidance') or '').strip()}".strip()
        ),
        "event_date": hit.get("earnings_date"),
        "event_label": "Earnings",
        "setup_label": "PEAD",
        "entry_style": "pead_screener",
        "stage": hit.get("stage"),
        "score": hit.get("composite_score"),
        "guidance": hit.get("guidance"),
    }


def _load_analyzer_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed.") from exc
    return yf


def _normalize_yfinance_history(history: Any) -> list[dict[str, Any]]:
    if history is None or getattr(history, "empty", True):
        return []
    frame = history.copy()
    if getattr(frame, "columns", None) is not None and hasattr(frame.columns, "nlevels") and frame.columns.nlevels > 1:
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    required = {"Open", "High", "Low", "Close", "Volume"}
    if not required.issubset(set(frame.columns)):
        return []
    frame = frame.loc[:, ["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["High", "Low", "Close", "Volume"])
    if frame.empty:
        return []
    normalized: list[dict[str, Any]] = []
    for index, row in frame.sort_index(ascending=False).iterrows():
        trade_date = index.date().isoformat() if hasattr(index, "date") else str(index)[:10]
        normalized.append(
            {
                "date": trade_date,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
        )
    return normalized


def _fetch_yfinance_daily_prices(
    symbol: str,
    *,
    run_date: dt.date,
    days: int,
) -> list[dict[str, Any]]:
    yf = _load_yfinance()
    start_date = run_date - dt.timedelta(days=max(days * 2, 180))
    history = yf.download(
        tickers=symbol,
        start=start_date.isoformat(),
        end=(run_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    normalized = _normalize_yfinance_history(history)
    return normalized[:days] if days > 0 else normalized


def _resolve_price_provider(args: argparse.Namespace) -> str:
    provider = str(getattr(args, "price_provider", "auto") or "auto").strip().lower()
    if provider == "auto":
        return "fmp" if ((args.api_key or os.getenv("FMP_API_KEY") or "").strip()) else "yfinance"
    return provider


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
    eligible_events = [event for event in weekly_events if event.eligible_on <= run_date]
    eligible_symbols = {event.ticker.upper() for event in eligible_events}
    grade_order = {"A": 0, "B": 1, "C": 2, "D": 3}

    artifact_paths = build_screener_artifact_paths(
        PROJECT_ROOT / "artifacts",
        strategy_id=STRATEGY_ID,
        date_label=date_label,
    )
    raw_path = artifact_paths.raw_results_path
    watchlist_path = artifact_paths.watchlist_path
    summary_path = artifact_paths.summary_path

    analyzer_raw_path = (
        Path(args.analyzer_raw)
        if args.analyzer_raw
        else build_screener_artifact_paths(
            PROJECT_ROOT / "artifacts",
            strategy_id=ANALYZER_STRATEGY_ID,
            date_label=date_label,
        ).raw_results_path
    )
    analyzer_payload = _load_analyzer_payload(analyzer_raw_path)
    analyzer_hits = analyzer_payload.get("hits") if isinstance(analyzer_payload.get("hits"), list) else []
    analyzer_by_symbol = {
        str(item.get("ticker") or item.get("symbol") or "").strip().upper(): item
        for item in analyzer_hits
        if isinstance(item, dict)
    }

    if not eligible_events:
        raw_payload = {
            "run_date": run_date.isoformat(),
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "strategy_id": STRATEGY_ID,
            "hits": [],
            "failed_tickers": [],
            "eligible_events": [event.to_dict() for event in weekly_events],
            "source_analyzer_file": str(analyzer_raw_path),
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

    price_provider = _resolve_price_provider(args)
    client: FMPClient | None = None
    if price_provider == "fmp":
        client = FMPClient(api_key=args.api_key, max_api_calls=args.max_api_calls)
    else:
        try:
            _load_yfinance()
        except RuntimeError as exc:
            raise SystemExit(
                "PEAD price history unavailable: no FMP API key and yfinance is not installed."
            ) from exc

    failures: list[dict[str, object]] = []
    results: list[dict[str, Any]] = []

    for event in eligible_events:
        symbol = event.ticker.upper()
        analyzer_hit = analyzer_by_symbol.get(symbol)
        if not isinstance(analyzer_hit, dict):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "analyzer_result_missing",
                    "earnings_date": event.event_date.isoformat(),
                    "eligible_on": event.eligible_on.isoformat(),
                }
            )
            continue
        grade = str(analyzer_hit.get("grade") or "D").upper()
        if grade_order.get(grade, 99) > grade_order.get(args.min_grade, 1):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "grade_below_min",
                    "grade": grade,
                    "min_grade": args.min_grade,
                    "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                    "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                }
            )
            continue
        if price_provider == "fmp":
            try:
                data = client.get_historical_prices(symbol, days=90) if client is not None else None
            except ApiCallBudgetExceeded:
                failures.append(
                    {
                        "ticker": symbol,
                        "symbol": symbol,
                        "error": "api_budget_exceeded",
                        "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                        "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                    }
                )
                break
            if not data or "historical" not in data:
                failures.append(
                    {
                        "ticker": symbol,
                        "symbol": symbol,
                        "error": "historical_data_missing",
                        "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                        "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                    }
                )
                continue
            daily_prices = data["historical"]
        else:
            try:
                daily_prices = _fetch_yfinance_daily_prices(symbol, run_date=run_date, days=90)
            except Exception as exc:
                failures.append(
                    {
                        "ticker": symbol,
                        "symbol": symbol,
                        "error": "yfinance_history_failed",
                        "details": str(exc),
                        "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                        "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                    }
                )
                continue
        if not isinstance(daily_prices, list) or not daily_prices:
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "historical_data_empty",
                    "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                    "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                }
            )
            continue
        current_price = float(daily_prices[0].get("close") or 0)
        analysis = analyze_stock(
            symbol=symbol,
            daily_prices=daily_prices,
            earnings_date=str(analyzer_hit.get("earnings_date") or event.event_date.isoformat()),
            earnings_timing=str(analyzer_hit.get("earnings_timing") or ""),
            gap_pct=float(analyzer_hit.get("gap_pct") or 0.0),
            current_price=current_price,
            watch_weeks=int(args.watch_weeks),
        )
        if not isinstance(analysis, dict):
            failures.append(
                {
                    "ticker": symbol,
                    "symbol": symbol,
                    "error": "analysis_failed",
                    "earnings_date": analyzer_hit.get("earnings_date") or event.event_date.isoformat(),
                    "eligible_on": analyzer_hit.get("eligible_on") or event.eligible_on.isoformat(),
                }
            )
            continue
        analysis["ticker"] = symbol
        analysis["symbol"] = symbol
        analysis["grade"] = grade
        analysis["eligible_on"] = analyzer_hit.get("eligible_on") or event.eligible_on.isoformat()
        analysis["earnings_summary"] = analyzer_hit.get("earnings_summary") or event.summary
        analysis["score"] = analysis.get("composite_score")
        analysis["reasons"] = [str(analysis.get("guidance") or "").strip()] if analysis.get("guidance") else []
        results.append(analysis)

    results.sort(
        key=lambda item: (
            {"BREAKOUT": 0, "SIGNAL_READY": 1, "MONITORING": 2, "EXPIRED": 3}.get(
                str(item.get("stage") or ""),
                9,
            ),
            -float(item.get("composite_score") or 0),
        )
    )
    watchlist = [_build_watchlist_entry(item) for item in results]
    raw_payload = {
        "run_date": run_date.isoformat(),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "strategy_id": STRATEGY_ID,
        "generator": "pead-screener-wrapper",
        "eligible_events": [event.to_dict() for event in eligible_events],
        "source_analyzer_file": str(analyzer_raw_path),
        "hits": results,
        "failed_tickers": failures,
        "metadata": {
            "min_grade": args.min_grade,
            "watch_weeks": int(args.watch_weeks),
            "price_provider": price_provider,
            "api_calls_made": client.api_calls_made if client is not None else 0,
            "max_api_calls": client.max_api_calls if client is not None else 0,
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
        "eligible_tickers": len(eligible_symbols),
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
