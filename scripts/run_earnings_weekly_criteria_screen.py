#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import load_app_config, today_label
from src.cookstock_bridge import load_configured_cookstock
from src.earnings_growth_screen import (
    AInvestGrowthClient,
    AKShareGrowthClient,
    EarningsGrowthScreenResult,
    OpenBBGrowthClient,
    YFinanceGrowthClient,
    _build_price_context,
    _compute_post_earnings_moves,
    _eps_is_improving,
    _eps_series_from_earnings,
    _historical_earnings_rows,
    _latest_revenue_context,
    _next_upcoming_earnings_row,
    _parse_session,
    _to_hit,
)
from src.earnings_growth_watchlist_builder import build_earnings_growth_watchlist
from src.pre_earnings_screen import PreEarningsEvent
from src.ticker_filters import filter_pre_earnings_events, load_excluded_tickers
from src.universe import load_universe
from src.webapp.config import load_webapp_config
from src.webapp.services.earnings_calendar_service import CRITERIA_STRATEGY_ID
from src.webapp.services.screener_history_service import ScreenerHistoryService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the next-week earnings weekly criteria screen.")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "market_config.json"))
    parser.add_argument("--limit", type=int, help="Limit the next-week candidate set.")
    parser.add_argument("--date-label", help="Override artifact date label (YYYY-MM-DD).")
    parser.add_argument("--reference-date", help="Reference date for next-week earnings watchlist (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--skip-persist", action="store_true", help="Skip persisting the screen run into the webapp DB.")
    parser.add_argument(
        "--pass-mode",
        choices=("strict", "loose"),
        default="loose",
        help="strict: require all configured checks. loose: only require MA stack, revenue YoY >= 100, and EPS improving.",
    )
    return parser.parse_args()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _next_week_events(config_path: str, reference_date: dt.date | None, limit: int | None) -> list[PreEarningsEvent]:
    config = load_app_config(config_path)
    excluded = load_excluded_tickers(config)
    universe = load_universe(config)
    sector_map = {item.symbol: (item.sector, item.exchange) for item in universe}
    cookstock = load_configured_cookstock(config)
    raw_events = cookstock.fetch_next_week_earnings_watchlist(reference_date=reference_date)

    events: list[PreEarningsEvent] = []
    seen: set[str] = set()
    for item in raw_events:
        ticker = str(item["ticker"]).upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        sector, exchange = sector_map.get(ticker, (None, None))
        events.append(
            PreEarningsEvent(
                ticker=ticker,
                earnings_date=str(item.get("event_date")) if item.get("event_date") else None,
                summary=str(item.get("summary")) if item.get("summary") else None,
                sector=sector,
                exchange=exchange,
            )
        )
    events = filter_pre_earnings_events(events, excluded)
    if limit is not None:
        return events[:limit]
    return events


def _build_criteria_map(
    *,
    config: Any,
    institutional_ownership_pct: float | None,
    ma_stack_bullish: bool,
    revenue_yoy_pct: float | None,
    latest_eps_actual: float | None,
    eps_improving: bool,
) -> dict[str, bool]:
    return {
        "institutional_ownership_ge_10": institutional_ownership_pct is not None
        and institutional_ownership_pct >= config.earnings_growth_min_institutional_ownership_pct,
        "bullish_ma_stack": bool(ma_stack_bullish),
        "revenue_yoy_ge_100": revenue_yoy_pct is not None and revenue_yoy_pct >= config.earnings_growth_min_revenue_yoy_pct,
        "latest_eps_negative": latest_eps_actual is not None and latest_eps_actual < 0,
        "eps_improving_last_4": bool(eps_improving),
    }


def _passes(criteria: dict[str, bool], pass_mode: str) -> bool:
    if pass_mode == "loose":
        return all(
            criteria[key]
            for key in (
                "bullish_ma_stack",
                "revenue_yoy_ge_100",
                "eps_improving_last_4",
            )
        )
    return all(criteria.values())


def _print_screening_log(ticker: str, criteria: dict[str, bool], *, pass_mode: str) -> None:
    passed = [key for key, value in criteria.items() if value]
    failed = [key for key, value in criteria.items() if not value]
    status = "PASS" if _passes(criteria, pass_mode) else "FAIL"
    print(f"{ticker}: {status} mode={pass_mode}")
    print(f"  matched: {', '.join(passed) if passed else '-'}")
    print(f"  not_pass: {', '.join(failed) if failed else '-'}")


def main() -> int:
    args = parse_args()
    os.environ.setdefault("MPLBACKEND", "Agg")
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")

    base_config = load_app_config(args.config)
    config = replace(
        base_config,
        earnings_growth_min_institutional_ownership_pct=10.0,
        earnings_growth_min_revenue_yoy_pct=100.0,
        earnings_growth_eps_improving_quarters=4,
        earnings_growth_min_move_occurrences=0,
        earnings_growth_min_move_pct=0.0,
        earnings_growth_min_quarter_revenue=0.0,
    )
    date_label = args.date_label or today_label()
    reference_date = dt.date.fromisoformat(args.reference_date) if args.reference_date else None
    run_date = reference_date or dt.date.today()

    ainvest_api_key = (os.getenv("AINVEST_API_KEY") or "").strip()
    earnings_sources: list[tuple[str, Any]] = []
    earnings_providers_used: list[str] = []
    try:
        earnings_sources.append(("openbb", OpenBBGrowthClient(timeout_seconds=config.request_timeout_seconds)))
    except Exception:
        print("warning: OpenBB is not available; falling back to downstream earnings providers.")
    if ainvest_api_key:
        earnings_sources.append(("ainvest", AInvestGrowthClient(ainvest_api_key, timeout_seconds=config.request_timeout_seconds)))
    else:
        print("warning: AINVEST_API_KEY is not set; skipping AInvest earnings provider.")

    yfinance_client = YFinanceGrowthClient()
    earnings_sources.append(("yfinance", yfinance_client))
    financial_client = yfinance_client
    financials_providers_used = ["yfinance"]
    akshare_client: Any | None = None
    try:
        akshare_client = AKShareGrowthClient()
    except Exception:
        akshare_client = None

    events = _next_week_events(args.config, reference_date, args.limit)
    cookstock = load_configured_cookstock(config)
    hits: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for position, event in enumerate(events, start=1):
        print(f"[{position}/{len(events)}] screening {event.ticker}")
        try:
            earnings_rows: list[dict[str, Any]] = []
            last_earnings_error: Exception | None = None
            for provider_name, provider_client in earnings_sources:
                try:
                    earnings_rows = provider_client.get_earnings(
                        event.ticker,
                        limit=max(12, config.earnings_growth_move_lookback_quarters + 4),
                    )
                except Exception as exc:
                    last_earnings_error = exc
                    continue
                if provider_name not in earnings_providers_used:
                    earnings_providers_used.append(provider_name)
                if earnings_rows:
                    break
            if not earnings_rows:
                if last_earnings_error is not None:
                    raise last_earnings_error
                raise RuntimeError("no earnings rows")

            historical_earnings = _historical_earnings_rows(earnings_rows, run_date)
            income_rows = financial_client.get_income_statements(event.ticker, limit=8)
            if not income_rows and akshare_client is not None:
                try:
                    income_rows = akshare_client.get_income_statements(event.ticker, limit=8)
                    if income_rows and "akshare" not in financials_providers_used:
                        financials_providers_used.append("akshare")
                except Exception:
                    income_rows = []

            latest_revenue, revenue_yoy_pct = _latest_revenue_context(income_rows)
            eps_series = _eps_series_from_earnings(historical_earnings, config.earnings_growth_eps_improving_quarters)
            latest_eps_actual = eps_series[0] if eps_series else None
            eps_improving = len(eps_series) >= config.earnings_growth_eps_improving_quarters and _eps_is_improving(
                eps_series,
                config.earnings_growth_eps_improving_quarters,
            )

            institutional_ownership_pct = financial_client.get_latest_institutional_ownership_pct(event.ticker)
            price_context = _build_price_context(cookstock, config, event.ticker)
            current_price, ma_short, ma_medium, ma_long, price_rows = price_context
            ma_stack_bullish = bool(current_price > ma_short > ma_medium > ma_long)
            criteria = _build_criteria_map(
                config=config,
                institutional_ownership_pct=institutional_ownership_pct,
                ma_stack_bullish=ma_stack_bullish,
                revenue_yoy_pct=revenue_yoy_pct,
                latest_eps_actual=latest_eps_actual,
                eps_improving=eps_improving,
            )
            _print_screening_log(event.ticker, criteria, pass_mode=args.pass_mode)

            if not _passes(criteria, args.pass_mode):
                failures.append(
                    {
                        "ticker": event.ticker,
                        "error": "criteria_not_met",
                        "criteria": criteria,
                        "pass_mode": args.pass_mode,
                    }
                )
                continue

            post_earnings_moves = _compute_post_earnings_moves(
                price_rows,
                historical_earnings,
                config.earnings_growth_move_lookback_quarters,
            )
            next_row = _next_upcoming_earnings_row(earnings_rows, run_date)
            next_session = _parse_session(next_row.get("time")) if next_row else None

            hit = _to_hit(
                config,
                event,
                price_context,
                latest_revenue or 0.0,
                revenue_yoy_pct or 0.0,
                latest_eps_actual or 0.0,
                eps_series,
                institutional_ownership_pct or 0.0,
                post_earnings_moves,
                next_session,
            ).to_dict()
            hit["criteria"] = criteria
            hit["pass_mode"] = args.pass_mode
            hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": event.ticker, "error": str(exc), "pass_mode": args.pass_mode})
            print(f"{event.ticker}: ERROR {exc}")

    hits.sort(
        key=lambda item: (
            float(item.get("revenue_yoy_pct") or 0.0),
            float(item.get("institutional_ownership_pct") or 0.0),
        ),
        reverse=True,
    )
    result = EarningsGrowthScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        earnings_provider="+".join(earnings_providers_used) if earnings_providers_used else "none",
        financials_provider="+".join(financials_providers_used) if financials_providers_used else "none",
        total_tickers=len(events),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=[],
    )
    raw_payload = result.to_dict()
    raw_payload["hits"] = hits
    raw_payload["pass_mode"] = args.pass_mode
    watchlist = build_earnings_growth_watchlist([_dict_to_hit(item) for item in hits])

    raw_path = PROJECT_ROOT / "artifacts" / "raw" / f"{CRITERIA_STRATEGY_ID}_{date_label}.json"
    watchlist_path = PROJECT_ROOT / "artifacts" / "watchlists" / f"{CRITERIA_STRATEGY_ID}_{date_label}.json"
    summary_path = PROJECT_ROOT / "artifacts" / "raw" / f"{CRITERIA_STRATEGY_ID}_run_summary_{date_label}.json"

    _write_json(raw_path, raw_payload)
    _write_json(watchlist_path, watchlist)
    summary_payload = {
        "date_label": date_label,
        "as_of_date": run_date.isoformat(),
        "source": "next-week-earnings",
        "reference_date": str(reference_date) if reference_date else None,
        "earnings_provider": result.earnings_provider,
        "financials_provider": result.financials_provider,
        "total_tickers": result.total_tickers,
        "passed_tickers": result.passed_tickers,
        "failed_tickers": len(result.failed_tickers),
        "pass_mode": args.pass_mode,
        "raw_results_file": str(raw_path),
        "watchlist_file": str(watchlist_path),
    }
    _write_json(summary_path, summary_payload)

    print(f"Wrote raw results to {raw_path}")
    print(f"Wrote watchlist to {watchlist_path}")
    print(f"Wrote run summary to {summary_path}")

    if not args.skip_persist:
        webapp_config = load_webapp_config()
        history_service = ScreenerHistoryService(
            database_url=webapp_config.database_url,
            artifacts_dir=webapp_config.artifacts_dir,
        )
        if history_service.is_configured():
            run_id = history_service.persist_screen_run(
                strategy_id=CRITERIA_STRATEGY_ID,
                options={
                    "limit": args.limit,
                    "reference_date": run_date.isoformat(),
                    "source": "next-week-earnings",
                    "market_data_source": "internet",
                    "pass_mode": args.pass_mode,
                },
                summary_payload=summary_payload,
                raw_payload=raw_payload,
            )
            print(f"Persisted screen run id={run_id}")
        else:
            print("warning: webapp database not configured; skipped persistence")
    return 0


def _dict_to_hit(payload: dict[str, Any]):
    from src.earnings_growth_screen import EarningsGrowthHit

    return EarningsGrowthHit(
        ticker=str(payload["ticker"]),
        earnings_date=payload.get("earnings_date"),
        earnings_summary=payload.get("earnings_summary"),
        sector=payload.get("sector"),
        exchange=payload.get("exchange"),
        benchmark_ticker=str(payload["benchmark_ticker"]),
        current_price=float(payload["current_price"]),
        ma_short=float(payload["ma_short"]),
        ma_medium=float(payload["ma_medium"]),
        ma_long=float(payload["ma_long"]),
        ma_short_length=int(payload["ma_short_length"]),
        ma_medium_length=int(payload["ma_medium_length"]),
        ma_long_length=int(payload["ma_long_length"]),
        ma_stack_bullish=bool(payload["ma_stack_bullish"]),
        latest_quarter_revenue=float(payload["latest_quarter_revenue"]),
        revenue_yoy_pct=float(payload["revenue_yoy_pct"]),
        latest_eps_actual=float(payload["latest_eps_actual"]),
        eps_improving_quarters=int(payload["eps_improving_quarters"]),
        eps_series=[float(value) for value in payload.get("eps_series", [])],
        institutional_ownership_pct=float(payload["institutional_ownership_pct"]),
        historical_earnings_moves_pct=[float(value) for value in payload.get("historical_earnings_moves_pct", [])],
        large_move_occurrences=int(payload["large_move_occurrences"]),
        median_post_earnings_move_pct=float(payload["median_post_earnings_move_pct"]),
        next_earnings_session=payload.get("next_earnings_session"),
        reasons=list(payload.get("reasons", [])),
    )


if __name__ == "__main__":
    raise SystemExit(main())
