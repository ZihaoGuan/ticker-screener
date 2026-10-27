from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

STRATEGY_ID = "martin_bar_watchlist_v1"
HORIZONS = (5, 21, 63)
BENCHMARKS = ("SPY", "QQQ")


def build_follow_through_page(repository: Any, run_id: int, *, limit: int = 100, offset: int = 0) -> dict[str, Any] | None:
    run = repository.get_screen_run(run_id, include_hits=False)
    if run is None:
        return None
    if run.get("strategy_id") != STRATEGY_ID:
        raise ValueError("Run is not a Martin Bar Watchlist V1 run.")
    run_date = run.get("run_date")
    if isinstance(run_date, str):
        run_date = dt.date.fromisoformat(run_date)
    hit_page = repository.list_passed_screen_run_hits(run_id, limit=limit + 1, offset=offset)
    has_more = len(hit_page) > limit
    hits = hit_page[:limit]
    tickers = [str(hit.get("ticker") or "").strip().upper() for hit in hits]
    bars = repository.list_daily_bars_after([*tickers, *BENCHMARKS], run_date, limit_per_ticker=70) if run_date else {}
    expected_sessions = _completed_xnys_sessions(run_date) if run_date else None
    created = run.get("created_at")
    published_on = None
    if isinstance(created, dt.datetime):
        published_on = created.astimezone(ZoneInfo("America/New_York")).date() if created.tzinfo else created.date()
    publication_status = "unknown"
    if published_on and run_date:
        publication_status = "retrospective" if published_on > run_date else "same_date_timing_unknown"
    retrospective = publication_status == "retrospective"
    rows = []
    for hit in hits:
        ticker = str(hit.get("ticker") or "").strip().upper()
        ticker_bars = bars.get(ticker, [])
        ticker_bars_by_date = {bar["trade_date"]: bar for bar in ticker_bars}
        horizons: dict[str, Any] = {}
        for horizon in HORIZONS:
            if expected_sessions is None:
                result = {"status": "insufficient_benchmark_history"}
            else:
                market_window = expected_sessions[:horizon]
                result = {"status": "pending_horizon"} if len(market_window) < horizon else None
            market_dates = market_window if expected_sessions is not None else []
            if result is None and any(session not in ticker_bars_by_date for session in market_dates):
                result = {"status": "missing_bars"}
            elif result is None:
                window = [ticker_bars_by_date[session] for session in market_dates]
                baseline = window[0]
                opening = float(baseline["open"]) if baseline.get("open") is not None else None
                closing = float(window[-1]["close"]) if window[-1].get("close") is not None else None
                lows = [float(item["low"]) for item in window if item.get("low") is not None]
                if not opening or closing is None:
                    result = {"status": "missing_bars"}
                else:
                    sources = {str(item.get("source") or "") for item in window}
                    split = any(float(item.get("split_factor") or 1) != 1 for item in window)
                    basis_issue = split or len(sources) > 1
                    result = {
                        "status": "adjustment_basis_issue" if basis_issue else "available",
                        "return_pct": (closing / opening - 1) * 100,
                        "downside_pct": (min(lows) / opening - 1) * 100 if lows else None,
                        "end_date": window[-1]["trade_date"],
                        "source": ", ".join(sorted(source for source in sources if source)),
                    }
            horizons[str(horizon)] = result
        benchmark_returns = {}
        end_dates = {str(h): horizons[str(h)].get("end_date") for h in HORIZONS}
        for benchmark in BENCHMARKS:
            benchmark_bars = bars.get(benchmark, [])
            values = {}
            for horizon in HORIZONS:
                end_date = end_dates[str(horizon)]
                matching = next((bar for bar in benchmark_bars if bar["trade_date"] == end_date), None) if end_date else None
                baseline_date = expected_sessions[0] if expected_sessions else None
                baseline = next((bar for bar in benchmark_bars if bar["trade_date"] == baseline_date), None) if baseline_date else None
                start_open = float(baseline["open"]) if baseline and baseline.get("open") is not None else None
                end_close = float(matching["close"]) if matching and matching.get("close") is not None else None
                values[str(horizon)] = ((end_close / start_open - 1) * 100) if start_open and end_close is not None else None
            benchmark_returns[benchmark] = values
        reasons = hit.get("reasons_json")
        if isinstance(reasons, str):
            import json
            try:
                reasons = json.loads(reasons)
            except ValueError:
                reasons = []
        rows.append({
            "ticker": ticker,
            "rank": hit.get("rank"),
            "signal_date": hit.get("signal_date"),
            "reasons": reasons if isinstance(reasons, list) else [],
            "horizons": horizons,
            "benchmarks": benchmark_returns,
            "retrospective_publication": retrospective,
            "publication_status": publication_status,
        })
    return {"run": {"id": run_id, "strategy_id": run.get("strategy_id"), "run_date": run_date,
                     "created_at": created, "hit_count": run.get("hit_count")},
            "offset": offset, "limit": limit, "rows": rows, "has_more": has_more,
            "baseline": "Next trading session open; horizons count market sessions.",
            "performance_note": "Retrospective price follow-through is descriptive and is not trade P&L."}


def _completed_xnys_sessions(after_date: dt.date) -> list[dt.date] | None:
    now = dt.datetime.now(dt.timezone.utc)
    today = now.astimezone(ZoneInfo("America/New_York")).date()
    start = after_date + dt.timedelta(days=1)
    if start > today:
        return []
    try:
        import exchange_calendars as xcals

        calendar = xcals.get_calendar("XNYS", start=start.isoformat(), end=today.isoformat())
        return [
            session.date()
            for session in calendar.sessions_in_range(start.isoformat(), today.isoformat())
            if calendar.session_close(session).to_pydatetime() <= now
        ]
    except Exception:
        return None
