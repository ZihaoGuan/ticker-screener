from __future__ import annotations

import datetime as dt
from typing import Any, Iterable

from .market_data_access import load_many_ticker_windows
from .power_gap_volume_watch import detect_many, enrich_shortlisted_gaps


def _rows_from_frame(frame: Any) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    rows = []
    for index, row in frame.iterrows():
        rows.append({"date": index.date().isoformat(), "open": row["Open"], "high": row["High"],
                     "low": row["Low"], "close": row["Close"], "volume": row["Volume"],
                     "adjustment_basis": "stored_daily_bars_ohlcv"})
    return rows


def run_power_gap_volume_watch(config: Any, tickers: Iterable[Any], *, as_of_date: dt.date | None = None,
                               provider: str | None = None) -> dict[str, Any]:
    """Database-first bounded batch screen; calendar/data issues prevent false matches."""
    requested = as_of_date or dt.date.today()
    try:
        import exchange_calendars as xcals
        import pandas as pd
        calendar = xcals.get_calendar("XNYS")
        sessions = calendar.sessions_in_range(pd.Timestamp(requested - dt.timedelta(days=14)), pd.Timestamp(requested))
        now_utc = dt.datetime.now(dt.timezone.utc)
        sessions = [session for session in sessions if calendar.session_close(session).to_pydatetime() <= now_utc]
        if not len(sessions):
            raise RuntimeError("No completed XNYS session exists on or before the requested date.")
        cutoff = sessions[-1].date()
    except Exception as exc:
        return {"strategy_id": "power_gap_volume_watch", "rules_version": "power_gap_volume_watch_v1",
                "run_date": requested.isoformat(), "total_tickers": 0, "passed_tickers": 0,
                "failed_tickers": [{"ticker": "*", "reason": f"XNYS calendar unavailable: {exc}"}], "results": [], "hits": []}

    items = list(tickers)
    metadata = {str(getattr(item, "symbol", item)).strip().upper():
                {key: getattr(item, key, None) for key in ("sector", "industry", "exchange")} for item in items}
    symbols = list(metadata)
    results = []
    # Five thousand sessions reaches materially farther back than a five-year
    # calendar slice. Batches bound both database result and in-memory history.
    for offset in range(0, len(symbols), 200):
        batch_symbols = symbols[offset:offset + 200]
        try:
            frames = load_many_ticker_windows(batch_symbols, cutoff, 5000)
        except Exception as exc:
            results.extend({"ticker": ticker, "signals": [], "power_gap_volume_context": None,
                            "coverage": {"issues": [f"daily history batch load failed: {exc}"],
                                         "history_start": None, "history_end": None, "daily_bars": 0,
                                         "hvy_complete": False, "hvy_history": "unknown",
                                         "listing_status": "unknown_without_listing_metadata",
                                         "available_history_short": False}} for ticker in batch_symbols)
            continue
        row_data: dict[str, list[dict[str, Any]]] = {}
        conversion_errors: dict[str, str] = {}
        for symbol in batch_symbols:
            try:
                row_data[symbol] = _rows_from_frame(frames.get(symbol))
            except Exception as exc:
                row_data[symbol] = []
                conversion_errors[symbol] = f"daily history conversion failed: {exc}"
        observed_dates = [dt.date.fromisoformat(row["date"]) for rows in row_data.values() for row in rows]
        if not observed_dates:
            expected_sessions = []
        else:
            all_sessions = calendar.sessions_in_range(pd.Timestamp(min(observed_dates) - dt.timedelta(days=7)), pd.Timestamp(cutoff))
            expected_sessions = [session.date() for session in all_sessions]
        batch_results = detect_many(row_data, cutoff, expected_sessions)
        for row in batch_results:
            if row["ticker"] in conversion_errors:
                row["coverage"]["issues"] = [conversion_errors[row["ticker"]]]
                row["signals"] = []
                row["power_gap_volume_context"] = None
            row.update({key: value for key, value in metadata.get(row["ticker"], {}).items() if value})
            results.append(row)

    shortlisted = [row for row in results if row.get("signals")]
    # Earnings is optional annotation. A provider/cache failure never removes a
    # valid technical candidate.
    if shortlisted:
        try:
            shortlisted = enrich_shortlisted_gaps(shortlisted, config, cutoff, provider=provider)
            enriched_by_ticker = {row["ticker"]: row for row in shortlisted}
            results = [enriched_by_ticker.get(row["ticker"], row) for row in results]
        except Exception as exc:
            for row in shortlisted:
                context = row["power_gap_volume_context"]
                if "gap" in row["signals"] and context.get("gap_event"):
                    context["earnings_context"] = {
                        "status": "lookup_failed", "confirmed": False, "match_reason": f"provider_lookup_failed: {type(exc).__name__}",
                        "signal_date": context["gap_event"]["date"], "evidence_mode": "retrospective_lookup"}
            enriched_by_ticker = {row["ticker"]: row for row in shortlisted}
            results = [enriched_by_ticker.get(row["ticker"], row) for row in results]
    shortlisted = [row for row in results if row.get("signals")]
    return {"strategy_id": "power_gap_volume_watch", "rules_version": "power_gap_volume_watch_v1",
            "run_date": cutoff.isoformat(), "total_tickers": len(results), "passed_tickers": len(shortlisted),
            "failed_tickers": [{"ticker": row["ticker"], "reason": "; ".join(row["coverage"].get("issues", []))}
                               for row in results if row["coverage"].get("issues")],
            "coverage_warnings": [{"ticker": row["ticker"], "reasons": list(row["coverage"].get("component_issues", {}).values())}
                                  for row in results if row["coverage"].get("component_issues") and row.get("signals")],
            "results": results, "hits": shortlisted}


def build_power_gap_volume_watchlist(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create event-discovery rows without inventing an entry plan or stop."""
    rows = []
    for hit in hits:
        context = hit["power_gap_volume_context"]
        gap = context.get("gap_event")
        signal_names = [name.upper() for name in hit.get("signals", [])]
        if gap and "gap" in hit.get("signals", []):
            age = gap["age_trading_sessions"]
            summary = f"Gap +{gap['gap_pct']:.1f}% · {gap['volume_ratio']:.1f}× volume · {age} sessions ago"
            event_date = gap["date"]
        else:
            summary = "Recent " + " + ".join(signal_names) + " volume event"
            event_date = max((context.get("volume_events", {}).get(kind) or {}).get("date", "")
                             for kind in ("hve", "hvy")) or None
        if gap and "gap" in hit.get("signals", []):
            earnings = context.get("earnings_context") or {}
            labels = {"earnings_confirmed": "confirmed", "earnings_nearby_timing_unknown": "nearby; timing uncertain",
                      "earnings_nearby_unconfirmed": "nearby; reaction unconfirmed", "catalyst_unknown": "context unknown",
                      "lookup_failed": "lookup failed"}
            summary += f" · Earnings {labels.get(earnings.get('status'), 'context unknown')}"
        rows.append({"ticker": hit["ticker"], "sector": hit.get("sector"), "industry": hit.get("industry"),
                     "exchange": hit.get("exchange"), "setup_label": "Power Gap & Volume Watch",
                     "summary": summary, "master_note": "Gap/volume discovery only; entry setup not confirmed.",
                     "event_date": event_date, "event_label": " + ".join(signal_names),
                     "signal_badges": signal_names, "power_gap_volume_context": context})
    return rows
