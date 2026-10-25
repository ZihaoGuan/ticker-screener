"""Reconstruct Martin inputs from dated evidence with explicit replay blockers.

This module never manufactures a contemporaneous Top Hits snapshot. It combines
deterministic price recalculation with eligible saved scanner evidence and keeps
unavailable dependencies distinct from executed-empty results.
"""
from __future__ import annotations

import datetime as dt
import dataclasses
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from src.cookstock_bridge import use_prefetched_market_data
from src.screener_catalog import build_screener_catalog
from src.screener_engine import ScreenerInputBundle, resolve_max_trading_days
from src.webapp.services.martin_forward_test_service import FOCUS_SCANNERS
from src.webapp.services.strike_zone import STRIKE_ZONE_SCANNER_IDS
from src.webapp.services.watchlist_service import _GURU_AVAILABLE_SCANNER_IDS, _SCANNER_BOARD_CONFIG

RECONSTRUCTION_VERSION = "martin_reconstructed_inputs_v1"
PLAN_SCANNER_IDS = frozenset({"ema21_pullback_buy", "ma_pullback_retest", "sma200_pullback_buy"})
DERIVED_SCANNER_IDS = frozenset({"rs_phase", "weinstein_stage_analysis", "rmv_tightness"})
PRICE_ONLY_INPUTS = frozenset({"daily_bars", "benchmark_bars", "metadata"})


def source_manifest() -> dict[str, Any]:
    # These are the same live definitions used by the Guru board and strike-zone.
    # The three plan dependencies are explicit because they gate supported plans.
    all_ids = set(FOCUS_SCANNERS) | set(_GURU_AVAILABLE_SCANNER_IDS) | set(STRIKE_ZONE_SCANNER_IDS)
    all_ids |= PLAN_SCANNER_IDS | DERIVED_SCANNER_IDS
    sources = {
        "focus": sorted(FOCUS_SCANNERS),
        "guru": sorted(_GURU_AVAILABLE_SCANNER_IDS),
        "strike_zone": sorted(STRIKE_ZONE_SCANNER_IDS),
        "plan": sorted(PLAN_SCANNER_IDS),
        "derived": sorted(DERIVED_SCANNER_IDS),
        "all": sorted(all_ids),
    }
    project_root = Path(__file__).resolve().parents[3]
    code_paths = (
        "src/screener_catalog.py", "src/screener_engine.py", "src/trade_plan_policies.py",
        "src/webapp/services/martin_forward_test_service.py", "src/webapp/services/watchlist_service.py",
        "src/webapp/services/strike_zone.py",
    )
    code_digest = hashlib.sha256()
    for relative_path in code_paths:
        code_digest.update(relative_path.encode())
        code_digest.update((project_root / relative_path).read_bytes())
    code_fingerprint = code_digest.hexdigest()
    canonical = json.dumps({"sources": sources, "code_fingerprint": code_fingerprint},
                           sort_keys=True, separators=(",", ":"))
    return {"version": RECONSTRUCTION_VERSION, "sources": sources,
            "code_fingerprint": code_fingerprint,
            "digest": hashlib.sha256(canonical.encode()).hexdigest()}


def _partition_missing_sources(source_ids: set[str], source_runs: dict[str, Any],
                              recalculated_ids: set[str], producer_ids: set[str]) -> tuple[list[str], list[str]]:
    missing = source_ids - set(source_runs) - recalculated_ids
    not_published = sorted(missing - producer_ids)
    unavailable = sorted(missing & producer_ids)
    return unavailable, not_published


def configuration_fingerprint(app_config: Any) -> str:
    values = dataclasses.asdict(app_config) if dataclasses.is_dataclass(app_config) else vars(app_config)
    encoded = json.dumps(values, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _source_hit(evidence: dict[str, Any], scanner_id: str) -> dict[str, Any]:
    item = evidence.get(scanner_id)
    if not isinstance(item, dict):
        return {}
    if item.get("status") == "saved_hit":
        return item
    nested = item.get("evidence")
    return dict(nested.get("hit") or {}) if isinstance(nested, dict) else {}


def _source_contributes_to_guru_board(scanner_id: str) -> bool:
    """Whether the live Guru builder treats this source as a scanner signal."""
    return scanner_id in _GURU_AVAILABLE_SCANNER_IDS or scanner_id in STRIKE_ZONE_SCANNER_IDS


def _append_guru_board_scanner(row: dict[str, Any], scanner_id: str, hit: dict[str, Any],
                               *, signal_date: Any, as_of_date: dt.date) -> bool:
    if not _source_contributes_to_guru_board(scanner_id):
        return False
    row.setdefault("scanners", []).append({"id": scanner_id, "strategy_id": scanner_id,
        "sort_date": signal_date, "as_of_date": as_of_date.isoformat(), **hit})
    return True


def _partial_source_candidate_state(scanner_id: str, source: dict[str, Any],
                                   source_hit: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any] | None] | None:
    if source.get("status") != "partial_failed":
        return None
    if source_hit:
        return ({"status": "saved_hit", "source_run_id": source["id"],
                 "source_failure_count": int(source.get("failure_count") or 0),
                 "partial_run": True, **source_hit}, None)
    return ({"status": "partial_run_no_candidate_hit", "source_run_id": source["id"],
             "source_failure_count": int(source.get("failure_count") or 0),
             "run_date": str(source["run_date"])},
            {"reason": "historical_scanner_partial_run_no_candidate_hit", "scanner_id": scanner_id,
             "source_run_id": source["id"], "failure_count": int(source.get("failure_count") or 0)})


def _candidate_source_hit_evidence(source: dict[str, Any], source_hit: dict[str, Any]) -> dict[str, Any]:
    """Represent a positive hit as known even when its run-level proof is incomplete."""
    evidence = {"status": "saved_hit", "source_run_id": source["id"], **source_hit}
    if source.get("status") == "partial_failed":
        evidence.update(partial_run=True, source_failure_count=int(source.get("failure_count") or 0))
    elif source.get("status") == "unavailable":
        evidence["source_run_provenance"] = "unavailable_positive_hit_individually_published"
    return evidence


def _utc(value: Any) -> dt.datetime:
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, dt.datetime) or value.tzinfo is None:
        raise ValueError("timezone-aware source timestamp required")
    return value.astimezone(dt.timezone.utc)


def preflight_session(cursor: Any, day: dt.date, *, next_open: Any) -> dict[str, Any]:
    """Return source coverage and blockers; never interpret absent as empty."""
    cutoff = _utc(next_open)
    manifest = source_manifest()
    ids = manifest["sources"]["all"]
    cursor.execute(
        """SELECT DISTINCT ON (strategy_id) id,strategy_id,run_date,created_at,
                  hit_count,failure_count,config_hash,source_kind
           FROM screen_runs
           WHERE deleted_at IS NULL AND strategy_id=ANY(%s) AND run_date=%s
             AND created_at < %s
           ORDER BY strategy_id,created_at DESC,id DESC""",
        (ids, day, cutoff),
    )
    records = cursor.fetchall()
    by_id = {str(row["strategy_id"]): row for row in records}
    executed = sorted(set(ids) & by_id.keys())
    empty = sorted(key for key in executed if int(by_id[key].get("hit_count") or 0) == 0
                   and int(by_id[key].get("failure_count") or 0) == 0)
    failed = sorted(key for key in executed if int(by_id[key].get("failure_count") or 0) > 0)
    unavailable = sorted(set(ids) - set(executed))
    # Existing stored scanner runs are observations, not a complete reconstruction.
    # Until every dependency has a source or an implemented deterministic evaluator,
    # the candidate universe and Guru order cannot be certified.
    blockers = []
    if unavailable:
        blockers.append({"reason": "required_scanner_source_unavailable", "scanner_ids": unavailable})
    if failed:
        blockers.append({"reason": "required_scanner_source_failed", "scanner_ids": failed})
    if records:
        blockers.append({"reason": "stored_scanner_rows_are_not_a_snapshot_compatible_reconstruction",
                         "detail": "Point-in-time candidate enrichment and ranking are not yet available."})
    else:
        blockers.append({"reason": "no_dated_scanner_sources_before_next_open"})
    return {
        "session_date": day.isoformat(), "data_cutoff": cutoff.isoformat(),
        "manifest_version": manifest["version"], "manifest_digest": manifest["digest"],
        "coverage": {"executed": executed, "executed_empty": empty,
                     "unavailable": unavailable, "failed": failed},
        "recalculated_sources": [], "candidate_count": None,
        "blocked_candidates": [], "rank_uncertainty": "session_order_not_certifiable",
        "source_fingerprint": hashlib.sha256(
            json.dumps({"manifest": manifest["digest"], "date": day.isoformat(),
                        "sources": [(str(row["strategy_id"]), int(row["id"])) for row in records]},
                       sort_keys=True).encode()).hexdigest(),
        "ready": False, "blockers": blockers,
    }


def preflight_range(cursor: Any, sessions: list[dt.date], *, calendar: Any) -> dict[dt.date, dict[str, Any]]:
    result = {}
    for day in sessions:
        next_label = calendar.next_session(day.isoformat())
        next_open = calendar.session_open(next_label)
        result[day] = preflight_session(cursor, day, next_open=next_open)
    return result


def load_dated_scanner_evidence(cursor: Any, day: dt.date, *, next_open: Any,
                                scanner_ids: set[str]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Load latest non-future persisted runs and their hit evidence for each scanner."""
    cutoff = _utc(next_open)
    strategy_map: dict[str, set[str]] = {scanner_id: {scanner_id} for scanner_id in scanner_ids}
    for item in _SCANNER_BOARD_CONFIG:
        scanner_id = str(item.get("id") or "")
        strategy_id = str(item.get("strategy_id") or "")
        if scanner_id in strategy_map and strategy_id:
            strategy_map[scanner_id].add(strategy_id)
    all_strategy_ids = sorted(set().union(*strategy_map.values()) if strategy_map else set())
    cursor.execute(
        """SELECT DISTINCT ON (sr.strategy_id) sr.id,sr.strategy_id,sr.run_date,sr.created_at,
                  sr.job_run_id,sr.hit_count,sr.failure_count,sr.config_hash,sr.source_kind,
                  job.status AS job_status,job.finished_at AS job_finished_at
           FROM screen_runs sr LEFT JOIN job_runs job ON job.id=sr.job_run_id
           WHERE sr.deleted_at IS NULL AND sr.strategy_id=ANY(%s) AND sr.run_date<=%s
           ORDER BY sr.strategy_id,sr.run_date DESC,sr.id DESC""",
        (all_strategy_ids, day),
    )
    run_rows = cursor.fetchall()
    runs_by_strategy = {str(row["strategy_id"]): row for row in run_rows}
    source_runs = {}
    run_ids = set()
    for scanner_id, possible_ids in strategy_map.items():
        options = [runs_by_strategy[item] for item in possible_ids if item in runs_by_strategy]
        if options:
            chosen = max(options, key=lambda item: (item["run_date"], int(item["id"])))
            updated = _utc(chosen["created_at"])
            job_finished_at = chosen.get("job_finished_at")
            if isinstance(job_finished_at, str):
                job_finished_at = dt.datetime.fromisoformat(job_finished_at.replace("Z", "+00:00"))
            provenance_valid = (
                chosen.get("job_run_id") is not None and
                chosen.get("job_status") == "success" and
                job_finished_at is not None and _utc(job_finished_at) < cutoff and
                updated < cutoff
            )
            if not provenance_valid:
                status = "unavailable"
            elif int(chosen.get("failure_count") or 0):
                # The run can contain valid passed hits alongside per-ticker
                # failures. Keep those positives usable; absence is uncertain.
                status = "partial_failed"
            elif int(chosen.get("hit_count") or 0) == 0:
                status = "executed_empty"
            else:
                status = "executed"
            source_runs[scanner_id] = {**chosen, "status": status,
                "publication_provenance_valid": provenance_valid}
            # Even when the run-level completion proof is invalid, inspect hit
            # rows individually: their own created_at survives as publication
            # evidence for passed positives, but never for an absent hit.
            run_ids.add(int(chosen["id"]))
    cursor.execute(
        """SELECT screen_run_id,ticker,signal_date,passed,rank,metrics_json,reasons_json,hit_payload_json,created_at
           FROM screen_run_hits WHERE screen_run_id=ANY(%s) ORDER BY screen_run_id,rank NULLS LAST,ticker""",
        (sorted(run_ids),),
    )
    hit_rows = cursor.fetchall()
    by_run = {run_id: [] for run_id in run_ids}
    for hit in hit_rows:
        hit_created_at = hit.get("created_at")
        if hit_created_at is None or _utc(hit_created_at) >= cutoff:
            continue
        payload = hit.get("hit_payload_json") if isinstance(hit.get("hit_payload_json"), dict) else {}
        signal_date = hit.get("signal_date")
        if signal_date is None:
            signal_date = payload.get("signal_date") or payload.get("event_date")
        if isinstance(signal_date, dt.datetime):
            signal_date = signal_date.date()
        if isinstance(signal_date, str):
            try:
                signal_date = dt.date.fromisoformat(signal_date[:10])
            except ValueError:
                signal_date = None
        if signal_date is not None and signal_date > day:
            continue
        by_run[int(hit["screen_run_id"])].append(hit)
    hits_by_source = {}
    for scanner_id, source in source_runs.items():
        matches = [hit for hit in by_run.get(int(source["id"]), [])
                   if bool(hit.get("passed")) and str(hit.get("ticker") or "").strip()]
        for hit in matches:
            payload = hit.get("hit_payload_json") if isinstance(hit.get("hit_payload_json"), dict) else {}
            evidence = {**payload, "ticker": str(hit["ticker"]).upper(),
                "source_id": scanner_id, "source_run_id": int(source["id"]),
                "source_run_date": str(source["run_date"]),
                "source_created_at": _utc(source["created_at"]).isoformat(),
                "source_config_hash": source.get("config_hash"),
                "source_kind": source.get("source_kind"),
                "source_hit_created_at": _utc(hit["created_at"]).isoformat(),
                "source_rank": hit.get("rank"),
                "signal_date": str(hit["signal_date"]) if hit.get("signal_date") else payload.get("signal_date")}
            hits_by_source.setdefault(scanner_id, {})[evidence["ticker"]] = evidence
    return source_runs, hits_by_source


def load_published_rating_evidence(cursor: Any, tickers: list[str], day: dt.date,
                                   *, next_open: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Load dated rating/fundamental rows whose persisted update preceded cutoff."""
    cutoff = _utc(next_open)
    if not tickers:
        return {}, {}
    cursor.execute(
        """SELECT DISTINCT ON (ticker) ticker,as_of_date,updated_at,daily_rs_rating,weekly_rs_rating,
                  overall_rating,industry_group,industry_group_rs_rank,industry_group_member_count
           FROM ticker_technical_rating_snapshots
           WHERE ticker=ANY(%s) AND as_of_date<=%s AND updated_at<%s
           ORDER BY ticker,as_of_date DESC,updated_at DESC""",
        (tickers, day, cutoff),
    )
    technical = {str(row["ticker"]).upper(): row for row in cursor.fetchall()}
    cursor.execute(
        """SELECT DISTINCT ON (ticker) ticker,as_of_date,updated_at,market_cap,sales_yoy_ttm_pct,
                  eps_yoy_ttm_pct,sector,industry
           FROM ticker_fundamentals_snapshots
           WHERE ticker=ANY(%s) AND as_of_date<=%s AND updated_at<%s
           ORDER BY ticker,as_of_date DESC,updated_at DESC""",
        (tickers, day, cutoff),
    )
    fundamentals = {str(row["ticker"]).upper(): row for row in cursor.fetchall()}
    return technical, fundamentals


def load_published_earnings_evidence(cursor: Any, tickers: list[str], day: dt.date,
                                     *, next_open: Any) -> dict[str, dict[str, Any]]:
    """Return only upcoming earnings records persisted before the next open."""
    cutoff = _utc(next_open)
    if not tickers:
        return {}
    cursor.execute(
        """SELECT DISTINCT ON (ticker) ticker,earnings_date,session_timing,source,updated_at
           FROM earnings_events WHERE ticker=ANY(%s) AND earnings_date>=%s
             AND earnings_date<=%s AND updated_at<%s
           ORDER BY ticker,earnings_date,updated_at DESC""",
        (tickers, day, day + dt.timedelta(days=90), cutoff),
    )
    return {str(row["ticker"]).upper(): row for row in cursor.fetchall()}


def evaluate_focus_price_sources(*, ticker: str, as_of_date: dt.date, bars: Any,
                                 benchmark_bars: Any, metadata: dict[str, Any],
                                 app_config: Any, scanner_ids: set[str] | None = None, database_url: str = "",
                                 technical_rating: dict[str, Any] | None = None,
                                 fundamentals: dict[str, Any] | None = None,
                                 catalog: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    """Evaluate Martin Focus scanners using only explicitly supplied as-of frames."""
    requested_scanners = set(scanner_ids or FOCUS_SCANNERS)
    if bars is None or getattr(bars, "empty", False):
        return {scanner_id: {"status": "unavailable", "reason": "missing_as_of_daily_bars"}
                for scanner_id in sorted(requested_scanners)}
    bars = bars.loc[pd.to_datetime(bars.index).date <= as_of_date]
    if bars.empty:
        return {scanner_id: {"status": "unavailable", "reason": "future_or_missing_daily_bars"}
                for scanner_id in sorted(requested_scanners)}
    latest_index = bars.index.max()
    latest_bar_date = latest_index.date() if hasattr(latest_index, "date") else latest_index
    if latest_bar_date != as_of_date:
        return {scanner_id: {"status": "unavailable", "reason": "future_or_missing_daily_bars"}
                for scanner_id in sorted(requested_scanners)}
    catalog = catalog or build_screener_catalog(app_config)
    specs = [catalog.get(scanner_id) for scanner_id in sorted(requested_scanners)]
    missing_specs = [scanner_id for scanner_id, spec in zip(sorted(requested_scanners), specs) if spec is None]
    valid_specs = [spec for spec in specs if spec is not None]
    input_errors = {}
    if "weekly_candidate_pool" in requested_scanners and not (technical_rating or {}).get("daily_rs_rating"):
        input_errors["weekly_candidate_pool"] = "published_as_of_rating_missing"
    if "qullamaggie" in requested_scanners and (
        not (technical_rating or {}).get("daily_rs_rating") or
        not (fundamentals or {}).get("market_cap")
    ):
        input_errors["qullamaggie"] = "published_as_of_rating_or_market_cap_missing"
    if "one_year_winners" in requested_scanners and not (fundamentals or {}).get("market_cap"):
        input_errors["one_year_winners"] = "published_as_of_market_cap_missing"
    days_needed = resolve_max_trading_days(valid_specs)
    if benchmark_bars is None or getattr(benchmark_bars, "empty", False):
        # Focus scans that need a benchmark cannot download one in replay.
        benchmark_bars = pd.DataFrame()
    else:
        benchmark_bars = benchmark_bars.loc[pd.to_datetime(benchmark_bars.index).date <= as_of_date]
    bundle = ScreenerInputBundle(ticker=ticker.upper(), as_of_date=as_of_date, bars=bars,
        benchmark_bars=benchmark_bars, metadata={**metadata, "ticker": ticker.upper()},
        extras={"config": app_config, "database_url": database_url,
                "technical_rating_snapshots": {ticker.upper(): technical_rating or {}},
                "fundamental_snapshots": {ticker.upper(): fundamentals or {}}})
    results = {}
    results.update({scanner_id: {"status": "unavailable", "reason": "missing_catalog_evaluator"}
                    for scanner_id in missing_specs})
    with use_prefetched_market_data(
        ticker_frames={ticker.upper(): bars},
        benchmark_frames={str(app_config.benchmark_ticker).upper(): benchmark_bars},
    ):
        for spec in specs:
            if spec is None:
                continue
            if spec.id in input_errors:
                results[spec.id] = {"status": "unavailable", "reason": input_errors[spec.id]}
                continue
            if any(name not in PRICE_ONLY_INPUTS for name in spec.required_inputs):
                results[spec.id] = {"status": "unavailable", "reason": "non_price_input_required"}
                continue
            try:
                evaluated = spec.evaluator(bundle) if spec.evaluator is not None else None
                if evaluated is None or evaluated.error:
                    results[spec.id] = {"status": "unavailable", "reason":
                        str(getattr(evaluated, "error", None) or "evaluator_not_configured")}
                else:
                    results[spec.id] = {"status": "executed_match" if evaluated.passed else "executed_empty",
                        "metrics": dict(evaluated.metrics), "hit": dict(evaluated.hit or {}),
                        "reasons": list(evaluated.reasons), "lookback_trading_days": days_needed}
            except Exception as exc:
                results[spec.id] = {"status": "unavailable", "reason": f"evaluator_error:{type(exc).__name__}"}
    return results


def reconstruct_price_focus_session(cursor: Any, day: dt.date, *, next_open: Any,
                                    database_url: str, app_config: Any,
                                    policy_id: str | None = None,
                                    ticker_limit: int | None = None) -> dict[str, Any]:
    """Recalculate the seven Focus scans from stored OHLCV, never from web fallback.

    This produces a snapshot-compatible reconstructed board from dated sources.
    Unknown optional context is preserved as unknown, matching the saved policy.
    """
    from src.webapp.services.martin_rs_replay import valid_bar

    cutoff = _utc(next_open)
    manifest = source_manifest()
    config_fingerprint = configuration_fingerprint(app_config)
    catalog = build_screener_catalog(app_config)
    from src.webapp.services.run_service import RunService
    producer_ids = set(catalog) | set(getattr(RunService, "_actions", {}))
    for board_item in _SCANNER_BOARD_CONFIG:
        if board_item.get("id") in producer_ids and board_item.get("strategy_id"):
            producer_ids.add(str(board_item["strategy_id"]))
    required_price_ids = set(FOCUS_SCANNERS) | set(PLAN_SCANNER_IDS) | set(DERIVED_SCANNER_IDS)
    focus_specs = [catalog[scanner_id] for scanner_id in sorted(required_price_ids) if scanner_id in catalog]
    missing_specs = sorted(FOCUS_SCANNERS - set(catalog))
    lookback = resolve_max_trading_days(focus_specs)
    import exchange_calendars as xcals
    calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=lookback * 3 + 30),
                                  end=day + dt.timedelta(days=10))
    history_sessions = calendar.sessions_in_range(
        (day - dt.timedelta(days=lookback * 3 + 30)).isoformat(), day.isoformat())
    history_dates = [label.date() for label in history_sessions]
    if len(history_dates) < lookback:
        return {"session_date": day.isoformat(), "ready": False,
                "blockers": [{"reason": "insufficient_xnys_warmup_sessions", "required": lookback,
                              "available": len(history_dates)}]}
    history_start = history_dates[-lookback]
    universe_window_start = day
    # The replay universe is the set of symbols with a valid observable bar on
    # the target session. Prior bars are then loaded for those symbols below.
    cursor.execute("""SELECT DISTINCT ticker FROM daily_bars
        WHERE trade_date=%s ORDER BY ticker""", (day,))
    universe = [str(row["ticker"]).strip().upper() for row in cursor.fetchall()
                if str(row.get("ticker") or "").strip()]
    if ticker_limit is not None:
        universe = universe[:max(0, int(ticker_limit))]
    benchmark = str(app_config.benchmark_ticker).upper()
    cursor.execute("""SELECT COUNT(*) AS count FROM daily_bars
        WHERE trade_date BETWEEN %s AND %s AND updated_at >= %s""", (history_start, day, cutoff))
    revised_bar_count = int(cursor.fetchone()["count"])
    cursor.execute("""SELECT ticker,trade_date,open,high,low,close,adj_close,volume,source,split_factor,updated_at
        FROM daily_bars WHERE ticker=ANY(%s) AND trade_date BETWEEN %s AND %s
        ORDER BY ticker,trade_date""", ([*universe, benchmark], history_start, day))
    bar_rows = cursor.fetchall()
    grouped_bars: dict[str, list[dict[str, Any]]] = {}
    for bar in bar_rows:
        grouped_bars.setdefault(str(bar["ticker"]).upper(), []).append(bar)
    frames = {}
    for ticker, ticker_bars in grouped_bars.items():
        frame = pd.DataFrame([{"Open": bar["open"], "High": bar["high"], "Low": bar["low"],
                               "Close": bar["close"], "Adj Close": bar["adj_close"],
                               "Volume": bar["volume"], "Split Factor": bar["split_factor"]} for bar in ticker_bars],
                             index=pd.to_datetime([bar["trade_date"] for bar in ticker_bars]))
        frame.attrs["bar_sources"] = sorted({str(bar["source"] or "") for bar in ticker_bars})
        frame.attrs["bar_max_updated_at"] = max(_utc(bar["updated_at"]) for bar in ticker_bars).isoformat()
        frame.attrs["bar_revision_fingerprint"] = hashlib.sha256(
            frame[["Open", "High", "Low", "Close", "Adj Close", "Volume", "Split Factor"]]
                .to_json(date_format="iso", double_precision=15).encode()).hexdigest()
        frames[ticker] = frame
    benchmark_bars = frames.get(benchmark)
    technical_by_ticker, fundamentals_by_ticker = load_published_rating_evidence(
        cursor, universe, day, next_open=cutoff)
    earnings_by_ticker = load_published_earnings_evidence(cursor, universe, day, next_open=cutoff)
    saved_source_runs, saved_source_hits = load_dated_scanner_evidence(
        cursor, day, next_open=cutoff, scanner_ids=set(manifest["sources"]["all"]))
    matched = []
    scanner_states: dict[str, int] = {source_id: 0 for source_id in sorted(FOCUS_SCANNERS)}
    unavailable_scanners: set[str] = set(missing_specs)
    stale_universe_tickers: list[str] = []
    history_coverage_issues: list[dict[str, Any]] = []
    scanner_history_exclusions: dict[str, int] = {source_id: 0 for source_id in sorted(FOCUS_SCANNERS)}
    ticker_history_gaps: dict[str, set[str]] = {}
    benchmark_dates = {index.date() if hasattr(index, "date") else index
                       for index in (benchmark_bars.index if benchmark_bars is not None else [])}
    benchmark_coverage_issues = []
    for spec in focus_specs:
        if "benchmark_bars" in spec.required_inputs:
            missing = sorted(set(history_dates[-spec.total_trading_days:]) - benchmark_dates)
            if missing:
                benchmark_coverage_issues.append({"scanner_id": spec.id,
                    "reason": "benchmark_xnys_history_missing",
                    "missing_dates": [item.isoformat() for item in missing]})
    unavailable_scanners.update(issue["scanner_id"] for issue in benchmark_coverage_issues)
    for ticker in universe:
        frame = frames.get(ticker)
        if frame is None or frame.empty:
            stale_universe_tickers.append(ticker)
            history_coverage_issues.append({"ticker": ticker, "reason": "target_session_bar_not_loaded"})
            unavailable_scanners.update(FOCUS_SCANNERS)
            continue
        available_dates = {index.date() if hasattr(index, "date") else index for index in frame.index}
        if day not in available_dates:
            stale_universe_tickers.append(ticker)
            history_coverage_issues.append({"ticker": ticker, "reason": "target_session_bar_missing"})
            # This should be rare because the universe is selected by target
            # date; preserve it as a session-level data integrity blocker.
            unavailable_scanners.update(FOCUS_SCANNERS)
            continue
        if any(not valid_bar(bar) for _, bar in frame.iterrows()):
            stale_universe_tickers.append(ticker)
            unavailable_scanners.update(FOCUS_SCANNERS)
            continue
        insufficient_focus_ids = set()
        for spec in focus_specs:
            expected_sessions = history_dates[-spec.total_trading_days:]
            missing_sessions = sorted(set(expected_sessions) - available_dates)
            if missing_sessions:
                history_coverage_issues.append({"ticker": ticker, "scanner_id": spec.id,
                    "reason": "required_xnys_history_missing",
                    "missing_dates": [item.isoformat() for item in missing_sessions]})
                ticker_history_gaps.setdefault(ticker, set()).add(spec.id)
                if spec.id in FOCUS_SCANNERS:
                    insufficient_focus_ids.add(spec.id)
                    scanner_history_exclusions[spec.id] += 1
        if benchmark_coverage_issues:
            continue
        scan_results = evaluate_focus_price_sources(ticker=ticker, as_of_date=day, bars=frame,
            benchmark_bars=benchmark_bars, metadata={"ticker": ticker}, app_config=app_config,
            database_url=database_url, technical_rating=technical_by_ticker.get(ticker, {}),
            fundamentals=fundamentals_by_ticker.get(ticker, {}), catalog=catalog,
            scanner_ids=set(FOCUS_SCANNERS) - insufficient_focus_ids)
        for scanner_id in insufficient_focus_ids:
            scan_results[scanner_id] = {"status": "excluded_insufficient_history",
                "reason": "required_xnys_history_missing"}
        focus_hits = []
        focus_evidence = {}
        for scanner_id, result in scan_results.items():
            focus_evidence[scanner_id] = {"status": result.get("status"),
                "source": "recalculated_price_evaluator", "as_of_date": day.isoformat(),
                "manifest_digest": manifest["digest"], "evidence": result}
            if result.get("status") == "executed_match":
                scanner_states[scanner_id] += 1
                hit = dict(result.get("hit") or {})
                focus_hits.append({"id": scanner_id, "strategy_id": scanner_id,
                    "sort_date": hit.get("signal_date") or day.isoformat(), "as_of_date": day.isoformat(),
                    "source": "recalculated_price_evaluator", **hit, "metrics": result.get("metrics") or {}})
            elif result.get("status") == "executed_empty":
                scanner_states[scanner_id] += 1
            elif result.get("status") != "excluded_insufficient_history":
                unavailable_scanners.add(scanner_id)
        if focus_hits:
            close = float(frame.loc[:pd.Timestamp(day)].iloc[-1]["Close"])
            matched.append({"ticker": ticker, "close": close, "scanner_ids": sorted(item["id"] for item in focus_hits),
                            "scanners": focus_hits,
                            "source_evidence": focus_evidence,
                            "scanner_count": len({item["id"] for item in focus_hits}),
                            "bar_as_of_date": day.isoformat(),
                            "bar_revision": "current_stored_ohlcv_retrospective",
                            "bar_revision_fingerprint": frame.attrs.get("bar_revision_fingerprint"),
                            "bar_sources": frame.attrs.get("bar_sources", []),
                            "bar_max_updated_at": frame.attrs.get("bar_max_updated_at"),
                            "adjustment_basis": "stored_ohlcv_and_split_factor"})
    tickers = [row["ticker"] for row in matched]
    price_recalculated_ids = set(FOCUS_SCANNERS) | set(PLAN_SCANNER_IDS) | set(DERIVED_SCANNER_IDS)
    unavailable_source_ids, not_published_source_ids = _partition_missing_sources(
        set(manifest["sources"]["all"]), saved_source_runs, price_recalculated_ids, producer_ids)
    unavailable_source_ids = sorted(set(unavailable_source_ids) |
        {source_id for source_id, source in saved_source_runs.items() if source.get("status") == "unavailable"})
    failed_source_ids = sorted(source_id for source_id, source in saved_source_runs.items()
                               if source.get("status") == "failed" and source_id not in price_recalculated_ids)
    partial_source_ids = sorted(source_id for source_id, source in saved_source_runs.items()
                                if source.get("status") == "partial_failed" and source_id not in price_recalculated_ids)
    blockers = []
    rows = []
    from src.trade_plan_policies import CURRENT_STOP_POLICY, STRUCTURAL_STOP_POLICY, current_plan_payload, structural_plan_payload
    from src.webapp.services.watchlist_service import (_build_guru_strike_zone, _build_trade_plan,
        _guru_row_sort_key, _trade_readiness)
    for row in matched:
        ticker = row["ticker"]
        market = fundamentals_by_ticker.get(ticker)
        rating = technical_by_ticker.get(ticker)
        candidate_issues = []
        frame = frames[ticker]
        evidence = dict(row.get("source_evidence") or {})
        plan_artifacts = []
        insufficient_candidate_ids = ticker_history_gaps.get(ticker, set())
        eligible_extra_ids = (set(PLAN_SCANNER_IDS) | set(DERIVED_SCANNER_IDS)) - insufficient_candidate_ids
        extra_price = evaluate_focus_price_sources(ticker=ticker, as_of_date=day, bars=frame,
            benchmark_bars=benchmark_bars, metadata={"ticker": ticker}, app_config=app_config,
            scanner_ids=eligible_extra_ids, database_url=database_url,
            technical_rating=rating or {}, fundamentals=market or {}, catalog=catalog)
        for scanner_id in insufficient_candidate_ids & (set(PLAN_SCANNER_IDS) | set(DERIVED_SCANNER_IDS)):
            extra_price[scanner_id] = {"status": "unavailable", "reason": "required_xnys_history_missing"}
        for scanner_id, result in extra_price.items():
            evidence[scanner_id] = {"status": result.get("status"), "source": "recalculated_price_evaluator",
                                    "as_of_date": day.isoformat(), "evidence": result}
            hit = dict(result.get("hit") or {}) if result.get("status") == "executed_match" else {}
            saved_source_hit = saved_source_hits.get(scanner_id, {}).get(ticker)
            if not hit and saved_source_hit:
                hit = dict(saved_source_hit)
                evidence[scanner_id] = {"status": "saved_hit", **saved_source_hit}
            if hit:
                signal_date = hit.get("signal_date") or day.isoformat()
                _append_guru_board_scanner(row, scanner_id, hit,
                    signal_date=signal_date, as_of_date=day)
                if scanner_id in PLAN_SCANNER_IDS:
                    plan_artifacts.append({**hit, "_source_strategy_id": scanner_id,
                        "_source_date": signal_date, "event_date": signal_date,
                        "signal_state": hit.get("signal_state") or "active"})
            elif result.get("status") == "unavailable":
                candidate_issues.append({"reason": "price_source_evaluator_unavailable",
                    "scanner_id": scanner_id, "detail": result.get("reason")})
        for scanner_id in sorted(set(manifest["sources"]["all"]) - price_recalculated_ids):
            source = saved_source_runs.get(scanner_id)
            source_hit = saved_source_hits.get(scanner_id, {}).get(ticker)
            if scanner_id in not_published_source_ids:
                evidence[scanner_id] = {"status": "not_published",
                    "reason": "no_registered_catalog_or_run_action_producer",
                    "as_of_date": day.isoformat()}
                continue
            partial_state = _partial_source_candidate_state(scanner_id, source, source_hit) if source else None
            if partial_state:
                evidence[scanner_id], partial_issue = partial_state
                if source_hit:
                    evidence[scanner_id] = _candidate_source_hit_evidence(source, source_hit)
                    row["scanners"].append({"id": scanner_id, "strategy_id": scanner_id,
                        "sort_date": source_hit.get("signal_date"), **source_hit})
                if partial_issue:
                    candidate_issues.append(partial_issue)
            elif source and source.get("status") == "unavailable" and source_hit:
                # A passed hit has its own immutable publication timestamp.
                # This proves the positive for this ticker, never a negative.
                evidence[scanner_id] = _candidate_source_hit_evidence(source, source_hit)
                row["scanners"].append({"id": scanner_id, "strategy_id": scanner_id,
                    "sort_date": source_hit.get("signal_date"), **source_hit})
            elif source and source.get("status") == "failed":
                evidence[scanner_id] = {"status": "failed", "source_run_id": source["id"]}
                candidate_issues.append({"reason": "historical_scanner_run_failed",
                    "scanner_id": scanner_id, "source_run_id": source["id"]})
            elif source and source.get("status") == "unavailable":
                evidence[scanner_id] = {"status": "unavailable", "source_run_id": source["id"],
                    "job_run_id": source.get("job_run_id"),
                    "job_status": source.get("job_status"),
                    "job_finished_at": (_utc(source["job_finished_at"]).isoformat()
                        if source.get("job_finished_at") else None),
                    "reason": "current_job_publication_provenance_invalid"}
                candidate_issues.append({"reason": "historical_scanner_job_provenance_unavailable",
                    "scanner_id": scanner_id, "source_run_id": source["id"],
                    "job_run_id": source.get("job_run_id"), "job_status": source.get("job_status")})
            elif source_hit:
                evidence[scanner_id] = {"status": "saved_hit", **source_hit}
                row["scanners"].append({"id": scanner_id, "strategy_id": scanner_id,
                    "sort_date": source_hit.get("signal_date"), **source_hit})
            elif source and source.get("status") in {"executed", "executed_empty"}:
                evidence[scanner_id] = {"status": "executed_empty_for_candidate", "source_run_id": source["id"],
                    "run_date": str(source["run_date"]), "created_at": _utc(source["created_at"]).isoformat()}
            else:
                evidence[scanner_id] = {"status": "unavailable"}
                candidate_issues.append({"reason": "historical_scanner_source_unavailable", "scanner_id": scanner_id})
        if market is None or market.get("market_cap") is None:
            candidate_issues.append({"reason": "as_of_market_cap_missing"})
        else:
            row["market_cap"] = float(market["market_cap"]) if market.get("market_cap") is not None else None
            row["market_cap_as_of"] = str(market.get("as_of_date") or "")
            row["market_cap_updated_at"] = _utc(market["updated_at"]).isoformat()
            row["market_cap_source"] = "ticker_fundamentals_snapshots_as_of"
        if rating is None or rating.get("daily_rs_rating") is None:
            candidate_issues.append({"reason": "as_of_daily_rs_rating_missing"})
        else:
            row["daily_rs_rating"] = float(rating["daily_rs_rating"])
            row["rating_as_of"] = str(rating.get("as_of_date") or "")
            row["rating_updated_at"] = _utc(rating["updated_at"]).isoformat()
            row["rating_source"] = "ticker_technical_rating_snapshots_as_of"
        if market:
            row["sector"] = market.get("sector")
            row["industry"] = market.get("industry")
        row["sector_momentum"] = None
        row["sector_momentum_evidence"] = {"state": "unknown", "reason": "no_dated_sector_momentum_record"}
        earnings = earnings_by_ticker.get(ticker)
        if earnings:
            row["earnings_days"] = (earnings["earnings_date"] - day).days
            row["earnings_evidence"] = {"state": "known", "earnings_date": str(earnings["earnings_date"]),
                "source": earnings.get("source"), "updated_at": _utc(earnings["updated_at"]).isoformat(),
                "session_timing": earnings.get("session_timing")}
        else:
            row["earnings_days"] = None
            row["earnings_evidence"] = {"state": "unknown", "reason": "no_record_available_before_cutoff"}
        quality_artifact = next((item for item in plan_artifacts
            if item.get("_source_strategy_id") == "ma_pullback_retest" and
            isinstance(item.get("pullback_quality"), dict)), None)
        if quality_artifact:
            row["pullback_quality"] = dict(quality_artifact["pullback_quality"])
            row["pullback_quality_evidence"] = {"state": "known", "source_strategy_id": "ma_pullback_retest",
                "signal_date": quality_artifact.get("_source_date"),
                "source_run_id": quality_artifact.get("source_run_id")}
        else:
            row["pullback_quality"] = None
            row["pullback_quality_evidence"] = {"state": "unknown", "reason": "no_dated_ma_pullback_quality"}
        row["sector_evidence"] = {"state": "known" if row.get("sector") else "unknown",
            "source": "ticker_fundamentals_snapshots_as_of" if row.get("sector") else None,
            "as_of_date": str((market or {}).get("as_of_date") or "") or None}
        row["day_close"] = row["close"]
        row["adr14_pct"] = float((((frame["High"] - frame["Low"]) / frame["Close"] * 100).tail(14)).mean())
        row["stage_analysis"] = _source_hit(evidence, "weinstein_stage_analysis") or None
        row["rmv"] = _source_hit(evidence, "rmv_tightness") or None
        phase = _source_hit(evidence, "rs_phase")
        row["rs_phase_state"] = phase.get("rs_phase_state")
        row["rs_phase_active_days"] = phase.get("rs_phase_active_days")
        row["trade_plan"] = _build_trade_plan(plan_artifacts)
        row["trade_plans"] = {CURRENT_STOP_POLICY: current_plan_payload(row, day),
                               STRUCTURAL_STOP_POLICY: structural_plan_payload(row, day, frame)}
        risk = (row.get("trade_plan") or {}).get("reference_risk_pct")
        adr = row.get("adr14_pct")
        if risk is not None and adr:
            row["stop_to_adr"] = round(float(risk) / float(adr), 2)
            if float(risk) > 5:
                row["adr_suitability"] = "risk_failure"
            elif float(adr) < 3:
                row["adr_suitability"] = "low"
            elif float(adr) < 5:
                row["adr_suitability"] = "wide_stop" if row["stop_to_adr"] > .5 else "caution"
            else:
                row["adr_suitability"] = "wide_stop" if row["stop_to_adr"] > .5 else "suitable"
        row["strike_zone"] = _build_guru_strike_zone(row, as_of_date=day)
        row["strike_score"] = row["strike_zone"].get("score")
        row["scanner_count"] = len({item.get("id") or item.get("strategy_id") for item in row["scanners"]})
        row["trade_readiness"] = _trade_readiness(row)
        row["source_evidence"] = evidence
        row["source_run_ids"] = sorted({int(item["source_run_id"]) for item in evidence.values()
                                         if item.get("source_run_id")})
        row["replay_rank_key"] = list(_guru_row_sort_key(row))
        row["rank"] = None
        if policy_id == "martin_sector_book_v1" and not row.get("sector"):
            # The existing Sector policy uses Unknown momentum but still requires
            # a sector key to apply its primary-per-sector quota.
            candidate_issues.append({"reason": "historical_sector_label_missing"})
        row["source_evidence_complete"] = not candidate_issues and not unavailable_source_ids and not failed_source_ids
        row["blocked"] = bool(candidate_issues)
        row["blockers"] = candidate_issues
        blockers.extend({"ticker": ticker, **issue} for issue in candidate_issues)
        rows.append(row)
    fingerprint_payload = {"date": day.isoformat(), "manifest": manifest["digest"],
                           "config": config_fingerprint,
                           "sources": {key: {"id": value.get("id"), "run_date": str(value.get("run_date")),
                                            "config_hash": value.get("config_hash"), "status": value.get("status")}
                                       for key, value in sorted(saved_source_runs.items())},
                           "rows": [{"ticker": row["ticker"], "scanner_ids": row["scanner_ids"],
                                     "source_run_ids": row.get("source_run_ids", []),
                                     "bar_revision_fingerprint": row.get("bar_revision_fingerprint"),
                                     "market_cap": row.get("market_cap"), "daily_rs_rating": row.get("daily_rs_rating"),
                                     "adr14_pct": row.get("adr14_pct"), "source_evidence": row.get("source_evidence")}
                                    for row in rows]}
    rows.sort(key=_guru_row_sort_key)
    for rank, row in enumerate(rows, start=1):
        row["rank"] = rank
        row["rank_type"] = ("provisional_reconstructed_order"
                            if failed_source_ids or unavailable_scanners or row.get("blocked")
                            else "reconstructed_guru_sort_key")
    # Optional run-level absence is evaluated per candidate: an early passed
    # hit proves a positive, while an unproven no-hit blocks that candidate.
    source_unavailable = sorted(set(unavailable_scanners) | set(failed_source_ids))
    ready = bool(universe) and not source_unavailable and not any(row.get("blocked") for row in rows)
    return {
        "session_date": day.isoformat(), "data_cutoff": cutoff.isoformat(),
        "manifest_version": manifest["version"], "manifest_digest": manifest["digest"],
        "code_fingerprint": manifest["code_fingerprint"],
        "config_fingerprint": config_fingerprint,
        "source_fingerprint": hashlib.sha256(json.dumps(fingerprint_payload, sort_keys=True).encode()).hexdigest(),
        "input_source": RECONSTRUCTION_VERSION,
        "coverage": {"focus_recalculated": sorted(FOCUS_SCANNERS - unavailable_scanners),
                     "focus_matches_by_scanner": scanner_states,
                     "universe_count": len(universe), "universe_window_start": universe_window_start.isoformat(),
                     "target_day_bar_count": len(universe),
                     "scanner_history_exclusions": scanner_history_exclusions,
                     "unavailable": sorted(unavailable_scanners),
                     "stale_or_missing_target_bar_tickers": sorted(set(stale_universe_tickers)),
                     "history_coverage_issues": history_coverage_issues,
                     "benchmark_coverage_issues": benchmark_coverage_issues,
                     "retrospectively_revised_bar_rows": revised_bar_count,
                     "source_run_count": len(saved_source_runs),
                     "source_runs": {key: {"id": value["id"], "run_date": str(value["run_date"]),
                         "created_at": _utc(value["created_at"]).isoformat(), "status": value["status"]}
                         for key, value in sorted(saved_source_runs.items())},
                     "unavailable_dependencies": unavailable_source_ids,
                     "not_published_dependencies": not_published_source_ids,
                     "failed_dependencies": failed_source_ids,
                     "partial_dependencies": partial_source_ids},
        "recalculated_sources": sorted(FOCUS_SCANNERS - unavailable_scanners),
        "candidate_count": len(rows), "blocked_candidates": blockers,
        "rank_uncertainty": ("not_applicable_zero_candidates" if not rows else
            "candidate_decision_inputs_uncertain" if any(row.get("blocked") for row in rows) else
            "guru_order_not_certifiable" if failed_source_ids or unavailable_scanners else "certified_from_available_inputs"),
        "rows": rows, "ready": ready,
        "blockers": ([{"reason": "focus_catalog_evaluator_missing", "scanner_ids": missing_specs}]
                     if missing_specs else []) + ([{"reason": "focus_evaluator_or_history_unavailable",
                                                    "scanner_ids": sorted(unavailable_scanners)}]
                     if unavailable_scanners else []) + ([{"reason": "optional_source_rank_uncertainty",
                        "scanner_ids": unavailable_source_ids}] if unavailable_source_ids and rows and any(row.get("blocked") for row in rows) else []) +
                    ([{"reason": "optional_source_rank_uncertainty_due_to_failed_runs", "scanner_ids": failed_source_ids}]
                     if failed_source_ids and rows else []) + ([{"reason": "candidate_inputs_blocked",
                        "blocked_candidate_count": len([row for row in rows if row.get("blocked")])}]
                     if any(row.get("blocked") for row in rows) else []) +
                    ([{"reason": "historical_universe_empty_or_unavailable"}] if not universe else []),
        "limitations": "Bars use current stored OHLCV revisions filtered by trade_date; retrospective adjustment changes remain possible. Non-price Guru enrichment is not reconstructed.",
    }
