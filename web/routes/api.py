from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from src.webapp.services.admin_service import AdminService
from src.webapp.services.backtest_service import BacktestService, DEFAULT_EXIT_RULES
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import (
    get_ad_hoc_screen_service,
    get_admin_service,
    get_backtest_service,
    get_dashboard_service,
    get_overlap_service,
    get_rrg_service,
    get_run_service,
    get_screener_history_service,
    get_watchlist_service,
)


router = APIRouter(prefix="/api", tags=["api"])


@router.get("/dashboard", response_class=JSONResponse)
def dashboard_data(service: DashboardService = Depends(get_dashboard_service)) -> JSONResponse:
    return JSONResponse(service.get_dashboard_context())


@router.get("/jobs", response_class=JSONResponse)
def jobs_data(service: RunService = Depends(get_run_service)) -> JSONResponse:
    return JSONResponse({"actions": service.list_actions(), "jobs": service.list_jobs()})


@router.get("/jobs/{job_id}", response_class=JSONResponse)
def job_detail(job_id: str, service: RunService = Depends(get_run_service)) -> JSONResponse:
    try:
        return JSONResponse(service.get_job(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/jobs/{job_id}/cancel", response_class=JSONResponse)
def cancel_job(job_id: str, service: RunService = Depends(get_run_service)) -> JSONResponse:
    try:
        return JSONResponse({"ok": True, "job": service.cancel(job_id)})
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if message.startswith("Unknown job") else 400
        raise HTTPException(status_code=status_code, detail=message) from exc


@router.post("/runs/{action_id}", response_class=JSONResponse)
def run_action(
    action_id: str,
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
) -> JSONResponse:
    try:
        job_id = service.launch(action_id, options=payload or {})
    except ValueError as exc:
        status_code = 404 if str(exc).startswith("Unknown run action") else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "job_id": job_id})


@router.post("/ad-hoc-screen", response_class=JSONResponse)
def ad_hoc_screen(
    payload: dict[str, object] | None = Body(default=None),
    service: AdHocScreenService = Depends(get_ad_hoc_screen_service),
) -> JSONResponse:
    request_payload = payload or {}
    ticker = str(request_payload.get("ticker") or "").strip()
    as_of_date_raw = str(request_payload.get("as_of_date") or "").strip()
    screener_ids_raw = request_payload.get("screeners")
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if not as_of_date_raw:
        raise HTTPException(status_code=400, detail="as_of_date is required")
    try:
        as_of_date = dt.date.fromisoformat(as_of_date_raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="as_of_date must be YYYY-MM-DD") from exc
    if not isinstance(screener_ids_raw, list) or not screener_ids_raw:
        raise HTTPException(status_code=400, detail="screeners must be a non-empty array")
    screener_ids = [str(item).strip() for item in screener_ids_raw if str(item).strip()]
    try:
        result = service.run(ticker=ticker, as_of_date=as_of_date, screener_ids=screener_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(result)


@router.get("/screener-runs", response_class=JSONResponse)
def screener_runs_data(
    strategy_id: str = Query(default="", alias="strategyId"),
    from_date: str = Query(default="", alias="from"),
    to_date: str = Query(default="", alias="to"),
    include_deleted: bool = Query(default=False, alias="includeDeleted"),
    config_hash: str = Query(default="", alias="configHash"),
    has_hits_raw: str = Query(default="", alias="hasHits"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
    run_service: RunService = Depends(get_run_service),
) -> JSONResponse:
    start_date = dt.date.fromisoformat(from_date) if from_date else None
    end_date = dt.date.fromisoformat(to_date) if to_date else None
    has_hits = None
    if has_hits_raw.strip().lower() in {"true", "1", "yes"}:
        has_hits = True
    elif has_hits_raw.strip().lower() in {"false", "0", "no"}:
        has_hits = False
    payload = {
        "configured": service.is_configured(),
        "runs": service.list_runs(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            include_deleted=include_deleted,
            config_hash=config_hash,
            has_hits=has_hits,
            limit=limit,
            offset=offset,
        ),
        "coverage": service.list_signal_cache_summary(start_date=start_date, end_date=end_date),
        "available_strategies": [{"id": item["id"], "label": item["label"]} for item in run_service.list_actions()],
    }
    return JSONResponse(jsonable_encoder(payload))


@router.get("/screener-runs/{run_id}", response_class=JSONResponse)
def screener_run_detail(
    run_id: int,
    include_hits: bool = Query(default=True, alias="includeHits"),
    hit_limit: int = Query(default=200, alias="hitLimit", ge=1, le=1000),
    hit_offset: int = Query(default=0, alias="hitOffset", ge=0),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
) -> JSONResponse:
    payload = service.get_run(run_id, include_hits=include_hits, hit_limit=hit_limit, hit_offset=hit_offset)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Unknown screener run: {run_id}")
    return JSONResponse(jsonable_encoder(payload))


@router.post("/screener-runs/{run_id}/delete", response_class=JSONResponse)
def soft_delete_screener_run(
    run_id: int,
    payload: dict[str, object] | None = Body(default=None),
    service: ScreenerHistoryService = Depends(get_screener_history_service),
) -> JSONResponse:
    request_payload = payload or {}
    deleted = service.soft_delete(run_id, reason=str(request_payload.get("reason") or ""))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Unknown screener run: {run_id}")
    return JSONResponse({"ok": True})


@router.post("/screener-runs/batch", response_class=JSONResponse)
def launch_screener_history_batch(
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
) -> JSONResponse:
    request_payload = payload or {}
    strategy_ids = request_payload.get("strategy_ids")
    start_date = str(request_payload.get("start_date") or "").strip()
    end_date = str(request_payload.get("end_date") or "").strip()
    if not isinstance(strategy_ids, list) or not strategy_ids:
        raise HTTPException(status_code=400, detail="strategy_ids must be a non-empty array")
    if not start_date or not end_date:
        raise HTTPException(status_code=400, detail="start_date and end_date are required")
    scope = request_payload.get("scope")
    if scope is not None and not isinstance(scope, dict):
        raise HTTPException(status_code=400, detail="scope must be an object")
    options = {
        "strategy_ids": strategy_ids,
        "start_date": start_date,
        "end_date": end_date,
        "market_data_source": str(request_payload.get("market_data_mode") or "database-first"),
        "overwrite_policy": str(request_payload.get("overwrite_policy") or "skip_existing"),
        "scope": dict(scope or {}),
    }
    job_id = service.launch("screener_history_batch", options=options)
    return JSONResponse({"ok": True, "job_id": job_id})


@router.get("/watchlists", response_class=JSONResponse)
def watchlists_data(service: WatchlistService = Depends(get_watchlist_service)) -> JSONResponse:
    return JSONResponse({"watchlists": service.list_recent()})


@router.get("/watchlists/{stem}", response_class=JSONResponse)
def watchlist_detail_data(stem: str, service: WatchlistService = Depends(get_watchlist_service)) -> JSONResponse:
    return JSONResponse(service.get_watchlist_detail(stem))


@router.get("/watchlists/{stem}/chart/{ticker}", response_class=JSONResponse)
def watchlist_chart_data(
    stem: str,
    ticker: str,
    period: str = Query(default="18mo"),
    service: WatchlistService = Depends(get_watchlist_service),
) -> JSONResponse:
    _ = stem
    return JSONResponse(service.get_chart_payload(ticker=ticker.upper(), period=period))


@router.get("/overlap/latest", response_class=JSONResponse)
def overlap_latest(service: OverlapService = Depends(get_overlap_service)) -> JSONResponse:
    return JSONResponse(service.get_latest_summary())


@router.get("/rrg/latest", response_class=JSONResponse)
def rrg_latest(service: RrgService = Depends(get_rrg_service)) -> JSONResponse:
    return JSONResponse(service.get_latest_report())


@router.get("/rrg/{universe}", response_class=JSONResponse)
def rrg_universe_data(
    universe: str,
    benchmark: str = Query(default="SPY"),
    period: str = Query(default="3y"),
    trail_weeks: int = Query(default=12, alias="trailWeeks", ge=4, le=52),
    cadence: str = Query(default="weekly"),
    service: RrgService = Depends(get_rrg_service),
) -> JSONResponse:
    if universe not in {"sector", "industry", "theme"}:
        raise HTTPException(status_code=404, detail=f"Unsupported RRG universe: {universe}")
    if cadence not in {"weekly", "daily-2m"}:
        raise HTTPException(status_code=400, detail=f"Unsupported RRG cadence: {cadence}")
    return JSONResponse(
        service.get_universe_report(
            universe=universe,
            benchmark=benchmark,
            period=period,
            trail_weeks=trail_weeks,
            cadence=cadence,
        )
    )


@router.get("/overlap/{date_label}", response_class=JSONResponse)
def overlap_by_date(date_label: str, service: OverlapService = Depends(get_overlap_service)) -> JSONResponse:
    return JSONResponse(service.get_summary(date_label))


@router.get("/backtests", response_class=JSONResponse)
def backtests_data(
    service: BacktestService = Depends(get_backtest_service),
    history_service: ScreenerHistoryService = Depends(get_screener_history_service),
    run_service: RunService = Depends(get_run_service),
) -> JSONResponse:
    return JSONResponse(
        jsonable_encoder(
            {
                "backtest_templates": service.default_templates(),
                "backtest_runs": service.list_runs(limit=20),
                "signal_cache": history_service.list_signal_cache_summary(),
                "available_strategies": [{"id": item["id"], "label": item["label"]} for item in run_service.list_actions()],
                "default_exit_rules": DEFAULT_EXIT_RULES,
            }
        )
    )


@router.post("/backtests", response_class=JSONResponse)
def launch_backtest(
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
) -> JSONResponse:
    request_payload = payload or {}
    entry_rule = request_payload.get("entry_rule")
    date_range = request_payload.get("date_range")
    exit_rules = request_payload.get("exit_rules")
    position_rules = request_payload.get("position_rules")
    if not isinstance(entry_rule, dict):
        raise HTTPException(status_code=400, detail="entry_rule must be an object")
    if not isinstance(date_range, dict):
        raise HTTPException(status_code=400, detail="date_range must be an object")
    if exit_rules is not None and not isinstance(exit_rules, list):
        raise HTTPException(status_code=400, detail="exit_rules must be an array")
    if position_rules is not None and not isinstance(position_rules, dict):
        raise HTTPException(status_code=400, detail="position_rules must be an object")
    options = {
        "entry_rule": entry_rule,
        "date_range": date_range,
        "exit_rules": list(exit_rules or DEFAULT_EXIT_RULES),
        "position_rules": dict(position_rules or {}),
        "signal_cache_policy": str(request_payload.get("signal_cache_policy") or "reuse_then_fill"),
        "market_data_mode": str(request_payload.get("market_data_mode") or "database_only"),
    }
    job_id = service.launch("backtest_v1", options=options)
    return JSONResponse({"ok": True, "job_id": job_id})


@router.get("/admin/exclusions", response_class=JSONResponse)
def exclusions_data(
    coverage_start: str = Query(default="2020-01-01", alias="coverageStart"),
    service: AdminService = Depends(get_admin_service),
) -> JSONResponse:
    return JSONResponse(service.get_context(coverage_start=coverage_start))


@router.get("/admin/partial-tickers/{ticker}", response_class=JSONResponse)
def partial_ticker_detail(
    ticker: str,
    coverage_start: str = Query(default="2020-01-01", alias="coverageStart"),
    service: AdminService = Depends(get_admin_service),
) -> JSONResponse:
    try:
        return JSONResponse(service.get_partial_ticker_detail(ticker=ticker, coverage_start=coverage_start))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/admin/exclusions", response_class=JSONResponse)
def add_exclusion(
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.add_exclusion(
            ticker=str(request_payload.get("ticker") or ""),
            reason=str(request_payload.get("reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "entry": entry})


@router.post("/admin/exclusions/{ticker}/remove", response_class=JSONResponse)
def remove_exclusion(
    ticker: str,
    payload: dict[str, object] | None = Body(default=None),
    service: AdminService = Depends(get_admin_service),
) -> JSONResponse:
    request_payload = payload or {}
    try:
        entry = service.remove_exclusion(
            ticker=ticker,
            reason=str(request_payload.get("reason") or ""),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "entry": entry})


@router.post("/admin/history-sync", response_class=JSONResponse)
def launch_history_sync(
    payload: dict[str, object] | None = Body(default=None),
    service: RunService = Depends(get_run_service),
) -> JSONResponse:
    request_payload = payload or {}
    options: dict[str, object] = {}
    for key in ("start_date", "end_date", "tickers", "chunk_size"):
        if key in request_payload:
            options[key] = request_payload[key]
    try:
        job_id = service.launch("sync_postgres_market_data", options=options)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse({"ok": True, "job_id": job_id})
