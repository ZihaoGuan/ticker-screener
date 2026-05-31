from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from src.webapp.services.admin_service import AdminService
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.run_service import RunService
from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import (
    get_ad_hoc_screen_service,
    get_admin_service,
    get_dashboard_service,
    get_overlap_service,
    get_rrg_service,
    get_run_service,
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
def backtests_data() -> JSONResponse:
    return JSONResponse(
        {
            "backtest_templates": [
                {
                    "label": "Overlap Count Backtest",
                    "description": "Historical overlap summary forward-return study.",
                    "command": "python scripts/build_overlap_backtest_report.py --start-date 2024-01-01 --end-date 2026-05-01",
                }
            ]
        }
    )


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
