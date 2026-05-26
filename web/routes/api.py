from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from src.config import load_app_config
from src.ticker_filters import load_excluded_tickers
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.run_service import RunService
from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import (
    get_dashboard_service,
    get_overlap_service,
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


@router.post("/runs/{action_id}", response_class=JSONResponse)
def run_action(
    action_id: str,
    limit: int | None = Query(default=None, ge=1, le=10000),
    service: RunService = Depends(get_run_service),
) -> JSONResponse:
    try:
        job_id = service.launch(action_id, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
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
def exclusions_data() -> JSONResponse:
    config = load_app_config()
    excluded = sorted(load_excluded_tickers(config))
    return JSONResponse({"excluded_tickers": excluded[:500], "excluded_count": len(excluded)})
