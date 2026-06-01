from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.webapp.services.watchlist_service import WatchlistService
from web.dependencies import get_watchlist_service, templates


router = APIRouter(prefix="/watchlists", tags=["watchlists"])


@router.get("", response_class=HTMLResponse)
def watchlists(
    request: Request,
    service: WatchlistService = Depends(get_watchlist_service),
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "watchlists.html",
        {
            "page_title": "Watchlists",
            "watchlists": service.list_recent(),
        },
    )


@router.get("/{stem}", response_class=HTMLResponse)
def watchlist_detail(
    stem: str,
    request: Request,
    service: WatchlistService = Depends(get_watchlist_service),
) -> HTMLResponse:
    detail = service.get_watchlist_detail(stem)
    return templates.TemplateResponse(
        request,
        "watchlist_detail.html",
        {
            "page_title": f"Watchlist {stem}",
            **detail,
        },
    )


@router.get("/api/chart/{ticker}", response_class=JSONResponse)
def watchlist_chart_data(
    ticker: str,
    period: str = Query(default="18mo"),
    as_of_date: dt.date | None = Query(default=None, alias="asOfDate"),
    service: WatchlistService = Depends(get_watchlist_service),
) -> JSONResponse:
    return JSONResponse(service.get_chart_payload(ticker=ticker.upper(), period=period, as_of_date=as_of_date))
