from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

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
