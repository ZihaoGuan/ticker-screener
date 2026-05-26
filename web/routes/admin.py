from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.config import load_app_config
from src.ticker_filters import load_excluded_tickers
from web.dependencies import templates


router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/exclusions", response_class=HTMLResponse)
def exclusions(request: Request) -> HTMLResponse:
    config = load_app_config()
    excluded = sorted(load_excluded_tickers(config))
    return templates.TemplateResponse(
        request,
        "admin_exclusions.html",
        {
            "page_title": "Exclusions",
            "excluded_tickers": excluded[:500],
            "excluded_count": len(excluded),
        },
    )
