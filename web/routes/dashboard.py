from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from src.webapp.services.dashboard_service import DashboardService
from web.dependencies import get_dashboard_service, templates


router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    service: DashboardService = Depends(get_dashboard_service),
) -> HTMLResponse:
    context = service.get_dashboard_context(include_deprecated_watchlists=False)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "page_title": "Dashboard",
            **context,
        },
    )
