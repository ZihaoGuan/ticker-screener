from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.webapp.services.run_service import RunService
from web.dependencies import get_run_service, require_run_screeners, templates


router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_class=HTMLResponse)
def runs(
    request: Request,
    service: RunService = Depends(get_run_service),
    _=Depends(require_run_screeners),
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "page_title": "Runs",
            "run_actions": service.list_actions(),
            "jobs": service.list_jobs(),
            "launched": request.query_params.get("launched", ""),
        },
    )


@router.post("/{action_id}")
def launch_run(
    action_id: str,
    limit: int | None = Form(default=None),
    service: RunService = Depends(get_run_service),
    _=Depends(require_run_screeners),
) -> RedirectResponse:
    service.launch(action_id, options={"limit": limit} if limit is not None else {})
    return RedirectResponse(url=f"/runs?launched={action_id}", status_code=303)
