from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.webapp.services.run_service import RunService
from web.dependencies import get_run_service, require_run_screeners, templates


router = APIRouter(tags=["runs"])


@router.get("/screeners", response_class=HTMLResponse)
def screeners(
    request: Request,
    service: RunService = Depends(get_run_service),
    _=Depends(require_run_screeners),
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "page_title": "Screeners",
            "run_actions": service.list_actions(),
            "jobs": service.list_jobs(),
            "launched": request.query_params.get("launched", ""),
        },
    )


@router.get("/runs", include_in_schema=False)
def legacy_runs_redirect() -> RedirectResponse:
    return RedirectResponse(url="/screeners", status_code=307)


@router.post("/screeners/{action_id}")
def launch_run(
    action_id: str,
    limit: int | None = Form(default=None),
    service: RunService = Depends(get_run_service),
    _=Depends(require_run_screeners),
) -> RedirectResponse:
    service.launch(action_id, options={"limit": limit} if limit is not None else {})
    return RedirectResponse(url=f"/screeners?launched={action_id}", status_code=303)


@router.post("/runs/{action_id}", include_in_schema=False)
def legacy_launch_run(
    action_id: str,
    limit: int | None = Form(default=None),
    service: RunService = Depends(get_run_service),
    _=Depends(require_run_screeners),
) -> RedirectResponse:
    service.launch(action_id, options={"limit": limit} if limit is not None else {})
    return RedirectResponse(url=f"/screeners?launched={action_id}", status_code=303)
