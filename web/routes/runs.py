from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from web.dependencies import templates


router = APIRouter(prefix="/runs", tags=["runs"])


@router.get("", response_class=HTMLResponse)
def runs(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "page_title": "Runs",
            "run_actions": [
                {"label": "Run RS", "command": "python scripts/run_rs_screen.py"},
                {"label": "Run VCP", "command": "python scripts/run_vcp_screen.py"},
                {"label": "Run Cup Handle", "command": "python scripts/run_cup_handle_screen.py"},
                {"label": "Run Overlap Backtest", "command": "python scripts/build_overlap_backtest_report.py"},
            ],
        },
    )
