from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from web.dependencies import templates


router = APIRouter(prefix="/backtests", tags=["backtests"])


@router.get("", response_class=HTMLResponse)
def backtests(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "backtests.html",
        {
            "page_title": "Backtests",
            "backtest_templates": [
                {
                    "label": "Overlap Count Backtest",
                    "description": "Historical overlap summary forward-return study.",
                    "command": "python scripts/build_overlap_backtest_report.py --start-date 2024-01-01 --end-date 2026-05-01",
                }
            ],
        },
    )
