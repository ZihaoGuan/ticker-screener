from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from src.webapp.services.backtest_service import BacktestService
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from web.dependencies import templates
from web.dependencies import get_backtest_service, get_run_service, get_screener_history_service


router = APIRouter(prefix="/backtests", tags=["backtests"])


@router.get("", response_class=HTMLResponse)
def backtests(
    request: Request,
    backtest_service: BacktestService = Depends(get_backtest_service),
    history_service: ScreenerHistoryService = Depends(get_screener_history_service),
    run_service: RunService = Depends(get_run_service),
) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "backtests.html",
        {
            "page_title": "Backtests",
            "backtest_templates": backtest_service.default_templates(),
            "backtest_runs": backtest_service.list_runs(limit=10),
            "signal_cache": history_service.list_signal_cache_summary(),
            "available_strategies": run_service.list_actions(),
        },
    )
