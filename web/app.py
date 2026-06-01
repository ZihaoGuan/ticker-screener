from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles

from src.webapp.config import PROJECT_ROOT, load_webapp_config
from web.routes.admin import router as admin_router
from web.routes.api import router as api_router
from web.routes.backtests import router as backtests_router
from web.routes.dashboard import router as dashboard_router
from web.routes.runs import router as runs_router
from web.routes.watchlists import router as watchlists_router
from web.dependencies import get_auth_service


config = load_webapp_config()
app = FastAPI(title=config.app_title)

static_dir = PROJECT_ROOT / "web" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.middleware("http")
async def auth_principal_middleware(request: Request, call_next):
    auth_service = get_auth_service()
    request.state.current_principal = auth_service.principal_from_signed_session(
        request.cookies.get(config.auth_session_cookie_name)
    )
    return await call_next(request)

app.include_router(dashboard_router)
app.include_router(runs_router)
app.include_router(watchlists_router)
app.include_router(backtests_router)
app.include_router(admin_router)
app.include_router(api_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
