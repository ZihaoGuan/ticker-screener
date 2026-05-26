from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.webapp.config import PROJECT_ROOT, load_webapp_config
from web.routes.admin import router as admin_router
from web.routes.backtests import router as backtests_router
from web.routes.dashboard import router as dashboard_router
from web.routes.runs import router as runs_router
from web.routes.watchlists import router as watchlists_router


config = load_webapp_config()
app = FastAPI(title=config.app_title)

static_dir = PROJECT_ROOT / "web" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

app.include_router(dashboard_router)
app.include_router(runs_router)
app.include_router(watchlists_router)
app.include_router(backtests_router)
app.include_router(admin_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
