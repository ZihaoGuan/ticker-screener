from __future__ import annotations

from fastapi import FastAPI
from fastapi.requests import Request

from src.webapp.config import load_webapp_config
from web.routes.api import router as api_router
from web.dependencies import get_auth_service


config = load_webapp_config()
app = FastAPI(title=config.app_title)

@app.middleware("http")
async def auth_principal_middleware(request: Request, call_next):
    auth_service = get_auth_service()
    request.state.current_principal = auth_service.principal_from_signed_session(
        request.cookies.get(config.auth_session_cookie_name)
    )
    return await call_next(request)

app.include_router(api_router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
