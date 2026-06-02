from __future__ import annotations

from fastapi import Depends, HTTPException, Request, Response, status
from fastapi.templating import Jinja2Templates

from src.webapp.access_control import (
    CAP_MANAGE_EXCLUSIONS,
    CAP_MANAGE_USERS,
    CAP_RUN_BACKTESTS,
    CAP_RUN_SCREENERS,
    CAP_SYNC_HISTORY,
    Principal,
    anonymous_principal,
)
from src.webapp.config import PROJECT_ROOT, load_webapp_config
from src.webapp.repositories.auth_repository import AuthRepository
from src.webapp.repositories.audit_repository import AuditRepository
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.admin_service import AdminService
from src.webapp.services.auth_service import AuthService, UserAdminService
from src.webapp.services.audit_service import AuditService
from src.webapp.services.backtest_service import BacktestService
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.earnings_calendar_service import EarningsCalendarService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.run_service import RunService
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.services.watchlist_service import WatchlistService


templates = Jinja2Templates(directory=str(PROJECT_ROOT / "web" / "templates"))
config = load_webapp_config()
auth_repository = AuthRepository(database_url=config.database_url)
audit_repository = AuditRepository(database_url=config.database_url)
auth_service = AuthService(config=config, repository=auth_repository)
user_admin_service = UserAdminService(repository=auth_repository, config=config)
audit_service = AuditService(repository=audit_repository)


def get_dashboard_service() -> DashboardService:
    return DashboardService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_earnings_calendar_service() -> EarningsCalendarService:
    return EarningsCalendarService(project_root=PROJECT_ROOT, database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_admin_service() -> AdminService:
    return AdminService(database_url=config.database_url)


def get_watchlist_service() -> WatchlistService:
    return WatchlistService(
        artifacts_dir=config.artifacts_dir,
        database_url=config.database_url,
        market_data_source=config.market_data_source,
    )


def get_run_service() -> RunService:
    return RunService(project_root=PROJECT_ROOT, database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_screener_history_service() -> ScreenerHistoryService:
    return ScreenerHistoryService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_backtest_service() -> BacktestService:
    return BacktestService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_overlap_service() -> OverlapService:
    return OverlapService(artifacts_dir=config.artifacts_dir)


def get_rrg_service() -> RrgService:
    return RrgService(output_dir=config.output_dir, reports_fqdn=config.reports_fqdn)


def get_ad_hoc_screen_service() -> AdHocScreenService:
    return AdHocScreenService(database_url=config.database_url)


def get_auth_service() -> AuthService:
    return auth_service


def get_user_admin_service() -> UserAdminService:
    return user_admin_service


def get_audit_service() -> AuditService:
    return audit_service


def get_current_principal(request: Request, service: AuthService = Depends(get_auth_service)) -> Principal:
    cached = getattr(request.state, "current_principal", None)
    if isinstance(cached, Principal):
        return cached
    session_cookie = request.cookies.get(config.auth_session_cookie_name)
    principal = service.principal_from_signed_session(session_cookie)
    request.state.current_principal = principal
    return principal


def set_auth_cookie(response: Response, signed_value: str) -> None:
    response.set_cookie(
        key=config.auth_session_cookie_name,
        value=signed_value,
        httponly=True,
        secure=config.auth_cookie_secure,
        samesite=config.auth_cookie_samesite,
        max_age=max(3600, int(config.auth_session_ttl_hours) * 3600),
        path="/",
    )


def clear_auth_cookie(response: Response) -> None:
    response.delete_cookie(
        key=config.auth_session_cookie_name,
        path="/",
        secure=config.auth_cookie_secure,
        samesite=config.auth_cookie_samesite,
    )


def require_authenticated(principal: Principal = Depends(get_current_principal)) -> Principal:
    if not principal.authenticated:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return principal


def require_capability(capability: str):
    def dependency(principal: Principal = Depends(get_current_principal)) -> Principal:
        if not principal.authenticated:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
        if not principal.can(capability):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="You do not have permission to perform this action.")
        return principal

    return dependency


def require_run_screeners(principal: Principal = Depends(require_capability(CAP_RUN_SCREENERS))) -> Principal:
    return principal


def require_run_backtests(principal: Principal = Depends(require_capability(CAP_RUN_BACKTESTS))) -> Principal:
    return principal


def require_manage_exclusions(principal: Principal = Depends(require_capability(CAP_MANAGE_EXCLUSIONS))) -> Principal:
    return principal


def require_sync_history(principal: Principal = Depends(require_capability(CAP_SYNC_HISTORY))) -> Principal:
    return principal


def require_manage_users(principal: Principal = Depends(require_capability(CAP_MANAGE_USERS))) -> Principal:
    return principal
