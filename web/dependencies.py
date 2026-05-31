from __future__ import annotations

from fastapi.templating import Jinja2Templates

from src.webapp.config import PROJECT_ROOT, load_webapp_config
from src.webapp.services.ad_hoc_screen_service import AdHocScreenService
from src.webapp.services.admin_service import AdminService
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.overlap_service import OverlapService
from src.webapp.services.rrg_service import RrgService
from src.webapp.services.run_service import RunService
from src.webapp.services.watchlist_service import WatchlistService


templates = Jinja2Templates(directory=str(PROJECT_ROOT / "web" / "templates"))
config = load_webapp_config()


def get_dashboard_service() -> DashboardService:
    return DashboardService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_admin_service() -> AdminService:
    return AdminService(database_url=config.database_url)


def get_watchlist_service() -> WatchlistService:
    return WatchlistService(artifacts_dir=config.artifacts_dir)


def get_run_service() -> RunService:
    return RunService(project_root=PROJECT_ROOT)


def get_overlap_service() -> OverlapService:
    return OverlapService(artifacts_dir=config.artifacts_dir)


def get_rrg_service() -> RrgService:
    return RrgService(output_dir=config.output_dir, reports_fqdn=config.reports_fqdn)


def get_ad_hoc_screen_service() -> AdHocScreenService:
    return AdHocScreenService(database_url=config.database_url)
