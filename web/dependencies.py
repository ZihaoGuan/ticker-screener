from __future__ import annotations

from fastapi.templating import Jinja2Templates

from src.webapp.config import PROJECT_ROOT, load_webapp_config
from src.webapp.services.dashboard_service import DashboardService
from src.webapp.services.watchlist_service import WatchlistService


templates = Jinja2Templates(directory=str(PROJECT_ROOT / "web" / "templates"))
config = load_webapp_config()


def get_dashboard_service() -> DashboardService:
    return DashboardService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)


def get_watchlist_service() -> WatchlistService:
    return WatchlistService(artifacts_dir=config.artifacts_dir)
