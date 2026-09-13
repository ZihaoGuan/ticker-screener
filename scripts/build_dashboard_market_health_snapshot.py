#!/usr/bin/env python3
"""Build the Dashboard market-health read-model outside the web request path."""
from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.webapp.config import load_webapp_config
from src.webapp.services.dashboard_service import DashboardService


def main() -> int:
    config = load_webapp_config()
    service = DashboardService(database_url=config.database_url, artifacts_dir=config.artifacts_dir)
    payload = service.persist_dashboard_market_health_snapshot()
    if payload is None:
        print("Dashboard market-health snapshot skipped: database is not configured.")
        return 1
    print(f"Dashboard market-health snapshot persisted: {payload.get('source_data_as_of') or 'unknown'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
