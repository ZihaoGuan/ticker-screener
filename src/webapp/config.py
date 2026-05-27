from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class WebAppConfig:
    app_env: str = os.getenv("TICKER_SCREENER_APP_ENV", "dev")
    app_title: str = os.getenv("TICKER_SCREENER_APP_TITLE", "Ticker Screener")
    database_url: str = os.getenv("TICKER_SCREENER_DATABASE_URL", "")
    artifacts_dir: Path = Path(os.getenv("TICKER_SCREENER_ARTIFACTS_DIR", str(PROJECT_ROOT / "artifacts")))
    output_dir: Path = Path(os.getenv("TICKER_SCREENER_OUTPUT_DIR", str(PROJECT_ROOT / "artifacts" / "output")))
    reports_fqdn: str = os.getenv("REPORTS_FQDN", "")


def load_webapp_config() -> WebAppConfig:
    return WebAppConfig()
