from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class WebAppConfig:
    app_env: str = os.getenv("TICKER_SCREENER_APP_ENV", "dev")
    app_title: str = os.getenv("TICKER_SCREENER_APP_TITLE", "Ticker Screener")
    app_base_url: str = os.getenv("TICKER_SCREENER_APP_BASE_URL", "")
    database_url: str = os.getenv("TICKER_SCREENER_DATABASE_URL", "")
    market_data_source: str = os.getenv("TICKER_SCREENER_MARKET_DATA_SOURCE", "internet")
    artifacts_dir: Path = Path(os.getenv("TICKER_SCREENER_ARTIFACTS_DIR", str(PROJECT_ROOT / "artifacts")))
    output_dir: Path = Path(os.getenv("TICKER_SCREENER_OUTPUT_DIR", str(PROJECT_ROOT / "artifacts" / "output")))
    reports_fqdn: str = os.getenv("REPORTS_FQDN", "")
    auth_secret_key: str = os.getenv("TICKER_SCREENER_AUTH_SECRET_KEY", "")
    auth_session_cookie_name: str = os.getenv("TICKER_SCREENER_AUTH_SESSION_COOKIE_NAME", "ticker_screener_session")
    auth_session_ttl_hours: int = int(os.getenv("TICKER_SCREENER_AUTH_SESSION_TTL_HOURS", "168"))
    auth_magic_link_ttl_minutes: int = int(os.getenv("TICKER_SCREENER_AUTH_MAGIC_LINK_TTL_MINUTES", "20"))
    auth_cookie_secure: bool = os.getenv("TICKER_SCREENER_AUTH_COOKIE_SECURE", "true").strip().lower() in {"1", "true", "yes", "on"}
    auth_cookie_samesite: str = os.getenv("TICKER_SCREENER_AUTH_COOKIE_SAMESITE", "lax")
    auth_bootstrap_admin_emails_raw: str = os.getenv("TICKER_SCREENER_AUTH_BOOTSTRAP_ADMIN_EMAILS", "")
    smtp_host: str = os.getenv("TICKER_SCREENER_SMTP_HOST", "")
    smtp_port: int = int(os.getenv("TICKER_SCREENER_SMTP_PORT", "587"))
    smtp_username: str = os.getenv("TICKER_SCREENER_SMTP_USERNAME", "")
    smtp_password: str = os.getenv("TICKER_SCREENER_SMTP_PASSWORD", "")
    smtp_from_address: str = os.getenv("TICKER_SCREENER_SMTP_FROM_ADDRESS", "")
    smtp_use_tls: bool = os.getenv("TICKER_SCREENER_SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "on"}
    smtp_use_ssl: bool = os.getenv("TICKER_SCREENER_SMTP_USE_SSL", "false").strip().lower() in {"1", "true", "yes", "on"}

    @property
    def auth_bootstrap_admin_emails(self) -> tuple[str, ...]:
        values = []
        for item in self.auth_bootstrap_admin_emails_raw.split(","):
            normalized = item.strip().lower()
            if normalized and normalized not in values:
                values.append(normalized)
        return tuple(values)


def load_webapp_config() -> WebAppConfig:
    return WebAppConfig()
