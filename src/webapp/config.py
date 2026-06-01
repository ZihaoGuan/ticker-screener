from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


@dataclass(frozen=True)
class WebAppConfig:
    app_env: str = _env("WEBAPP_ENV", "TICKER_SCREENER_APP_ENV", default="dev")
    app_title: str = _env("WEBAPP_TITLE", "TICKER_SCREENER_APP_TITLE", default="Ticker Screener")
    app_base_url: str = _env("WEBAPP_BASE_URL", "TICKER_SCREENER_APP_BASE_URL", default="")
    database_url: str = _env("TICKER_SCREENER_DATABASE_URL", default="")
    market_data_source: str = _env("TICKER_SCREENER_MARKET_DATA_SOURCE", default="internet")
    artifacts_dir: Path = Path(_env("TICKER_SCREENER_ARTIFACTS_DIR", default=str(PROJECT_ROOT / "artifacts")))
    output_dir: Path = Path(_env("TICKER_SCREENER_OUTPUT_DIR", default=str(PROJECT_ROOT / "artifacts" / "output")))
    reports_fqdn: str = os.getenv("REPORTS_FQDN", "")
    auth_secret_key: str = _env("WEBAPP_AUTH_SECRET_KEY", "TICKER_SCREENER_AUTH_SECRET_KEY", default="")
    auth_session_cookie_name: str = _env(
        "WEBAPP_AUTH_SESSION_COOKIE_NAME",
        "TICKER_SCREENER_AUTH_SESSION_COOKIE_NAME",
        default="ticker_screener_session",
    )
    auth_session_ttl_hours: int = int(_env("WEBAPP_AUTH_SESSION_TTL_HOURS", "TICKER_SCREENER_AUTH_SESSION_TTL_HOURS", default="168"))
    auth_magic_link_ttl_minutes: int = int(
        _env("WEBAPP_AUTH_MAGIC_LINK_TTL_MINUTES", "TICKER_SCREENER_AUTH_MAGIC_LINK_TTL_MINUTES", default="20")
    )
    auth_cookie_secure: bool = _env("WEBAPP_AUTH_COOKIE_SECURE", "TICKER_SCREENER_AUTH_COOKIE_SECURE", default="true").strip().lower() in {"1", "true", "yes", "on"}
    auth_cookie_samesite: str = _env("WEBAPP_AUTH_COOKIE_SAMESITE", "TICKER_SCREENER_AUTH_COOKIE_SAMESITE", default="lax")
    auth_bootstrap_admin_emails_raw: str = _env(
        "WEBAPP_AUTH_BOOTSTRAP_ADMIN_EMAILS",
        "TICKER_SCREENER_AUTH_BOOTSTRAP_ADMIN_EMAILS",
        default="",
    )
    smtp_host: str = _env("WEBAPP_SMTP_HOST", "TICKER_SCREENER_SMTP_HOST", default="")
    smtp_port: int = int(_env("WEBAPP_SMTP_PORT", "TICKER_SCREENER_SMTP_PORT", default="587"))
    smtp_username: str = _env("WEBAPP_SMTP_USERNAME", "TICKER_SCREENER_SMTP_USERNAME", default="")
    smtp_password: str = _env("WEBAPP_SMTP_PASSWORD", "TICKER_SCREENER_SMTP_PASSWORD", default="")
    smtp_from_address: str = _env("WEBAPP_SMTP_FROM_ADDRESS", "TICKER_SCREENER_SMTP_FROM_ADDRESS", default="")
    smtp_use_tls: bool = _env("WEBAPP_SMTP_USE_TLS", "TICKER_SCREENER_SMTP_USE_TLS", default="true").strip().lower() in {"1", "true", "yes", "on"}
    smtp_use_ssl: bool = _env("WEBAPP_SMTP_USE_SSL", "TICKER_SCREENER_SMTP_USE_SSL", default="false").strip().lower() in {"1", "true", "yes", "on"}

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
