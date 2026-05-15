from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import datetime as dt
import json
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    benchmark_ticker: str = "SPY"
    rs_new_high_daily_lookback_days: int = 250
    rs_new_high_weekly_lookback_weeks: int = 52
    rs_new_high_history_days: int = 400
    rs_new_high_require_before_price: bool = True
    year_high_proximity: float = 0.15
    request_timeout_seconds: int = 20
    exchanges: tuple[str, ...] = ("nyse", "nasdaq", "amex")
    max_tickers: int | None = None
    default_chart_period: str = "18mo"
    default_chart_lookback: int = 120
    default_split_pages: int = 4
    default_montage_columns: int = 2
    default_card_width: int = 700

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_config_path() -> Path:
    return project_root() / "config" / "market_config.json"


def load_app_config(path: str | Path | None = None) -> AppConfig:
    config_path = Path(path) if path else default_config_path()
    data = json.loads(config_path.read_text(encoding="utf-8"))
    normalized = dict(data)
    if "exchanges" in normalized and isinstance(normalized["exchanges"], list):
        normalized["exchanges"] = tuple(str(value).lower() for value in normalized["exchanges"])
    return AppConfig(**normalized)


def override_config(config: AppConfig, **updates: object) -> AppConfig:
    clean_updates = {key: value for key, value in updates.items() if value is not None}
    return replace(config, **clean_updates)


def today_label(today: dt.date | None = None) -> str:
    return (today or dt.date.today()).isoformat()
