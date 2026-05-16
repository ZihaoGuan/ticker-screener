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
    ticker_timeout_seconds: int = 12
    exchanges: tuple[str, ...] = ("nyse", "nasdaq", "amex")
    max_tickers: int | None = None
    default_chart_period: str = "18mo"
    default_chart_lookback: int = 120
    default_split_pages: int = 4
    default_montage_columns: int = 2
    default_card_width: int = 700
    earnings_watchlist_ics_url: str = "https://earnings.beavern.com/ics/all.ics"
    earnings_watchlist_exclude_ics_urls: tuple[str, ...] = ()
    earnings_surprise_provider: str = "auto"
    peg_lookback_days: int = 20
    peg_earnings_tolerance_days: int = 3
    peg_min_gap_pct: float = 0.10
    peg_min_volume_ratio: float = 3.0
    peg_monster_gap_pct: float = 0.20
    peg_monster_volume_ratio: float = 4.0
    peg_min_eps_surprise_pct: float = 20.0
    peg_max_entry_distance_pct: float = 0.03
    peg_min_close_position_ratio: float = 0.6
    peg_require_earnings_event: bool = False
    peg_require_green_candle: bool = True
    peg_primary_entry_mode: str = "peg_low"
    peg_secondary_entry_fast_ema: int = 9
    peg_secondary_entry_slow_ema: int = 21
    peg_distribution_lookback_days: int = 10
    peg_distribution_volume_ratio: float = 1.5
    pre_earnings_retry_timeout_seconds: int = 24

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
    if "earnings_watchlist_exclude_ics_urls" in normalized and isinstance(
        normalized["earnings_watchlist_exclude_ics_urls"], list
    ):
        normalized["earnings_watchlist_exclude_ics_urls"] = tuple(
            str(value).strip()
            for value in normalized["earnings_watchlist_exclude_ics_urls"]
            if str(value).strip()
        )
    return AppConfig(**normalized)


def override_config(config: AppConfig, **updates: object) -> AppConfig:
    clean_updates = {key: value for key, value in updates.items() if value is not None}
    return replace(config, **clean_updates)


def today_label(today: dt.date | None = None) -> str:
    return (today or dt.date.today()).isoformat()
