from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import datetime as dt
import json
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    benchmark_ticker: str = "SPY"
    engine_version: str = "v1"
    screen_profile: str = "legacy"
    rs_lookback_days: int = 90
    rs_new_high_daily_lookback_days: int = 250
    rs_new_high_weekly_lookback_weeks: int = 52
    rs_new_high_history_days: int = 400
    rs_new_high_require_before_price: bool = True
    rs_weekly_recent_signal_weeks: int = 4
    htf_history_days: int = 365
    htf_runup_window_days: int = 40
    htf_min_runup_pct: float = 100.0
    htf_max_correction_pct: float = 25.0
    weekly_htf_ema8_breach_tolerance_pct: float = 0.02
    gap_fill_history_days: int = 365
    gap_fill_lookback_days: int = 180
    gap_fill_min_gap_pct: float = 0.03
    gap_fill_min_avg_volume: int = 1_000_000
    gap_fill_min_avg_dollar_volume: float = 20_000_000.0
    gap_fill_min_distance_to_gap_bottom_pct: float = -0.05
    gap_fill_max_distance_to_gap_bottom_pct: float = 0.12
    gap_fill_min_distance_to_gap_top_pct: float = 0.01
    gap_fill_max_distance_to_gap_top_pct: float = 0.20
    gap_fill_tight_range_lookback_days: int = 10
    year_high_proximity: float = 0.15
    breakout_volume_ratio: float = 1.4
    final_contraction_max: float = 0.1
    min_vcp_contractions: int = 2
    pivot_extension_ratio: float = 0.05
    volume_threshold: int = 100000
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
    excluded_tickers_file: str = "config/smallcap_exclude_tickers.txt"
    manual_excluded_tickers_file: str = "config/manual_exclude_tickers.txt"
    auto_excluded_tickers_dir: str = "config/auto_exclude_tickers"
    special_security_tickers_file: str = "artifacts/special_security_tickers_to_filter.csv"
    earnings_surprise_provider: str = "auto"
    earnings_enrichment_provider: str = "yfinance"
    peg_lookback_days: int = 20
    peg_earnings_tolerance_days: int = 3
    peg_min_gap_pct: float = 0.10
    peg_min_volume_ratio: float = 3.0
    peg_monster_gap_pct: float = 0.20
    peg_monster_volume_ratio: float = 4.0
    peg_min_eps_surprise_pct: float = 20.0
    peg_max_entry_distance_pct: float = 0.03
    peg_min_close_position_ratio: float = 0.25
    peg_require_earnings_event: bool = False
    peg_require_green_candle: bool = False
    peg_primary_entry_mode: str = "peg_low_or_ema_zone"
    peg_secondary_entry_fast_ema: int = 9
    peg_secondary_entry_slow_ema: int = 21
    peg_distribution_lookback_days: int = 10
    peg_distribution_volume_ratio: float = 1.5
    peg_sean_min_setup_age_days: int = 2
    peg_sean_min_adr_pct: float = 2.0
    peg_sean_min_avg_volume: int = 500_000
    peg_sean_recent_window_days: int = 10
    peg_sean_tight_range_max_pct: float = 0.15
    peg_sean_breakout_proximity_pct: float = 0.03
    peg_sean_dema_tolerance_pct: float = 0.02
    peg_sean_ema21_tolerance_pct: float = 0.02
    pre_earnings_retry_timeout_seconds: int = 24
    earnings_growth_move_lookback_quarters: int = 4
    earnings_growth_min_move_pct: float = 7.0
    earnings_growth_min_move_occurrences: int = 2
    earnings_growth_min_revenue_yoy_pct: float = 100.0
    earnings_growth_min_quarter_revenue: float = 50_000_000.0
    earnings_growth_eps_improving_quarters: int = 3
    earnings_growth_min_institutional_ownership_pct: float = 10.0
    earnings_growth_ma_short: int = 20
    earnings_growth_ma_medium: int = 50
    earnings_growth_ma_long: int = 200
    cup_handle_history_period: str = "18mo"
    cup_handle_pivot_span: int = 8
    cup_handle_min_cup_bars: int = 30
    cup_handle_max_cup_bars: int = 140
    cup_handle_min_depth_pct: float = 0.12
    cup_handle_max_depth_pct: float = 0.45
    cup_handle_rim_tolerance_pct: float = 0.06
    cup_handle_min_handle_retrace: float = 0.10
    cup_handle_max_handle_retrace: float = 0.55
    cup_handle_max_handle_bars_ratio: float = 0.40
    cup_handle_min_containment_ratio: float = 0.65
    cup_handle_curve_tolerance_ratio: float = 0.20
    cup_handle_breakout_lookback_bars: int = 5
    cup_handle_require_volume_confirmation: bool = True
    cup_handle_volume_average_days: int = 50
    cup_handle_enable_bullish: bool = True
    cup_handle_enable_bearish: bool = False

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
