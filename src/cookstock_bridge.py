from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import ModuleType

from .config import AppConfig, project_root


def vendor_cookstock_root() -> Path:
    return project_root() / "vendor" / "cookstock"


def _ensure_vendor_paths() -> Path:
    root = vendor_cookstock_root()
    src_dir = root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    return src_dir


def load_cookstock_module() -> ModuleType:
    _ensure_vendor_paths()
    return importlib.import_module("cookStock")


def apply_config_to_cookstock(module: ModuleType, config: AppConfig) -> None:
    algo = module.algoParas
    algo.BENCHMARK_TICKER = config.benchmark_ticker.upper()
    algo.RS_NEW_HIGH_DAILY_LOOKBACK_DAYS = int(config.rs_new_high_daily_lookback_days)
    algo.RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS = int(config.rs_new_high_weekly_lookback_weeks)
    algo.RS_NEW_HIGH_HISTORY_DAYS = int(config.rs_new_high_history_days)
    algo.RS_NEW_HIGH_REQUIRE_BEFORE_PRICE = bool(config.rs_new_high_require_before_price)
    algo.YEAR_HIGH_PROXIMITY = float(config.year_high_proximity)
    algo.REQUEST_TIMEOUT_SECONDS = int(config.request_timeout_seconds)
    algo.TICKER_TIMEOUT_SECONDS = int(config.ticker_timeout_seconds)
    algo.EARNINGS_WATCHLIST_ICS_URL = str(config.earnings_watchlist_ics_url).strip()
    algo.EARNINGS_WATCHLIST_EXCLUDE_ICS_URLS = [
        str(url).strip() for url in config.earnings_watchlist_exclude_ics_urls if str(url).strip()
    ]
    algo.EARNINGS_SURPRISE_PROVIDER = str(config.earnings_surprise_provider).strip().lower()
    algo.PEG_LOOKBACK_DAYS = int(config.peg_lookback_days)
    algo.PEG_EARNINGS_TOLERANCE_DAYS = int(config.peg_earnings_tolerance_days)
    algo.PEG_MIN_GAP_PCT = float(config.peg_min_gap_pct)
    algo.PEG_MIN_VOLUME_RATIO = float(config.peg_min_volume_ratio)
    algo.PEG_MONSTER_GAP_PCT = float(config.peg_monster_gap_pct)
    algo.PEG_MONSTER_VOLUME_RATIO = float(config.peg_monster_volume_ratio)
    algo.PEG_MIN_EPS_SURPRISE_PCT = float(config.peg_min_eps_surprise_pct)
    algo.PEG_MAX_ENTRY_DISTANCE_PCT = float(config.peg_max_entry_distance_pct)
    algo.PEG_MIN_CLOSE_POSITION_RATIO = float(config.peg_min_close_position_ratio)
    algo.PEG_REQUIRE_EARNINGS_EVENT = bool(config.peg_require_earnings_event)
    algo.PEG_REQUIRE_GREEN_CANDLE = bool(config.peg_require_green_candle)
    algo.PEG_PRIMARY_ENTRY_MODE = str(config.peg_primary_entry_mode).strip().lower()
    algo.PEG_SECONDARY_ENTRY_FAST_EMA = int(config.peg_secondary_entry_fast_ema)
    algo.PEG_SECONDARY_ENTRY_SLOW_EMA = int(config.peg_secondary_entry_slow_ema)
    algo.PEG_DISTRIBUTION_LOOKBACK_DAYS = int(config.peg_distribution_lookback_days)
    algo.PEG_DISTRIBUTION_VOLUME_RATIO = float(config.peg_distribution_volume_ratio)
    algo.PRE_EARNINGS_RETRY_TIMEOUT_SECONDS = int(config.pre_earnings_retry_timeout_seconds)


def load_configured_cookstock(config: AppConfig) -> ModuleType:
    module = load_cookstock_module()
    apply_config_to_cookstock(module, config)
    return module
