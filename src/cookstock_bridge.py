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


def load_configured_cookstock(config: AppConfig) -> ModuleType:
    module = load_cookstock_module()
    apply_config_to_cookstock(module, config)
    return module
