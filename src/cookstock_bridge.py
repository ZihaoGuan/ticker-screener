from __future__ import annotations

from contextlib import contextmanager
import datetime as real_dt
import importlib
import sys
from pathlib import Path
from types import ModuleType

from .config import AppConfig, project_root
from .market_data_access import (
    build_cookstock_payload_from_frame,
    build_cookstock_price_list_from_frame,
    db_frame_has_recent_coverage,
    load_daily_bars_frame_from_db,
    resolve_market_data_source,
)


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
    algo.ENGINE_VERSION = str(config.engine_version).strip().lower()
    algo.SCREEN_PROFILE = str(config.screen_profile).strip().lower()
    algo.RS_LOOKBACK_DAYS = int(config.rs_lookback_days)
    algo.RS_NEW_HIGH_DAILY_LOOKBACK_DAYS = int(config.rs_new_high_daily_lookback_days)
    algo.RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS = int(config.rs_new_high_weekly_lookback_weeks)
    algo.RS_NEW_HIGH_HISTORY_DAYS = int(config.rs_new_high_history_days)
    algo.RS_NEW_HIGH_REQUIRE_BEFORE_PRICE = bool(config.rs_new_high_require_before_price)
    algo.RS_WEEKLY_RECENT_SIGNAL_WEEKS = int(config.rs_weekly_recent_signal_weeks)
    algo.HTF_HISTORY_DAYS = int(config.htf_history_days)
    algo.HTF_RUNUP_WINDOW_DAYS = int(config.htf_runup_window_days)
    algo.HTF_MIN_RUNUP_PCT = float(config.htf_min_runup_pct)
    algo.HTF_MAX_CORRECTION_PCT = float(config.htf_max_correction_pct)
    algo.YEAR_HIGH_PROXIMITY = float(config.year_high_proximity)
    algo.BREAKOUT_VOLUME_RATIO = float(config.breakout_volume_ratio)
    algo.FINAL_CONTRACTION_MAX = float(config.final_contraction_max)
    algo.MIN_VCP_CONTRACTIONS = int(config.min_vcp_contractions)
    algo.PIVOT_EXTENSION_RATIO = float(config.pivot_extension_ratio)
    algo.VOLUME_THRESHOLD = int(config.volume_threshold)
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


def _apply_market_data_source_patches(module: ModuleType, market_data_source: str | None) -> None:
    strategy = resolve_market_data_source(market_data_source)
    setattr(module.algoParas, "MARKET_DATA_SOURCE", strategy)

    cook_financials_cls = module.cookFinancials
    if not hasattr(cook_financials_cls, "_ticker_screener_original_get_historical_price_data"):
        cook_financials_cls._ticker_screener_original_get_historical_price_data = cook_financials_cls.get_historical_price_data
    if not hasattr(cook_financials_cls, "_ticker_screener_original_get_benchmark_price_data"):
        cook_financials_cls._ticker_screener_original_get_benchmark_price_data = cook_financials_cls._get_benchmark_price_data

    original_get_historical_price_data = cook_financials_cls._ticker_screener_original_get_historical_price_data
    original_get_benchmark_price_data = cook_financials_cls._ticker_screener_original_get_benchmark_price_data

    def patched_get_historical_price_data(self, start_date, end_date, time_interval):
        active_strategy = resolve_market_data_source(getattr(module.algoParas, "MARKET_DATA_SOURCE", strategy))
        if active_strategy == "database-first" and time_interval == "daily":
            start_dt = real_dt.date.fromisoformat(str(start_date))
            end_dt = real_dt.date.fromisoformat(str(end_date))
            frame = load_daily_bars_frame_from_db(str(self.ticker), start_dt, end_dt)
            if frame is not None and db_frame_has_recent_coverage(frame, end_dt):
                payload = build_cookstock_payload_from_frame(str(self.ticker), frame)
                if payload is not None:
                    print(f"market-data source=db ticker={self.ticker} bars={len(frame)}")
                    return payload
        return original_get_historical_price_data(self, start_date, end_date, time_interval)

    def patched_get_benchmark_price_data(self, benchmarkTicker=None):
        active_strategy = resolve_market_data_source(getattr(module.algoParas, "MARKET_DATA_SOURCE", strategy))
        if active_strategy == "database-first":
            ticker = self._resolve_benchmark_ticker(benchmarkTicker)
            cache_key = (ticker, int(getattr(self, "history_lookback_days", 365)))
            if cache_key in self.benchmark_price_cache:
                return self.benchmark_price_cache[cache_key]
            today = module.dt.date.today()
            start_dt = today - real_dt.timedelta(days=int(getattr(self, "history_lookback_days", 365)))
            frame = load_daily_bars_frame_from_db(ticker, start_dt, today)
            if frame is not None and db_frame_has_recent_coverage(frame, today):
                prices = build_cookstock_price_list_from_frame(frame)
                self.benchmark_price_cache[cache_key] = prices
                print(f"market-data source=db benchmark={ticker} bars={len(prices)}")
                return prices
        return original_get_benchmark_price_data(self, benchmarkTicker)

    cook_financials_cls.get_historical_price_data = patched_get_historical_price_data
    cook_financials_cls._get_benchmark_price_data = patched_get_benchmark_price_data

    batch_process_cls = getattr(module, "batch_process", None)
    if batch_process_cls is not None:
        if not hasattr(batch_process_cls, "_ticker_screener_original_warm_shared_benchmark_cache"):
            batch_process_cls._ticker_screener_original_warm_shared_benchmark_cache = batch_process_cls._warm_shared_benchmark_cache
        original_warm_shared_benchmark_cache = batch_process_cls._ticker_screener_original_warm_shared_benchmark_cache

        def patched_warm_shared_benchmark_cache(self, history_days):
            active_strategy = resolve_market_data_source(getattr(module.algoParas, "MARKET_DATA_SOURCE", strategy))
            if active_strategy == "database-first":
                cache_key = (self.benchmark_ticker.upper(), int(history_days))
                if cache_key in module.cookFinancials.benchmark_price_cache:
                    self.shared_benchmark_cache_warmed = True
                    return
                today = module.dt.date.today()
                start_dt = today - real_dt.timedelta(days=int(history_days))
                frame = load_daily_bars_frame_from_db(self.benchmark_ticker.upper(), start_dt, today)
                if frame is not None and db_frame_has_recent_coverage(frame, today):
                    prices = build_cookstock_price_list_from_frame(frame)
                    module.cookFinancials.benchmark_price_cache[cache_key] = prices
                    self.shared_benchmark_cache_warmed = True
                    print(f"market-data source=db benchmark-cache={self.benchmark_ticker.upper()} bars={len(prices)}")
                    return
            return original_warm_shared_benchmark_cache(self, history_days)

        batch_process_cls._warm_shared_benchmark_cache = patched_warm_shared_benchmark_cache


def load_configured_cookstock(config: AppConfig, *, market_data_source: str | None = None) -> ModuleType:
    module = load_cookstock_module()
    apply_config_to_cookstock(module, config)
    _apply_market_data_source_patches(module, market_data_source)
    return module


@contextmanager
def freeze_cookstock_today(module: ModuleType, as_of_date: real_dt.date | None):
    if as_of_date is None:
        yield
        return

    original_dt = getattr(module, "dt", None)
    if original_dt is None:
        yield
        return

    class FrozenDate(real_dt.date):
        @classmethod
        def today(cls) -> "FrozenDate":
            return cls(as_of_date.year, as_of_date.month, as_of_date.day)

    class FrozenDateModule:
        date = FrozenDate

        def __getattr__(self, name: str):
            return getattr(real_dt, name)

    module.dt = FrozenDateModule()
    try:
        yield
    finally:
        module.dt = original_dt
