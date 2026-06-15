from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import datetime as real_dt
import importlib
import math
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterable

from .config import AppConfig, project_root
from .market_data_access import (
    build_cookstock_payload_from_frame,
    build_cookstock_price_list_from_frame,
    db_frame_has_recent_coverage,
    load_many_ticker_windows,
    load_daily_bars_frame_from_db,
    resolve_market_data_source,
    resolve_database_url,
)


_prefetched_market_data: ContextVar[dict[str, object] | None] = ContextVar("_prefetched_market_data", default=None)


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
        prefetched = _prefetched_market_data.get()
        if prefetched and time_interval == "daily":
            ticker_frames = prefetched.get("ticker_frames", {})
            frame = ticker_frames.get(str(self.ticker).upper())
            if frame is not None:
                start_dt = real_dt.date.fromisoformat(str(start_date))
                end_dt = real_dt.date.fromisoformat(str(end_date))
                sliced = frame.loc[(frame.index.date >= start_dt) & (frame.index.date <= end_dt)]
                payload = build_cookstock_payload_from_frame(str(self.ticker), sliced)
                if payload is not None:
                    return payload
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
        prefetched = _prefetched_market_data.get()
        if prefetched:
            ticker = self._resolve_benchmark_ticker(benchmarkTicker).upper()
            benchmark_frames = prefetched.get("benchmark_frames", {})
            frame = benchmark_frames.get(ticker)
            if frame is None:
                frame = prefetched.get("ticker_frames", {}).get(ticker)
            if frame is not None:
                cache_key = (ticker, int(getattr(self, "history_lookback_days", 365)))
                if cache_key not in self.benchmark_price_cache:
                    self.benchmark_price_cache[cache_key] = build_cookstock_price_list_from_frame(frame)
                return self.benchmark_price_cache[cache_key]
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
            prefetched = _prefetched_market_data.get()
            if prefetched:
                cache_key = (self.benchmark_ticker.upper(), int(history_days))
                benchmark_frames = prefetched.get("benchmark_frames", {})
                frame = benchmark_frames.get(self.benchmark_ticker.upper())
                if frame is not None:
                    module.cookFinancials.benchmark_price_cache[cache_key] = build_cookstock_price_list_from_frame(frame)
                    self.shared_benchmark_cache_warmed = True
                    return
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


def resolve_prefetch_batch_size(total_tickers: int, *, override: int | None = None) -> int:
    if override is not None and override > 0:
        return override
    candidate = 250
    raw = str(Path.cwd().joinpath(".").name)  # keep deterministic branch below even without env
    _ = raw
    try:
        import os

        env_value = os.getenv("TICKER_SCREENER_PREFETCH_BATCH_SIZE", "").strip()
        if env_value:
            candidate = int(env_value)
    except Exception:
        candidate = 250
    return max(1, min(max(1, total_tickers), candidate))


def should_use_prefetched_market_data(*, market_data_source: str | None = None, database_url: str | None = None) -> bool:
    return resolve_market_data_source(market_data_source) == "database-first" and bool(resolve_database_url(database_url))


@contextmanager
def use_prefetched_market_data(*, ticker_frames: dict[str, object], benchmark_frames: dict[str, object]) -> None:
    token = _prefetched_market_data.set(
        {
            "ticker_frames": {str(key).upper(): value for key, value in ticker_frames.items()},
            "benchmark_frames": {str(key).upper(): value for key, value in benchmark_frames.items()},
        }
    )
    try:
        yield
    finally:
        _prefetched_market_data.reset(token)


@contextmanager
def prefetched_cookstock_market_data(
    config: AppConfig,
    tickers: Iterable[str],
    *,
    as_of_date: real_dt.date | None,
    history_lookback_days: int,
    benchmark_ticker: str | None = None,
    market_data_source: str | None = None,
    database_url: str | None = None,
):
    if not should_use_prefetched_market_data(market_data_source=market_data_source, database_url=database_url):
        yield
        return

    target_date = as_of_date or real_dt.date.today()
    ticker_list = [str(item).strip().upper() for item in tickers if str(item).strip()]
    benchmark = (benchmark_ticker or config.benchmark_ticker).strip().upper()
    if benchmark:
        ticker_list.append(benchmark)
    trading_days_needed = max(1, int(math.ceil(int(history_lookback_days) * 0.75)) + 20)
    frames = load_many_ticker_windows(ticker_list, target_date, trading_days_needed, database_url=database_url)
    ticker_frames = {ticker: frame for ticker, frame in frames.items() if ticker != benchmark}
    benchmark_frames = {benchmark: frames[benchmark]} if benchmark in frames else {}
    print(
        "market-data prefetch="
        f"tickers:{len(ticker_frames)} "
        f"benchmark:{benchmark if benchmark_frames else 'missing'} "
        f"trading_days:{trading_days_needed}"
    )
    with use_prefetched_market_data(ticker_frames=ticker_frames, benchmark_frames=benchmark_frames):
        yield


def iter_prefetched_cookstock_batches(
    config: AppConfig,
    tickers: list[object],
    *,
    as_of_date: real_dt.date | None,
    history_lookback_days: int,
    benchmark_ticker: str | None = None,
    batch_size: int | None = None,
    market_data_source: str | None = None,
    database_url: str | None = None,
):
    if not tickers:
        return
    if not should_use_prefetched_market_data(market_data_source=market_data_source, database_url=database_url):
        yield tickers
        return

    size = resolve_prefetch_batch_size(len(tickers), override=batch_size)
    for start in range(0, len(tickers), size):
        batch = tickers[start : start + size]
        symbols = [str(getattr(item, "symbol", item)).strip().upper() for item in batch if str(getattr(item, "symbol", item)).strip()]
        with prefetched_cookstock_market_data(
            config,
            symbols,
            as_of_date=as_of_date,
            history_lookback_days=history_lookback_days,
            benchmark_ticker=benchmark_ticker,
            market_data_source=market_data_source,
            database_url=database_url,
        ):
            yield batch


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
