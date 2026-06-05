from __future__ import annotations

from dataclasses import asdict
import datetime as dt
from typing import Callable

from .config import AppConfig
from .cup_handle_screen import run_cup_handle_screen
from .fearzone_screen import find_recent_fearzone_hit
from .ftd_sweep_screen import find_recent_ftd_sweep_hit
from .gap_fill_screen import run_gap_fill_screen
from .hve_screen import find_recent_hve_hit
from .htf_runup_screen import run_htf_runup_screen
from .lost_21ema_screen import run_lost_21ema_screen
from .near_200ma_screen import run_near_200ma_screen
from .rs_screen import run_rs_screen
from .screener_engine import ScreenerEvaluationResult, ScreenerInputBundle, ScreenerSpec
from .universe import UniverseTicker
from .vcp_screen import run_vcp_screen
from .weekly_htf_pullback_screen import run_weekly_htf_pullback_screen


def _ticker_from_bundle(bundle: ScreenerInputBundle) -> UniverseTicker:
    metadata = dict(bundle.metadata or {})
    return UniverseTicker(
        symbol=bundle.ticker.upper(),
        sector=str(metadata["sector"]) if metadata.get("sector") else None,
        industry=str(metadata["industry"]) if metadata.get("industry") else None,
        exchange=str(metadata["exchange"]) if metadata.get("exchange") else None,
    )


def _single_ticker_result(
    bundle: ScreenerInputBundle,
    runner: Callable[..., object],
    config: AppConfig,
    **kwargs: object,
) -> ScreenerEvaluationResult:
    ticker = _ticker_from_bundle(bundle)
    result = runner(config, [ticker], as_of_date=bundle.as_of_date, **kwargs)
    hits = list(getattr(result, "hits", []))
    failures = list(getattr(result, "failed_tickers", []))
    if failures:
        return ScreenerEvaluationResult(
            passed=False,
            error=str(failures[0].get("error") or "unknown screener failure"),
        )
    if not hits:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    hit = hits[0]
    payload = hit.to_dict() if hasattr(hit, "to_dict") else asdict(hit)
    reasons = tuple(str(item) for item in payload.get("reasons", []))
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "screener": kwargs.get("signal_profile") or runner.__name__},
        reasons=reasons,
        hit=payload,
    )


def _run_rs_daily(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_rs_screen, config)


def _run_rs_weekly(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_rs_screen, config, signal_profile="weekly")


def _run_vcp(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_vcp_screen, config)


def _run_cup_handle(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_cup_handle_screen, config)


def _run_gap_fill(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_gap_fill_screen, config)


def _run_hve(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_hve_hit(bundle.bars, ticker=_ticker_from_bundle(bundle))
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "volume_buzz_pct": payload["volume_buzz_pct"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_ftd_sweep(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    ticker = _ticker_from_bundle(bundle)
    hit = find_recent_ftd_sweep_hit(
        bundle.bars,
        ticker=ticker,
        benchmark_ticker=config.benchmark_ticker,
        config=config,
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "bars_since_breakout": payload["bars_since_breakout"],
            "breakout_date": payload["sweep_breakout_date"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_fearzone(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    ticker = _ticker_from_bundle(bundle)
    hit = find_recent_fearzone_hit(
        bundle.bars,
        ticker=ticker,
        benchmark_ticker=config.benchmark_ticker,
        config=config,
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "signal_age_bars": payload["signal_age_bars"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_weekly_htf_pullback(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_weekly_htf_pullback_screen, config)


def _run_htf_runup(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    return _single_ticker_result(bundle, run_htf_runup_screen, config)


def _run_near_200ma(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    result = run_near_200ma_screen(config, [_ticker_from_bundle(bundle)])
    hits = list(getattr(result, "hits", []))
    failures = list(getattr(result, "failed_tickers", []))
    if failures:
        return ScreenerEvaluationResult(passed=False, error=str(failures[0].get("error") or "unknown screener failure"))
    if not hits:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hits[0].to_dict()
    return ScreenerEvaluationResult(passed=True, metrics={"ticker": bundle.ticker}, reasons=tuple(payload.get("reasons", [])), hit=payload)


def _run_lost_21ema(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    result = run_lost_21ema_screen(config, [_ticker_from_bundle(bundle)])
    hits = list(getattr(result, "hits", []))
    failures = list(getattr(result, "failed_tickers", []))
    if failures:
        return ScreenerEvaluationResult(passed=False, error=str(failures[0].get("error") or "unknown screener failure"))
    if not hits:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hits[0].to_dict()
    return ScreenerEvaluationResult(passed=True, metrics={"ticker": bundle.ticker}, reasons=tuple(payload.get("reasons", [])), hit=payload)


def build_screener_catalog(config: AppConfig) -> dict[str, ScreenerSpec]:
    max_rs_days = int(config.rs_new_high_history_days)
    max_vcp_days = max(int(config.rs_new_high_history_days), 365)
    return {
        "rs": ScreenerSpec(
            id="rs",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max_rs_days,
            warmup_trading_days=40,
            evaluator=_run_rs_daily,
        ),
        "weekly_rs": ScreenerSpec(
            id="weekly_rs",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max_rs_days,
            warmup_trading_days=40,
            evaluator=_run_rs_weekly,
        ),
        "vcp": ScreenerSpec(
            id="vcp",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max_vcp_days,
            warmup_trading_days=40,
            evaluator=_run_vcp,
        ),
        "cup_handle": ScreenerSpec(
            id="cup_handle",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=420,
            warmup_trading_days=40,
            evaluator=_run_cup_handle,
        ),
        "gap_fill": ScreenerSpec(
            id="gap_fill",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max(int(config.gap_fill_history_days), int(config.gap_fill_lookback_days) + 30, 120),
            warmup_trading_days=20,
            evaluator=_run_gap_fill,
        ),
        "hve": ScreenerSpec(
            id="hve",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=320,
            warmup_trading_days=5,
            evaluator=_run_hve,
        ),
        "ftd_sweep": ScreenerSpec(
            id="ftd_sweep",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=max(int(config.ftd_sweep_history_days), 160),
            warmup_trading_days=10,
            evaluator=_run_ftd_sweep,
        ),
        "fearzone": ScreenerSpec(
            id="fearzone",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=max(int(config.fearzone_band_period) + 20, int(config.fearzone_ma_long_period) + 20, 260),
            warmup_trading_days=10,
            evaluator=_run_fearzone,
        ),
        "weekly_htf_pullback": ScreenerSpec(
            id="weekly_htf_pullback",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max(int(config.rs_new_high_history_days), int(config.htf_history_days), 365),
            warmup_trading_days=40,
            evaluator=_run_weekly_htf_pullback,
        ),
        "htf_8w_runup": ScreenerSpec(
            id="htf_8w_runup",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max(int(config.htf_history_days), int(config.htf_runup_window_days), 90),
            warmup_trading_days=20,
            evaluator=_run_htf_runup,
        ),
        "near_200ma": ScreenerSpec(
            id="near_200ma",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=320,
            warmup_trading_days=20,
            evaluator=_run_near_200ma,
        ),
        "lost_21ema": ScreenerSpec(
            id="lost_21ema",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=320,
            warmup_trading_days=20,
            evaluator=_run_lost_21ema,
        ),
    }
