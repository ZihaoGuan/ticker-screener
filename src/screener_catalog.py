from __future__ import annotations

from dataclasses import asdict
import datetime as dt
from typing import Callable

from .bb_squeeze_screen import find_recent_bb_squeeze_hit
from .config import AppConfig
from .base_detection_screen import find_active_base_detection_hit
from .cup_detection_screen import find_active_cup_detection_hit
from .double_bottom_detection_screen import find_active_double_bottom_detection_hit
from .ema21_pullback_buy_screen import find_recent_ema21_pullback_buy_hit
from .cup_handle_screen import run_cup_handle_screen
from .fearzone_zeiierman_screen import find_recent_fearzone_zeiierman_hit
from .fearzone_screen import find_recent_fearzone_hit
from .ftd_sweep_screen import find_recent_ftd_sweep_hit
from .gap_fill_screen import run_gap_fill_screen
from .high_tight_flag_screen import HTF_SLOPE_LOOKBACK, HTF_SMA_LONG_PERIOD, find_high_tight_flag_hit
from .high_tight_flag_setup_screen import HTF_SETUP_HISTORY_DAYS, find_high_tight_flag_setup_hit
from .hve_screen import find_recent_hve_hit
from .htf_runup_screen import run_htf_runup_screen
from .inside_dryup_screen import find_recent_inside_dryup_hit
from .inside_dryup_v2_screen import HISTORY_DAYS as INSIDE_DRYUP_V2_HISTORY_DAYS, find_recent_inside_dryup_v2_hit
from .leif_high_tight_flag_screen import LEIF_HTF_LOOKBACK_DAYS, find_leif_high_tight_flag_hit
from .lost_21ema_screen import run_lost_21ema_screen
from .macd_screen import find_recent_macd_hit
from .near_200ma_screen import run_near_200ma_screen
from .rti_screen import find_recent_rti_hit
from .sean_breakout_screen import find_recent_sean_breakout_hit
from .rsi_ma_bb_screen import find_recent_rsi_ma_bb_hit
from .rs_screen import run_rs_screen
from .sma200_pullback_buy_screen import find_recent_sma200_pullback_buy_hit
from .sepa_vcp_screen import SEPA_HISTORY_DAYS, find_recent_sepa_vcp_hit
from .screener_engine import ScreenerEvaluationResult, ScreenerInputBundle, ScreenerSpec
from .td_sequential_screen import find_recent_td_sequential_hit
from .three_weeks_tight_screen import find_three_weeks_tight_hit
from .trend_template_screen import run_trend_template_screen
from .universe import UniverseTicker
from .vcp_screen import run_vcp_screen
from .vcs_screen import find_recent_vcs_hit
from .weinstein_stage2_early_screen import (
    WEINSTEIN_STAGE2_EARLY_HISTORY_DAYS,
    find_weinstein_stage2_early_hit,
)
from .weekly_tight_close_screen import find_weekly_tight_close_breakout_hit, find_weekly_tight_close_hit
from .weekly_htf_pullback_screen import run_weekly_htf_pullback_screen
from .wyckoff_analysis import WYCKOFF_HISTORY_DAYS, find_recent_wyckoff_signal_hit


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


def _run_td9_bullish(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_td_sequential_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="bullish",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_td9_bearish(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_td_sequential_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="bearish",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_macd_golden_cross(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_macd_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="golden_cross",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_macd_dead_cross(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_macd_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="dead_cross",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_rsi_ma_bb_bullish(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_rsi_ma_bb_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="bullish",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_rsi_ma_bb_bearish(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_rsi_ma_bb_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        direction="bearish",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "direction": payload["direction"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_bb_squeeze(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_bb_squeeze_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "signal_kind": payload["signal_kind"],
            "bb_squeeze_ratio": payload["bb_squeeze_ratio"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_ema21_pullback_buy(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_ema21_pullback_buy_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "test_date": payload["test_date"],
            "test_count": payload["test_count"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_sma200_pullback_buy(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_sma200_pullback_buy_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "test_date": payload["test_date"],
            "test_count": payload["test_count"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_high_tight_flag(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_high_tight_flag_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "runup_40_ratio": payload["runup_40_ratio"],
            "atr_ratio": payload["atr_ratio"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_high_tight_flag_setup(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_high_tight_flag_setup_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "pole_gain_ratio": payload["pole_gain_ratio"],
            "distance_to_pivot_pct": payload["distance_to_pivot_pct"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_leif_high_tight_flag(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    benchmark_ticker = str(bundle.extras.get("benchmark_ticker") or "SPY").strip().upper() or "SPY"
    hit = find_leif_high_tight_flag_hit(
        bundle.bars,
        bundle.benchmark_bars,
        ticker=_ticker_from_bundle(bundle),
        benchmark_ticker=benchmark_ticker,
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "score": payload["score"],
            "rs_rating": payload["rs_rating"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_rti(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_rti_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "signal_kind": payload["signal_kind"],
            "rti_value": payload["rti_value"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_sean_breakout(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_sean_breakout_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "avg_volume_10": payload["avg_volume_10"],
            "adr_pct_20": payload["adr_pct_20"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_sepa_vcp(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_sepa_vcp_hit(
        bundle.bars,
        bundle.benchmark_bars,
        ticker=_ticker_from_bundle(bundle),
        benchmark_ticker=str(bundle.extras["config"].benchmark_ticker),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "rpr_score": payload["rpr_score"],
            "buy_risk_status": payload["buy_risk_status"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_vcs_setup_stage(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_vcs_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        signal_profile="setup_stage",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "vcs_score": payload["vcs_score"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_vcs_critical_tightness(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_vcs_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        signal_profile="critical_tightness",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "vcs_score": payload["vcs_score"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_base_detection(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_active_base_detection_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "base_type": payload["base_type"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_cup_detection(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_active_cup_detection_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "shape_mode": payload["shape_mode"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_double_bottom_detection(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_active_double_bottom_detection_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "breakout_price": payload["breakout_price"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_weekly_tight_close(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_weekly_tight_close_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "breakout_price": payload["breakout_price"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_weinstein_stage2_early(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_weinstein_stage2_early_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "run_length_weeks": payload["run_length_weeks"],
            "weekly_ma30": payload["weekly_ma30"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_weekly_tight_close_breakout(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_weekly_tight_close_breakout_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "breakout_price": payload["breakout_price"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_three_weeks_tight(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_three_weeks_tight_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "buy_price": payload["buy_price"]},
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


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


def _run_inside_dryup(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_inside_dryup_hit(bundle.bars, ticker=_ticker_from_bundle(bundle))
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "quality_score": payload["quality_score"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_inside_dryup_v2(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_inside_dryup_v2_hit(bundle.bars, ticker=_ticker_from_bundle(bundle))
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "price_volume_ratio": payload["price_volume_ratio"],
            "dry_count": payload["dry_count"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_wyckoff_buy_signal(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_wyckoff_signal_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        signal_type="buy",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "phase": payload["phase"],
            "accum_score": payload["accum_score"],
        },
        reasons=tuple(str(item) for item in payload.get("reasons", [])),
        hit=payload,
    )


def _run_wyckoff_sell_signal(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    hit = find_recent_wyckoff_signal_hit(
        bundle.bars,
        ticker=_ticker_from_bundle(bundle),
        signal_type="sell",
    )
    if hit is None:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hit.to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={
            "ticker": bundle.ticker,
            "signal_date": payload["signal_date"],
            "phase": payload["phase"],
            "dist_score": payload["dist_score"],
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


def _run_fearzone_zeiierman(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    ticker = _ticker_from_bundle(bundle)
    hit = find_recent_fearzone_zeiierman_hit(
        bundle.bars,
        ticker=ticker,
        config=bundle.extras["config"],
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


def _run_trend_template(bundle: ScreenerInputBundle) -> ScreenerEvaluationResult:
    config = bundle.extras["config"]
    result = run_trend_template_screen(config, [_ticker_from_bundle(bundle)], as_of_date=bundle.as_of_date)
    hits = list(getattr(result, "hits", []))
    failures = list(getattr(result, "failed_tickers", []))
    if failures:
        return ScreenerEvaluationResult(passed=False, error=str(failures[0].get("error") or "unknown screener failure"))
    if not hits:
        return ScreenerEvaluationResult(passed=False, metrics={"ticker": bundle.ticker})
    payload = hits[0].to_dict()
    return ScreenerEvaluationResult(
        passed=True,
        metrics={"ticker": bundle.ticker, "signal_date": payload["signal_date"], "criteria_passed": payload["criteria_passed"]},
        reasons=tuple(payload.get("reasons", [])),
        hit=payload,
    )


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
        "daily_rs_new_high": ScreenerSpec(
            id="daily_rs_new_high",
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
        "weekly_rs_new_high": ScreenerSpec(
            id="weekly_rs_new_high",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max_rs_days,
            warmup_trading_days=40,
            evaluator=_run_rs_weekly,
        ),
        "weekly_rs_before_price": ScreenerSpec(
            id="weekly_rs_before_price",
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
            lookback_trading_days=5000,
            warmup_trading_days=5,
            evaluator=_run_hve,
        ),
        "inside_dryup": ScreenerSpec(
            id="inside_dryup",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=260,
            warmup_trading_days=10,
            evaluator=_run_inside_dryup,
        ),
        "inside_dryup_v2": ScreenerSpec(
            id="inside_dryup_v2",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=INSIDE_DRYUP_V2_HISTORY_DAYS,
            warmup_trading_days=10,
            evaluator=_run_inside_dryup_v2,
        ),
        "wyckoff_buy_signal": ScreenerSpec(
            id="wyckoff_buy_signal",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=WYCKOFF_HISTORY_DAYS,
            warmup_trading_days=10,
            evaluator=_run_wyckoff_buy_signal,
        ),
        "wyckoff_sell_signal": ScreenerSpec(
            id="wyckoff_sell_signal",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=WYCKOFF_HISTORY_DAYS,
            warmup_trading_days=10,
            evaluator=_run_wyckoff_sell_signal,
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
        "fearzone_zeiierman": ScreenerSpec(
            id="fearzone_zeiierman",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=max(
                int(config.fearzone_zeiierman_high_period) + int(config.fearzone_zeiierman_stdev_period) + 40,
                int(config.fearzone_zeiierman_stdev_period) * 3,
                220,
            ),
            warmup_trading_days=10,
            evaluator=_run_fearzone_zeiierman,
        ),
        "td9_bullish": ScreenerSpec(
            id="td9_bullish",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=120,
            warmup_trading_days=10,
            evaluator=_run_td9_bullish,
        ),
        "td9_bearish": ScreenerSpec(
            id="td9_bearish",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=120,
            warmup_trading_days=10,
            evaluator=_run_td9_bearish,
        ),
        "macd_golden_cross": ScreenerSpec(
            id="macd_golden_cross",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=180,
            warmup_trading_days=20,
            evaluator=_run_macd_golden_cross,
        ),
        "macd_dead_cross": ScreenerSpec(
            id="macd_dead_cross",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=180,
            warmup_trading_days=20,
            evaluator=_run_macd_dead_cross,
        ),
        "rsi_ma_bb_bullish": ScreenerSpec(
            id="rsi_ma_bb_bullish",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=120,
            warmup_trading_days=20,
            evaluator=_run_rsi_ma_bb_bullish,
        ),
        "rsi_ma_bb_bearish": ScreenerSpec(
            id="rsi_ma_bb_bearish",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=120,
            warmup_trading_days=20,
            evaluator=_run_rsi_ma_bb_bearish,
        ),
        "bb_squeeze": ScreenerSpec(
            id="bb_squeeze",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=90,
            warmup_trading_days=20,
            evaluator=_run_bb_squeeze,
        ),
        "ema21_pullback_buy": ScreenerSpec(
            id="ema21_pullback_buy",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=140,
            warmup_trading_days=20,
            evaluator=_run_ema21_pullback_buy,
        ),
        "sma200_pullback_buy": ScreenerSpec(
            id="sma200_pullback_buy",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=320,
            warmup_trading_days=20,
            evaluator=_run_sma200_pullback_buy,
        ),
        "high_tight_flag": ScreenerSpec(
            id="high_tight_flag",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=HTF_SMA_LONG_PERIOD + HTF_SLOPE_LOOKBACK,
            warmup_trading_days=20,
            evaluator=_run_high_tight_flag,
        ),
        "high_tight_flag_setup": ScreenerSpec(
            id="high_tight_flag_setup",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=HTF_SETUP_HISTORY_DAYS,
            warmup_trading_days=20,
            evaluator=_run_high_tight_flag_setup,
        ),
        "leif_high_tight_flag": ScreenerSpec(
            id="leif_high_tight_flag",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=LEIF_HTF_LOOKBACK_DAYS,
            warmup_trading_days=20,
            evaluator=_run_leif_high_tight_flag,
        ),
        "sepa_vcp": ScreenerSpec(
            id="sepa_vcp",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=SEPA_HISTORY_DAYS,
            warmup_trading_days=20,
            evaluator=_run_sepa_vcp,
        ),
        "rti": ScreenerSpec(
            id="rti",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=20,
            warmup_trading_days=5,
            evaluator=_run_rti,
        ),
        "sean_breakout": ScreenerSpec(
            id="sean_breakout",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=90,
            warmup_trading_days=20,
            evaluator=_run_sean_breakout,
        ),
        "vcs_setup_stage": ScreenerSpec(
            id="vcs_setup_stage",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=90,
            warmup_trading_days=10,
            evaluator=_run_vcs_setup_stage,
        ),
        "vcs_critical_tightness": ScreenerSpec(
            id="vcs_critical_tightness",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=90,
            warmup_trading_days=10,
            evaluator=_run_vcs_critical_tightness,
        ),
        "base_detection": ScreenerSpec(
            id="base_detection",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=520,
            warmup_trading_days=20,
            evaluator=_run_base_detection,
        ),
        "cup_detection": ScreenerSpec(
            id="cup_detection",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=520,
            warmup_trading_days=20,
            evaluator=_run_cup_detection,
        ),
        "double_bottom_detection": ScreenerSpec(
            id="double_bottom_detection",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=520,
            warmup_trading_days=20,
            evaluator=_run_double_bottom_detection,
        ),
        "weekly_tight_close": ScreenerSpec(
            id="weekly_tight_close",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=220,
            warmup_trading_days=20,
            evaluator=_run_weekly_tight_close,
        ),
        "weinstein_stage2_early": ScreenerSpec(
            id="weinstein_stage2_early",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=WEINSTEIN_STAGE2_EARLY_HISTORY_DAYS,
            warmup_trading_days=20,
            evaluator=_run_weinstein_stage2_early,
        ),
        "weekly_tight_close_breakout": ScreenerSpec(
            id="weekly_tight_close_breakout",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=220,
            warmup_trading_days=20,
            evaluator=_run_weekly_tight_close_breakout,
        ),
        "three_weeks_tight": ScreenerSpec(
            id="three_weeks_tight",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=40,
            warmup_trading_days=5,
            evaluator=_run_three_weeks_tight,
        ),
        "weekly_htf_pullback": ScreenerSpec(
            id="weekly_htf_pullback",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max(int(config.rs_new_high_history_days), int(config.htf_history_days), 365),
            warmup_trading_days=40,
            evaluator=_run_weekly_htf_pullback,
        ),
        "eight_week_100_runup": ScreenerSpec(
            id="eight_week_100_runup",
            required_inputs=("daily_bars", "benchmark_bars", "metadata"),
            lookback_trading_days=max(int(config.htf_history_days), int(config.htf_runup_window_days), 90),
            warmup_trading_days=20,
            evaluator=_run_htf_runup,
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
        "trend_template": ScreenerSpec(
            id="trend_template",
            required_inputs=("daily_bars", "metadata"),
            lookback_trading_days=320,
            warmup_trading_days=20,
            evaluator=_run_trend_template,
        ),
    }
