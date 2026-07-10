from __future__ import annotations

import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, resolve_database_url
from .universe import UniverseTicker
from .vcp_scored_screen import (
    VcpScoredHit,
    VcpScoredScreenResult,
    WATCHLIST_MIN_SCORE_EXCLUSIVE,
    _classify_pattern,
    _compute_contraction_score,
    _compute_pivot_score,
    _compute_volume_score,
    _compute_rs_score,
    _rating_from_score,
    _state_cap,
)
from .weekly_vcp_screen import WEEKLY_VCP_HISTORY_DAYS, run_weekly_vcp_screen
from .weekly_vcp_utils import to_weekly_price_frame


def _log(message: str) -> None:
    print(message, flush=True)


def _compute_weekly_trend_score(bars: pd.DataFrame) -> tuple[float, int, int]:
    close = bars["Close"].astype(float)
    ma10 = close.rolling(10).mean()
    ma30 = close.rolling(30).mean()
    ma40 = close.rolling(40).mean()
    if len(close) < 44 or pd.isna(ma10.iloc[-1]) or pd.isna(ma30.iloc[-1]) or pd.isna(ma40.iloc[-1]):
        return 0.0, 0, 6
    latest_close = float(close.iloc[-1])
    latest_ma10 = float(ma10.iloc[-1])
    latest_ma30 = float(ma30.iloc[-1])
    latest_ma40 = float(ma40.iloc[-1])
    high_52w = float(bars["High"].astype(float).tail(min(52, len(bars))).max())
    low_52w = float(bars["Low"].astype(float).tail(min(52, len(bars))).min())
    prior_ma40 = float(ma40.iloc[-4]) if pd.notna(ma40.iloc[-4]) else latest_ma40
    criteria = [
        latest_close > latest_ma10,
        latest_ma10 > latest_ma30,
        latest_ma30 > latest_ma40,
        latest_ma40 > prior_ma40,
        low_52w > 0 and latest_close > low_52w * 1.25,
        high_52w > 0 and latest_close > high_52w * 0.75,
    ]
    passed = sum(1 for item in criteria if item)
    total = len(criteria)
    return round((passed / total) * 100.0, 1), passed, total


def _compute_weekly_execution_state(current_price: float, support_price: float, pivot_price: float, bars: pd.DataFrame, *, breakout_confirmed: bool) -> str:
    close = bars["Close"].astype(float)
    if len(close) < 44:
        return "Invalid"
    ma10 = close.rolling(10).mean()
    ma40 = close.rolling(40).mean()
    if pd.isna(ma10.iloc[-1]) or pd.isna(ma40.iloc[-1]):
        return "Invalid"
    latest_ma10 = float(ma10.iloc[-1])
    latest_ma40 = float(ma40.iloc[-1])
    if current_price < latest_ma10 and latest_ma10 < latest_ma40:
        return "Invalid"
    if current_price < support_price or current_price < latest_ma10:
        return "Damaged"
    if latest_ma40 > 0 and ((current_price / latest_ma40) - 1.0) * 100.0 > 35.0:
        return "Overextended"
    distance_pct = ((current_price / pivot_price) - 1.0) * 100.0 if pivot_price > 0 else 0.0
    if distance_pct > 12.0:
        return "Overextended"
    if distance_pct > 6.0:
        return "Extended"
    if distance_pct >= 3.0:
        return "Early-post-breakout"
    if distance_pct >= 0.0:
        return "Breakout" if breakout_confirmed else "Early-post-breakout"
    return "Pre-breakout"


def score_weekly_vcp_hit(hit, *, bars: pd.DataFrame, benchmark_bars: pd.DataFrame) -> VcpScoredHit | None:
    weekly_bars = to_weekly_price_frame(bars)
    weekly_benchmark = to_weekly_price_frame(benchmark_bars, include_volume=False)
    if weekly_bars.empty or weekly_benchmark.empty:
        return None
    trend_score, trend_passed, trend_total = _compute_weekly_trend_score(weekly_bars)
    contraction_score = _compute_contraction_score(hit)
    volume_score = _compute_volume_score(hit)
    pivot_score, distance_from_pivot_pct = _compute_pivot_score(hit)
    rs_score = _compute_rs_score(weekly_bars, weekly_benchmark)
    if rs_score is None:
        rs_score = 0.0
    composite_score = (
        trend_score * 0.30
        + contraction_score * 0.25
        + volume_score * 0.20
        + pivot_score * 0.10
        + rs_score * 0.15
    )
    composite_score = round(composite_score, 1)
    execution_state = _compute_weekly_execution_state(
        hit.current_price,
        hit.support_price,
        hit.pivot_price,
        weekly_bars,
        breakout_confirmed=hit.is_breakout_volume_confirmed,
    )
    rating = _rating_from_score(composite_score)
    rating, state_cap_applied = _state_cap(rating, execution_state)
    pattern_type = _classify_pattern(hit, execution_state, composite_score)
    return VcpScoredHit(
        ticker=hit.ticker,
        sector=hit.sector,
        exchange=hit.exchange,
        signal_date=hit.signal_date,
        benchmark_ticker=hit.benchmark_ticker,
        screen_profile=hit.screen_profile,
        current_price=hit.current_price,
        support_price=hit.support_price,
        pivot_price=hit.pivot_price,
        vcp_contractions_count=hit.vcp_contractions_count,
        is_vcp_structure_valid=hit.is_vcp_structure_valid,
        is_good_pivot=hit.is_good_pivot,
        is_deep_correction=hit.is_deep_correction,
        is_demand_dry=hit.is_demand_dry,
        is_breakout_volume_confirmed=hit.is_breakout_volume_confirmed,
        breakout_day_volume=hit.breakout_day_volume,
        breakout_avg_volume_50=hit.breakout_avg_volume_50,
        reasons=list(hit.reasons),
        trend_template_score=trend_score,
        contraction_quality_score=round(contraction_score, 1),
        volume_pattern_score=round(volume_score, 1),
        pivot_proximity_score=round(pivot_score, 1),
        relative_strength_score=round(rs_score, 1),
        composite_score=composite_score,
        rating=rating,
        execution_state=execution_state,
        pattern_type=pattern_type,
        state_cap_applied=state_cap_applied,
        distance_from_pivot_pct=distance_from_pivot_pct,
        rs_rating=round(rs_score, 1),
        trend_criteria_passed=trend_passed,
        trend_criteria_total=trend_total,
    )


def build_weekly_vcp_scored_result(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> VcpScoredScreenResult:
    old_result = run_weekly_vcp_screen(config, tickers, as_of_date=as_of_date, database_url=database_url)
    resolved_database_url = resolve_database_url(database_url)
    run_date = as_of_date or dt.date.today()
    benchmark_symbol = config.benchmark_ticker.upper()
    frames = load_many_ticker_windows(
        [hit.ticker for hit in old_result.hits] + [benchmark_symbol],
        run_date,
        WEEKLY_VCP_HISTORY_DAYS,
        database_url=resolved_database_url,
    )
    benchmark_bars = frames.get(benchmark_symbol)
    scored_hits: list[VcpScoredHit] = []
    failures = list(old_result.failed_tickers)
    for position, hit in enumerate(old_result.hits, start=1):
        bars = frames.get(hit.ticker)
        _log(f"[{position}/{len(old_result.hits)}] scoring {hit.ticker} | scored={len(scored_hits)}")
        if bars is None or benchmark_bars is None:
            failures.append({"ticker": hit.ticker, "error": "missing_daily_bars_for_scoring"})
            _log(f"[{position}/{len(old_result.hits)}] {hit.ticker} score failed: missing daily_bars")
            continue
        scored_hit = score_weekly_vcp_hit(hit, bars=bars, benchmark_bars=benchmark_bars)
        if scored_hit is None:
            failures.append({"ticker": hit.ticker, "error": "scoring_returned_none"})
            _log(f"[{position}/{len(old_result.hits)}] {hit.ticker} score failed: scoring returned none")
            continue
        scored_hits.append(scored_hit)
        _log(
            f"[{position}/{len(old_result.hits)}] {hit.ticker} scored "
            f"{scored_hit.composite_score:.1f} rating={scored_hit.rating} state={scored_hit.execution_state}"
        )
    scored_hits.sort(key=lambda item: (item.composite_score, item.relative_strength_score), reverse=True)
    watchlist_passed = sum(1 for item in scored_hits if item.composite_score > WATCHLIST_MIN_SCORE_EXCLUSIVE)
    _log(
        "completed weekly vcp scored screen: "
        f"universe={len(tickers)} candidates={len(old_result.hits)} scored={len(scored_hits)} "
        f"watchlist_gt80={watchlist_passed} failed={len(failures)}"
    )
    return VcpScoredScreenResult(
        run_date=old_result.run_date,
        benchmark_ticker=old_result.benchmark_ticker,
        total_tickers=old_result.total_tickers,
        candidate_tickers=len(old_result.hits),
        passed_tickers=len(scored_hits),
        watchlist_passed_tickers=watchlist_passed,
        failed_tickers=failures,
        hits=scored_hits,
    )
