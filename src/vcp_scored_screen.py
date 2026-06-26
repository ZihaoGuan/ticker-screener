from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, resolve_database_url
from .rs_rating_screen import approximate_rs_rating, compute_latest_weighted_rs_score
from .trend_template_screen import evaluate_trend_template
from .universe import UniverseTicker
from .vcp_screen import VcpHit, run_vcp_screen


VCP_SCORED_HISTORY_DAYS = 320
DB_BATCH_SIZE = 400
WATCHLIST_MIN_SCORE_EXCLUSIVE = 80.0


def _log(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class VcpScoredHit:
    ticker: str
    sector: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    screen_profile: str
    current_price: float
    support_price: float
    pivot_price: float
    vcp_contractions_count: int
    is_vcp_structure_valid: bool
    is_good_pivot: bool
    is_deep_correction: bool
    is_demand_dry: bool
    is_breakout_volume_confirmed: bool
    breakout_day_volume: float
    breakout_avg_volume_50: float
    reasons: list[str]
    trend_template_score: float
    contraction_quality_score: float
    volume_pattern_score: float
    pivot_proximity_score: float
    relative_strength_score: float
    composite_score: float
    rating: str
    execution_state: str
    pattern_type: str
    state_cap_applied: bool
    distance_from_pivot_pct: float
    rs_rating: float | None
    trend_criteria_passed: int
    trend_criteria_total: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class VcpScoredScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    candidate_tickers: int
    passed_tickers: int
    watchlist_passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[VcpScoredHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "candidate_tickers": self.candidate_tickers,
            "passed_tickers": self.passed_tickers,
            "watchlist_passed_tickers": self.watchlist_passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["High", "Low", "Close", "Volume"]
    available = {str(column).lower(): column for column in frame.columns}
    missing = [column for column in required if column.lower() not in available]
    if missing:
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    normalized = normalized.dropna(subset=required).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _rating_from_score(score: float) -> str:
    if score >= 90:
        return "Textbook VCP"
    if score >= 80:
        return "Strong VCP"
    if score >= 70:
        return "Good VCP"
    if score >= 60:
        return "Developing VCP"
    if score >= 50:
        return "Weak VCP"
    return "No VCP"


def _state_cap(rating: str, execution_state: str) -> tuple[str, bool]:
    max_rating = {
        "Invalid": "No VCP",
        "Damaged": "No VCP",
        "Overextended": "Weak VCP",
        "Extended": "Developing VCP",
        "Early-post-breakout": "Strong VCP",
        "Breakout": None,
        "Pre-breakout": None,
    }.get(execution_state)
    if max_rating is None:
        return rating, False

    order = ["No VCP", "Weak VCP", "Developing VCP", "Good VCP", "Strong VCP", "Textbook VCP"]
    if order.index(rating) > order.index(max_rating):
        return max_rating, True
    return rating, False


def _compute_execution_state(hit: VcpHit, bars: pd.DataFrame) -> str:
    if bars.empty or len(bars) < 200:
        return "Invalid"
    close = bars["Close"].astype(float)
    ma50 = float(close.rolling(50).mean().iloc[-1])
    ma200 = float(close.rolling(200).mean().iloc[-1])
    current_price = float(close.iloc[-1])
    if current_price < ma50 and ma50 < ma200:
        return "Invalid"
    if current_price < float(hit.support_price) or current_price < ma50:
        return "Damaged"
    if ma200 > 0 and ((current_price / ma200) - 1.0) * 100.0 > 50.0:
        return "Overextended"
    distance_pct = ((current_price / float(hit.pivot_price)) - 1.0) * 100.0 if hit.pivot_price > 0 else 0.0
    if distance_pct > 10.0:
        return "Overextended"
    if distance_pct > 5.0:
        return "Extended"
    if distance_pct >= 3.0:
        return "Early-post-breakout"
    if distance_pct >= 0.0:
        return "Breakout" if hit.is_breakout_volume_confirmed else "Early-post-breakout"
    return "Pre-breakout"


def _classify_pattern(hit: VcpHit, execution_state: str, composite_score: float) -> str:
    if execution_state in {"Invalid", "Damaged"}:
        return "Damaged"
    if execution_state in {"Overextended", "Extended"}:
        return "Extended Leader"
    if execution_state in {"Breakout", "Early-post-breakout"}:
        return "Post-breakout"
    if hit.is_vcp_structure_valid and hit.vcp_contractions_count >= 3 and composite_score >= 90:
        return "Textbook VCP"
    if hit.is_vcp_structure_valid:
        return "VCP-adjacent"
    return "Developing"


def _compute_trend_score(bars: pd.DataFrame) -> tuple[float, int, int]:
    snapshot = evaluate_trend_template(bars)
    if snapshot is None:
        return 0.0, 0, 7
    criteria = dict(snapshot.criteria)
    criteria.pop("rs_rating_above_70", None)
    criteria_passed = sum(1 for passed in criteria.values() if passed)
    criteria_total = len(criteria)
    return round((criteria_passed / criteria_total) * 100.0, 1), criteria_passed, criteria_total


def _compute_contraction_score(hit: VcpHit) -> float:
    if hit.vcp_contractions_count >= 4:
        score = 90.0
    elif hit.vcp_contractions_count == 3:
        score = 80.0
    elif hit.vcp_contractions_count == 2:
        score = 60.0
    else:
        score = 30.0
    if hit.is_vcp_structure_valid:
        score += 10.0
    if not hit.is_deep_correction:
        score += 10.0
    return min(score, 100.0)


def _compute_volume_score(hit: VcpHit) -> float:
    breakout_ratio = hit.breakout_day_volume / hit.breakout_avg_volume_50 if hit.breakout_avg_volume_50 > 0 else 0.0
    if hit.is_demand_dry:
        score = 75.0
    elif hit.breakout_avg_volume_50 > 0 and breakout_ratio < 1.0:
        score = 35.0
    else:
        score = 50.0
    if hit.is_breakout_volume_confirmed:
        score += 10.0
    if (hit.demand_dry_volume_slope or 0.0) < 0:
        score += 10.0
    if (hit.demand_dry_recent_volume_slope or 0.0) < 0:
        score += 5.0
    if breakout_ratio >= 3.0:
        score += 10.0
    elif breakout_ratio >= 1.5:
        score += 5.0
    return min(score, 100.0)


def _compute_pivot_score(hit: VcpHit) -> tuple[float, float]:
    if hit.pivot_price <= 0:
        return 0.0, 0.0
    distance_pct = ((hit.current_price / hit.pivot_price) - 1.0) * 100.0
    if 0 <= distance_pct <= 3:
        score = 90.0 + (10.0 if hit.is_breakout_volume_confirmed else 0.0)
    elif 3 < distance_pct <= 5:
        score = 65.0 + (10.0 if hit.is_breakout_volume_confirmed else 0.0)
    elif 5 < distance_pct <= 10:
        score = 50.0
    elif 10 < distance_pct <= 20:
        score = 35.0
    elif distance_pct > 20:
        score = 20.0
    elif -2 <= distance_pct < 0:
        score = 90.0
    elif -5 <= distance_pct < -2:
        score = 75.0
    elif -8 <= distance_pct < -5:
        score = 60.0
    elif -10 <= distance_pct < -8:
        score = 45.0
    elif -15 <= distance_pct < -10:
        score = 30.0
    else:
        score = 10.0
    return min(score, 100.0), round(distance_pct, 2)


def _compute_rs_score(bars: pd.DataFrame, benchmark_bars: pd.DataFrame) -> float | None:
    if bars.empty or benchmark_bars.empty:
        return None
    stock = bars["Close"].astype(float)
    benchmark = benchmark_bars["Close"].astype(float)
    weighted_score = compute_latest_weighted_rs_score(stock, benchmark)
    if weighted_score is None:
        return None
    rs_rating = approximate_rs_rating(weighted_score)
    return float(rs_rating) if rs_rating is not None else None


def score_vcp_hit(
    hit: VcpHit,
    *,
    bars: pd.DataFrame,
    benchmark_bars: pd.DataFrame,
) -> VcpScoredHit | None:
    normalized_bars = _normalize_price_frame(bars)
    normalized_benchmark = _normalize_price_frame(benchmark_bars)
    if normalized_bars.empty or normalized_benchmark.empty:
        return None

    trend_score, trend_passed, trend_total = _compute_trend_score(normalized_bars)
    contraction_score = _compute_contraction_score(hit)
    volume_score = _compute_volume_score(hit)
    pivot_score, distance_from_pivot_pct = _compute_pivot_score(hit)
    rs_score = _compute_rs_score(normalized_bars, normalized_benchmark)
    if rs_score is None:
        rs_score = 0.0

    composite_score = (
        trend_score * 0.25
        + contraction_score * 0.25
        + volume_score * 0.20
        + pivot_score * 0.15
        + rs_score * 0.15
    )
    composite_score = round(composite_score, 1)
    execution_state = _compute_execution_state(hit, normalized_bars)
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
        trend_template_score=round(trend_score, 1),
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
        rs_rating=round(rs_score, 1) if rs_score is not None else None,
        trend_criteria_passed=trend_passed,
        trend_criteria_total=trend_total,
    )


def build_vcp_scored_result(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> VcpScoredScreenResult:
    old_result = run_vcp_screen(config, tickers, as_of_date=as_of_date)
    resolved_database_url = resolve_database_url(database_url)
    run_date = as_of_date or dt.date.today()
    benchmark_symbol = config.benchmark_ticker.upper()
    frames = load_many_ticker_windows(
        [hit.ticker for hit in old_result.hits] + [benchmark_symbol],
        run_date,
        VCP_SCORED_HISTORY_DAYS,
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
        scored_hit = score_vcp_hit(hit, bars=bars, benchmark_bars=benchmark_bars)
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
        "completed vcp scored screen: "
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
