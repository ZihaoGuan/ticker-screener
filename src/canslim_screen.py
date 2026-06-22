from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any

import pandas as pd

from .config import AppConfig, project_root
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .ratings.finviz_insider import load_finviz_insider_signal_map
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


CANSLIM_HISTORY_DAYS = 320
CANSLIM_MIN_SCORE = 9
CANSLIM_MIN_WATCHLIST_COUNT = 5
CANSLIM_WATCHLIST_FALLBACK_COUNT = 10
CANSLIM_MIN_AVG_VOLUME_20D = 500_000.0
CANSLIM_INSIDER_LOOKBACK_DAYS = 90
CANSLIM_INSIDER_BUY_BONUS_AMOUNT = 500_000.0
CANSLIM_INSIDER_SELL_PENALTY_AMOUNT = 2_000_000.0


@dataclass(frozen=True)
class CanslimHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    as_of_date: str
    score: int
    max_score: int
    rank: int
    letter_scores: dict[str, int]
    letter_passes: dict[str, bool]
    metrics: dict[str, object]
    reasons: list[str]
    leader_flags: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CanslimScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    minimum_score: int
    failed_tickers: list[dict[str, str]]
    hits: list[CanslimHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "minimum_score": self.minimum_score,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _score_c(current: dict[str, Any]) -> tuple[int, list[str]]:
    eps_qq = _coerce_float(current.get("eps_qq_pct"))
    sales_qq = _coerce_float(current.get("sales_qq_pct"))
    score = 0
    if eps_qq is not None and eps_qq >= 25.0 and sales_qq is not None and sales_qq >= 25.0:
        score = 2
    elif (eps_qq is not None and eps_qq >= 20.0) or (sales_qq is not None and sales_qq >= 20.0):
        score = 1
    reasons = []
    if eps_qq is not None:
        reasons.append(f"EPS Q/Q {eps_qq:.1f}%")
    if sales_qq is not None:
        reasons.append(f"Sales Q/Q {sales_qq:.1f}%")
    return score, reasons


def _score_a(current: dict[str, Any]) -> tuple[int, list[str]]:
    eps_this_y = _coerce_float(current.get("eps_this_y_pct"))
    eps_next_5y = _coerce_float(current.get("eps_next_5y_pct"))
    roe = _coerce_float(current.get("roe_pct"))
    passing_checks = sum(
        1
        for condition in (
            eps_this_y is not None and eps_this_y >= 20.0,
            eps_next_5y is not None and eps_next_5y >= 20.0,
            roe is not None and roe >= 17.0,
        )
        if condition
    )
    score = 2 if passing_checks >= 3 else 1 if passing_checks >= 2 else 0
    reasons = []
    if eps_this_y is not None:
        reasons.append(f"EPS this Y {eps_this_y:.1f}%")
    if eps_next_5y is not None:
        reasons.append(f"EPS next 5Y {eps_next_5y:.1f}%")
    if roe is not None:
        reasons.append(f"ROE {roe:.1f}%")
    return score, reasons


def _score_n(metrics: dict[str, float | None], leadership_score: float | None) -> tuple[int, list[str], list[str]]:
    distance = metrics.get("distance_from_52w_high_pct")
    breakout = bool(metrics.get("is_20d_breakout"))
    score = 0
    if distance is not None and distance <= 5.0 and (breakout or (leadership_score is not None and leadership_score >= 85.0)):
        score = 2
    elif (distance is not None and distance <= 15.0) or breakout:
        score = 1
    reasons: list[str] = []
    flags: list[str] = []
    if distance is not None:
        reasons.append(f"{distance:.1f}% below 52W high")
        if distance <= 15.0:
            flags.append("near_52w_high")
    if breakout:
        reasons.append("20D breakout")
        flags.append("breakout")
    return score, reasons, flags


def _score_s(
    current: dict[str, Any],
    metrics: dict[str, float | None],
    insider_signal: dict[str, float | int] | None = None,
) -> tuple[int, list[str]]:
    shares_float = _coerce_float(current.get("shares_float"))
    shares_outstanding = _coerce_float(current.get("shares_outstanding"))
    supply_proxy = shares_float if shares_float is not None else shares_outstanding
    avg_volume = metrics.get("avg_volume_20d")
    up_down_volume_ratio = metrics.get("up_down_volume_ratio_20d")
    score = 0
    if (
        supply_proxy is not None
        and supply_proxy <= 500_000_000.0
        and avg_volume is not None
        and avg_volume >= 1_000_000.0
        and up_down_volume_ratio is not None
        and up_down_volume_ratio > 1.2
    ):
        score = 2
    elif supply_proxy is not None and supply_proxy <= 2_000_000_000.0 and avg_volume is not None and avg_volume >= CANSLIM_MIN_AVG_VOLUME_20D:
        score = 1

    buy_amount = _coerce_float(insider_signal.get("buy_amount")) if insider_signal else None
    buy_count = int(insider_signal.get("buy_count") or 0) if insider_signal else 0
    discretionary_sell_amount = _coerce_float(insider_signal.get("discretionary_sell_amount")) if insider_signal else None
    discretionary_sell_count = int(insider_signal.get("discretionary_sell_count") or 0) if insider_signal else 0
    net_amount_excl_10b5_1 = _coerce_float(insider_signal.get("net_amount_excl_10b5_1")) if insider_signal else None
    positive_insider = (
        supply_proxy is not None
        and supply_proxy <= 2_000_000_000.0
        and avg_volume is not None
        and avg_volume >= CANSLIM_MIN_AVG_VOLUME_20D
        and buy_amount is not None
        and buy_amount >= CANSLIM_INSIDER_BUY_BONUS_AMOUNT
        and buy_count >= 1
        and net_amount_excl_10b5_1 is not None
        and net_amount_excl_10b5_1 > 0.0
    )
    negative_insider = (
        discretionary_sell_amount is not None
        and discretionary_sell_amount >= CANSLIM_INSIDER_SELL_PENALTY_AMOUNT
        and discretionary_sell_count >= 2
        and net_amount_excl_10b5_1 is not None
        and net_amount_excl_10b5_1 <= -CANSLIM_INSIDER_SELL_PENALTY_AMOUNT
    )
    if positive_insider and score < 2:
        score += 1
    if negative_insider and score > 0:
        score -= 1

    reasons = []
    if supply_proxy is not None:
        reasons.append(f"Float/outstanding {supply_proxy:,.0f}")
    if avg_volume is not None:
        reasons.append(f"Avg volume 20D {avg_volume:,.0f}")
    if up_down_volume_ratio is not None:
        reasons.append(f"Up/down volume {up_down_volume_ratio:.2f}")
    if buy_amount is not None and buy_amount > 0.0:
        reasons.append(f"Insider buys {buy_amount:,.0f}")
    if discretionary_sell_amount is not None and discretionary_sell_amount > 0.0:
        reasons.append(f"Insider sells ex-10b5-1 {discretionary_sell_amount:,.0f}")
    if net_amount_excl_10b5_1 is not None:
        reasons.append(f"Insider net ex-10b5-1 {net_amount_excl_10b5_1:,.0f}")
    return score, reasons


def _score_l(leadership_score: float | None) -> tuple[int, list[str], list[str]]:
    score = 2 if leadership_score is not None and leadership_score >= 85.0 else 1 if leadership_score is not None and leadership_score >= 70.0 else 0
    reasons = [f"Leadership score {leadership_score:.1f}"] if leadership_score is not None else []
    flags = ["leader"] if score == 2 else ["improving_leader"] if score == 1 else []
    return score, reasons, flags


def _score_i(current: dict[str, Any]) -> tuple[int, list[str]]:
    inst_own = _coerce_float(current.get("institutional_ownership_pct"))
    inst_trans = _coerce_float(current.get("institutional_transactions_pct"))
    insider_own = _coerce_float(current.get("insider_ownership_pct"))
    insider_trans = _coerce_float(current.get("insider_transactions_pct"))

    sponsorship_points = 0.0
    if inst_own is not None and inst_own >= 20.0:
        sponsorship_points += 1.0
    elif inst_own is not None and inst_own >= 10.0:
        sponsorship_points += 0.5
    if inst_trans is not None and inst_trans > 0.0:
        sponsorship_points += 1.0
    if inst_trans is not None and inst_trans >= 5.0:
        sponsorship_points += 0.5
    if insider_own is not None and insider_own >= 1.0:
        sponsorship_points += 0.5
    if insider_trans is not None and insider_trans > 0.0:
        sponsorship_points += 0.5
    if inst_trans is not None and inst_trans <= -5.0:
        sponsorship_points -= 0.5
    if insider_trans is not None and insider_trans < 0.0:
        sponsorship_points -= 0.25

    score = 2 if sponsorship_points >= 2.0 else 1 if sponsorship_points >= 1.0 else 0
    reasons = []
    if inst_own is not None:
        reasons.append(f"Inst ownership {inst_own:.1f}%")
    if inst_trans is not None:
        reasons.append(f"Inst trans {inst_trans:.1f}%")
    if insider_own is not None:
        reasons.append(f"Insider ownership {insider_own:.1f}%")
    if insider_trans is not None:
        reasons.append(f"Insider trans {insider_trans:.1f}%")
    return score, reasons


def _score_m(benchmark_metrics: dict[str, float | bool | None]) -> tuple[int, list[str], bool]:
    close = _coerce_float(benchmark_metrics.get("close"))
    sma50 = _coerce_float(benchmark_metrics.get("sma50"))
    sma200 = _coerce_float(benchmark_metrics.get("sma200"))
    score = 0
    market_pass = False
    if close is not None and sma50 is not None and sma200 is not None:
        if close > sma50 and sma50 > sma200:
            score = 2
            market_pass = True
        elif close > sma50:
            score = 1
    reasons = []
    if close is not None and sma50 is not None and sma200 is not None:
        reasons.append(f"Benchmark close {close:.2f} vs SMA50 {sma50:.2f} / SMA200 {sma200:.2f}")
    return score, reasons, market_pass


def _frame_metrics(frame: pd.DataFrame) -> dict[str, float | bool | None]:
    bars = frame.sort_index().copy()
    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    volume = bars["Volume"].astype(float)
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    avg_volume_20d = volume.rolling(20).mean()
    up_days = close.diff() > 0
    down_days = close.diff() < 0
    up_volume_sum = volume.where(up_days, 0.0).rolling(20).sum()
    down_volume_sum = volume.where(down_days, 0.0).rolling(20).sum()
    prior_20_high = high.shift(1).rolling(20).max()
    high_52w = high.tail(252).max() if len(high) >= 252 else high.max()
    current_close = float(close.iloc[-1]) if not close.empty else None
    up_down_volume_ratio = None
    latest_down = float(down_volume_sum.iloc[-1]) if pd.notna(down_volume_sum.iloc[-1]) else 0.0
    latest_up = float(up_volume_sum.iloc[-1]) if pd.notna(up_volume_sum.iloc[-1]) else 0.0
    if latest_down > 0:
        up_down_volume_ratio = float(latest_up / latest_down)
    breakout = bool(pd.notna(prior_20_high.iloc[-1]) and current_close is not None and current_close > float(prior_20_high.iloc[-1]))
    distance = None
    if current_close is not None and high_52w and float(high_52w) > 0:
        distance = float(((float(high_52w) - current_close) / float(high_52w)) * 100.0)
    return {
        "close": current_close,
        "sma50": float(sma50.iloc[-1]) if pd.notna(sma50.iloc[-1]) else None,
        "sma200": float(sma200.iloc[-1]) if pd.notna(sma200.iloc[-1]) else None,
        "avg_volume_20d": float(avg_volume_20d.iloc[-1]) if pd.notna(avg_volume_20d.iloc[-1]) else None,
        "up_down_volume_ratio_20d": up_down_volume_ratio,
        "distance_from_52w_high_pct": distance,
        "is_20d_breakout": breakout,
        "high_52w": float(high_52w) if pd.notna(high_52w) else None,
    }


def compute_canslim_frame_metrics(frame: pd.DataFrame) -> dict[str, float | bool | None]:
    return _frame_metrics(frame)


def evaluate_canslim_ticker(
    ticker: UniverseTicker,
    *,
    current: dict[str, Any] | None,
    technical: dict[str, Any] | None,
    frame: pd.DataFrame | None,
    benchmark_metrics: dict[str, float | bool | None],
    as_of_date: dt.date,
    insider_signal: dict[str, float | int] | None = None,
) -> tuple[CanslimHit | None, str | None]:
    if not current or str(current.get("parse_status") or "").lower() != "ok":
        return None, "missing finviz fundamentals snapshot"
    if not technical or str(technical.get("technical_status") or "").lower() != "ok":
        return None, "missing technical rating snapshot"
    if frame is None or frame.empty or not db_frame_has_recent_coverage(frame, as_of_date):
        return None, "missing cached price history"

    metrics = _frame_metrics(frame)
    avg_volume = _coerce_float(metrics.get("avg_volume_20d"))
    if avg_volume is None or avg_volume < CANSLIM_MIN_AVG_VOLUME_20D:
        return None, "below minimum liquidity floor"

    market_score, market_reasons, market_pass = _score_m(benchmark_metrics)
    leadership_score = _coerce_float(technical.get("leadership_score"))
    c_score, c_reasons = _score_c(current)
    a_score, a_reasons = _score_a(current)
    n_score, n_reasons, n_flags = _score_n(metrics, leadership_score)
    s_score, s_reasons = _score_s(current, metrics, insider_signal)
    l_score, l_reasons, l_flags = _score_l(leadership_score)
    i_score, i_reasons = _score_i(current)
    letter_scores = {
        "C": c_score,
        "A": a_score,
        "N": n_score,
        "S": s_score,
        "L": l_score,
        "I": i_score,
        "M": market_score,
    }
    letter_passes = {key: value >= 1 for key, value in letter_scores.items()}
    total_score = sum(letter_scores.values())
    hit_metrics: dict[str, object] = {
        "as_of_date": current.get("as_of_date"),
        "eps_qq_pct": current.get("eps_qq_pct"),
        "sales_qq_pct": current.get("sales_qq_pct"),
        "eps_this_y_pct": current.get("eps_this_y_pct"),
        "eps_next_5y_pct": current.get("eps_next_5y_pct"),
        "roe_pct": current.get("roe_pct"),
        "institutional_ownership_pct": current.get("institutional_ownership_pct"),
        "institutional_transactions_pct": current.get("institutional_transactions_pct"),
        "insider_ownership_pct": current.get("insider_ownership_pct"),
        "insider_transactions_pct": current.get("insider_transactions_pct"),
        "shares_float": current.get("shares_float"),
        "shares_outstanding": current.get("shares_outstanding"),
        "insider_buy_amount": insider_signal.get("buy_amount") if insider_signal else None,
        "insider_buy_count": insider_signal.get("buy_count") if insider_signal else None,
        "insider_discretionary_sell_amount": insider_signal.get("discretionary_sell_amount") if insider_signal else None,
        "insider_discretionary_sell_count": insider_signal.get("discretionary_sell_count") if insider_signal else None,
        "insider_net_amount_excl_10b5_1": insider_signal.get("net_amount_excl_10b5_1") if insider_signal else None,
        "leadership_score": leadership_score,
        "distance_from_52w_high_pct": metrics.get("distance_from_52w_high_pct"),
        "avg_volume_20d": metrics.get("avg_volume_20d"),
        "up_down_volume_ratio_20d": metrics.get("up_down_volume_ratio_20d"),
        "market_pass": market_pass,
    }
    reasons = [
        *(c_reasons[:2]),
        *(a_reasons[:2]),
        *(n_reasons[:2]),
        *(s_reasons[:2]),
        *(l_reasons[:1]),
        *(i_reasons[:1]),
    ]
    return (
        CanslimHit(
            ticker=ticker.symbol.upper(),
            sector=ticker.sector or current.get("sector"),
            industry=ticker.industry or current.get("industry"),
            exchange=ticker.exchange,
            as_of_date=as_of_date.isoformat(),
            score=total_score,
            max_score=14,
            rank=0,
            letter_scores=letter_scores,
            letter_passes=letter_passes,
            metrics=hit_metrics,
            reasons=reasons + market_reasons,
            leader_flags=[*n_flags, *l_flags],
        ),
        None,
    )


def canslim_sort_key(item: CanslimHit) -> tuple[int, int, float, float, str]:
    distance = _coerce_float(item.metrics.get("distance_from_52w_high_pct"))
    leadership = _coerce_float(item.metrics.get("leadership_score")) or 0.0
    market = 1 if item.letter_passes.get("M") else 0
    return (-market, -item.score, -(leadership or 0.0), distance if distance is not None else 999.0, item.ticker)


def run_canslim_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str = "",
) -> CanslimScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    repository = RatingsRepository(resolved_database_url)
    symbols = [item.symbol.upper() for item in tickers]
    fundamentals_map = repository.load_latest_fundamentals_snapshots_for_tickers(symbols, as_of_date=run_date)
    technical_map = repository.load_latest_technical_rating_snapshots_for_tickers(symbols, as_of_date=run_date)
    frame_map = load_many_ticker_windows([*symbols, config.benchmark_ticker], run_date, CANSLIM_HISTORY_DAYS, database_url=resolved_database_url)
    benchmark_frame = frame_map.get(config.benchmark_ticker.upper())
    if benchmark_frame is None:
        benchmark_frame = frame_map.get(config.benchmark_ticker)
    failures: list[dict[str, str]] = []
    if benchmark_frame is None or benchmark_frame.empty or not db_frame_has_recent_coverage(benchmark_frame, run_date):
        return CanslimScreenResult(
            run_date=run_date.isoformat(),
            total_tickers=len(tickers),
            passed_tickers=0,
            minimum_score=CANSLIM_MIN_SCORE,
            failed_tickers=[{"ticker": config.benchmark_ticker.upper(), "error": "missing benchmark history"}],
            hits=[],
        )
    benchmark_metrics = _frame_metrics(benchmark_frame)
    insider_signal_map = load_finviz_insider_signal_map(
        symbols,
        as_of_date=run_date,
        lookback_days=CANSLIM_INSIDER_LOOKBACK_DAYS,
        artifacts_dir=project_root() / "artifacts",
    )
    scored_hits: list[CanslimHit] = []
    for ticker in tickers:
        symbol = ticker.symbol.upper()
        hit, failure_reason = evaluate_canslim_ticker(
            ticker,
            current=fundamentals_map.get(symbol),
            technical=technical_map.get(symbol),
            frame=frame_map.get(symbol),
            benchmark_metrics=benchmark_metrics,
            as_of_date=run_date,
            insider_signal=insider_signal_map.get(symbol),
        )
        if hit is None:
            failures.append({"ticker": symbol, "error": failure_reason or "unknown canslim error"})
            continue
        scored_hits.append(hit)

    ordered = sorted(scored_hits, key=canslim_sort_key)
    ranked_hits = [
        CanslimHit(
            ticker=item.ticker,
            sector=item.sector,
            industry=item.industry,
            exchange=item.exchange,
            as_of_date=item.as_of_date,
            score=item.score,
            max_score=item.max_score,
            rank=index,
            letter_scores=item.letter_scores,
            letter_passes=item.letter_passes,
            metrics=item.metrics,
            reasons=item.reasons,
            leader_flags=item.leader_flags,
        )
        for index, item in enumerate(ordered, start=1)
    ]
    return CanslimScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(ranked_hits),
        minimum_score=CANSLIM_MIN_SCORE,
        failed_tickers=failures,
        hits=ranked_hits,
    )
