from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt
from typing import Any

import pandas as pd

from .canslim_screen import CANSLIM_INSIDER_LOOKBACK_DAYS, CANSLIM_MIN_AVG_VOLUME_20D, compute_canslim_frame_metrics
from .config import AppConfig, project_root
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, resolve_database_url
from .ratings.finviz_insider import load_finviz_insider_signal_map
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


CANSLIM_V2_HISTORY_DAYS = 320
WATCHLIST_MIN_SCORE_EXCLUSIVE = 80.0

_C_WEIGHT = 15.0
_A_WEIGHT = 20.0
_N_WEIGHT = 15.0
_S_WEIGHT = 15.0
_L_WEIGHT = 20.0
_I_WEIGHT = 10.0
_M_WEIGHT = 5.0


def _log(message: str) -> None:
    print(message, flush=True)


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rating_from_score(score: float) -> str:
    if score >= 90.0:
        return "Exceptional+"
    if score >= 80.0:
        return "Exceptional"
    if score >= 70.0:
        return "Strong"
    if score >= 60.0:
        return "Above Average"
    return "Watchlist"


@dataclass(frozen=True)
class CanslimV2Hit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    as_of_date: str
    composite_score: float
    rating: str
    rank: int
    component_scores: dict[str, float]
    component_passes: dict[str, bool]
    metrics: dict[str, object]
    reasons: list[str]
    leader_flags: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CanslimV2ScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    watchlist_passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[CanslimV2Hit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "watchlist_passed_tickers": self.watchlist_passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _score_c(current: dict[str, Any]) -> tuple[float, list[str]]:
    eps_qq = _coerce_float(current.get("eps_qq_pct"))
    sales_qq = _coerce_float(current.get("sales_qq_pct"))
    score = 0.0
    if eps_qq is not None:
        score += min(max((eps_qq / 80.0) * 70.0, 0.0), 70.0)
    if sales_qq is not None:
        score += min(max((sales_qq / 40.0) * 30.0, 0.0), 30.0)
    reasons: list[str] = []
    if eps_qq is not None:
        reasons.append(f"EPS Q/Q {eps_qq:.1f}%")
    if sales_qq is not None:
        reasons.append(f"Sales Q/Q {sales_qq:.1f}%")
    return round(min(score, 100.0), 1), reasons


def _score_a(current: dict[str, Any]) -> tuple[float, list[str]]:
    eps_this_y = _coerce_float(current.get("eps_this_y_pct"))
    eps_next_5y = _coerce_float(current.get("eps_next_5y_pct"))
    roe = _coerce_float(current.get("roe_pct"))
    score = 0.0
    if eps_this_y is not None:
        score += min(max((eps_this_y / 50.0) * 40.0, 0.0), 40.0)
    if eps_next_5y is not None:
        score += min(max((eps_next_5y / 25.0) * 35.0, 0.0), 35.0)
    if roe is not None:
        score += min(max((roe / 20.0) * 25.0, 0.0), 25.0)
    reasons: list[str] = []
    if eps_this_y is not None:
        reasons.append(f"EPS this Y {eps_this_y:.1f}%")
    if eps_next_5y is not None:
        reasons.append(f"EPS next 5Y {eps_next_5y:.1f}%")
    if roe is not None:
        reasons.append(f"ROE {roe:.1f}%")
    return round(min(score, 100.0), 1), reasons


def _score_n(metrics: dict[str, float | bool | None], leadership_score: float | None) -> tuple[float, list[str], list[str]]:
    distance = _coerce_float(metrics.get("distance_from_52w_high_pct"))
    breakout = bool(metrics.get("is_20d_breakout"))
    score = 0.0
    flags: list[str] = []
    reasons: list[str] = []
    if distance is not None:
        if distance <= 2.0:
            score += 70.0
        elif distance <= 5.0:
            score += 60.0
        elif distance <= 10.0:
            score += 45.0
        elif distance <= 15.0:
            score += 30.0
        if distance <= 15.0:
            flags.append("near_52w_high")
        reasons.append(f"{distance:.1f}% below 52W high")
    if breakout:
        score += 20.0
        flags.append("breakout")
        reasons.append("20D breakout")
    if leadership_score is not None:
        score += min(max((leadership_score - 60.0) * 0.75, 0.0), 20.0)
    return round(min(score, 100.0), 1), reasons, flags


def _score_s(current: dict[str, Any], metrics: dict[str, float | bool | None], insider_signal: dict[str, float | int] | None) -> tuple[float, list[str]]:
    shares_float = _coerce_float(current.get("shares_float"))
    shares_outstanding = _coerce_float(current.get("shares_outstanding"))
    supply_proxy = shares_float if shares_float is not None else shares_outstanding
    avg_volume = _coerce_float(metrics.get("avg_volume_20d"))
    up_down_ratio = _coerce_float(metrics.get("up_down_volume_ratio_20d"))
    buy_amount = _coerce_float(insider_signal.get("buy_amount")) if insider_signal else None
    discretionary_sell_amount = _coerce_float(insider_signal.get("discretionary_sell_amount")) if insider_signal else None
    net_amount = _coerce_float(insider_signal.get("net_amount_excl_10b5_1")) if insider_signal else None

    score = 0.0
    if supply_proxy is not None:
        if supply_proxy <= 500_000_000.0:
            score += 30.0
        elif supply_proxy <= 2_000_000_000.0:
            score += 20.0
        elif supply_proxy <= 10_000_000_000.0:
            score += 10.0
    if avg_volume is not None:
        score += min(max((avg_volume / 3_000_000.0) * 20.0, 0.0), 20.0)
    if up_down_ratio is not None:
        if up_down_ratio >= 1.5:
            score += 35.0
        elif up_down_ratio >= 1.2:
            score += 25.0
        elif up_down_ratio >= 1.0:
            score += 15.0
    if buy_amount is not None and buy_amount >= 500_000.0 and (net_amount or 0.0) > 0.0:
        score += 15.0
    if discretionary_sell_amount is not None and discretionary_sell_amount >= 2_000_000.0 and (net_amount or 0.0) < 0.0:
        score -= 15.0

    reasons: list[str] = []
    if supply_proxy is not None:
        reasons.append(f"Float/outstanding {supply_proxy:,.0f}")
    if avg_volume is not None:
        reasons.append(f"Avg volume 20D {avg_volume:,.0f}")
    if up_down_ratio is not None:
        reasons.append(f"Up/down volume {up_down_ratio:.2f}")
    if buy_amount is not None and buy_amount > 0.0:
        reasons.append(f"Insider buys {buy_amount:,.0f}")
    if discretionary_sell_amount is not None and discretionary_sell_amount > 0.0:
        reasons.append(f"Insider sells ex-10b5-1 {discretionary_sell_amount:,.0f}")
    return round(max(min(score, 100.0), 0.0), 1), reasons


def _score_l(technical: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    leadership_score = _coerce_float(technical.get("leadership_score"))
    if leadership_score is None:
        return 0.0, [], []
    flags = ["leader"] if leadership_score >= 85.0 else ["improving_leader"] if leadership_score >= 70.0 else []
    return round(min(max(leadership_score, 0.0), 100.0), 1), [f"Leadership score {leadership_score:.1f}"], flags


def _score_i(current: dict[str, Any]) -> tuple[float, list[str]]:
    inst_own = _coerce_float(current.get("institutional_ownership_pct"))
    inst_trans = _coerce_float(current.get("institutional_transactions_pct"))
    insider_own = _coerce_float(current.get("insider_ownership_pct"))
    insider_trans = _coerce_float(current.get("insider_transactions_pct"))

    score = 0.0
    if inst_own is not None:
        score += min(max((inst_own / 70.0) * 55.0, 0.0), 55.0)
    if inst_trans is not None:
        score += min(max(((inst_trans + 5.0) / 10.0) * 25.0, 0.0), 25.0)
    if insider_own is not None:
        score += min(max((insider_own / 5.0) * 10.0, 0.0), 10.0)
    if insider_trans is not None:
        score += min(max(((insider_trans + 2.0) / 4.0) * 10.0, 0.0), 10.0)

    reasons: list[str] = []
    if inst_own is not None:
        reasons.append(f"Inst ownership {inst_own:.1f}%")
    if inst_trans is not None:
        reasons.append(f"Inst trans {inst_trans:.1f}%")
    if insider_own is not None:
        reasons.append(f"Insider ownership {insider_own:.1f}%")
    if insider_trans is not None:
        reasons.append(f"Insider trans {insider_trans:.1f}%")
    return round(min(score, 100.0), 1), reasons


def _score_m(benchmark_metrics: dict[str, float | bool | None]) -> tuple[float, list[str], bool]:
    close = _coerce_float(benchmark_metrics.get("close"))
    sma50 = _coerce_float(benchmark_metrics.get("sma50"))
    sma200 = _coerce_float(benchmark_metrics.get("sma200"))
    score = 0.0
    market_pass = False
    if close is not None and sma50 is not None and sma200 is not None:
        if close > sma50 and sma50 > sma200:
            score = 100.0
            market_pass = True
        elif close > sma50:
            score = 60.0
        elif close > sma200:
            score = 35.0
    reasons: list[str] = []
    if close is not None and sma50 is not None and sma200 is not None:
        reasons.append(f"Benchmark close {close:.2f} vs SMA50 {sma50:.2f} / SMA200 {sma200:.2f}")
    return score, reasons, market_pass


def _weighted_total(component_scores: dict[str, float]) -> float:
    total = (
        component_scores["C"] * _C_WEIGHT
        + component_scores["A"] * _A_WEIGHT
        + component_scores["N"] * _N_WEIGHT
        + component_scores["S"] * _S_WEIGHT
        + component_scores["L"] * _L_WEIGHT
        + component_scores["I"] * _I_WEIGHT
        + component_scores["M"] * _M_WEIGHT
    ) / 100.0
    if (
        component_scores["C"] >= 75.0
        and component_scores["A"] >= 75.0
        and component_scores["N"] >= 60.0
        and component_scores["L"] >= 85.0
        and component_scores["M"] >= 60.0
    ):
        total += 5.0
    return round(total, 1)


def evaluate_canslim_v2_ticker(
    ticker: UniverseTicker,
    *,
    current: dict[str, Any] | None,
    technical: dict[str, Any] | None,
    frame: pd.DataFrame | None,
    benchmark_metrics: dict[str, float | bool | None],
    as_of_date: dt.date,
    insider_signal: dict[str, float | int] | None = None,
) -> tuple[CanslimV2Hit | None, str | None]:
    if not current or str(current.get("parse_status") or "").lower() != "ok":
        return None, "missing finviz fundamentals snapshot"
    if not technical or str(technical.get("technical_status") or "").lower() != "ok":
        return None, "missing technical rating snapshot"
    if frame is None or frame.empty or not db_frame_has_recent_coverage(frame, as_of_date):
        return None, "missing cached price history"

    metrics = compute_canslim_frame_metrics(frame)
    avg_volume = _coerce_float(metrics.get("avg_volume_20d"))
    if avg_volume is None or avg_volume < CANSLIM_MIN_AVG_VOLUME_20D:
        return None, "below minimum liquidity floor"

    leadership_score = _coerce_float(technical.get("leadership_score"))
    c_score, c_reasons = _score_c(current)
    a_score, a_reasons = _score_a(current)
    n_score, n_reasons, n_flags = _score_n(metrics, leadership_score)
    s_score, s_reasons = _score_s(current, metrics, insider_signal)
    l_score, l_reasons, l_flags = _score_l(technical)
    i_score, i_reasons = _score_i(current)
    m_score, m_reasons, market_pass = _score_m(benchmark_metrics)
    component_scores = {
        "C": c_score,
        "A": a_score,
        "N": n_score,
        "S": s_score,
        "L": l_score,
        "I": i_score,
        "M": m_score,
    }
    component_passes = {key: value >= 60.0 for key, value in component_scores.items()}
    composite_score = _weighted_total(component_scores)
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
        "insider_discretionary_sell_amount": insider_signal.get("discretionary_sell_amount") if insider_signal else None,
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
        *m_reasons[:1],
    ]
    return (
        CanslimV2Hit(
            ticker=ticker.symbol.upper(),
            sector=ticker.sector or current.get("sector"),
            industry=ticker.industry or current.get("industry"),
            exchange=ticker.exchange,
            as_of_date=as_of_date.isoformat(),
            composite_score=composite_score,
            rating=_rating_from_score(composite_score),
            rank=0,
            component_scores=component_scores,
            component_passes=component_passes,
            metrics=hit_metrics,
            reasons=reasons,
            leader_flags=[*n_flags, *l_flags],
        ),
        None,
    )


def _sort_key(item: CanslimV2Hit) -> tuple[float, float, float, str]:
    leadership = _coerce_float(item.metrics.get("leadership_score")) or 0.0
    distance = _coerce_float(item.metrics.get("distance_from_52w_high_pct"))
    market = 1.0 if item.component_passes.get("M") else 0.0
    return (-market, -item.composite_score, distance if distance is not None else 999.0, item.ticker)


def run_canslim_v2_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str = "",
) -> CanslimV2ScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    repository = RatingsRepository(resolved_database_url)
    symbols = [item.symbol.upper() for item in tickers]
    _log(f"loading canslim_v2 fundamentals: tickers={len(symbols)}")
    fundamentals_map = repository.load_latest_fundamentals_snapshots_for_tickers(symbols, as_of_date=run_date)
    _log(f"loading canslim_v2 technical snapshots: tickers={len(symbols)}")
    technical_map = repository.load_latest_technical_rating_snapshots_for_tickers(
        symbols,
        as_of_date=run_date,
        allow_older_as_of_date=True,
    )
    _log(f"loading canslim_v2 price history: tickers={len(symbols) + 1}")
    frame_map = load_many_ticker_windows([*symbols, config.benchmark_ticker], run_date, CANSLIM_V2_HISTORY_DAYS, database_url=resolved_database_url)
    benchmark_frame = frame_map.get(config.benchmark_ticker.upper())
    if benchmark_frame is None:
        benchmark_frame = frame_map.get(config.benchmark_ticker)
    failures: list[dict[str, str]] = []
    if benchmark_frame is None or benchmark_frame.empty or not db_frame_has_recent_coverage(benchmark_frame, run_date):
        return CanslimV2ScreenResult(
            run_date=run_date.isoformat(),
            total_tickers=len(tickers),
            passed_tickers=0,
            watchlist_passed_tickers=0,
            failed_tickers=[{"ticker": config.benchmark_ticker.upper(), "error": "missing benchmark history"}],
            hits=[],
        )
    benchmark_metrics = compute_canslim_frame_metrics(benchmark_frame)
    _log(f"loading canslim_v2 insider signals: tickers={len(symbols)}")
    insider_signal_map = load_finviz_insider_signal_map(
        symbols,
        as_of_date=run_date,
        lookback_days=CANSLIM_INSIDER_LOOKBACK_DAYS,
        artifacts_dir=project_root() / "artifacts",
    )

    scored_hits: list[CanslimV2Hit] = []
    for index, ticker in enumerate(tickers, start=1):
        symbol = ticker.symbol.upper()
        hit, failure_reason = evaluate_canslim_v2_ticker(
            ticker,
            current=fundamentals_map.get(symbol),
            technical=technical_map.get(symbol),
            frame=frame_map.get(symbol),
            benchmark_metrics=benchmark_metrics,
            as_of_date=run_date,
            insider_signal=insider_signal_map.get(symbol),
        )
        if hit is None:
            failures.append({"ticker": symbol, "error": failure_reason or "unknown canslim_v2 error"})
            _log(f"[{index}/{len(tickers)}] skip {symbol}: {failure_reason or 'unknown canslim_v2 error'}")
            continue
        scored_hits.append(hit)
        _log(f"[{index}/{len(tickers)}] score {symbol}: {hit.composite_score:.1f} ({hit.rating})")

    ordered = sorted(scored_hits, key=_sort_key)
    ranked_hits = [
        CanslimV2Hit(
            ticker=item.ticker,
            sector=item.sector,
            industry=item.industry,
            exchange=item.exchange,
            as_of_date=item.as_of_date,
            composite_score=item.composite_score,
            rating=item.rating,
            rank=index,
            component_scores=item.component_scores,
            component_passes=item.component_passes,
            metrics=item.metrics,
            reasons=item.reasons,
            leader_flags=item.leader_flags,
        )
        for index, item in enumerate(ordered, start=1)
    ]
    watchlist_passed = sum(1 for item in ranked_hits if item.composite_score > WATCHLIST_MIN_SCORE_EXCLUSIVE)
    return CanslimV2ScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(ranked_hits),
        watchlist_passed_tickers=watchlist_passed,
        failed_tickers=failures,
        hits=ranked_hits,
    )
