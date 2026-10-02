from __future__ import annotations

import datetime as dt
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


RTS_HISTORY_DAYS = 252


@dataclass(frozen=True)
class RelativeTrendStrengthSnapshot:
    ticker: str
    as_of_date: str
    sector: str | None
    sector_etf: str | None
    rts_score: float
    rts_state: str
    confidence: str
    close_price: float
    stock_vs_spy_21d_pct: float | None
    stock_vs_spy_63d_pct: float | None
    stock_vs_sector_63d_pct: float | None
    alpha_acceleration_pct: float | None
    market_relative_score: float
    sector_relative_score: float
    acceleration_score: float
    structure_score: float
    evidence: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_relative_trend_strength_snapshot(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: str,
    sector: str | None = None,
    sector_etf: str | None = None,
    sector_frame: pd.DataFrame | None = None,
    as_of_date: dt.date | None = None,
) -> RelativeTrendStrengthSnapshot | None:
    stock = _close_series(frame, as_of_date)
    benchmark = _close_series(benchmark_frame, as_of_date)
    if stock is None or benchmark is None:
        return None
    aligned = pd.concat([stock.rename("stock"), benchmark.rename("benchmark")], axis=1, join="inner").dropna()
    if len(aligned) < 64:
        return None
    sector_aligned = None
    if sector_frame is not None:
        sector_close = _close_series(sector_frame, as_of_date)
        if sector_close is not None:
            sector_aligned = pd.concat([stock.rename("stock"), sector_close.rename("sector")], axis=1, join="inner").dropna()

    stock_vs_spy_21d = _relative_return(aligned, "stock", "benchmark", 21)
    stock_vs_spy_63d = _relative_return(aligned, "stock", "benchmark", 63)
    stock_vs_sector_63d = _relative_return(sector_aligned, "stock", "sector", 63) if sector_aligned is not None else None
    acceleration = (stock_vs_spy_21d - stock_vs_spy_63d) if stock_vs_spy_21d is not None and stock_vs_spy_63d is not None else None

    market_score = _bucket_score(stock_vs_spy_63d, ((20, 30), (10, 24), (0, 16), (-10, 8)))
    sector_score = _bucket_score(stock_vs_sector_63d, ((15, 20), (5, 16), (0, 10), (-10, 5))) if stock_vs_sector_63d is not None else 10
    acceleration_score = _bucket_score(acceleration, ((10, 20), (0, 14), (-10, 7)))
    structure_score, structure_evidence = _structure_score(stock)
    score = min(100.0, market_score + sector_score + acceleration_score + structure_score)
    state = _state(stock_vs_spy_63d, stock_vs_sector_63d, acceleration, structure_score)
    confidence = "high" if stock_vs_sector_63d is not None else "medium"
    resolved_date = stock.index[-1].date().isoformat()
    return RelativeTrendStrengthSnapshot(
        ticker=str(ticker).strip().upper(),
        as_of_date=resolved_date,
        sector=sector,
        sector_etf=sector_etf,
        rts_score=round(score, 1),
        rts_state=state,
        confidence=confidence,
        close_price=round(float(stock.iloc[-1]), 4),
        stock_vs_spy_21d_pct=_round(stock_vs_spy_21d),
        stock_vs_spy_63d_pct=_round(stock_vs_spy_63d),
        stock_vs_sector_63d_pct=_round(stock_vs_sector_63d),
        alpha_acceleration_pct=_round(acceleration),
        market_relative_score=market_score,
        sector_relative_score=sector_score,
        acceleration_score=acceleration_score,
        structure_score=structure_score,
        evidence={
            "formula_version": "rts-v1",
            "market_relative": "63d stock return minus SPY return",
            "sector_relative": "63d stock return minus mapped sector ETF return",
            **structure_evidence,
        },
    )


def build_leadership_health(history: list[dict[str, Any]]) -> dict[str, object] | None:
    """Summarize persisted RTS history without changing entry or position scores."""
    ordered = sorted(
        (row for row in history if row.get("as_of_date") is not None),
        key=lambda row: str(row.get("as_of_date")),
    )
    if not ordered:
        return None
    latest = ordered[-1]
    previous = ordered[-2] if len(ordered) > 1 else None
    weak_states = {"contracting", "lagging"}
    deterioration_sessions = 0
    for row in reversed(ordered):
        if str(row.get("rts_state") or "") not in weak_states:
            break
        deterioration_sessions += 1
    latest_score = _number(latest.get("rts_score"))
    comparison = ordered[-6] if len(ordered) >= 6 else ordered[0]
    comparison_score = _number(comparison.get("rts_score"))
    score_change_5d = latest_score - comparison_score if latest_score is not None and comparison_score is not None else None
    recent = ordered[-10:]
    close_values = [_number(row.get("close_price")) for row in recent]
    score_values = [_number(row.get("rts_score")) for row in recent]
    valid_closes = [value for value in close_values if value is not None]
    valid_scores = [value for value in score_values if value is not None]
    price_high_rts_divergence = bool(
        latest_score is not None
        and _number(latest.get("close_price")) is not None
        and valid_closes
        and valid_scores
        and _number(latest.get("close_price")) >= max(valid_closes)
        and latest_score <= max(valid_scores) - 10
    )
    current_state = str(latest.get("rts_state") or "unavailable")
    previous_state = str(previous.get("rts_state") or "") if previous else None
    persistent_warning = deterioration_sessions >= 3
    return {
        "state": current_state,
        "status": "warning" if persistent_warning or price_high_rts_divergence else "healthy" if current_state == "expanding" else "neutral",
        "transition": f"{previous_state}_to_{current_state}" if previous_state and previous_state != current_state else None,
        "deterioration_sessions": deterioration_sessions,
        "persistent_warning": persistent_warning,
        "price_high_rts_divergence": price_high_rts_divergence,
        "score_change_5d": round(score_change_5d, 2) if score_change_5d is not None else None,
        "history": [
            {
                "as_of_date": str(row.get("as_of_date")),
                "score": _number(row.get("rts_score")),
                "state": str(row.get("rts_state") or ""),
                "close_price": _number(row.get("close_price")),
            }
            for row in ordered[-20:]
        ],
        "note": "Leadership context only; it does not alter Strike Zone or Position Action scores.",
    }


def _close_series(frame: pd.DataFrame | None, as_of_date: dt.date | None) -> pd.Series | None:
    if frame is None or frame.empty or "Close" not in frame.columns:
        return None
    close = pd.to_numeric(frame["Close"], errors="coerce").dropna().sort_index()
    if not isinstance(close.index, pd.DatetimeIndex):
        close.index = pd.to_datetime(close.index)
    if as_of_date is not None:
        close = close.loc[close.index.date <= as_of_date]
    return close if len(close) >= 64 else None


def _relative_return(frame: pd.DataFrame | None, left: str, right: str, days: int) -> float | None:
    if frame is None or len(frame) <= days:
        return None
    start = frame.iloc[-days - 1]
    end = frame.iloc[-1]
    if start[left] <= 0 or start[right] <= 0:
        return None
    return (((end[left] / start[left]) - 1) - ((end[right] / start[right]) - 1)) * 100.0


def _bucket_score(value: float | None, buckets: tuple[tuple[float, int], ...]) -> float:
    if value is None:
        return 0.0
    for threshold, score in buckets:
        if value >= threshold:
            return float(score)
    return 0.0


def _structure_score(close: pd.Series) -> tuple[float, dict[str, object]]:
    ema21 = close.ewm(span=21, adjust=False).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    current = float(close.iloc[-1])
    high_63 = float(close.tail(63).max())
    sma50_slope = float(close.rolling(50).mean().iloc[-1] - close.rolling(50).mean().iloc[-6])
    aligned = bool(current >= ema21 >= sma50 and sma50_slope > 0)
    near_high = bool(current >= high_63 * 0.9)
    score = (20.0 if aligned else 0.0) + (10.0 if near_high else 0.0)
    return score, {"above_ema21_sma50": aligned, "near_63d_high": near_high, "sma50_rising": sma50_slope > 0}


def _state(market: float | None, sector: float | None, acceleration: float | None, structure: float) -> str:
    if market is None:
        return "unavailable"
    if market >= 0 and (sector is None or sector >= 0) and (acceleration or 0) >= 0 and structure >= 20:
        return "expanding"
    if market >= 0 and (sector is None or sector >= 0):
        return "stable"
    if market >= 0:
        return "contracting"
    return "lagging"


def _round(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


def _number(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
