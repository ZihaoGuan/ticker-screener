from __future__ import annotations

import datetime as dt
import statistics
from typing import Any

import pandas as pd


HORIZONS = (5, 10, 20, 63)


def build_rts_validation_report(
    snapshots: list[dict[str, Any]],
    frames: dict[str, pd.DataFrame],
    *,
    benchmark_ticker: str = "SPY",
) -> dict[str, Any]:
    benchmark = _close_frame(frames.get(benchmark_ticker.upper()))
    observations: list[dict[str, Any]] = []
    skipped = 0
    for snapshot in snapshots:
        ticker = str(snapshot.get("ticker") or "").upper()
        stock = _close_frame(frames.get(ticker))
        as_of_date = _date(snapshot.get("as_of_date"))
        if stock is None or benchmark is None or as_of_date is None:
            skipped += 1
            continue
        stock_position = _position_on_or_before(stock, as_of_date)
        benchmark_position = _position_on_or_before(benchmark, as_of_date)
        if stock_position is None or benchmark_position is None:
            skipped += 1
            continue
        observation: dict[str, Any] = {
            "ticker": ticker,
            "as_of_date": as_of_date.isoformat(),
            "score": _number(snapshot.get("rts_score")),
            "score_bucket": _score_bucket(_number(snapshot.get("rts_score"))),
            "state": str(snapshot.get("rts_state") or "unavailable"),
            "sector": str(snapshot.get("sector") or "Unknown"),
            "market_regime": _market_regime(benchmark, benchmark_position),
            "returns": {},
        }
        completed = False
        for horizon in HORIZONS:
            if stock_position + horizon >= len(stock) or benchmark_position + horizon >= len(benchmark):
                continue
            entry = float(stock.iloc[stock_position]["Close"])
            exit_price = float(stock.iloc[stock_position + horizon]["Close"])
            benchmark_entry = float(benchmark.iloc[benchmark_position]["Close"])
            benchmark_exit = float(benchmark.iloc[benchmark_position + horizon]["Close"])
            if entry <= 0 or benchmark_entry <= 0:
                continue
            stock_return = ((exit_price / entry) - 1.0) * 100.0
            benchmark_return = ((benchmark_exit / benchmark_entry) - 1.0) * 100.0
            forward_window = stock.iloc[stock_position + 1 : stock_position + horizon + 1]
            low_column = "Low" if "Low" in forward_window else "Close"
            max_drawdown = ((float(forward_window[low_column].min()) / entry) - 1.0) * 100.0
            observation["returns"][str(horizon)] = {
                "return_pct": round(stock_return, 4),
                "spy_return_pct": round(benchmark_return, 4),
                "excess_return_pct": round(stock_return - benchmark_return, 4),
                "max_drawdown_pct": round(max_drawdown, 4),
            }
            completed = True
        if completed:
            observations.append(observation)
        else:
            skipped += 1

    return {
        "formula_version": "rts-v1",
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "snapshot_count": len(snapshots),
        "observation_count": len(observations),
        "skipped_count": skipped,
        "horizons": list(HORIZONS),
        "overall": _summarize(observations),
        "by_score_bucket": _group(observations, "score_bucket"),
        "by_state": _group(observations, "state"),
        "by_sector": _group(observations, "sector", minimum_count=5),
        "by_market_regime": _group(observations, "market_regime"),
        "limitations": [
            "Results use persisted daily RTS snapshots only; no synthetic history is created.",
            "Stage 2A/2B cohorting is not included because stage was not persisted with RTS v1 snapshots.",
            "Overlapping forward windows are descriptive and are not independent trades.",
            "The report does not modify Strike Zone or Position Action scoring.",
        ],
    }


def _group(rows: list[dict[str, Any]], key: str, *, minimum_count: int = 1) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get(key) or "Unknown"), []).append(row)
    return {label: _summarize(items) for label, items in sorted(buckets.items()) if len(items) >= minimum_count}


def _summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {"count": len(rows), "horizons": {}}
    for horizon in HORIZONS:
        metrics = [row["returns"].get(str(horizon)) for row in rows]
        metrics = [item for item in metrics if isinstance(item, dict)]
        returns = [float(item["return_pct"]) for item in metrics]
        excess = [float(item["excess_return_pct"]) for item in metrics]
        drawdowns = [float(item["max_drawdown_pct"]) for item in metrics]
        result["horizons"][str(horizon)] = {
            "count": len(metrics),
            "average_return_pct": _mean(returns),
            "median_return_pct": _median(returns),
            "win_rate_pct": _rate(value > 0 for value in returns),
            "average_excess_return_pct": _mean(excess),
            "excess_win_rate_pct": _rate(value > 0 for value in excess),
            "average_max_drawdown_pct": _mean(drawdowns),
        }
    return result


def _close_frame(frame: pd.DataFrame | None) -> pd.DataFrame | None:
    if frame is None or frame.empty or "Close" not in frame:
        return None
    result = frame.copy().sort_index()
    if not isinstance(result.index, pd.DatetimeIndex):
        result.index = pd.to_datetime(result.index)
    result["Close"] = pd.to_numeric(result["Close"], errors="coerce")
    if "Low" in result:
        result["Low"] = pd.to_numeric(result["Low"], errors="coerce")
    return result.dropna(subset=["Close"])


def _position_on_or_before(frame: pd.DataFrame, value: dt.date) -> int | None:
    positions = frame.index.date <= value
    indexes = [index for index, matched in enumerate(positions) if matched]
    return indexes[-1] if indexes else None


def _market_regime(benchmark: pd.DataFrame, position: int) -> str:
    if position < 199:
        return "insufficient_history"
    close = float(benchmark.iloc[position]["Close"])
    sma200 = float(benchmark.iloc[position - 199 : position + 1]["Close"].mean())
    return "above_spy_sma200" if close >= sma200 else "below_spy_sma200"


def _score_bucket(score: float | None) -> str:
    if score is None:
        return "unavailable"
    if score >= 80:
        return "80-100"
    if score >= 60:
        return "60-79"
    if score >= 40:
        return "40-59"
    return "0-39"


def _date(value: object) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _number(value: object) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _mean(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 3) if values else None


def _median(values: list[float]) -> float | None:
    return round(statistics.median(values), 3) if values else None


def _rate(values: Any) -> float | None:
    items = list(values)
    return round((sum(1 for value in items if value) / len(items)) * 100.0, 2) if items else None
