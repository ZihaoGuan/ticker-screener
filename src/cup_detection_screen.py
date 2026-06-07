from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .base_detection_screen import (
    BASE_DETECTION_DEPTH_RATIO,
    BASE_DETECTION_HISTORY_DAYS,
    BASE_DETECTION_LENGTH_BARS,
    _build_price_frame,
    _normalize_bars_frame,
    _is_pivot_high,
    _price_tolerance,
)
from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


CUP_DETECTION_MIN_DEPTH_RATIO = 0.08
CUP_DETECTION_MIN_LENGTH_BARS = 30


@dataclass(frozen=True)
class CupDetectionHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    base_start_date: str
    signal_age_bars: int
    cup_age_bars: int
    cup_weeks: int
    cup_high: float
    cup_low: float
    cup_midpoint: float
    cup_depth_pct: float
    current_price: float
    breakout_price: float
    shape_mode: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class CupDetectionScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[CupDetectionHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _compute_shape_mode(closes: list[float], midpoint: float) -> str | None:
    base_count = len(closes)
    if base_count < CUP_DETECTION_MIN_LENGTH_BARS:
        return None

    base_tier = base_count // 3
    base_fourth = base_count // 4

    thirds_ok = False
    if base_tier >= 2:
        first_third = closes[:base_tier]
        middle_third = closes[base_tier : base_tier * 2]
        if first_third and middle_third:
            first_above = sum(1 for value in first_third if value >= midpoint) / len(first_third)
            middle_below = sum(1 for value in middle_third if value <= midpoint) / len(middle_third)
            thirds_ok = first_above >= 0.30 and middle_below >= 0.90

    quarters_ok = False
    if base_fourth >= 2 and (base_fourth * 3) <= base_count:
        first_quarter = closes[:base_fourth]
        middle_half = closes[base_fourth : base_fourth * 3]
        if first_quarter and middle_half:
            first_above = sum(1 for value in first_quarter if value >= midpoint) / len(first_quarter)
            middle_below = sum(1 for value in middle_half if value <= midpoint) / len(middle_half)
            quarters_ok = first_above >= 0.30 and middle_below >= 0.90

    if thirds_ok:
        return "thirds"
    if quarters_ok:
        return "quarters"
    return None


def find_active_cup_detection_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> CupDetectionHit | None:
    normalized = _normalize_bars_frame(frame)
    if normalized.empty:
        return None

    highs_all = normalized["High"].astype(float).tolist()
    lows_all = normalized["Low"].astype(float).tolist()
    closes_all = normalized["Close"].astype(float).tolist()
    last_index = len(normalized) - 1
    best_candidate: dict[str, object] | None = None
    best_score = float("-inf")

    for start_index in range(9, last_index - CUP_DETECTION_MIN_LENGTH_BARS + 2):
        age_bars = last_index - start_index + 1
        if age_bars < CUP_DETECTION_MIN_LENGTH_BARS or age_bars > BASE_DETECTION_LENGTH_BARS:
            continue
        if not _is_pivot_high(highs_all, start_index, 9, 9):
            continue

        cup_high = float(highs_all[start_index])
        tolerance = _price_tolerance(cup_high)
        if max(highs_all[start_index:]) > cup_high + tolerance:
            continue

        prior_leg_start = max(0, start_index - 78)
        if cup_high < (min(lows_all[prior_leg_start : start_index + 1]) * 1.20) - tolerance:
            continue

        cup_low = float(min(lows_all[start_index:]))
        cup_depth_ratio = (cup_high - cup_low) / cup_high if cup_high > 0 else 0.0
        if cup_depth_ratio < CUP_DETECTION_MIN_DEPTH_RATIO or cup_depth_ratio > BASE_DETECTION_DEPTH_RATIO:
            continue

        midpoint = cup_low + ((cup_high - cup_low) * 0.5)
        current_high = float(highs_all[-1])
        if current_high < midpoint:
            continue

        closes = closes_all[start_index:]
        shape_mode = _compute_shape_mode(closes, midpoint)
        if shape_mode is None:
            continue

        score = (cup_depth_ratio * 1000.0) + age_bars
        if score <= best_score:
            continue
        best_score = score
        best_candidate = {
            "start_index": start_index,
            "cup_high": cup_high,
            "cup_low": cup_low,
            "midpoint": midpoint,
            "shape_mode": shape_mode,
            "age_bars": age_bars,
            "depth_pct": cup_depth_ratio * 100.0,
        }

    if best_candidate is None:
        return None

    reasons = [
        "cup pattern active now",
        f"cup age {max(int(round(int(best_candidate['age_bars']) / 5.0)), 1)} week(s), depth {float(best_candidate['depth_pct']):.1f}%",
        f"midpoint {float(best_candidate['midpoint']):.2f}, current high {float(highs_all[-1]):.2f}",
        f"shape check passed by {str(best_candidate['shape_mode'])}",
    ]
    dates = list(normalized.index)
    return CupDetectionHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=dates[int(best_candidate["start_index"])].date().isoformat(),
        base_start_date=dates[int(best_candidate["start_index"])].date().isoformat(),
        signal_age_bars=last_index - int(best_candidate["start_index"]),
        cup_age_bars=int(best_candidate["age_bars"]),
        cup_weeks=max(int(round(int(best_candidate["age_bars"]) / 5.0)), 1),
        cup_high=float(best_candidate["cup_high"]),
        cup_low=float(best_candidate["cup_low"]),
        cup_midpoint=float(best_candidate["midpoint"]),
        cup_depth_pct=float(best_candidate["depth_pct"]),
        current_price=float(closes_all[-1]),
        breakout_price=float(best_candidate["cup_high"]),
        shape_mode=str(best_candidate["shape_mode"]),
        reasons=reasons,
    )


def run_cup_detection_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> CupDetectionScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[CupDetectionHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        f"starting cup detection screen: total={total_tickers}, min_depth_pct={CUP_DETECTION_MIN_DEPTH_RATIO * 100:.0f}, max_depth_pct={BASE_DETECTION_DEPTH_RATIO * 100:.0f}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=BASE_DETECTION_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=BASE_DETECTION_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_active_cup_detection_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no active cup | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed active cup | depth={hit.cup_depth_pct:.1f}% weeks={hit.cup_weeks} mode={hit.shape_mode} passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished cup detection screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return CupDetectionScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
