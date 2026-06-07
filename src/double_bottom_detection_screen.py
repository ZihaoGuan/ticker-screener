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


DOUBLE_BOTTOM_MIN_DEPTH_RATIO = 0.10
DOUBLE_BOTTOM_HISTORY_BARS = 520
PIVOT_LENGTH = 9


@dataclass(frozen=True)
class DoubleBottomDetectionHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    base_start_date: str
    signal_age_bars: int
    pattern_age_bars: int
    pattern_weeks: int
    top_price: float
    middle_high_price: float
    first_bottom_price: float
    second_bottom_price: float
    breakout_price: float
    current_price: float
    depth_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DoubleBottomDetectionScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[DoubleBottomDetectionHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _is_pivot_low(lows: list[float], center: int, left: int, right: int) -> bool:
    if center < left or center + right >= len(lows):
        return False
    candidate = lows[center]
    window = lows[center - left : center + right + 1]
    return candidate == min(window)


def find_active_double_bottom_detection_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> DoubleBottomDetectionHit | None:
    normalized = _normalize_bars_frame(frame)
    if normalized.empty or len(normalized) < 120:
        return None

    highs_all = normalized["High"].astype(float).tolist()
    lows_all = normalized["Low"].astype(float).tolist()
    closes_all = normalized["Close"].astype(float).tolist()
    dates = list(normalized.index)
    last_index = len(normalized) - 1

    pivot_highs: list[tuple[int, float]] = []
    pivot_lows: list[tuple[int, float]] = []
    for index in range(PIVOT_LENGTH, len(normalized) - PIVOT_LENGTH):
        if _is_pivot_high(highs_all, index, PIVOT_LENGTH, PIVOT_LENGTH):
            pivot_highs.append((index, float(highs_all[index])))
        if _is_pivot_low(lows_all, index, PIVOT_LENGTH, PIVOT_LENGTH):
            pivot_lows.append((index, float(lows_all[index])))

    best_candidate: dict[str, object] | None = None
    best_score = float("-inf")

    for high_idx_a in range(len(pivot_highs) - 1):
        a_index, a_price = pivot_highs[high_idx_a]
        tolerance = _price_tolerance(a_price)
        prior_leg_start = max(0, a_index - 78)
        if a_price < (min(lows_all[prior_leg_start : a_index + 1]) * 1.20) - tolerance:
            continue

        for low_idx_b in range(len(pivot_lows) - 1):
            b_index, b_price = pivot_lows[low_idx_b]
            if b_index <= a_index:
                continue
            c_candidates = [(idx, price) for idx, price in pivot_highs if idx > b_index]
            if not c_candidates:
                continue
            c_index, c_price = c_candidates[0]
            d_candidates = [(idx, price) for idx, price in pivot_lows if idx > c_index]
            if not d_candidates:
                continue
            d_index, d_price = d_candidates[0]

            if not (a_index < b_index < c_index < d_index):
                continue
            pattern_age_bars = last_index - a_index + 1
            if pattern_age_bars > BASE_DETECTION_LENGTH_BARS:
                continue

            if a_price <= c_price:
                continue
            if b_price * 0.97 <= d_price:
                continue
            if b_price >= c_price:
                continue
            if d_price < (1.0 - BASE_DETECTION_DEPTH_RATIO) * a_price - tolerance:
                continue
            if d_price > (1.0 - DOUBLE_BOTTOM_MIN_DEPTH_RATIO) * a_price + tolerance:
                continue

            if ((a_price - d_price) * 0.6 + d_price) > c_price + tolerance:
                continue
            if ((a_price - d_price) * 0.95 + d_price) < c_price - tolerance:
                continue

            first_leg = a_price - b_price
            second_leg = c_price - d_price
            if second_leg <= 0:
                continue
            if (first_leg / second_leg) < 0.70:
                continue
            if ((first_leg / 2.0) + b_price) > c_price + tolerance:
                continue

            left_side = c_index - a_index
            right_side = last_index - c_index
            if left_side <= 0 or right_side <= 0:
                continue
            if left_side * 2 < right_side or left_side > right_side * 2:
                continue

            ab_len = b_index - a_index
            bc_len = c_index - b_index
            cd_len = d_index - c_index
            if ab_len > 2 * bc_len or bc_len > 2 * ab_len:
                continue
            if cd_len > 2 * bc_len or cd_len < 0.5 * bc_len:
                continue

            if max(highs_all[c_index:]) > c_price + tolerance:
                continue
            if min(lows_all[d_index:]) < d_price - tolerance:
                continue
            if highs_all[-1] > c_price + tolerance:
                continue

            depth_pct = ((a_price - d_price) / a_price) * 100.0 if a_price > 0 else 0.0
            score = depth_pct * 100.0 - abs((b_price - d_price) / max(a_price, 1e-9)) * 10.0
            if score <= best_score:
                continue
            best_score = score
            best_candidate = {
                "a_index": a_index,
                "a_price": a_price,
                "b_price": b_price,
                "c_index": c_index,
                "c_price": c_price,
                "d_price": d_price,
                "pattern_age_bars": pattern_age_bars,
                "depth_pct": depth_pct,
            }

    if best_candidate is None:
        return None

    reasons = [
        "double bottom active now",
        f"pattern age {max(int(round(int(best_candidate['pattern_age_bars']) / 5.0)), 1)} week(s), depth {float(best_candidate['depth_pct']):.1f}%",
        f"middle high {float(best_candidate['c_price']):.2f}, second bottom {float(best_candidate['d_price']):.2f}",
        f"breakout price {float(best_candidate['c_price']):.2f}",
    ]

    a_index = int(best_candidate["a_index"])
    return DoubleBottomDetectionHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=dates[a_index].date().isoformat(),
        base_start_date=dates[a_index].date().isoformat(),
        signal_age_bars=last_index - a_index,
        pattern_age_bars=int(best_candidate["pattern_age_bars"]),
        pattern_weeks=max(int(round(int(best_candidate["pattern_age_bars"]) / 5.0)), 1),
        top_price=float(best_candidate["a_price"]),
        middle_high_price=float(best_candidate["c_price"]),
        first_bottom_price=float(best_candidate["b_price"]),
        second_bottom_price=float(best_candidate["d_price"]),
        breakout_price=float(best_candidate["c_price"]),
        current_price=float(closes_all[-1]),
        depth_pct=float(best_candidate["depth_pct"]),
        reasons=reasons,
    )


def run_double_bottom_detection_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> DoubleBottomDetectionScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[DoubleBottomDetectionHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        f"starting double bottom detection screen: total={total_tickers}, min_depth_pct={DOUBLE_BOTTOM_MIN_DEPTH_RATIO * 100:.0f}, max_depth_pct={BASE_DETECTION_DEPTH_RATIO * 100:.0f}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=DOUBLE_BOTTOM_HISTORY_BARS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=DOUBLE_BOTTOM_HISTORY_BARS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_active_double_bottom_detection_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no active double bottom | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed active double bottom | depth={hit.depth_pct:.1f}% weeks={hit.pattern_weeks} passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished double bottom detection screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return DoubleBottomDetectionScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
