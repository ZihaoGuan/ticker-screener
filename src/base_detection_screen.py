from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


BASE_DETECTION_DEPTH_RATIO = 0.50
BASE_DETECTION_FLAT_BASE_MAX_DEPTH_RATIO = 0.15
BASE_DETECTION_LENGTH_WEEKS = 65
BASE_DETECTION_LENGTH_BARS = BASE_DETECTION_LENGTH_WEEKS * 5
BASE_DETECTION_PIVOT_LENGTH = 9
BASE_DETECTION_HIGH_OFFSET = 25
BASE_DETECTION_HIGH_WINDOW = 25
BASE_DETECTION_COMPARISON_WINDOW = 50
BASE_DETECTION_LEG_WINDOW = 103
BASE_DETECTION_HISTORY_DAYS = 520


@dataclass(frozen=True)
class BaseDetectionHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    base_start_date: str
    signal_age_bars: int
    base_age_bars: int
    base_weeks: int
    base_type: str
    base_high: float
    base_low: float
    base_depth_pct: float
    current_price: float
    breakout_price: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ActiveBaseState:
    signal_bar: int
    base_start_bar: int
    base_high: float
    base_low: float
    signal_age_bars: int
    base_age_bars: int


@dataclass(frozen=True)
class BaseDetectionScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[BaseDetectionHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_bars_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"]
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


def _build_price_frame(financials) -> pd.DataFrame:
    rows = financials._get_clean_price_data()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            "Date": pd.to_datetime([row.get("formatted_date") for row in rows]),
            "Open": [row.get("open") for row in rows],
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _is_pivot_high(highs: list[float], center: int, left: int, right: int) -> bool:
    if center < left or center + right >= len(highs):
        return False
    candidate = highs[center]
    window = highs[center - left : center + right + 1]
    return candidate == max(window)


def _price_tolerance(value: float) -> float:
    return max(abs(value) * 1e-6, 1e-8)


def _find_active_base_state(frame: pd.DataFrame) -> tuple[pd.DataFrame, ActiveBaseState | None]:
    bars = _normalize_bars_frame(frame)
    minimum_bars = max(BASE_DETECTION_LEG_WINDOW + BASE_DETECTION_PIVOT_LENGTH + 10, 140)
    if bars.empty or len(bars) < minimum_bars:
        return bars, None

    highs = [float(value) for value in bars["High"]]
    lows = [float(value) for value in bars["Low"]]
    active_flags = [False] * len(bars)
    recent_pivot_highs: list[float] = []

    active = False
    signal_bar: int | None = None
    base_start_bar: int | None = None
    base_high: float | None = None
    base_low: float | None = None

    for bar_index in range(len(bars)):
        pivot_center = bar_index - BASE_DETECTION_PIVOT_LENGTH
        if _is_pivot_high(highs, pivot_center, BASE_DETECTION_PIVOT_LENGTH, BASE_DETECTION_PIVOT_LENGTH):
            recent_pivot_highs.insert(0, highs[pivot_center])
            if len(recent_pivot_highs) > 12:
                recent_pivot_highs = recent_pivot_highs[:12]

        base_cond = False
        candidate_high = 0.0
        lowest_base_low = 0.0
        if bar_index >= BASE_DETECTION_LEG_WINDOW - 1 and bar_index >= BASE_DETECTION_HIGH_OFFSET and len(recent_pivot_highs) >= 3:
            candidate_index = bar_index - BASE_DETECTION_HIGH_OFFSET
            candidate_high = highs[candidate_index]
            tolerance = _price_tolerance(candidate_high)
            highest_of_base = max(highs[bar_index - BASE_DETECTION_HIGH_WINDOW + 1 : bar_index + 1])
            highest_of_comparison = max(highs[bar_index - BASE_DETECTION_COMPARISON_WINDOW + 1 : bar_index + 1])
            lowest_base_low = min(lows[bar_index - BASE_DETECTION_HIGH_WINDOW + 1 : bar_index + 1])
            lowest_point_leg = min(lows[bar_index - BASE_DETECTION_LEG_WINDOW + 1 : bar_index + 1])
            bool_high_base = any(abs(candidate_high - value) <= tolerance for value in recent_pivot_highs[:3])
            bool_higher_pivot = candidate_high >= highest_of_comparison - tolerance
            leg_up_cond = candidate_high >= (lowest_point_leg * 1.20) - tolerance
            first_base_depth = candidate_high * (1.0 - BASE_DETECTION_DEPTH_RATIO) <= lowest_base_low + tolerance
            no_candle_above = highest_of_base <= candidate_high + tolerance
            no_base_in_base = not active_flags[candidate_index]
            base_cond = all((bool_high_base, bool_higher_pivot, leg_up_cond, first_base_depth, no_candle_above, no_base_in_base))

        previous_active = active
        active = bool(base_cond or previous_active)

        if base_cond and not previous_active:
            signal_bar = bar_index
            base_start_bar = bar_index - BASE_DETECTION_HIGH_OFFSET
            base_high = candidate_high
            base_low = lowest_base_low

        if active and base_low is not None:
            base_low = min(base_low, lows[bar_index])

        if active and base_high is not None and base_start_bar is not None:
            base_age_bars = bar_index - base_start_bar + 1
            tolerance = _price_tolerance(base_high)
            if lows[bar_index] < (base_high * (1.0 - BASE_DETECTION_DEPTH_RATIO)) - tolerance or base_age_bars > BASE_DETECTION_LENGTH_BARS:
                active = False
            elif previous_active and highs[bar_index] > base_high + tolerance:
                active = False

        if not active and not previous_active and not base_cond:
            signal_bar = signal_bar

        active_flags[bar_index] = active

    if not active or signal_bar is None or base_start_bar is None or base_high is None or base_low is None:
        return bars, None

    signal_age_bars = len(bars) - 1 - signal_bar
    base_age_bars = len(bars) - base_start_bar
    return bars, ActiveBaseState(
        signal_bar=signal_bar,
        base_start_bar=base_start_bar,
        base_high=float(base_high),
        base_low=float(base_low),
        signal_age_bars=signal_age_bars,
        base_age_bars=base_age_bars,
    )


def find_active_base_detection_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> BaseDetectionHit | None:
    bars, state = _find_active_base_state(frame)
    if state is None:
        return None

    closes = [float(value) for value in bars["Close"]]
    dates = list(bars.index)
    base_depth_ratio = (state.base_high - state.base_low) / state.base_high if state.base_high > 0 else 0.0
    base_depth_pct = max(base_depth_ratio * 100.0, 0.0)
    base_type = "Flat Base" if base_depth_ratio <= BASE_DETECTION_FLAT_BASE_MAX_DEPTH_RATIO else "Consolidation Base"
    base_weeks = max(int(round(state.base_age_bars / 5.0)), 1)

    reasons = [
        f"{base_type} active now",
        f"base age {base_weeks} week(s), depth {base_depth_pct:.1f}%",
        f"base high {state.base_high:.2f}, base low {state.base_low:.2f}",
        f"breakout price {state.base_high:.2f}",
    ]

    return BaseDetectionHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=dates[state.signal_bar].date().isoformat(),
        base_start_date=dates[state.base_start_bar].date().isoformat(),
        signal_age_bars=state.signal_age_bars,
        base_age_bars=state.base_age_bars,
        base_weeks=base_weeks,
        base_type=base_type,
        base_high=float(state.base_high),
        base_low=float(state.base_low),
        base_depth_pct=float(base_depth_pct),
        current_price=float(closes[-1]),
        breakout_price=float(state.base_high),
        reasons=reasons,
    )


def run_base_detection_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> BaseDetectionScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[BaseDetectionHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        f"starting base detection screen: total={total_tickers}, pivot={BASE_DETECTION_PIVOT_LENGTH}, max_depth_pct={BASE_DETECTION_DEPTH_RATIO * 100:.0f}, max_length_weeks={BASE_DETECTION_LENGTH_WEEKS}"
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
                    hit = find_active_base_detection_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no active base | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed {hit.base_type.lower()} | depth={hit.base_depth_pct:.1f}% weeks={hit.base_weeks} passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished base detection screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return BaseDetectionScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
