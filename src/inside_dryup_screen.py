from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


FAST_EMA_LENGTH = 21
MID_EMA_LENGTH = 55
SLOW_EMA_LENGTH = 144
VOLUME_MA_LENGTH = 50
MIN_PULLBACK_BARS = 3
MAX_PULLBACK_BARS = 10
EMA_LANE_TOLERANCE_PCT = 0.025
LATEST_CLOSE_TO_EMA21_TOLERANCE_PCT = 0.015
MAX_PULLBACK_VOLUME_RATIO = 0.80
MAX_LATEST_VOLUME_RATIO = 0.70
MAX_PULLBACK_DEPTH_PCT = 0.12
HISTORY_DAYS = 260


@dataclass(frozen=True)
class InsideDryupHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    inside_day_high: float
    inside_day_low: float
    trigger_price: float
    stop_price: float
    ema21: float
    ema55: float
    ema144: float
    volume_ma_50: float
    latest_volume: float
    latest_volume_ratio: float
    avg_pullback_volume: float
    avg_pullback_volume_ratio: float
    pullback_bars: int
    pullback_high: float
    pullback_low: float
    pullback_depth_pct: float
    touch_ema21: bool
    touch_ema55: bool
    inside_day: bool
    watch_ready: bool
    trigger_ready: bool
    quality_score: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class InsideDryupScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[InsideDryupHit]

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


def _score_candidate(
    *,
    avg_pullback_volume_ratio: float,
    latest_volume_ratio: float,
    pullback_bars: int,
    pullback_depth_pct: float,
    touch_ema21: bool,
    touch_ema55: bool,
) -> int:
    score = 40
    if avg_pullback_volume_ratio <= 0.65:
        score += 20
    elif avg_pullback_volume_ratio <= MAX_PULLBACK_VOLUME_RATIO:
        score += 12
    if latest_volume_ratio <= 0.55:
        score += 15
    elif latest_volume_ratio <= MAX_LATEST_VOLUME_RATIO:
        score += 8
    if MIN_PULLBACK_BARS <= pullback_bars <= 6:
        score += 10
    elif pullback_bars <= MAX_PULLBACK_BARS:
        score += 6
    if pullback_depth_pct <= 0.06:
        score += 10
    elif pullback_depth_pct <= MAX_PULLBACK_DEPTH_PCT:
        score += 6
    if touch_ema21:
        score += 5
    if touch_ema55:
        score += 5
    return max(0, min(100, score))


def find_recent_inside_dryup_hit(frame: pd.DataFrame, *, ticker: UniverseTicker) -> InsideDryupHit | None:
    normalized = _normalize_bars_frame(frame)
    minimum_bars = max(SLOW_EMA_LENGTH + 10, VOLUME_MA_LENGTH + MAX_PULLBACK_BARS + 2)
    if normalized.empty or len(normalized) < minimum_bars:
        return None

    normalized = normalized.copy()
    normalized["ema21"] = normalized["Close"].ewm(span=FAST_EMA_LENGTH, adjust=False).mean()
    normalized["ema55"] = normalized["Close"].ewm(span=MID_EMA_LENGTH, adjust=False).mean()
    normalized["ema144"] = normalized["Close"].ewm(span=SLOW_EMA_LENGTH, adjust=False).mean()
    normalized["volume_ma_50"] = normalized["Volume"].rolling(VOLUME_MA_LENGTH).mean()

    latest = normalized.iloc[-1]
    previous = normalized.iloc[-2]
    if not (
        float(latest["High"]) < float(previous["High"])
        and float(latest["Low"]) > float(previous["Low"])
    ):
        return None

    ema21 = float(latest["ema21"])
    ema55 = float(latest["ema55"])
    ema144 = float(latest["ema144"])
    if min(ema21, ema55, ema144) <= 0:
        return None

    current_price = float(latest["Close"])
    if not (ema21 > ema55 > ema144 and current_price > ema55):
        return None

    ema55_prev = float(normalized["ema55"].iloc[-11])
    if ema55 <= ema55_prev:
        return None

    volume_ma_50 = float(latest["volume_ma_50"])
    if volume_ma_50 <= 0:
        return None

    best_hit: InsideDryupHit | None = None
    best_score = -1

    for pullback_bars in range(MIN_PULLBACK_BARS, MAX_PULLBACK_BARS + 1):
        window = normalized.iloc[-pullback_bars:]
        if len(window) < pullback_bars:
            continue
        pullback_high = float(window["High"].max())
        pullback_low = float(window["Low"].min())
        avg_pullback_volume = float(window["Volume"].mean())
        latest_volume = float(latest["Volume"])
        avg_pullback_volume_ratio = avg_pullback_volume / volume_ma_50
        latest_volume_ratio = latest_volume / volume_ma_50
        if avg_pullback_volume_ratio > MAX_PULLBACK_VOLUME_RATIO or latest_volume_ratio > MAX_LATEST_VOLUME_RATIO:
            continue

        highest_before_window = normalized.iloc[: -pullback_bars]["High"]
        if highest_before_window.empty:
            continue
        recent_reference_high = float(highest_before_window.tail(20).max())
        if recent_reference_high <= 0 or pullback_high < recent_reference_high * 0.97:
            continue

        pullback_depth_pct = ((pullback_high - pullback_low) / pullback_high) if pullback_high > 0 else 0.0
        if pullback_depth_pct > MAX_PULLBACK_DEPTH_PCT:
            continue

        lane_pct_to_ema21 = ((window["Low"] - window["ema21"]).abs() / window["ema21"]).min()
        lane_pct_to_ema55 = ((window["Low"] - window["ema55"]).abs() / window["ema55"]).min()
        touch_ema21 = bool(pd.notna(lane_pct_to_ema21) and float(lane_pct_to_ema21) <= EMA_LANE_TOLERANCE_PCT)
        touch_ema55 = bool(pd.notna(lane_pct_to_ema55) and float(lane_pct_to_ema55) <= EMA_LANE_TOLERANCE_PCT)
        latest_close_near_ema21 = abs(current_price - ema21) / ema21 <= LATEST_CLOSE_TO_EMA21_TOLERANCE_PCT
        if not (touch_ema21 or touch_ema55):
            continue

        if pullback_low < (ema55 * 0.97) or pullback_low < ema144:
            continue
        if not latest_close_near_ema21 and not touch_ema55:
            continue

        quality_score = _score_candidate(
            avg_pullback_volume_ratio=avg_pullback_volume_ratio,
            latest_volume_ratio=latest_volume_ratio,
            pullback_bars=pullback_bars,
            pullback_depth_pct=pullback_depth_pct,
            touch_ema21=touch_ema21,
            touch_ema55=touch_ema55,
        )
        reasons = [
            f"inside day after {pullback_bars}-bar pullback",
            f"avg pullback volume {avg_pullback_volume_ratio:.2f}x of 50D avg",
            f"latest volume {latest_volume_ratio:.2f}x of 50D avg",
            f"pullback depth {pullback_depth_pct * 100.0:.1f}%",
            f"holding EMA stack {ema21:.2f} > {ema55:.2f} > {ema144:.2f}",
        ]
        if touch_ema21:
            reasons.append("pulled back into 21 EMA lane")
        if touch_ema55:
            reasons.append("pulled back into 55 EMA lane")
        if latest_close_near_ema21:
            reasons.append("latest close sitting near 21 EMA")

        hit = InsideDryupHit(
            ticker=ticker.symbol,
            sector=ticker.sector,
            industry=ticker.industry,
            exchange=ticker.exchange,
            signal_date=normalized.index[-1].date().isoformat(),
            current_price=current_price,
            inside_day_high=float(latest["High"]),
            inside_day_low=float(latest["Low"]),
            trigger_price=float(latest["High"]),
            stop_price=float(latest["Low"]),
            ema21=ema21,
            ema55=ema55,
            ema144=ema144,
            volume_ma_50=volume_ma_50,
            latest_volume=latest_volume,
            latest_volume_ratio=latest_volume_ratio,
            avg_pullback_volume=avg_pullback_volume,
            avg_pullback_volume_ratio=avg_pullback_volume_ratio,
            pullback_bars=pullback_bars,
            pullback_high=pullback_high,
            pullback_low=pullback_low,
            pullback_depth_pct=pullback_depth_pct,
            touch_ema21=touch_ema21,
            touch_ema55=touch_ema55,
            inside_day=True,
            watch_ready=True,
            trigger_ready=False,
            quality_score=quality_score,
            reasons=reasons,
        )
        if quality_score > best_score:
            best_hit = hit
            best_score = quality_score

    return best_hit


def run_inside_dryup_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> InsideDryupScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[InsideDryupHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting inside-day dry-up screen: "
        f"total={total_tickers}, "
        f"ema={FAST_EMA_LENGTH}/{MID_EMA_LENGTH}/{SLOW_EMA_LENGTH}, "
        f"pullback={MIN_PULLBACK_BARS}-{MAX_PULLBACK_BARS} bars"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_inside_dryup_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no inside dry-up setup | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"quality {hit.quality_score}, trigger {hit.trigger_price:.2f}, stop {hit.stop_price:.2f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            -item.quality_score,
            item.avg_pullback_volume_ratio,
            item.pullback_depth_pct,
            item.ticker,
        )
    )

    print(
        "finished inside-day dry-up screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return InsideDryupScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
