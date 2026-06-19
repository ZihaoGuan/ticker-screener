from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


HTF_SETUP_SMA_LONG_PERIOD = 200
HTF_SETUP_SMA_SHORT_PERIOD = 50
HTF_SETUP_VOLUME_SMA_PERIOD = 50
HTF_SETUP_ATR_PERIOD = 14
HTF_SETUP_SLOPE_LOOKBACK = 10
HTF_SETUP_HISTORY_DAYS = 260
HTF_SETUP_PATTERN_LOOKBACK = 60
HTF_SETUP_MAX_POLE_DAYS = 45
HTF_SETUP_MIN_POLE_GAIN_RATIO = 1.9
HTF_SETUP_MIN_FLAG_DAYS = 5
HTF_SETUP_MAX_FLAG_DAYS = 20
HTF_SETUP_MIN_FLAG_DRAWDOWN = 0.0
HTF_SETUP_MAX_FLAG_DRAWDOWN = 0.25
HTF_SETUP_MAX_ATR_RATIO = 0.08
HTF_SETUP_MIN_RUNUP_60_RATIO = 1.5
HTF_SETUP_MAX_DISTANCE_TO_PIVOT_PCT = 0.08
HTF_SETUP_BREAKOUT_BUFFER = 0.10
HTF_SETUP_MIN_UP_DAY_PCT = 0.55
HTF_SETUP_MIN_POLE_VOLUME_RATIO = 1.40
HTF_SETUP_MAX_FLAG_VOLUME_RATIO = 0.80


@dataclass(frozen=True)
class HighTightFlagSetupHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    high_price: float
    low_price: float
    sma_50: float
    sma_200: float
    sma_200_slope_10: float
    avg_volume_50: float
    avg_volume_50_slope_10: float
    atr_14: float
    atr_14_slope_10: float
    atr_ratio: float
    runup_60_ratio: float
    pole_low: float
    pole_high: float
    pole_gain_ratio: float
    pole_days: int
    pole_volume_ratio: float
    up_day_pct: float
    flag_days: int
    flag_high: float
    flag_low: float
    flag_drawdown_pct: float
    flag_volume_ratio: float
    pivot_price: float
    distance_to_pivot_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class HighTightFlagSetupScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[HighTightFlagSetupHit]

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


def _true_range(frame: pd.DataFrame) -> pd.Series:
    previous_close = frame["Close"].shift(1)
    return pd.concat(
        [
            frame["High"] - frame["Low"],
            (frame["High"] - previous_close).abs(),
            (frame["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def find_high_tight_flag_setup_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> HighTightFlagSetupHit | None:
    bars = _normalize_bars_frame(frame)
    min_history = max(
        HTF_SETUP_SMA_LONG_PERIOD + HTF_SETUP_SLOPE_LOOKBACK,
        HTF_SETUP_VOLUME_SMA_PERIOD + HTF_SETUP_SLOPE_LOOKBACK,
        HTF_SETUP_ATR_PERIOD + HTF_SETUP_SLOPE_LOOKBACK,
        HTF_SETUP_PATTERN_LOOKBACK,
    )
    if bars.empty or len(bars) < min_history:
        return None

    sma_50 = bars["Close"].rolling(HTF_SETUP_SMA_SHORT_PERIOD).mean()
    sma_200 = bars["Close"].rolling(HTF_SETUP_SMA_LONG_PERIOD).mean()
    avg_volume_50 = bars["Volume"].rolling(HTF_SETUP_VOLUME_SMA_PERIOD).mean()
    atr_14 = _true_range(bars).rolling(HTF_SETUP_ATR_PERIOD).mean()

    latest = bars.iloc[-1]
    latest_close = float(latest["Close"])
    latest_sma_50 = sma_50.iloc[-1]
    latest_sma_200 = sma_200.iloc[-1]
    latest_avg_volume_50 = avg_volume_50.iloc[-1]
    latest_atr_14 = atr_14.iloc[-1]
    close_60 = bars["Close"].iloc[-(HTF_SETUP_PATTERN_LOOKBACK + 1)]

    if any(
        pd.isna(value)
        for value in (
            latest_sma_50,
            latest_sma_200,
            latest_avg_volume_50,
            latest_atr_14,
            close_60,
            sma_200.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)],
            avg_volume_50.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)],
            atr_14.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)],
        )
    ):
        return None

    sma_200_slope_10 = float((latest_sma_200 - sma_200.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)]) / HTF_SETUP_SLOPE_LOOKBACK)
    avg_volume_50_slope_10 = float(
        (latest_avg_volume_50 - avg_volume_50.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)]) / HTF_SETUP_SLOPE_LOOKBACK
    )
    atr_14_slope_10 = float((latest_atr_14 - atr_14.iloc[-(HTF_SETUP_SLOPE_LOOKBACK + 1)]) / HTF_SETUP_SLOPE_LOOKBACK)
    atr_ratio = float(latest_atr_14 / latest_close) if latest_close else 0.0
    runup_60_ratio = float(latest_close / float(close_60)) if float(close_60) else 0.0

    if not (
        sma_200_slope_10 > 0.0
        and avg_volume_50_slope_10 < 0.0
        and latest_close >= float(latest_sma_50)
        and latest_close >= float(latest_sma_200)
        and float(latest_sma_50) >= float(latest_sma_200)
        and runup_60_ratio > HTF_SETUP_MIN_RUNUP_60_RATIO
        and atr_ratio < HTF_SETUP_MAX_ATR_RATIO
        and atr_14_slope_10 < 0.0
    ):
        return None

    last_index = len(bars) - 1
    setup_window_start = max(0, len(bars) - HTF_SETUP_PATTERN_LOOKBACK)
    best_candidate: HighTightFlagSetupHit | None = None
    best_score: tuple[float, float, float] | None = None

    latest_pole_end = last_index - (HTF_SETUP_MIN_FLAG_DAYS - 1)
    earliest_pole_end = max(last_index - (HTF_SETUP_MAX_FLAG_DAYS - 1), 0)
    for pole_end_idx in range(latest_pole_end, earliest_pole_end - 1, -1):
        flag_days = last_index - pole_end_idx + 1
        if flag_days < HTF_SETUP_MIN_FLAG_DAYS or flag_days > HTF_SETUP_MAX_FLAG_DAYS:
            continue

        pole_window_start = max(setup_window_start, pole_end_idx - HTF_SETUP_MAX_POLE_DAYS)
        pole_slice = bars.iloc[pole_window_start : pole_end_idx + 1]
        if len(pole_slice) < 2:
            continue

        pole_bottom_label = pole_slice["Low"].idxmin()
        pole_start_idx = bars.index.get_loc(pole_bottom_label)
        if not isinstance(pole_start_idx, int) or pole_start_idx >= pole_end_idx:
            continue

        pole_days = pole_end_idx - pole_start_idx
        if pole_days <= 0 or pole_days > HTF_SETUP_MAX_POLE_DAYS:
            continue

        pole_low = float(bars["Low"].iloc[pole_start_idx])
        pole_high = float(bars["High"].iloc[pole_end_idx])
        if pole_low <= 0.0:
            continue
        pole_gain_ratio = pole_high / pole_low
        if pole_gain_ratio < HTF_SETUP_MIN_POLE_GAIN_RATIO:
            continue

        pole_closes = bars["Close"].iloc[pole_start_idx : pole_end_idx + 1]
        up_days = sum(1 for index in range(1, len(pole_closes)) if pole_closes.iloc[index] > pole_closes.iloc[index - 1])
        up_day_pct = up_days / max(len(pole_closes) - 1, 1)
        if up_day_pct < HTF_SETUP_MIN_UP_DAY_PCT:
            continue

        pre_pole_volume = bars["Volume"].iloc[max(pole_start_idx - 20, 0) : pole_start_idx]
        pole_volume = bars["Volume"].iloc[pole_start_idx : pole_end_idx + 1]
        flag_volume = bars["Volume"].iloc[pole_end_idx : last_index + 1]
        pole_volume_avg = float(pole_volume.mean()) if len(pole_volume) else 0.0
        pre_pole_volume_avg = float(pre_pole_volume.mean()) if len(pre_pole_volume) else 0.0
        flag_volume_avg = float(flag_volume.mean()) if len(flag_volume) else 0.0
        pole_volume_ratio = pole_volume_avg / pre_pole_volume_avg if pre_pole_volume_avg > 0.0 else 0.0
        flag_volume_ratio = flag_volume_avg / pole_volume_avg if pole_volume_avg > 0.0 else 0.0
        if pole_volume_ratio < HTF_SETUP_MIN_POLE_VOLUME_RATIO:
            continue
        if flag_volume_ratio > HTF_SETUP_MAX_FLAG_VOLUME_RATIO:
            continue

        flag_closes = bars["Close"].iloc[pole_end_idx : last_index + 1]
        flag_lows = bars["Low"].iloc[pole_end_idx : last_index + 1]
        flag_highs = bars["High"].iloc[pole_end_idx : last_index + 1]
        flag_low = float(flag_closes.min())
        flag_high = float(flag_highs.max())
        flag_drawdown_pct = (pole_high - flag_low) / pole_high if pole_high > 0.0 else 0.0
        if flag_drawdown_pct < HTF_SETUP_MIN_FLAG_DRAWDOWN or flag_drawdown_pct > HTF_SETUP_MAX_FLAG_DRAWDOWN:
            continue

        pole_height = pole_high - pole_low
        if pole_height <= 0.0:
            continue
        if any((float(close) - pole_low) < 0.80 * pole_height for close in flag_closes):
            continue

        pivot_price = flag_high + HTF_SETUP_BREAKOUT_BUFFER
        if latest_close >= pivot_price:
            continue
        distance_to_pivot_pct = (pivot_price - latest_close) / pivot_price if pivot_price > 0.0 else 0.0
        if distance_to_pivot_pct > HTF_SETUP_MAX_DISTANCE_TO_PIVOT_PCT:
            continue

        reasons = [
            f"pole advanced {(pole_gain_ratio - 1.0) * 100.0:.1f}% in {pole_days} bars with {up_day_pct * 100.0:.1f}% up days",
            f"flag held {(1.0 - flag_drawdown_pct) * 100.0:.1f}% of the pole across {flag_days} bars",
            f"setup is {distance_to_pivot_pct * 100.0:.1f}% below pivot {pivot_price:.2f}",
            f"200 SMA slope {sma_200_slope_10:.3f}, ATR slope {atr_14_slope_10:.3f}, 50-day volume slope {avg_volume_50_slope_10:,.0f}",
            f"pole volume {pole_volume_ratio:.2f}x pre-pole and flag volume {flag_volume_ratio:.2f}x pole volume",
        ]

        candidate = HighTightFlagSetupHit(
            ticker=ticker.symbol,
            sector=ticker.sector,
            industry=ticker.industry,
            exchange=ticker.exchange,
            signal_date=bars.index[-1].date().isoformat(),
            current_price=latest_close,
            high_price=float(latest["High"]),
            low_price=float(latest["Low"]),
            sma_50=float(latest_sma_50),
            sma_200=float(latest_sma_200),
            sma_200_slope_10=sma_200_slope_10,
            avg_volume_50=float(latest_avg_volume_50),
            avg_volume_50_slope_10=avg_volume_50_slope_10,
            atr_14=float(latest_atr_14),
            atr_14_slope_10=atr_14_slope_10,
            atr_ratio=atr_ratio,
            runup_60_ratio=runup_60_ratio,
            pole_low=round(pole_low, 4),
            pole_high=round(pole_high, 4),
            pole_gain_ratio=round(pole_gain_ratio, 4),
            pole_days=pole_days,
            pole_volume_ratio=round(pole_volume_ratio, 4),
            up_day_pct=round(up_day_pct, 4),
            flag_days=flag_days,
            flag_high=round(flag_high, 4),
            flag_low=round(float(flag_lows.min()), 4),
            flag_drawdown_pct=round(flag_drawdown_pct, 4),
            flag_volume_ratio=round(flag_volume_ratio, 4),
            pivot_price=round(pivot_price, 4),
            distance_to_pivot_pct=round(distance_to_pivot_pct, 4),
            reasons=reasons,
        )
        score = (distance_to_pivot_pct, -pole_gain_ratio, flag_drawdown_pct)
        if best_score is None or score < best_score:
            best_candidate = candidate
            best_score = score

    return best_candidate


def run_high_tight_flag_setup_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> HighTightFlagSetupScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[HighTightFlagSetupHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting high tight flag setup screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=HTF_SETUP_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=HTF_SETUP_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_high_tight_flag_setup_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no high tight flag setup | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed high tight flag setup "
                        f"pole={((hit.pole_gain_ratio - 1.0) * 100.0):.1f}% "
                        f"flag={hit.flag_drawdown_pct * 100.0:.1f}% "
                        f"pivot_gap={hit.distance_to_pivot_pct * 100.0:.1f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda hit: (hit.distance_to_pivot_pct, -hit.pole_gain_ratio, hit.flag_drawdown_pct, hit.ticker))

    return HighTightFlagSetupScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
