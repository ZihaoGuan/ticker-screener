from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


HTF_SMA_LONG_PERIOD = 200
HTF_SMA_SHORT_PERIOD = 50
HTF_VOLUME_SMA_PERIOD = 50
HTF_ATR_PERIOD = 14
HTF_SLOPE_LOOKBACK = 10
HTF_RUNUP_60_BARS = 60
HTF_RUNUP_40_BARS = 40
HTF_MIN_RUNUP_60_RATIO = 1.5
HTF_MIN_RUNUP_40_RATIO = 1.9
HTF_MAX_ATR_RATIO = 0.08
HTF_HISTORY_DAYS = 260


@dataclass(frozen=True)
class HighTightFlagHit:
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
    runup_40_ratio: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class HighTightFlagScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[HighTightFlagHit]

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


def find_high_tight_flag_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> HighTightFlagHit | None:
    bars = _normalize_bars_frame(frame)
    min_history = max(
        HTF_SMA_LONG_PERIOD + HTF_SLOPE_LOOKBACK,
        HTF_VOLUME_SMA_PERIOD + HTF_SLOPE_LOOKBACK,
        HTF_ATR_PERIOD + HTF_SLOPE_LOOKBACK,
        HTF_RUNUP_60_BARS + 1,
        HTF_RUNUP_40_BARS + 1,
    )
    if bars.empty or len(bars) < min_history:
        return None

    sma_50 = bars["Close"].rolling(HTF_SMA_SHORT_PERIOD).mean()
    sma_200 = bars["Close"].rolling(HTF_SMA_LONG_PERIOD).mean()
    avg_volume_50 = bars["Volume"].rolling(HTF_VOLUME_SMA_PERIOD).mean()
    atr_14 = _true_range(bars).rolling(HTF_ATR_PERIOD).mean()

    latest = bars.iloc[-1]
    latest_close = float(latest["Close"])
    latest_sma_50 = sma_50.iloc[-1]
    latest_sma_200 = sma_200.iloc[-1]
    latest_avg_volume_50 = avg_volume_50.iloc[-1]
    latest_atr_14 = atr_14.iloc[-1]
    close_60 = bars["Close"].iloc[-(HTF_RUNUP_60_BARS + 1)]
    close_40 = bars["Close"].iloc[-(HTF_RUNUP_40_BARS + 1)]

    if any(
        pd.isna(value)
        for value in (
            latest_sma_50,
            latest_sma_200,
            latest_avg_volume_50,
            latest_atr_14,
            close_60,
            close_40,
            sma_200.iloc[-(HTF_SLOPE_LOOKBACK + 1)],
            avg_volume_50.iloc[-(HTF_SLOPE_LOOKBACK + 1)],
            atr_14.iloc[-(HTF_SLOPE_LOOKBACK + 1)],
        )
    ):
        return None

    sma_200_slope_10 = float((latest_sma_200 - sma_200.iloc[-(HTF_SLOPE_LOOKBACK + 1)]) / HTF_SLOPE_LOOKBACK)
    avg_volume_50_slope_10 = float(
        (latest_avg_volume_50 - avg_volume_50.iloc[-(HTF_SLOPE_LOOKBACK + 1)]) / HTF_SLOPE_LOOKBACK
    )
    atr_14_slope_10 = float((latest_atr_14 - atr_14.iloc[-(HTF_SLOPE_LOOKBACK + 1)]) / HTF_SLOPE_LOOKBACK)
    atr_ratio = float(latest_atr_14 / latest_close) if latest_close else 0.0
    runup_60_ratio = float(latest_close / float(close_60)) if float(close_60) else 0.0
    runup_40_ratio = float(latest_close / float(close_40)) if float(close_40) else 0.0

    if not (
        sma_200_slope_10 > 0.0
        and avg_volume_50_slope_10 < 0.0
        and latest_close >= float(latest_sma_50)
        and latest_close >= float(latest_sma_200)
        and float(latest_sma_50) >= float(latest_sma_200)
        and runup_60_ratio > HTF_MIN_RUNUP_60_RATIO
        and atr_ratio < HTF_MAX_ATR_RATIO
        and atr_14_slope_10 < 0.0
        and runup_40_ratio > HTF_MIN_RUNUP_40_RATIO
    ):
        return None

    reasons = [
        f"200 SMA rising {sma_200_slope_10:.3f} points/day over last {HTF_SLOPE_LOOKBACK} bars",
        f"50-day volume slope {avg_volume_50_slope_10:,.0f}/day shows moderation",
        f"Close {latest_close:.2f} above 50 SMA {float(latest_sma_50):.2f} and 200 SMA {float(latest_sma_200):.2f}",
        f"{HTF_RUNUP_60_BARS}-bar runup {((runup_60_ratio - 1.0) * 100.0):.1f}%",
        f"{HTF_RUNUP_40_BARS}-bar runup {((runup_40_ratio - 1.0) * 100.0):.1f}%",
        f"ATR14/close {atr_ratio:.3f} with ATR slope {atr_14_slope_10:.3f}/day",
    ]

    return HighTightFlagHit(
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
        runup_40_ratio=runup_40_ratio,
        reasons=reasons,
    )


def run_high_tight_flag_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> HighTightFlagScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[HighTightFlagHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting high tight flag screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=HTF_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=HTF_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_high_tight_flag_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no high tight flag | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed high tight flag "
                        f"40d={((hit.runup_40_ratio - 1.0) * 100.0):.1f}% "
                        f"60d={((hit.runup_60_ratio - 1.0) * 100.0):.1f}% "
                        f"atr={hit.atr_ratio:.3f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    hits.sort(key=lambda hit: (-hit.runup_40_ratio, -hit.runup_60_ratio, hit.atr_ratio, hit.ticker))

    return HighTightFlagScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
