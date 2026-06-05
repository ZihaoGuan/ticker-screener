from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


HV1_LOOKBACK_DAYS = 252
HVE_VOLUME_MA_LENGTH = 50
HVE_ATR_LENGTH = 14
HVE_HISTORY_DAYS = 5000


@dataclass(frozen=True)
class HveHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    open_price: float
    high_price: float
    low_price: float
    current_volume: float
    highest_volume_ever: float
    highest_volume_ever_date: str
    highest_volume_52w: float
    highest_volume_52w_date: str
    volume_ma_50: float
    volume_buzz_pct: float
    price_change_pct: float
    ma50: float
    distance_to_ma50_pct: float
    atr14: float
    atr_multiple_from_ma50: float
    is_hve: bool
    is_hv1: bool
    is_up_day: bool
    is_above_ma50: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class HveScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[HveHit]

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


def _compute_atr(frame: pd.DataFrame, length: int) -> pd.Series:
    previous_close = frame["Close"].shift(1)
    true_range = pd.concat(
        [
            frame["High"] - frame["Low"],
            (frame["High"] - previous_close).abs(),
            (frame["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.rolling(length).mean()


def find_recent_hve_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    lookback_days: int = HV1_LOOKBACK_DAYS,
    volume_ma_length: int = HVE_VOLUME_MA_LENGTH,
    atr_length: int = HVE_ATR_LENGTH,
) -> HveHit | None:
    normalized = _normalize_bars_frame(frame)
    minimum_bars = max(lookback_days, volume_ma_length, atr_length + 1)
    if normalized.empty or len(normalized) < minimum_bars:
        return None

    normalized = normalized.copy()
    normalized["volume_ma_50"] = normalized["Volume"].rolling(volume_ma_length).mean()
    normalized["ma50"] = normalized["Close"].rolling(50).mean()
    normalized["atr14"] = _compute_atr(normalized, atr_length)

    latest = normalized.iloc[-1]
    lookback_window = normalized.iloc[-lookback_days:]
    if lookback_window.empty:
        return None

    highest_volume_ever = float(normalized["Volume"].max())
    highest_volume = float(lookback_window["Volume"].max())
    current_volume = float(latest["Volume"])
    is_hve = current_volume >= highest_volume_ever
    is_hv1 = current_volume >= highest_volume
    if not is_hve:
        return None

    volume_ma_50 = float(latest["volume_ma_50"]) if pd.notna(latest["volume_ma_50"]) else 0.0
    ma50 = float(latest["ma50"]) if pd.notna(latest["ma50"]) else 0.0
    atr14 = float(latest["atr14"]) if pd.notna(latest["atr14"]) else 0.0
    current_price = float(latest["Close"])
    open_price = float(latest["Open"])
    high_price = float(latest["High"])
    low_price = float(latest["Low"])
    previous_close = float(normalized["Close"].iloc[-2])
    volume_buzz_pct = ((current_volume / volume_ma_50) - 1.0) * 100.0 if volume_ma_50 > 0 else 0.0
    price_change_pct = ((current_price / previous_close) - 1.0) * 100.0 if previous_close > 0 else 0.0
    distance_to_ma50_pct = ((current_price / ma50) - 1.0) * 100.0 if ma50 > 0 else 0.0
    atr_multiple_from_ma50 = ((current_price - ma50) / atr14) if ma50 > 0 and atr14 > 0 else 0.0
    is_up_day = current_price >= open_price
    is_above_ma50 = ma50 > 0 and current_price >= ma50

    reasons = [
        "current bar is highest volume ever",
        f"volume buzz {volume_buzz_pct:+.1f}% vs 50D avg",
        f"price change {price_change_pct:+.1f}% on signal bar",
    ]
    if is_above_ma50:
        reasons.append(f"holding above 50D MA {ma50:.2f}")
    else:
        reasons.append(f"below 50D MA {ma50:.2f}")
    if atr14 > 0:
        reasons.append(f"{atr_multiple_from_ma50:+.2f} ATR from 50D MA")

    return HveHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=normalized.index[-1].date().isoformat(),
        current_price=current_price,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        current_volume=current_volume,
        highest_volume_ever=highest_volume_ever,
        highest_volume_ever_date=normalized.loc[normalized["Volume"] == highest_volume_ever].index[-1].date().isoformat(),
        highest_volume_52w=highest_volume,
        highest_volume_52w_date=lookback_window.index[lookback_window["Volume"] == highest_volume][-1].date().isoformat(),
        volume_ma_50=volume_ma_50,
        volume_buzz_pct=volume_buzz_pct,
        price_change_pct=price_change_pct,
        ma50=ma50,
        distance_to_ma50_pct=distance_to_ma50_pct,
        atr14=atr14,
        atr_multiple_from_ma50=atr_multiple_from_ma50,
        is_hve=is_hve,
        is_hv1=is_hv1,
        is_up_day=is_up_day,
        is_above_ma50=is_above_ma50,
        reasons=reasons,
    )


def run_hve_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> HveScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[HveHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)

    print(
        "starting HVE 52W screen: "
        f"total={total_tickers}, "
        f"hv1_lookback={HV1_LOOKBACK_DAYS}, "
        f"volume_ma={HVE_VOLUME_MA_LENGTH}, "
        f"atr={HVE_ATR_LENGTH}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=HVE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=HVE_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_hve_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no HVE signal | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"vol {hit.current_volume:,.0f}, buzz {hit.volume_buzz_pct:+.1f}%, "
                        f"change {hit.price_change_pct:+.1f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            -item.volume_buzz_pct,
            -item.price_change_pct,
            -item.atr_multiple_from_ma50,
            item.ticker,
        )
    )

    print(
        "finished HVE screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return HveScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
