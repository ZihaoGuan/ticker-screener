from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import numpy as np
import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


BB_SQUEEZE_KELTNER_PERIOD = 20
BB_SQUEEZE_KELTNER_FACTOR = 1.5
BB_SQUEEZE_CCI_PERIOD = 50
BB_SQUEEZE_BANDS_PERIOD = 20
BB_SQUEEZE_BANDS_DEVIATIONS = 2.0
BB_SQUEEZE_HISTORY_DAYS = 120


@dataclass(frozen=True)
class BbSqueezeHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_kind: str
    current_price: float
    high_price: float
    low_price: float
    bb_squeeze_ratio: float
    atr_value: float
    keltner_width: float
    bollinger_std: float
    cci_value: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BbSqueezeScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[BbSqueezeHit]

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


def _cci(series_high: pd.Series, series_low: pd.Series, series_close: pd.Series, period: int) -> pd.Series:
    typical_price = (series_high + series_low + series_close) / 3.0
    sma = typical_price.rolling(period).mean()
    mean_deviation = typical_price.rolling(period).apply(
        lambda values: float(np.mean(np.abs(values - np.mean(values)))),
        raw=True,
    )
    denominator = (0.015 * mean_deviation).replace(0.0, np.nan)
    return (typical_price - sma) / denominator


def find_recent_bb_squeeze_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> BbSqueezeHit | None:
    bars = _normalize_bars_frame(frame)
    if bars.empty or len(bars) < BB_SQUEEZE_CCI_PERIOD:
        return None

    atr_value = _true_range(bars).rolling(BB_SQUEEZE_KELTNER_PERIOD).mean()
    keltner_width = atr_value * BB_SQUEEZE_KELTNER_FACTOR
    bollinger_std = bars["Close"].rolling(BB_SQUEEZE_BANDS_PERIOD).std(ddof=0)
    bb_squeeze_ratio = (BB_SQUEEZE_BANDS_DEVIATIONS * bollinger_std) / keltner_width.replace(0.0, np.nan)
    cci_value = _cci(bars["High"], bars["Low"], bars["Close"], BB_SQUEEZE_CCI_PERIOD)

    latest_ratio = bb_squeeze_ratio.iloc[-1]
    latest_cci = cci_value.iloc[-1]
    if pd.isna(latest_ratio) or pd.isna(latest_cci) or float(latest_ratio) >= 1.0:
        return None

    latest = bars.iloc[-1]
    ratio_value = float(latest_ratio)
    atr_latest = float(atr_value.iloc[-1]) if pd.notna(atr_value.iloc[-1]) else 0.0
    keltner_latest = float(keltner_width.iloc[-1]) if pd.notna(keltner_width.iloc[-1]) else 0.0
    std_latest = float(bollinger_std.iloc[-1]) if pd.notna(bollinger_std.iloc[-1]) else 0.0
    cci_latest = float(latest_cci)
    signal_kind = "positive_cci" if cci_latest > 0.0 else "non_positive_cci"

    reasons = [
        f"BB squeeze ratio {ratio_value:.3f} below 1.000",
        f"CCI{BB_SQUEEZE_CCI_PERIOD} {cci_latest:.2f}",
        f"Bollinger std {std_latest:.3f} vs Keltner width {keltner_latest:.3f}",
    ]

    return BbSqueezeHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        signal_kind=signal_kind,
        current_price=float(latest["Close"]),
        high_price=float(latest["High"]),
        low_price=float(latest["Low"]),
        bb_squeeze_ratio=ratio_value,
        atr_value=atr_latest,
        keltner_width=keltner_latest,
        bollinger_std=std_latest,
        cci_value=cci_latest,
        reasons=reasons,
    )


def run_bb_squeeze_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> BbSqueezeScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[BbSqueezeHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting bb squeeze screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=BB_SQUEEZE_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=BB_SQUEEZE_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_bb_squeeze_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no bb squeeze | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed bb squeeze "
                        f"{hit.bb_squeeze_ratio:.3f} {hit.signal_kind} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    return BbSqueezeScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
