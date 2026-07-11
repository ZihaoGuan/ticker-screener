from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


BOLLINGER_LENGTH = 20
BOLLINGER_STD_MULTIPLIER = 2.0
BOLLINGER_HISTORY_DAYS = 120


@dataclass(frozen=True)
class BollingerBandSnapshot:
    close: float
    middle_band: float
    upper_band: float
    lower_band: float
    status: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BollingerBandBreakoutHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    middle_band: float
    upper_band: float
    lower_band: float
    close_vs_upper_pct: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class BollingerBandBreakoutScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[BollingerBandBreakoutHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def normalize_bollinger_frame(frame: pd.DataFrame) -> pd.DataFrame:
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


def build_price_frame(financials) -> pd.DataFrame:
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


def compute_latest_bollinger_snapshot(frame: pd.DataFrame) -> BollingerBandSnapshot | None:
    bars = normalize_bollinger_frame(frame)
    if bars.empty or len(bars) < BOLLINGER_LENGTH:
        return None
    close = bars["Close"]
    middle_band = close.rolling(BOLLINGER_LENGTH, min_periods=BOLLINGER_LENGTH).mean()
    band_std = close.rolling(BOLLINGER_LENGTH, min_periods=BOLLINGER_LENGTH).std(ddof=0)
    latest_close = float(close.iloc[-1]) if pd.notna(close.iloc[-1]) else None
    latest_mid = float(middle_band.iloc[-1]) if pd.notna(middle_band.iloc[-1]) else None
    latest_std = float(band_std.iloc[-1]) if pd.notna(band_std.iloc[-1]) else None
    if latest_close is None or latest_mid is None or latest_std is None:
        return None
    latest_upper = latest_mid + (BOLLINGER_STD_MULTIPLIER * latest_std)
    latest_lower = latest_mid - (BOLLINGER_STD_MULTIPLIER * latest_std)
    if latest_close >= latest_upper:
        status = "above_upper_band"
    elif latest_close <= latest_lower:
        status = "below_lower_band"
    else:
        status = "within_bands"
    return BollingerBandSnapshot(
        close=latest_close,
        middle_band=latest_mid,
        upper_band=latest_upper,
        lower_band=latest_lower,
        status=status,
    )


def find_recent_bollinger_band_breakout_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> BollingerBandBreakoutHit | None:
    snapshot = compute_latest_bollinger_snapshot(frame)
    if snapshot is None or snapshot.status != "above_upper_band":
        return None
    close_vs_upper_pct = ((snapshot.close / snapshot.upper_band) - 1.0) * 100.0 if snapshot.upper_band > 0 else 0.0
    reasons = [
        f"Close {snapshot.close:.2f} above upper band {snapshot.upper_band:.2f}",
        f"20SMA {snapshot.middle_band:.2f}, lower band {snapshot.lower_band:.2f}",
        f"Close {close_vs_upper_pct:.2f}% above upper band",
    ]
    return BollingerBandBreakoutHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=frame.index[-1].date().isoformat(),
        current_price=snapshot.close,
        middle_band=snapshot.middle_band,
        upper_band=snapshot.upper_band,
        lower_band=snapshot.lower_band,
        close_vs_upper_pct=close_vs_upper_pct,
        reasons=reasons,
    )


def run_bollinger_band_breakout_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> BollingerBandBreakoutScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[BollingerBandBreakoutHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(f"starting bollinger band breakout screen: total={total_tickers}")

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=BOLLINGER_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=BOLLINGER_HISTORY_DAYS,
                    )
                    frame = build_price_frame(financials)
                    hit = find_recent_bollinger_band_breakout_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: not above upper band | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed bollinger breakout "
                        f"+{hit.close_vs_upper_pct:.2f}% above upper band | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    return BollingerBandBreakoutScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
