from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


SEAN_BREAKOUT_MIN_CLOSE = 3.0
SEAN_BREAKOUT_EMA_FAST = 21
SEAN_BREAKOUT_EMA_SLOW = 50
SEAN_BREAKOUT_AVG_VOLUME_PERIOD = 10
SEAN_BREAKOUT_MIN_AVG_VOLUME = 500_000.0
SEAN_BREAKOUT_ADR_PERIOD = 20
SEAN_BREAKOUT_MIN_ADR_PCT = 2.0
SEAN_BREAKOUT_HISTORY_DAYS = 120


@dataclass(frozen=True)
class SeanBreakoutHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_kind: str
    current_price: float
    high_price: float
    low_price: float
    ema21_value: float
    ema50_value: float
    avg_volume_10: float
    adr_pct_20: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SeanBreakoutScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[SeanBreakoutHit]

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


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
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


def find_recent_sean_breakout_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> SeanBreakoutHit | None:
    bars = _normalize_bars_frame(frame)
    min_bars = max(SEAN_BREAKOUT_EMA_SLOW, SEAN_BREAKOUT_ADR_PERIOD, SEAN_BREAKOUT_AVG_VOLUME_PERIOD)
    if bars.empty or len(bars) < min_bars:
        return None

    close_series = bars["Close"].astype(float)
    high_series = bars["High"].astype(float)
    low_series = bars["Low"].astype(float)
    volume_series = bars["Volume"].astype(float)

    ema21 = close_series.ewm(span=SEAN_BREAKOUT_EMA_FAST, adjust=False).mean()
    ema50 = close_series.ewm(span=SEAN_BREAKOUT_EMA_SLOW, adjust=False).mean()
    avg_volume_10 = volume_series.rolling(SEAN_BREAKOUT_AVG_VOLUME_PERIOD).mean()
    adr_pct_20 = (((high_series - low_series) / close_series) * 100.0).rolling(SEAN_BREAKOUT_ADR_PERIOD).mean()

    latest = bars.iloc[-1]
    latest_close = float(close_series.iloc[-1])
    latest_ema21 = float(ema21.iloc[-1]) if pd.notna(ema21.iloc[-1]) else 0.0
    latest_ema50 = float(ema50.iloc[-1]) if pd.notna(ema50.iloc[-1]) else 0.0
    latest_avg_volume_10 = float(avg_volume_10.iloc[-1]) if pd.notna(avg_volume_10.iloc[-1]) else 0.0
    latest_adr_pct_20 = float(adr_pct_20.iloc[-1]) if pd.notna(adr_pct_20.iloc[-1]) else 0.0

    if latest_close < SEAN_BREAKOUT_MIN_CLOSE:
        return None
    if latest_close <= latest_ema21 or latest_close <= latest_ema50:
        return None
    if latest_avg_volume_10 <= SEAN_BREAKOUT_MIN_AVG_VOLUME:
        return None
    if latest_adr_pct_20 < SEAN_BREAKOUT_MIN_ADR_PCT:
        return None

    reasons = [
        f"close {latest_close:.2f} above EMA{SEAN_BREAKOUT_EMA_FAST} {latest_ema21:.2f} and EMA{SEAN_BREAKOUT_EMA_SLOW} {latest_ema50:.2f}",
        f"average volume {SEAN_BREAKOUT_AVG_VOLUME_PERIOD}d {latest_avg_volume_10:,.0f} above {SEAN_BREAKOUT_MIN_AVG_VOLUME:,.0f}",
        f"ADR{SEAN_BREAKOUT_ADR_PERIOD} {latest_adr_pct_20:.2f}% at or above {SEAN_BREAKOUT_MIN_ADR_PCT:.2f}%",
        f"close {latest_close:.2f} at or above {SEAN_BREAKOUT_MIN_CLOSE:.2f}",
    ]

    return SeanBreakoutHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        signal_kind="sean_breakout",
        current_price=latest_close,
        high_price=float(latest["High"]),
        low_price=float(latest["Low"]),
        ema21_value=latest_ema21,
        ema50_value=latest_ema50,
        avg_volume_10=latest_avg_volume_10,
        adr_pct_20=latest_adr_pct_20,
        reasons=reasons,
    )


def run_sean_breakout_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> SeanBreakoutScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[SeanBreakoutHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting Sean breakout screen: "
        f"total={total_tickers}, min_close={SEAN_BREAKOUT_MIN_CLOSE:.2f}, "
        f"min_avg_volume={SEAN_BREAKOUT_MIN_AVG_VOLUME:,.0f}, min_adr={SEAN_BREAKOUT_MIN_ADR_PCT:.2f}%"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=SEAN_BREAKOUT_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=SEAN_BREAKOUT_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_sean_breakout_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no Sean breakout | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed Sean breakout "
                        f"close={hit.current_price:.2f} adr20={hit.adr_pct_20:.2f}% | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished Sean breakout screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return SeanBreakoutScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
