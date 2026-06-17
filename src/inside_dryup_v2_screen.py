from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


PRICE_VOLUME_MA_PERIOD = 20
DRY_UP_THRESHOLD = 0.50
EXTREME_DRY_UP_THRESHOLD = 0.30
DRY_UP_MIN_BARS = 1
PRIOR_RUNUP_BARS = 50
PRIOR_RUNUP_PCT = 20.0
HISTORY_DAYS = 260


@dataclass(frozen=True)
class InsideDryupV2Hit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    inside_day_high: float
    inside_day_low: float
    previous_day_high: float
    previous_day_low: float
    price_volume: float
    price_volume_ma_20: float
    price_volume_ratio: float
    dry_count: int
    had_prior_runup: bool
    had_strong_move: bool
    prior_runup_pct: float
    qualified_extreme: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class InsideDryupV2ScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[InsideDryupV2Hit]

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


def _consecutive_dry_count(is_dry: pd.Series) -> pd.Series:
    counts: list[int] = []
    current = 0
    for value in is_dry.fillna(False).tolist():
        current = current + 1 if bool(value) else 0
        counts.append(current)
    return pd.Series(counts, index=is_dry.index, dtype="int64")


def find_recent_inside_dryup_v2_hit(frame: pd.DataFrame, *, ticker: UniverseTicker) -> InsideDryupV2Hit | None:
    bars = _normalize_bars_frame(frame)
    minimum_bars = max(PRICE_VOLUME_MA_PERIOD + DRY_UP_MIN_BARS + 1, PRIOR_RUNUP_BARS + 1)
    if bars.empty or len(bars) < minimum_bars:
        return None

    bars = bars.copy()
    bars["price_volume"] = bars["Close"] * bars["Volume"]
    bars["price_volume_ma_20"] = bars["price_volume"].rolling(PRICE_VOLUME_MA_PERIOD).mean()
    bars["is_dry_bar"] = (bars["price_volume_ma_20"] > 0) & (bars["price_volume"] < bars["price_volume_ma_20"] * DRY_UP_THRESHOLD)
    bars["dry_count"] = _consecutive_dry_count(bars["is_dry_bar"])

    latest = bars.iloc[-1]
    previous = bars.iloc[-2]
    inside_day = float(latest["High"]) < float(previous["High"]) and float(latest["Low"]) > float(previous["Low"])
    if not inside_day:
        return None

    price_volume_ma_20 = float(latest["price_volume_ma_20"])
    if price_volume_ma_20 <= 0:
        return None

    price_volume = float(latest["price_volume"])
    price_volume_ratio = price_volume / price_volume_ma_20
    dry_count = int(latest["dry_count"])
    qualified_extreme = price_volume_ratio < EXTREME_DRY_UP_THRESHOLD and dry_count >= DRY_UP_MIN_BARS
    if not qualified_extreme:
        return None

    lowest_close = float(bars["Close"].iloc[-PRIOR_RUNUP_BARS:].min())
    highest_close = float(bars["Close"].iloc[-PRIOR_RUNUP_BARS:].max())
    current_price = float(latest["Close"])
    had_prior_runup = ((current_price / lowest_close) - 1.0) * 100.0 >= (PRIOR_RUNUP_PCT / 2.0) if lowest_close > 0 else False
    prior_runup_pct = ((highest_close / lowest_close) - 1.0) * 100.0 if lowest_close > 0 else 0.0
    had_strong_move = prior_runup_pct >= PRIOR_RUNUP_PCT

    reasons = [
        "inside day vs previous bar",
        f"price x volume {price_volume_ratio:.2f}x of 20D average",
        f"extreme dry-up threshold {EXTREME_DRY_UP_THRESHOLD:.2f}x met",
        f"dry-up streak {dry_count} bar(s)",
        f"prior {PRIOR_RUNUP_BARS}-bar run-up {prior_runup_pct:.1f}%",
    ]

    return InsideDryupV2Hit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        current_price=current_price,
        inside_day_high=float(latest["High"]),
        inside_day_low=float(latest["Low"]),
        previous_day_high=float(previous["High"]),
        previous_day_low=float(previous["Low"]),
        price_volume=price_volume,
        price_volume_ma_20=price_volume_ma_20,
        price_volume_ratio=price_volume_ratio,
        dry_count=dry_count,
        had_prior_runup=had_prior_runup,
        had_strong_move=had_strong_move,
        prior_runup_pct=prior_runup_pct,
        qualified_extreme=True,
        reasons=reasons,
    )


def run_inside_dryup_v2_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> InsideDryupV2ScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[InsideDryupV2Hit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting inside-day dry-up v2 screen: "
        f"total={total_tickers}, pxv_ma={PRICE_VOLUME_MA_PERIOD}, "
        f"dry<{DRY_UP_THRESHOLD:.2f}x, extreme<{EXTREME_DRY_UP_THRESHOLD:.2f}x"
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
                    hit = find_recent_inside_dryup_v2_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no inside-day extreme dry-up | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed: "
                        f"pxv ratio {hit.price_volume_ratio:.2f}, dry streak {hit.dry_count} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(key=lambda item: (item.price_volume_ratio, -item.dry_count, item.ticker))

    print(
        "finished inside-day dry-up v2 screen: "
        f"passed={len(hits)}, failed={len(failures)}, total={total_tickers}"
    )

    return InsideDryupV2ScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
