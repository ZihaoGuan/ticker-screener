from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_active_universe_from_db,
    load_latest_market_caps,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    load_ticker_window,
    resolve_database_url,
)
from .universe import UniverseTicker


KAI_S2_STRATEGY_ID = "kai_s2"
KAI_S2_HISTORY_DAYS = 320
BETA_LOOKBACK_DAYS = 252
BETA_MIN_OBSERVATIONS = 120
AVG_VOLUME_LOOKBACK_DAYS = 30
MIN_PRICE = 5.0
MIN_MARKET_CAP = 1_000_000_000.0
MIN_AVG_VOLUME_30 = 2_000_000.0
MIN_PRICE_TIMES_AVG_VOLUME_30 = 100_000_000.0
MIN_BETA_1Y = 1.0


@dataclass(frozen=True)
class KaiS2Snapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    sma20: float
    sma50: float
    sma150: float
    sma200: float
    avg_volume_30: float
    price_times_avg_volume_30: float
    beta_1y: float | None
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class KaiS2Hit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float | None
    sma20: float
    sma50: float
    sma150: float
    sma200: float
    avg_volume_30: float
    price_times_avg_volume_30: float
    beta_1y: float | None
    criteria_passed: int
    criteria_total: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class KaiS2ScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[KaiS2Hit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _log(message: str) -> None:
    print(message, flush=True)


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ["High", "Low", "Close", "Volume"]
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
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _compute_beta_1y(stock_close: pd.Series, benchmark_frame: pd.DataFrame | None) -> float | None:
    if benchmark_frame is None:
        return None
    benchmark_bars = _normalize_price_frame(benchmark_frame)
    if benchmark_bars.empty:
        return None
    benchmark_close = benchmark_bars["Close"].astype(float)
    aligned = pd.concat(
        [
            stock_close.pct_change().rename("stock"),
            benchmark_close.pct_change().rename("benchmark"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    aligned = aligned.tail(BETA_LOOKBACK_DAYS)
    if len(aligned) < BETA_MIN_OBSERVATIONS:
        return None
    benchmark_variance = float(aligned["benchmark"].var())
    if benchmark_variance <= 0:
        return None
    return float(aligned["stock"].cov(aligned["benchmark"]) / benchmark_variance)


def evaluate_kai_s2(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame | None,
    *,
    market_cap: float | None,
) -> KaiS2Snapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < 200:
        return None

    close = bars["Close"].astype(float)
    volume = bars["Volume"].astype(float)
    latest_index = bars.index[-1]
    current_price = float(close.loc[latest_index])
    sma20 = float(close.rolling(20).mean().loc[latest_index])
    sma50 = float(close.rolling(50).mean().loc[latest_index])
    sma150 = float(close.rolling(150).mean().loc[latest_index])
    sma200 = float(close.rolling(200).mean().loc[latest_index])
    avg_volume_30 = float(volume.tail(AVG_VOLUME_LOOKBACK_DAYS).mean())
    price_times_avg_volume_30 = current_price * avg_volume_30
    beta_1y = _compute_beta_1y(close, benchmark_frame)

    if any(pd.isna(value) for value in (sma20, sma50, sma150, sma200, avg_volume_30)):
        return None

    criteria = {
        "price_gt_5": current_price > MIN_PRICE,
        "market_cap_gt_1b": market_cap is not None and market_cap > MIN_MARKET_CAP,
        "sma150_gt_sma200": sma150 > sma200,
        "price_gt_sma200": current_price > sma200,
        "avg_volume_30_gt_2m": avg_volume_30 > MIN_AVG_VOLUME_30,
        "price_times_avg_volume_30_gt_100m": price_times_avg_volume_30 > MIN_PRICE_TIMES_AVG_VOLUME_30,
        "beta_1y_gt_1": beta_1y is not None and beta_1y > MIN_BETA_1Y,
        "price_gt_sma50": current_price > sma50,
        "price_gt_sma20": current_price > sma20,
    }
    criteria_passed = sum(1 for passed in criteria.values() if passed)
    criteria_total = len(criteria)
    reasons = [
        f"close {current_price:.2f} vs price floor {MIN_PRICE:.2f}",
        f"market cap {market_cap / 1_000_000_000:.2f}B vs minimum 1.00B" if market_cap is not None else "market cap unavailable",
        f"150D/200D SMA stack {sma150:.2f} / {sma200:.2f}",
        f"close {current_price:.2f} vs 200D SMA {sma200:.2f}, 50D SMA {sma50:.2f}, 20D SMA {sma20:.2f}",
        f"30D average volume {avg_volume_30:,.0f} vs minimum {MIN_AVG_VOLUME_30:,.0f}",
        f"close x 30D average volume {price_times_avg_volume_30:,.0f} vs minimum {MIN_PRICE_TIMES_AVG_VOLUME_30:,.0f}",
        f"1Y beta {beta_1y:.2f} vs minimum {MIN_BETA_1Y:.1f}" if beta_1y is not None else "1Y beta unavailable",
    ]
    return KaiS2Snapshot(
        matched=criteria_passed == criteria_total,
        current_price=current_price,
        market_cap=market_cap,
        sma20=sma20,
        sma50=sma50,
        sma150=sma150,
        sma200=sma200,
        avg_volume_30=avg_volume_30,
        price_times_avg_volume_30=price_times_avg_volume_30,
        beta_1y=beta_1y,
        criteria_passed=criteria_passed,
        criteria_total=criteria_total,
        criteria=criteria,
        reasons=reasons,
    )


def find_kai_s2_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame | None,
    *,
    ticker: UniverseTicker,
    market_cap: float | None,
    signal_date: dt.date,
) -> KaiS2Hit | None:
    snapshot = evaluate_kai_s2(frame, benchmark_frame, market_cap=market_cap)
    if snapshot is None or not snapshot.matched:
        return None
    return KaiS2Hit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        market_cap=snapshot.market_cap,
        sma20=snapshot.sma20,
        sma50=snapshot.sma50,
        sma150=snapshot.sma150,
        sma200=snapshot.sma200,
        avg_volume_30=snapshot.avg_volume_30,
        price_times_avg_volume_30=snapshot.price_times_avg_volume_30,
        beta_1y=snapshot.beta_1y,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
    )


def _load_benchmark_frame(config: AppConfig, run_date: dt.date, database_url: str) -> pd.DataFrame | None:
    benchmark_frame = load_ticker_window(
        config.benchmark_ticker,
        run_date,
        KAI_S2_HISTORY_DAYS,
        database_url=database_url,
    )
    if benchmark_frame is not None and db_frame_has_recent_coverage(benchmark_frame, run_date):
        return benchmark_frame

    cookstock = load_configured_cookstock(config)
    with freeze_cookstock_today(cookstock, run_date):
        financials = cookstock.cookFinancials(
            config.benchmark_ticker,
            benchmarkTicker=config.benchmark_ticker,
            historyLookbackDays=KAI_S2_HISTORY_DAYS,
        )
    fallback_frame = _build_price_frame(financials)
    return fallback_frame if not fallback_frame.empty else None


def run_kai_s2_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> KaiS2ScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    total_tickers = len(tickers)
    symbols = [ticker.symbol for ticker in tickers]
    hits: list[KaiS2Hit] = []
    failures: list[dict[str, str]] = []

    _log(
        "starting kai s2 screen: "
        f"total={total_tickers}, price>{MIN_PRICE:.0f}, market_cap>{MIN_MARKET_CAP / 1_000_000_000:.0f}B, "
        f"avg_vol30>{MIN_AVG_VOLUME_30:,.0f}, close_x_avg_vol30>{MIN_PRICE_TIMES_AVG_VOLUME_30:,.0f}, beta>{MIN_BETA_1Y:.0f}"
    )

    frame_map = load_many_ticker_windows(symbols, run_date, KAI_S2_HISTORY_DAYS, database_url=resolved_database_url)
    metadata_map = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    market_caps = load_latest_market_caps(symbols, as_of_date=run_date, database_url=resolved_database_url)
    benchmark_frame = _load_benchmark_frame(config, run_date, resolved_database_url)
    fallback_tickers: list[tuple[int, UniverseTicker]] = []

    for position, ticker in enumerate(tickers, start=1):
        frame = frame_map.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < 200:
            fallback_tickers.append((position, ticker))
            continue
        metadata = metadata_map.get(ticker.symbol.upper(), {})
        runtime_ticker = UniverseTicker(
            symbol=ticker.symbol.upper(),
            sector=ticker.sector or str(metadata.get("sector") or "") or None,
            industry=ticker.industry or str(metadata.get("industry") or "") or None,
            exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
        )
        _log(f"[{position}/{total_tickers}] screening {runtime_ticker.symbol} from DB | passed={len(hits)}")
        try:
            hit = find_kai_s2_hit(
                frame,
                benchmark_frame,
                ticker=runtime_ticker,
                market_cap=market_caps.get(runtime_ticker.symbol),
                signal_date=run_date,
            )
        except Exception as exc:
            failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} error: {exc} | passed={len(hits)}")
            continue
        if hit is None:
            _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} filtered: kai s2 failed | passed={len(hits)}")
            continue
        hits.append(hit)
        _log(
            f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed: "
            f"price={hit.current_price:.2f} beta={hit.beta_1y:.2f} dollar_vol30={hit.price_times_avg_volume_30:,.0f} | passed={len(hits)}"
        )

    if fallback_tickers:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for position, ticker in fallback_tickers:
                metadata = metadata_map.get(ticker.symbol.upper(), {})
                runtime_ticker = UniverseTicker(
                    symbol=ticker.symbol.upper(),
                    sector=ticker.sector or str(metadata.get("sector") or "") or None,
                    industry=ticker.industry or str(metadata.get("industry") or "") or None,
                    exchange=ticker.exchange or str(metadata.get("exchange") or "") or None,
                )
                _log(f"[{position}/{total_tickers}] screening {runtime_ticker.symbol} from internet fallback | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        runtime_ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=KAI_S2_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_kai_s2_hit(
                        frame,
                        benchmark_frame,
                        ticker=runtime_ticker,
                        market_cap=market_caps.get(runtime_ticker.symbol),
                        signal_date=run_date,
                    )
                    if hit is None:
                        _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} filtered: kai s2 failed | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    _log(
                        f"[{position}/{total_tickers}] {runtime_ticker.symbol} passed: "
                        f"price={hit.current_price:.2f} beta={hit.beta_1y:.2f} dollar_vol30={hit.price_times_avg_volume_30:,.0f} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": runtime_ticker.symbol, "error": str(exc)})
                    _log(f"[{position}/{total_tickers}] {runtime_ticker.symbol} error: {exc} | passed={len(hits)}")

    hits.sort(
        key=lambda item: (
            -float(item.beta_1y or 0.0),
            -item.price_times_avg_volume_30,
            -(item.market_cap or 0.0),
            item.ticker,
        )
    )
    return KaiS2ScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def load_kai_s2_universe(
    *,
    as_of_date: dt.date | None = None,
    limit: int | None = None,
    database_url: str | None = None,
) -> list[UniverseTicker]:
    return load_active_universe_from_db(
        as_of_date=as_of_date,
        limit=limit,
        database_url=resolve_database_url(database_url),
    )
