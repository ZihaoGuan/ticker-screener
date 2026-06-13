from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


SEPA_MA50_LENGTH = 50
SEPA_MA150_LENGTH = 150
SEPA_MA200_LENGTH = 200
SEPA_52W_LOOKBACK = 252
SEPA_MA200_SLOPE_LOOKBACK = 20
SEPA_PRESSURE_LOOKBACK = 20
SEPA_RPR_3M_LENGTH = 63
SEPA_RPR_6M_LENGTH = 126
SEPA_RPR_9M_LENGTH = 189
SEPA_RPR_12M_LENGTH = 252
SEPA_VCP_LOOKBACK = 5
SEPA_VCP_THRESHOLD_PCT = 2.5
SEPA_SIGNAL_LOOKBACK_BARS = 5
SEPA_HISTORY_DAYS = 320


@dataclass(frozen=True)
class SepaDashboardSnapshot:
    snapshot_date: str
    benchmark_ticker: str
    latest_close: float
    ma50: float
    ma150: float
    ma200: float
    high_52wk: float
    low_52wk: float
    tpr_pass: bool
    tpr_status: str
    buy_risk_status: str
    buy_risk_distance_pct: float
    pressure_status: str
    pressure_buying: bool
    buy_volume_20d: float
    sell_volume_20d: float
    rpr_score: float
    rpr_status: str
    vcp_status: str
    vcp_trigger: bool
    vcp_range_pct: float
    recent_vcp_signal: bool
    recent_vcp_signal_date: str | None
    recent_vcp_signal_high: float | None
    recent_vcp_signal_low: float | None
    recent_vcp_signal_close: float | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SepaVcpHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    signal_kind: str
    current_price: float
    high_price: float
    low_price: float
    trigger_price: float
    stop_price: float
    tpr_pass: bool
    tpr_status: str
    buy_risk_status: str
    buy_risk_distance_pct: float
    pressure_status: str
    pressure_buying: bool
    buy_volume_20d: float
    sell_volume_20d: float
    rpr_score: float
    rpr_status: str
    vcp_status: str
    vcp_trigger: bool
    vcp_range_pct: float
    ma50: float
    ma150: float
    ma200: float
    high_52wk: float
    low_52wk: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class SepaVcpScreenResult:
    run_date: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[SepaVcpHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "benchmark_ticker": self.benchmark_ticker,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame, *, include_volume: bool = True) -> pd.DataFrame:
    required = ["Open", "High", "Low", "Close", "Volume"] if include_volume else ["Close"]
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


def _roc(series: pd.Series, length: int) -> pd.Series:
    return ((series - series.shift(length)) / series.shift(length)) * 100.0


def build_sepa_dashboard_snapshot(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    benchmark_ticker: str,
    recent_signal_lookback_bars: int = SEPA_SIGNAL_LOOKBACK_BARS,
) -> SepaDashboardSnapshot | None:
    bars = _normalize_price_frame(frame)
    benchmark_bars = _normalize_price_frame(benchmark_frame, include_volume=False)
    min_required = max(SEPA_MA200_LENGTH + SEPA_MA200_SLOPE_LOOKBACK, SEPA_RPR_12M_LENGTH + 1, SEPA_52W_LOOKBACK)
    if bars.empty or benchmark_bars.empty or len(bars) < min_required:
        return None

    benchmark_close = benchmark_bars["Close"].astype(float).reindex(bars.index).ffill().bfill()
    if benchmark_close.isna().any():
        return None

    close = bars["Close"].astype(float)
    ma50 = close.rolling(SEPA_MA50_LENGTH).mean()
    ma150 = close.rolling(SEPA_MA150_LENGTH).mean()
    ma200 = close.rolling(SEPA_MA200_LENGTH).mean()
    high_52wk = bars["High"].astype(float).rolling(SEPA_52W_LOOKBACK).max()
    low_52wk = bars["Low"].astype(float).rolling(SEPA_52W_LOOKBACK).min()

    volume = bars["Volume"].astype(float)
    open_values = bars["Open"].astype(float)
    buy_vol = volume.where(close > open_values, 0.0).rolling(SEPA_PRESSURE_LOOKBACK).sum()
    sell_vol = volume.where(close <= open_values, 0.0).rolling(SEPA_PRESSURE_LOOKBACK).sum()

    sym_roc3 = _roc(close, SEPA_RPR_3M_LENGTH)
    sym_roc6 = _roc(close, SEPA_RPR_6M_LENGTH)
    sym_roc9 = _roc(close, SEPA_RPR_9M_LENGTH)
    sym_roc12 = _roc(close, SEPA_RPR_12M_LENGTH)
    bm_roc3 = _roc(benchmark_close, SEPA_RPR_3M_LENGTH)
    bm_roc6 = _roc(benchmark_close, SEPA_RPR_6M_LENGTH)
    bm_roc9 = _roc(benchmark_close, SEPA_RPR_9M_LENGTH)
    bm_roc12 = _roc(benchmark_close, SEPA_RPR_12M_LENGTH)

    rs_raw = sym_roc3 * 0.4 + sym_roc6 * 0.2 + sym_roc9 * 0.2 + sym_roc12 * 0.2
    bm_raw = bm_roc3 * 0.4 + bm_roc6 * 0.2 + bm_roc9 * 0.2 + bm_roc12 * 0.2
    rpr_score_series = (50.0 + rs_raw - bm_raw).clip(lower=1.0, upper=99.0)

    vcp_highest = close.rolling(SEPA_VCP_LOOKBACK).max()
    vcp_lowest = close.rolling(SEPA_VCP_LOOKBACK).min()
    vcp_range_pct = ((vcp_highest - vcp_lowest) / close) * 100.0
    vcp_trigger = vcp_range_pct < SEPA_VCP_THRESHOLD_PCT

    latest_index = bars.index[-1]
    latest_close = float(close.loc[latest_index])
    latest_ma50 = ma50.loc[latest_index]
    latest_ma150 = ma150.loc[latest_index]
    latest_ma200 = ma200.loc[latest_index]
    latest_high_52wk = high_52wk.loc[latest_index]
    latest_low_52wk = low_52wk.loc[latest_index]
    latest_buy_vol = buy_vol.loc[latest_index]
    latest_sell_vol = sell_vol.loc[latest_index]
    latest_rpr = rpr_score_series.loc[latest_index]
    latest_vcp_range_pct = vcp_range_pct.loc[latest_index]
    latest_vcp_trigger = vcp_trigger.loc[latest_index]
    ma200_slope_base = ma200.shift(SEPA_MA200_SLOPE_LOOKBACK).loc[latest_index]

    required_values = [
        latest_ma50,
        latest_ma150,
        latest_ma200,
        latest_high_52wk,
        latest_low_52wk,
        latest_buy_vol,
        latest_sell_vol,
        latest_rpr,
        latest_vcp_range_pct,
        ma200_slope_base,
    ]
    if any(pd.isna(value) for value in required_values):
        return None

    c1 = latest_close > float(latest_ma150) and latest_close > float(latest_ma200)
    c2 = float(latest_ma150) > float(latest_ma200)
    c3 = float(latest_ma200) > float(ma200_slope_base)
    c4 = float(latest_ma50) > float(latest_ma150) and float(latest_ma50) > float(latest_ma200)
    c5 = latest_close > float(latest_ma50)
    c6 = latest_close > float(latest_low_52wk) * 1.25
    c7 = latest_close > float(latest_high_52wk) * 0.75
    tpr_pass = bool(c1 and c2 and c3 and c4 and c5 and c6 and c7)
    tpr_status = "PASSED" if tpr_pass else "WAIT"

    dist_50 = ((latest_close - float(latest_ma50)) / float(latest_ma50)) * 100.0 if float(latest_ma50) != 0.0 else 0.0
    if dist_50 < 0:
        buy_risk_status = "Broken"
    elif dist_50 <= 15:
        buy_risk_status = "Low Risk"
    elif dist_50 <= 25:
        buy_risk_status = "Caution"
    else:
        buy_risk_status = "Extended"

    pressure_buying = float(latest_buy_vol) > float(latest_sell_vol)
    pressure_status = "Buying" if pressure_buying else "Selling"

    latest_rpr_value = float(latest_rpr)
    if latest_rpr_value > 80:
        rpr_status = "Leader"
    elif latest_rpr_value > 70:
        rpr_status = "Good"
    else:
        rpr_status = "Laggard"

    recent_window = vcp_trigger.tail(max(1, int(recent_signal_lookback_bars)))
    recent_true = recent_window[recent_window.fillna(False)]
    recent_signal_index = recent_true.index[-1] if not recent_true.empty else None

    recent_high = None
    recent_low = None
    recent_close = None
    if recent_signal_index is not None:
        signal_row = bars.loc[recent_signal_index]
        recent_high = float(signal_row["High"])
        recent_low = float(signal_row["Low"])
        recent_close = float(signal_row["Close"])

    return SepaDashboardSnapshot(
        snapshot_date=latest_index.date().isoformat(),
        benchmark_ticker=benchmark_ticker,
        latest_close=latest_close,
        ma50=float(latest_ma50),
        ma150=float(latest_ma150),
        ma200=float(latest_ma200),
        high_52wk=float(latest_high_52wk),
        low_52wk=float(latest_low_52wk),
        tpr_pass=tpr_pass,
        tpr_status=tpr_status,
        buy_risk_status=buy_risk_status,
        buy_risk_distance_pct=float(dist_50),
        pressure_status=pressure_status,
        pressure_buying=pressure_buying,
        buy_volume_20d=float(latest_buy_vol),
        sell_volume_20d=float(latest_sell_vol),
        rpr_score=latest_rpr_value,
        rpr_status=rpr_status,
        vcp_status="SQUEEZE" if bool(latest_vcp_trigger) else "Normal",
        vcp_trigger=bool(latest_vcp_trigger),
        vcp_range_pct=float(latest_vcp_range_pct),
        recent_vcp_signal=recent_signal_index is not None,
        recent_vcp_signal_date=recent_signal_index.date().isoformat() if recent_signal_index is not None else None,
        recent_vcp_signal_high=recent_high,
        recent_vcp_signal_low=recent_low,
        recent_vcp_signal_close=recent_close,
    )


def find_recent_sepa_vcp_hit(
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    benchmark_ticker: str,
    recent_signal_lookback_bars: int = SEPA_SIGNAL_LOOKBACK_BARS,
) -> SepaVcpHit | None:
    snapshot = build_sepa_dashboard_snapshot(
        frame,
        benchmark_frame,
        benchmark_ticker=benchmark_ticker,
        recent_signal_lookback_bars=recent_signal_lookback_bars,
    )
    if snapshot is None or not snapshot.recent_vcp_signal or snapshot.recent_vcp_signal_date is None:
        return None

    reasons = [
        f"5D VCP squeeze hit within last {recent_signal_lookback_bars} bars",
        f"TPR {snapshot.tpr_status}",
        f"Buy risk {snapshot.buy_risk_status} ({snapshot.buy_risk_distance_pct:.1f}% from 50D)",
        f"Pressure {snapshot.pressure_status}",
        f"RPR {snapshot.rpr_score:.1f} ({snapshot.rpr_status})",
        f"Latest 5D range {snapshot.vcp_range_pct:.2f}%",
    ]

    return SepaVcpHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=snapshot.recent_vcp_signal_date,
        benchmark_ticker=benchmark_ticker,
        signal_kind="recent_vcp_squeeze",
        current_price=float(snapshot.latest_close),
        high_price=float(snapshot.recent_vcp_signal_high or 0.0),
        low_price=float(snapshot.recent_vcp_signal_low or 0.0),
        trigger_price=float(snapshot.recent_vcp_signal_high or 0.0),
        stop_price=float(snapshot.recent_vcp_signal_low or 0.0),
        tpr_pass=snapshot.tpr_pass,
        tpr_status=snapshot.tpr_status,
        buy_risk_status=snapshot.buy_risk_status,
        buy_risk_distance_pct=snapshot.buy_risk_distance_pct,
        pressure_status=snapshot.pressure_status,
        pressure_buying=snapshot.pressure_buying,
        buy_volume_20d=snapshot.buy_volume_20d,
        sell_volume_20d=snapshot.sell_volume_20d,
        rpr_score=snapshot.rpr_score,
        rpr_status=snapshot.rpr_status,
        vcp_status=snapshot.vcp_status,
        vcp_trigger=snapshot.vcp_trigger,
        vcp_range_pct=snapshot.vcp_range_pct,
        ma50=snapshot.ma50,
        ma150=snapshot.ma150,
        ma200=snapshot.ma200,
        high_52wk=snapshot.high_52wk,
        low_52wk=snapshot.low_52wk,
        reasons=reasons,
    )


def run_sepa_vcp_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> SepaVcpScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[SepaVcpHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting SEPA VCP screen: "
        f"total={total_tickers}, recent_window={SEPA_SIGNAL_LOOKBACK_BARS}, vcp_lookback={SEPA_VCP_LOOKBACK}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        benchmark_frame = pd.DataFrame()
        try:
            benchmark_financials = cookstock.cookFinancials(
                config.benchmark_ticker,
                benchmarkTicker=config.benchmark_ticker,
                historyLookbackDays=SEPA_HISTORY_DAYS,
            )
            benchmark_frame = _build_price_frame(benchmark_financials)
        except Exception as exc:
            benchmark_frame = pd.DataFrame()
            failures.append({"ticker": config.benchmark_ticker.upper(), "error": f"benchmark load failed: {exc}"})

        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=SEPA_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    if benchmark_frame.empty:
                        raise ValueError("benchmark frame unavailable")
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=SEPA_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_recent_sepa_vcp_hit(
                        frame,
                        benchmark_frame,
                        ticker=ticker,
                        benchmark_ticker=config.benchmark_ticker,
                    )
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no recent SEPA VCP squeeze | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed SEPA VCP "
                        f"{hit.signal_date} RPR {hit.rpr_score:.1f} {hit.buy_risk_status} | passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished SEPA VCP screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return SepaVcpScreenResult(
        run_date=run_date.isoformat(),
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
