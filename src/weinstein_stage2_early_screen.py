from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .universe import UniverseTicker


WEINSTEIN_STAGE2_EARLY_HISTORY_DAYS = 420
WEINSTEIN_STAGE2_EARLY_MA_LENGTH = 30
WEINSTEIN_STAGE2_EARLY_SLOPE_LOOKBACK = 5
WEINSTEIN_STAGE2_EARLY_PRICE_BAND_PCT = 0.03
WEINSTEIN_STAGE2_EARLY_SLOPE_THRESHOLD_PCT = 0.001
WEINSTEIN_STAGE2_EARLY_MIN_BARS_STAGE = 5
WEINSTEIN_STAGE2_EARLY_MAX_BARS_STAGE = 18
WEINSTEIN_STAGE2_EARLY_LATE_EXTENSION_PCT = 0.08


@dataclass(frozen=True)
class WeinsteinStage2EarlyHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    previous_stage: str
    current_stage: str
    maturity: str
    sentiment: str
    weekly_close: float
    weekly_ma30: float
    slope_ratio: float
    extension_pct: float
    run_length_weeks: int
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class WeinsteinStage2EarlyScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[WeinsteinStage2EarlyHit]

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


def _to_weekly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    weekly = frame.resample("W-FRI").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    return weekly.dropna(subset=["Open", "High", "Low", "Close"])


def _stage_name(stage: int) -> str:
    return {
        1: "Stage 1 - Base",
        2: "Stage 2 - Advance",
        3: "Stage 3 - Top",
        4: "Stage 4 - Decline",
    }.get(int(stage), f"Stage {stage}")


def _sentiment_name(stage: int, maturity: str) -> str:
    if stage == 2:
        return "Extended Bullish" if maturity == "Late" else "Bullish"
    if stage == 4:
        return "Extended Bearish" if maturity == "Late" else "Bearish"
    if stage == 3:
        return "Cautious / Distribution"
    return "Balanced / Basing"


def find_weinstein_stage2_early_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
) -> WeinsteinStage2EarlyHit | None:
    daily = _normalize_bars_frame(frame)
    if daily.empty:
        return None
    weekly = _to_weekly_frame(daily)
    min_weeks = WEINSTEIN_STAGE2_EARLY_MA_LENGTH + WEINSTEIN_STAGE2_EARLY_SLOPE_LOOKBACK + 2
    if len(weekly) < min_weeks:
        return None

    close = weekly["Close"].astype(float)
    ma30 = close.ewm(span=WEINSTEIN_STAGE2_EARLY_MA_LENGTH, adjust=False).mean()
    stage_history: list[int] = []
    run_lengths: list[int] = []
    previous_distinct_stage = 1
    previous_distinct_run_length = 0

    for idx in range(len(weekly)):
        ma_value = ma30.iloc[idx]
        prior_idx = idx - WEINSTEIN_STAGE2_EARLY_SLOPE_LOOKBACK
        if pd.isna(ma_value) or prior_idx < 0:
            stage = 1 if not stage_history else stage_history[-1]
        else:
            lookback_ma = ma30.iloc[prior_idx]
            if pd.isna(lookback_ma) or float(ma_value) == 0.0:
                stage = stage_history[-1] if stage_history else 1
            else:
                slope_ratio = (float(ma_value) - float(lookback_ma)) / float(ma_value)
                close_value = float(close.iloc[idx])
                above = close_value > float(ma_value) * (1.0 + WEINSTEIN_STAGE2_EARLY_PRICE_BAND_PCT)
                below = close_value < float(ma_value) * (1.0 - WEINSTEIN_STAGE2_EARLY_PRICE_BAND_PCT)
                flat = abs(slope_ratio) <= WEINSTEIN_STAGE2_EARLY_SLOPE_THRESHOLD_PCT
                previous_stage = stage_history[-1] if stage_history else 1
                if slope_ratio > WEINSTEIN_STAGE2_EARLY_SLOPE_THRESHOLD_PCT and above:
                    stage = 2
                elif slope_ratio < -WEINSTEIN_STAGE2_EARLY_SLOPE_THRESHOLD_PCT and below:
                    stage = 4
                elif flat:
                    stage = 3 if previous_stage == 2 else 1
                else:
                    stage = previous_stage
        run_length = 1 if not stage_history or stage != stage_history[-1] else run_lengths[-1] + 1
        if stage_history and stage != stage_history[-1]:
            previous_distinct_stage = stage_history[-1]
            previous_distinct_run_length = run_lengths[-1]
        stage_history.append(stage)
        run_lengths.append(run_length)

    current_stage = stage_history[-1]
    previous_stage = previous_distinct_stage
    run_length = run_lengths[-1]
    current_close = float(close.iloc[-1])
    current_ma = float(ma30.iloc[-1])
    lookback_ma = float(ma30.iloc[-1 - WEINSTEIN_STAGE2_EARLY_SLOPE_LOOKBACK])
    slope_ratio = ((current_ma - lookback_ma) / current_ma) if current_ma else 0.0
    extension_pct = ((current_close / (current_ma * (1.0 + WEINSTEIN_STAGE2_EARLY_PRICE_BAND_PCT))) - 1.0) if current_ma else 0.0
    is_late_by_extension = current_stage == 2 and extension_pct >= WEINSTEIN_STAGE2_EARLY_LATE_EXTENSION_PCT

    if run_length < WEINSTEIN_STAGE2_EARLY_MIN_BARS_STAGE and not is_late_by_extension:
        maturity = "Early"
    elif run_length <= WEINSTEIN_STAGE2_EARLY_MAX_BARS_STAGE and not is_late_by_extension:
        maturity = "Mature"
    else:
        maturity = "Late"

    if current_stage != 2 or maturity != "Early" or previous_stage != 1:
        return None

    reasons = [
        f"current weekly stage is {_stage_name(current_stage)} and maturity is {maturity}",
        f"previous distinct weekly stage was {_stage_name(previous_stage)} for {previous_distinct_run_length} weeks",
        f"30W EMA {current_ma:.2f}, weekly close {current_close:.2f}, slope ratio {slope_ratio * 100:.2f}%",
        f"run length {run_length} weeks, extension beyond +3% band {extension_pct * 100:.2f}%",
    ]

    return WeinsteinStage2EarlyHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=weekly.index[-1].date().isoformat(),
        previous_stage=_stage_name(previous_stage),
        current_stage=_stage_name(current_stage),
        maturity=maturity,
        sentiment=_sentiment_name(current_stage, maturity),
        weekly_close=current_close,
        weekly_ma30=current_ma,
        slope_ratio=slope_ratio * 100.0,
        extension_pct=extension_pct * 100.0,
        run_length_weeks=run_length,
        reasons=reasons,
    )


def run_weinstein_stage2_early_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
) -> WeinsteinStage2EarlyScreenResult:
    cookstock = load_configured_cookstock(config)
    hits: list[WeinsteinStage2EarlyHit] = []
    failures: list[dict[str, str]] = []
    total_tickers = len(tickers)
    run_date = as_of_date or dt.date.today()

    print(
        "starting weinstein stage2 early screen: "
        f"total={total_tickers}, ma_length={WEINSTEIN_STAGE2_EARLY_MA_LENGTH}, slope_lookback={WEINSTEIN_STAGE2_EARLY_SLOPE_LOOKBACK}"
    )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=WEINSTEIN_STAGE2_EARLY_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f"[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}")
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=WEINSTEIN_STAGE2_EARLY_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    hit = find_weinstein_stage2_early_hit(frame, ticker=ticker)
                    if hit is None:
                        print(f"[{position}/{total_tickers}] {ticker.symbol} filtered: no weinstein stage 2 early | passed={len(hits)}")
                        continue
                    hits.append(hit)
                    print(
                        f"[{position}/{total_tickers}] {ticker.symbol} passed weinstein stage 2 early | "
                        f"run_length={hit.run_length_weeks} slope={hit.slope_ratio:.2f}% passed={len(hits)}"
                    )
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})
                    print(f"[{position}/{total_tickers}] {ticker.symbol} failed: {exc}")

    print(f"finished weinstein stage2 early screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return WeinsteinStage2EarlyScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
