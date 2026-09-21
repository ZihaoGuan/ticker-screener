from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker


STOCKBEE_MOVER_HISTORY_DAYS = 10
DB_BATCH_SIZE = 400

STOCKBEE_MOVER_PROFILES: dict[str, dict[str, object]] = {
    "stockbee_9m_movers": {"label": "Stockbee 9 Million Movers", "kind": "volume", "minimum": 9_000_000},
    "stockbee_20pct_weekly_movers": {"label": "Stockbee 20% Weekly Movers", "kind": "weekly_return", "minimum": 20.0},
    "stockbee_4pct_daily_movers": {"label": "Stockbee 4% Daily Movers", "kind": "daily_return", "minimum": 4.0},
}


@dataclass(frozen=True)
class StockbeeMoverHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    profile: str
    profile_label: str
    current_price: float
    volume: int
    daily_change_pct: float
    weekly_change_pct: float
    trigger_value: float
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StockbeeMoverScreenResult:
    run_date: str
    profile: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[StockbeeMoverHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "profile": self.profile,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [hit.to_dict() for hit in self.hits],
        }


def _normalize_bars(frame: pd.DataFrame) -> pd.DataFrame:
    columns = {str(column).lower(): column for column in frame.columns}
    required = ("close", "volume")
    if any(column not in columns for column in required):
        return pd.DataFrame()
    normalized = frame[[columns["close"], columns["volume"]]].copy()
    normalized.columns = ["Close", "Volume"]
    normalized = normalized.dropna(subset=["Close", "Volume"]).sort_index()
    if not isinstance(normalized.index, pd.DatetimeIndex):
        normalized.index = pd.to_datetime(normalized.index)
    return normalized


def _pct_change(current: float, prior: float) -> float:
    return ((current / prior) - 1.0) * 100.0 if prior > 0 else 0.0


def evaluate_stockbee_mover_frame(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    profile: str,
) -> StockbeeMoverHit | None:
    profile_config = STOCKBEE_MOVER_PROFILES.get(profile)
    if profile_config is None:
        raise ValueError(f"Unknown Stockbee mover profile: {profile}")
    bars = _normalize_bars(frame)
    minimum_bars = 6 if profile_config["kind"] == "weekly_return" else 2
    if len(bars) < minimum_bars:
        return None

    latest = bars.iloc[-1]
    current_price = float(latest["Close"])
    volume = int(float(latest["Volume"]))
    previous_close = float(bars.iloc[-2]["Close"])
    weekly_close = float(bars.iloc[-6]["Close"]) if len(bars) >= 6 else previous_close
    daily_change_pct = _pct_change(current_price, previous_close)
    weekly_change_pct = _pct_change(current_price, weekly_close)
    kind = str(profile_config["kind"])
    minimum = float(profile_config["minimum"])
    trigger_value = float(volume) if kind == "volume" else weekly_change_pct if kind == "weekly_return" else daily_change_pct
    if trigger_value < minimum:
        return None

    label = str(profile_config["label"])
    if kind == "volume":
        reason = f"Volume {volume:,} shares meets the 9,000,000-share threshold"
    elif kind == "weekly_return":
        reason = f"Five-session change {weekly_change_pct:.2f}% meets the 20% threshold"
    else:
        reason = f"Daily close change {daily_change_pct:.2f}% meets the 4% threshold"
    return StockbeeMoverHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=bars.index[-1].date().isoformat(),
        profile=profile,
        profile_label=label,
        current_price=round(current_price, 4),
        volume=volume,
        daily_change_pct=round(daily_change_pct, 2),
        weekly_change_pct=round(weekly_change_pct, 2),
        trigger_value=round(trigger_value, 4),
        reasons=[reason],
    )


def run_stockbee_mover_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    profile: str,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> StockbeeMoverScreenResult:
    if profile not in STOCKBEE_MOVER_PROFILES:
        raise ValueError(f"Unknown Stockbee mover profile: {profile}")
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    hits: list[StockbeeMoverHit] = []
    failures: list[dict[str, str]] = []
    for start in range(0, len(tickers), DB_BATCH_SIZE):
        batch = tickers[start : start + DB_BATCH_SIZE]
        symbols = [ticker.symbol for ticker in batch]
        frames = load_many_ticker_windows(symbols, run_date, STOCKBEE_MOVER_HISTORY_DAYS, database_url=resolved_database_url)
        metadata = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
        for ticker in batch:
            frame = frames.get(ticker.symbol)
            if frame is None or frame.empty:
                failures.append({"ticker": ticker.symbol, "error": "missing_daily_bars"})
                continue
            values = metadata.get(ticker.symbol, {})
            runtime_ticker = UniverseTicker(
                symbol=ticker.symbol,
                sector=str(values["sector"]) if values.get("sector") else ticker.sector,
                industry=str(values["industry"]) if values.get("industry") else ticker.industry,
                exchange=str(values["exchange"]) if values.get("exchange") else ticker.exchange,
            )
            try:
                hit = evaluate_stockbee_mover_frame(frame, ticker=runtime_ticker, profile=profile)
            except Exception as exc:
                failures.append({"ticker": ticker.symbol, "error": str(exc)})
                continue
            if hit is not None:
                hits.append(hit)
    hits.sort(key=lambda hit: (hit.trigger_value, hit.volume), reverse=True)
    return StockbeeMoverScreenResult(
        run_date=run_date.isoformat(), profile=profile, total_tickers=len(tickers),
        passed_tickers=len(hits), failed_tickers=failures, hits=hits,
    )
