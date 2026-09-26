from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows, load_ticker_metadata_map, resolve_database_url
from .universe import UniverseTicker


RMV_HISTORY_DAYS = 260
RMV_TIGHT_LEVEL = 15.0
RMV_WARM_LEVEL = 20.0


@dataclass(frozen=True)
class RmvSnapshot:
    rmv: float
    rmv_smooth: float
    rank_tier: int
    a_plus: bool
    tightness_setup: bool
    vdu_setup: bool
    trough: bool
    tight_streak: int
    bottom_breakout: bool
    breakout_cross: bool


@dataclass(frozen=True)
class RmvHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    signal_kind: str
    rmv: float
    rmv_smooth: float
    rank_tier: int
    a_plus: bool
    tightness_setup: bool
    vdu_setup: bool
    trough: bool
    tight_streak: int
    bottom_breakout: bool
    breakout_cross: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class RmvScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[RmvHit]

    def to_dict(self) -> dict[str, object]:
        return {"run_date": self.run_date, "total_tickers": self.total_tickers, "passed_tickers": self.passed_tickers, "failed_tickers": self.failed_tickers, "hits": [item.to_dict() for item in self.hits]}


def _bars(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("Open", "High", "Low", "Close", "Volume")
    columns = {str(column).lower(): column for column in frame.columns}
    if any(name.lower() not in columns for name in required):
        return pd.DataFrame()
    result = frame[[columns[name.lower()] for name in required]].copy()
    result.columns = required
    result = result.dropna(subset=required).sort_index()
    if not isinstance(result.index, pd.DatetimeIndex):
        result.index = pd.to_datetime(result.index)
    return result


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    previous_close = close.shift(1)
    true_range = pd.concat([high - low, (high - previous_close).abs(), (low - previous_close).abs()], axis=1).max(axis=1)
    return true_range.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def _trailing_true(values: pd.Series) -> int:
    count = 0
    for value in reversed(values.fillna(False).tolist()):
        if bool(value):
            count += 1
        else:
            break
    return count


def evaluate_rmv(frame: pd.DataFrame) -> RmvSnapshot | None:
    bars = _bars(frame)
    if len(bars) < 220:
        return None
    open_, high, low, close, volume = (bars[name].astype(float) for name in ("Open", "High", "Low", "Close", "Volume"))
    day_range = high - low
    safe_range = day_range.clip(lower=max(0.001, float(close.iloc[-1]) * 0.0001))
    body = (open_ - close).abs()
    atr3_prior = _atr(high, low, close, 3).shift(1)
    atr5_prior = _atr(high, low, close, 5).shift(1)
    baseline = pd.concat([atr3_prior, day_range.rolling(3).max().shift(1)], axis=1).max(axis=1)
    open_pos = (open_ - low) / safe_range * 100
    close_pos = (close - low) / safe_range * 100
    use_body = (day_range <= atr5_prior * 0.75) | ((open_pos > 50) & (close_pos > 50))
    rmv = ((body.where(use_body, day_range) / baseline.clip(lower=max(0.001, float(close.iloc[-1]) * 0.0001))) * 50).clip(upper=100)
    smooth = rmv.rolling(5).mean()

    ema7, ema10, ema21, ema50, ema200 = (close.ewm(span=length, adjust=False).mean() for length in (7, 10, 21, 50, 200))
    pct_change = close.pct_change() * 100
    runaway = ((open_ == low).rolling(10).sum() >= 3) & ((close == high).rolling(10).sum() >= 3)
    tightness = (
        (day_range <= _atr(high, low, close, 3)) & (low <= ema10 * 1.04) & (close >= ema10 * 0.99)
        & (close >= ema21) & (close >= ema50) & (close >= ema200) & ~runaway & (close >= open_ * 0.985)
        & (pct_change <= 4) & (pct_change >= -1) & (pct_change.shift(1) <= 7.5)
        & ((pct_change + pct_change.shift(1)) <= 7) & (close >= high.shift(1).rolling(7).max() * 0.9)
        & ((close > close.shift(1)).rolling(9).sum() != 9)
    )
    volume_ema21 = volume.ewm(span=21, adjust=False).mean()
    vdu = (
        (volume <= volume_ema21 * 0.5) | (volume.shift(1) <= volume_ema21 * 0.5) | (volume.shift(2) <= volume_ema21 * 0.6)
        | (volume == volume.rolling(90).min()) | (volume == volume.rolling(365).min())
    )
    vdu_setup = (
        vdu & (ema21 >= ema50 * 1.03) & (ema50 >= ema200) & (close <= ema10 * 1.06) & ~runaway & (pct_change <= 4)
        & (pct_change >= -2) & (pct_change.shift(1) <= 4) & (pct_change.shift(1) > -3) & (ema10 > ema10.shift(3))
        & (ema7 > ema10) & (ema10 > ema21) & (ema21 > ema50) & (ema50 > ema200) & ((pct_change <= -5).rolling(7).sum() == 0)
    )
    a_plus = bool((tightness.iloc[-1] or vdu_setup.iloc[-1]) and rmv.iloc[-1] <= RMV_TIGHT_LEVEL)
    trough = bool(rmv.iloc[-1] == rmv.rolling(5).min().iloc[-1])
    latest_rmv = float(rmv.iloc[-1])
    rank = 1 if a_plus and trough else 2 if a_plus else 3 if trough else 4 if latest_rmv <= RMV_WARM_LEVEL else 0
    bottom_breakout = bool(smooth.rolling(5).min().iloc[-1] <= RMV_TIGHT_LEVEL and latest_rmv >= 50)
    breakout_cross = bool(len(rmv) > 1 and rmv.iloc[-2] < 50 <= latest_rmv)
    return RmvSnapshot(
        rmv=latest_rmv, rmv_smooth=float(smooth.iloc[-1]), rank_tier=rank, a_plus=a_plus,
        tightness_setup=bool(tightness.iloc[-1]), vdu_setup=bool(vdu_setup.iloc[-1]), trough=trough,
        tight_streak=_trailing_true(rmv < RMV_TIGHT_LEVEL), bottom_breakout=bottom_breakout, breakout_cross=breakout_cross,
    )


def find_rmv_hit(frame: pd.DataFrame, *, ticker: UniverseTicker, signal_date: dt.date) -> RmvHit | None:
    snapshot = evaluate_rmv(frame)
    if snapshot is None or (snapshot.rank_tier == 0 and not snapshot.bottom_breakout):
        return None
    signal_kind = "release" if snapshot.rank_tier == 0 else {1: "confluence", 2: "a_plus", 3: "trough", 4: "viable"}[snapshot.rank_tier]
    reasons = [f"RMV {snapshot.rmv:.1f}; smoothed {snapshot.rmv_smooth:.1f}", f"rank {snapshot.rank_tier or 'release'}; tight streak {snapshot.tight_streak} bars"]
    if snapshot.a_plus:
        reasons.append(f"A+ via {'Tightness' if snapshot.tightness_setup else ''}{' + ' if snapshot.tightness_setup and snapshot.vdu_setup else ''}{'VDU' if snapshot.vdu_setup else ''}")
    if snapshot.bottom_breakout:
        reasons.append("Smoothed RMV bottom followed by expansion")
    return RmvHit(ticker=ticker.symbol, sector=ticker.sector, industry=ticker.industry, exchange=ticker.exchange, signal_date=signal_date.isoformat(), current_price=float(_bars(frame)["Close"].iloc[-1]), signal_kind=signal_kind, reasons=reasons, **asdict(snapshot))


def run_rmv_screen(config: AppConfig, tickers: list[UniverseTicker], *, as_of_date: dt.date | None = None, database_url: str | None = None) -> RmvScreenResult:
    del config
    run_date = as_of_date or dt.date.today()
    symbols = [ticker.symbol.upper() for ticker in tickers]
    frames = load_many_ticker_windows(symbols, run_date, RMV_HISTORY_DAYS, database_url=resolve_database_url(database_url))
    metadata = load_ticker_metadata_map(symbols, database_url=resolve_database_url(database_url))
    hits: list[RmvHit] = []
    failures: list[dict[str, str]] = []
    for ticker in tickers:
        symbol = ticker.symbol.upper()
        frame = frames.get(symbol)
        if frame is None or not db_frame_has_recent_coverage(frame, run_date):
            continue
        try:
            meta = metadata.get(symbol, {})
            hit = find_rmv_hit(frame, ticker=UniverseTicker(symbol=symbol, sector=ticker.sector or meta.get("sector"), industry=ticker.industry or meta.get("industry"), exchange=ticker.exchange or meta.get("exchange")), signal_date=run_date)
            if hit:
                hits.append(hit)
        except Exception as exc:
            failures.append({"ticker": symbol, "error": str(exc)})
    priorities = {"confluence": 0, "a_plus": 1, "trough": 2, "release": 3, "viable": 4}
    hits.sort(key=lambda item: (priorities[item.signal_kind], item.rmv, -item.tight_streak, item.ticker))
    return RmvScreenResult(run_date=run_date.isoformat(), total_tickers=len(tickers), passed_tickers=len(hits), failed_tickers=failures, hits=hits)
