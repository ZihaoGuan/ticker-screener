from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .market_data_access import (
    db_frame_has_recent_coverage,
    load_daily_bars_frame_from_db,
    resolve_market_data_source,
)
from .webapp.repositories.my_picks_repository import MyPicksRepository


EMA9_PERIOD = 9
EMA21_PERIOD = 21
SMA50_PERIOD = 50
MY_PICKS_SMA50_RECLAIM_HISTORY_DAYS = 140


@dataclass(frozen=True)
class MyPicksSma50ReclaimHit:
    ticker: str
    signal_date: str
    current_price: float
    session_open: float
    session_high: float
    session_low: float
    ema9: float
    ema21: float
    sma50: float
    notes: str
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MyPicksSma50ReclaimScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[MyPicksSma50ReclaimHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _load_yfinance():
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed.") from exc
    return yf


def _normalize_history_frame(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return pd.DataFrame()
    normalized = history.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = normalized.columns.get_level_values(0)
    normalized = normalized.rename(columns=str)
    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [column for column in required if column not in normalized.columns]
    if missing:
        return pd.DataFrame()
    normalized = normalized.loc[:, required].copy()
    normalized.index = pd.to_datetime(normalized.index)
    for column in required:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalized.dropna(subset=required).sort_index()
    return normalized


def _fetch_yfinance_history(ticker: str, *, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    yf = _load_yfinance()
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    normalized = _normalize_history_frame(history)
    if normalized.empty:
        raise RuntimeError("No daily history returned.")
    return normalized


def _load_price_frame(
    ticker: str,
    *,
    start_date: dt.date,
    end_date: dt.date,
    market_data_source: str,
    database_url: str,
) -> pd.DataFrame:
    resolved_source = resolve_market_data_source(market_data_source)
    if resolved_source == "database-first":
        db_frame = load_daily_bars_frame_from_db(ticker, start_date, end_date, database_url=database_url)
        if db_frame is not None and db_frame_has_recent_coverage(db_frame, end_date) and len(db_frame) >= SMA50_PERIOD:
            return db_frame.loc[:, ["Open", "High", "Low", "Close", "Volume"]].copy()
    return _fetch_yfinance_history(ticker, start_date=start_date, end_date=end_date)


def find_my_picks_sma50_reclaim_hit(
    frame: pd.DataFrame,
    *,
    ticker: str,
    notes: str = "",
) -> MyPicksSma50ReclaimHit | None:
    if frame is None or frame.empty or len(frame) < SMA50_PERIOD:
        return None

    bars = frame.copy().sort_index()
    close = pd.to_numeric(bars["Close"], errors="coerce")
    open_ = pd.to_numeric(bars["Open"], errors="coerce")
    high = pd.to_numeric(bars["High"], errors="coerce")
    low = pd.to_numeric(bars["Low"], errors="coerce")
    volume = pd.to_numeric(bars["Volume"], errors="coerce")
    bars = pd.DataFrame({"Open": open_, "High": high, "Low": low, "Close": close, "Volume": volume}, index=bars.index)
    bars = bars.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if bars.empty or len(bars) < SMA50_PERIOD:
        return None

    close = bars["Close"]
    ema9 = close.ewm(span=EMA9_PERIOD, adjust=False).mean()
    ema21 = close.ewm(span=EMA21_PERIOD, adjust=False).mean()
    sma50 = close.rolling(SMA50_PERIOD).mean()

    latest = bars.iloc[-1]
    latest_ema9 = float(ema9.iloc[-1]) if pd.notna(ema9.iloc[-1]) else None
    latest_ema21 = float(ema21.iloc[-1]) if pd.notna(ema21.iloc[-1]) else None
    latest_sma50 = float(sma50.iloc[-1]) if pd.notna(sma50.iloc[-1]) else None
    if latest_ema9 is None or latest_ema21 is None or latest_sma50 is None:
        return None

    latest_close = float(latest["Close"])
    latest_low = float(latest["Low"])
    reclaimed_sma50 = latest_low <= latest_sma50 and latest_close > latest_sma50
    if not reclaimed_sma50:
        return None
    if latest_ema9 <= latest_sma50 or latest_ema21 <= latest_sma50:
        return None

    signal_date = pd.Timestamp(bars.index[-1]).date().isoformat()
    reasons = [
        f"Latest bar reclaimed the 50 SMA intraday and closed back above it ({latest_close:.2f} vs SMA50 {latest_sma50:.2f})",
        f"EMA9 {latest_ema9:.2f} and EMA21 {latest_ema21:.2f} both remain above SMA50 {latest_sma50:.2f}",
    ]
    if str(notes or "").strip():
        reasons.append(f"My Picks note: {str(notes).strip()}")

    return MyPicksSma50ReclaimHit(
        ticker=str(ticker or "").strip().upper(),
        signal_date=signal_date,
        current_price=latest_close,
        session_open=float(latest["Open"]),
        session_high=float(latest["High"]),
        session_low=latest_low,
        ema9=latest_ema9,
        ema21=latest_ema21,
        sma50=latest_sma50,
        notes=str(notes or "").strip(),
        reasons=reasons,
    )


def run_my_picks_sma50_reclaim_screen(
    *,
    as_of_date: dt.date | None = None,
    market_data_source: str = "database-first",
    database_url: str = "",
    tickers: list[str] | None = None,
) -> MyPicksSma50ReclaimScreenResult:
    repository = MyPicksRepository(database_url=database_url)
    run_date = as_of_date or dt.date.today()
    requested_tickers = [str(item).strip().upper() for item in (tickers or []) if str(item).strip()]
    if requested_tickers:
        notes_by_ticker = {ticker: "" for ticker in requested_tickers}
        ordered_tickers = requested_tickers
    else:
        picks = repository.list_picks()
        notes_by_ticker: dict[str, str] = {}
        ordered_tickers = []
        for row in picks:
            ticker = str(row.get("ticker") or "").strip().upper()
            if not ticker or ticker in notes_by_ticker:
                continue
            notes_by_ticker[ticker] = str(row.get("notes") or "").strip()
            ordered_tickers.append(ticker)

    total_tickers = len(ordered_tickers)
    hits: list[MyPicksSma50ReclaimHit] = []
    failures: list[dict[str, str]] = []
    start_date = run_date - dt.timedelta(days=MY_PICKS_SMA50_RECLAIM_HISTORY_DAYS)

    print(
        "starting my picks 50 SMA reclaim screen: "
        f"total={total_tickers}, market_data_source={resolve_market_data_source(market_data_source)}, "
        f"history_days={MY_PICKS_SMA50_RECLAIM_HISTORY_DAYS}"
    )

    for position, ticker in enumerate(ordered_tickers, start=1):
        print(f"[{position}/{total_tickers}] screening {ticker} | passed={len(hits)}")
        try:
            frame = _load_price_frame(
                ticker,
                start_date=start_date,
                end_date=run_date,
                market_data_source=market_data_source,
                database_url=database_url,
            )
            hit = find_my_picks_sma50_reclaim_hit(frame, ticker=ticker, notes=notes_by_ticker.get(ticker, ""))
            if hit is None:
                print(f"[{position}/{total_tickers}] {ticker} filtered: no 50 SMA reclaim setup | passed={len(hits)}")
                continue
            hits.append(hit)
            print(f"[{position}/{total_tickers}] {ticker} passed my picks 50 SMA reclaim | passed={len(hits)}")
        except Exception as exc:
            failures.append({"ticker": ticker, "error": str(exc)})
            print(f"[{position}/{total_tickers}] {ticker} failed: {exc}")

    print(f"finished my picks 50 SMA reclaim screen: passed={len(hits)}, failed={len(failures)}, total={total_tickers}")
    return MyPicksSma50ReclaimScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
