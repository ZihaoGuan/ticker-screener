from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, load_configured_cookstock
from .market_data_access import (
    db_frame_has_recent_coverage,
    load_active_universe_from_db,
    load_many_ticker_windows,
    load_ticker_metadata_map,
    resolve_database_url,
)
from .ratings.repository import RatingsRepository
from .universe import UniverseTicker


QULLAMAGGIE_STRATEGY_ID = "qullamaggie"
QULLAMAGGIE_HISTORY_DAYS = 380
THREE_MONTH_SESSIONS = 63
SIX_MONTH_SESSIONS = 126
ONE_YEAR_SESSIONS = 252
ADR_PERIOD_DAYS = 20
AVERAGE_VOLUME_DAYS = 20
TIGHT_FLAG_DAYS = 10
MIN_RECENT_RETURN_PCT = 30.0
MIN_MARKET_CAP = 100_000_000.0
MIN_AVG_VOLUME = 1_000_000.0
MIN_ADR_PCT = 4.0
MIN_DAILY_RS_RATING = 80.0
MAX_DISTANCE_FROM_RECENT_HIGH_PCT = 5.0
MAX_DISTANCE_FROM_52W_HIGH_PCT = 15.0
MAX_TIGHT_FLAG_RANGE_PCT = 15.0
MAX_FAST_MA_DISTANCE_PCT = 5.0
MAX_BASE_RANGE_PCT = 25.0


@dataclass(frozen=True)
class QullamaggieSnapshot:
    matched: bool
    current_price: float
    market_cap: float | None
    avg_volume_20: float
    adr_pct_20: float
    daily_rs_rating: float | None
    return_3m_pct: float
    return_1m_pct: float
    return_6m_pct: float
    recent_return_pct: float
    sma10: float
    sma20: float
    sma50: float
    sma200: float
    high_3m: float
    high_6m: float
    high_52w: float
    distance_from_recent_high_pct: float
    distance_from_52w_high_pct: float
    tight_flag_range_pct: float
    fast_ma_distance_pct: float
    tight_flag: bool
    pullback_near_fast_ma: bool
    criteria_passed: int
    criteria_total: int
    criteria: dict[str, bool]
    reasons: list[str]
    setup_state: str
    pivot: float
    breakout_day_low: float | None
    impulse_return_pct: float
    consolidation_sessions: int
    higher_low: bool
    range_contracting: bool
    volume_contracting: bool
    rising_fast_mas: bool


@dataclass(frozen=True)
class QullamaggieHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    current_price: float
    market_cap: float
    avg_volume_20: float
    adr_pct_20: float
    daily_rs_rating: float
    return_3m_pct: float
    return_1m_pct: float
    return_6m_pct: float
    recent_return_pct: float
    sma10: float
    sma20: float
    sma50: float
    sma200: float
    high_52w: float
    distance_from_52w_high_pct: float
    tight_flag_range_pct: float
    fast_ma_distance_pct: float
    tight_flag: bool
    pullback_near_fast_ma: bool
    criteria_passed: int
    criteria_total: int
    reasons: list[str]
    setup_state: str
    pivot: float
    breakout_day_low: float | None
    impulse_return_pct: float
    consolidation_sessions: int
    higher_low: bool
    range_contracting: bool
    volume_contracting: bool
    rising_fast_mas: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class QullamaggieScreenResult:
    run_date: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[QullamaggieHit]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_date": self.run_date,
            "total_tickers": self.total_tickers,
            "passed_tickers": self.passed_tickers,
            "failed_tickers": self.failed_tickers,
            "hits": [item.to_dict() for item in self.hits],
        }


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = ("High", "Low", "Close", "Volume")
    available = {str(column).lower(): column for column in frame.columns}
    if any(column.lower() not in available for column in required):
        return pd.DataFrame()
    normalized = frame[[available[column.lower()] for column in required]].copy()
    normalized.columns = required
    if "open" in available:
        normalized["Open"] = frame[available["open"]]
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
            "High": [row.get("high") for row in rows],
            "Low": [row.get("low") for row in rows],
            "Close": [row.get("close") for row in rows],
            "Volume": [row.get("volume") for row in rows],
        }
    )
    return frame.dropna(subset=["Date", "High", "Low", "Close", "Volume"]).set_index("Date").sort_index()


def _return_pct(close: pd.Series, sessions: int) -> float:
    prior = float(close.iloc[-(sessions + 1)])
    return ((float(close.iloc[-1]) / prior) - 1.0) * 100.0 if prior > 0.0 else 0.0


def evaluate_qullamaggie_ep(frame: pd.DataFrame) -> dict[str, object] | None:
    """Daily EP candidate only; daily bars cannot establish opening-range confirmation."""
    bars = _normalize_price_frame(frame)
    if bars.empty or "Open" not in bars or len(bars) < 61:
        return None
    prior_close = float(bars["Close"].iloc[-2])
    gap_pct = (float(bars["Open"].iloc[-1]) / prior_close - 1.0) * 100.0
    volume_avg = float(bars["Volume"].iloc[-21:-1].mean())
    relative_volume = float(bars["Volume"].iloc[-1]) / volume_avg if volume_avg > 0 else 0.0
    prior = bars.iloc[-61:-1]
    prior_range_pct = (float(prior["High"].max()) / float(prior["Low"].min()) - 1.0) * 100.0
    if gap_pct < 10.0 or relative_volume < 2.0 or prior_range_pct > 60.0:
        return None
    return {"gap_pct": gap_pct, "relative_volume": relative_volume, "prior_range_pct": prior_range_pct, "state": "gap_candidate"}


def evaluate_qullamaggie_parabolic_short(frame: pd.DataFrame) -> dict[str, object] | None:
    """Daily extension candidate; exhaustion and entry confirmation require intraday bars."""
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < 30:
        return None
    close, volume = bars["Close"].astype(float), bars["Volume"].astype(float)
    current = float(close.iloc[-1])
    sma10, sma20 = float(close.rolling(10).mean().iloc[-1]), float(close.rolling(20).mean().iloc[-1])
    advance = (current / float(close.iloc[-21]) - 1.0) * 100.0
    consecutive_up_days = 0
    for change in close.diff().tail(5).iloc[::-1]:
        if change <= 0:
            break
        consecutive_up_days += 1
    extension10 = (current / sma10 - 1.0) * 100.0
    extension20 = (current / sma20 - 1.0) * 100.0
    relative_volume = float(volume.iloc[-1] / volume.iloc[-21:-1].mean()) if volume.iloc[-21:-1].mean() > 0 else 0.0
    if advance < 80.0 or consecutive_up_days < 4 or max(extension10, extension20) < 20.0 or relative_volume < 1.5:
        return None
    state = "exhaustion_watch" if consecutive_up_days == 5 and relative_volume >= 2.0 else "extended"
    return {"advance_1m_pct": advance, "consecutive_up_days_5": consecutive_up_days, "extension_10sma_pct": extension10, "extension_20sma_pct": extension20, "relative_volume": relative_volume, "state": state}


def evaluate_qullamaggie(
    frame: pd.DataFrame,
    *,
    market_cap: float | None,
    daily_rs_rating: float | None,
) -> QullamaggieSnapshot | None:
    bars = _normalize_price_frame(frame)
    if bars.empty or len(bars) < ONE_YEAR_SESSIONS + 1:
        return None

    close = bars["Close"].astype(float)
    high = bars["High"].astype(float)
    low = bars["Low"].astype(float)
    volume = bars["Volume"].astype(float)
    current_price = float(close.iloc[-1])
    sma10 = float(close.rolling(10).mean().iloc[-1])
    sma20 = float(close.rolling(20).mean().iloc[-1])
    sma50 = float(close.rolling(50).mean().iloc[-1])
    sma200 = float(close.rolling(200).mean().iloc[-1])
    avg_volume_20 = float(volume.tail(AVERAGE_VOLUME_DAYS).mean())
    adr_pct_20 = float(((high / low - 1.0) * 100.0).tail(ADR_PERIOD_DAYS).mean())
    return_1m_pct = _return_pct(close, 21)
    return_3m_pct = _return_pct(close, THREE_MONTH_SESSIONS)
    return_6m_pct = _return_pct(close, SIX_MONTH_SESSIONS)
    recent_return_pct = max(return_1m_pct, return_3m_pct, return_6m_pct)
    high_3m = float(high.tail(THREE_MONTH_SESSIONS).max())
    high_6m = float(high.tail(SIX_MONTH_SESSIONS).max())
    high_52w = float(high.tail(ONE_YEAR_SESSIONS).max())
    recent_high = max(high_3m, high_6m)
    distance_from_recent_high_pct = ((recent_high - current_price) / recent_high) * 100.0 if recent_high > 0.0 else 100.0
    distance_from_52w_high_pct = ((high_52w - current_price) / high_52w) * 100.0 if high_52w > 0.0 else 100.0
    tight_window = bars.tail(TIGHT_FLAG_DAYS)
    tight_high = float(tight_window["High"].max())
    tight_low = float(tight_window["Low"].min())
    tight_flag_range_pct = ((tight_high - tight_low) / tight_low) * 100.0 if tight_low > 0.0 else 100.0
    fast_ma_distance_pct = min(abs(current_price - sma10) / sma10, abs(current_price - sma20) / sma20) * 100.0
    tight_flag = tight_flag_range_pct <= MAX_TIGHT_FLAG_RANGE_PCT
    pullback_near_fast_ma = fast_ma_distance_pct <= MAX_FAST_MA_DISTANCE_PCT
    normalized_rs = float(daily_rs_rating) if daily_rs_rating is not None else None

    # Candidate bases must follow a chronological low-to-high impulse and contract structurally.
    setups: list[tuple[tuple[float, ...], pd.DataFrame, int, float, bool, bool, bool]] = []
    for base_days in range(10, 41):
        base_start = len(bars) - base_days
        base = bars.iloc[base_start:]
        prior = bars.iloc[max(0, base_start - 63):base_start]
        if len(prior) < 20:
            continue
        low_position = int(prior["Low"].values.argmin())
        anchor = float(prior["Low"].iloc[low_position])
        later_high = float(prior["High"].iloc[low_position + 1:].max()) if low_position + 1 < len(prior) else 0.0
        impulse = ((later_high / anchor) - 1.0) * 100.0 if anchor > 0 else 0.0
        if impulse < 30.0:
            continue
        base_low, base_high = float(base["Low"].min()), float(base["High"].max())
        if base_low <= 0.0 or (base_high / base_low - 1.0) * 100.0 > MAX_BASE_RANGE_PCT:
            continue
        if later_high <= 0.0 or (base_high / later_high - 1.0) * 100.0 > 15.0:
            continue
        first, second = base.iloc[: base_days // 2], base.iloc[base_days // 2:]
        first_low, second_low = float(first["Low"].min()), float(second["Low"].min())
        higher_low = second_low > first_low
        first_range = float(first["High"].max()) / first_low - 1.0 if first_low > 0.0 else float("inf")
        second_range = float(second["High"].max()) / second_low - 1.0 if second_low > 0.0 else float("inf")
        range_contracting = first_range > 0.0 and second_range < first_range * 0.8
        first_volume, second_volume = float(first["Volume"].mean()), float(second["Volume"].mean())
        volume_contracting = first_volume > 0.0 and second_volume < first_volume * 0.8
        if not higher_low or not (range_contracting or volume_contracting):
            continue
        higher_low_pct = (second_low / first_low - 1.0) * 100.0
        contraction = min(second_range / first_range if first_range else 1.0, second_volume / first_volume if first_volume else 1.0)
        score = (float(range_contracting + volume_contracting), higher_low_pct, -contraction, impulse, float(base_days))
        setups.append((score, base, base_days, impulse, higher_low, range_contracting, volume_contracting))
    setup = max(setups, key=lambda candidate: candidate[0]) if setups else None
    if setup is None:
        base, consolidation_sessions, impulse_return_pct = bars.tail(10), 0, 0.0
        higher_low = range_contracting = volume_contracting = False
    else:
        _, base, consolidation_sessions, impulse_return_pct, higher_low, range_contracting, volume_contracting = setup
    pivot_bars = base.iloc[:-1] if len(base) > 1 else base
    pivot = float(pivot_bars["High"].max())
    rising_fast_mas = sma10 > float(close.rolling(10).mean().iloc[-6]) and sma20 > float(close.rolling(20).mean().iloc[-6])
    crossed_pivot = setup is not None and len(close) > 1 and float(close.iloc[-2]) <= pivot and current_price > pivot
    setup_structure = setup is not None
    setup_state = "triggered" if crossed_pivot else ("ready" if setup_structure and current_price >= pivot * 0.95 else "forming")
    criteria = {
        "prior_impulse_gt_30pct": impulse_return_pct >= 30.0,
        "post_impulse_base_10_40_sessions": 10 <= consolidation_sessions <= 40,
        "higher_low_plus_range_or_volume_contraction": higher_low and (range_contracting or volume_contracting),
        "rising_10_20_sma": rising_fast_mas,
        "near_3m_or_6m_high": distance_from_recent_high_pct <= MAX_DISTANCE_FROM_RECENT_HIGH_PCT,
        "near_52w_high": distance_from_52w_high_pct <= MAX_DISTANCE_FROM_52W_HIGH_PCT,
        "price_above_10_20_50_200_sma": current_price > max(sma10, sma20, sma50, sma200),
        "avg_volume_20_gt_1m": avg_volume_20 > MIN_AVG_VOLUME,
        "market_cap_gt_100m": market_cap is not None and float(market_cap) > MIN_MARKET_CAP,
        "adr20_gt_4pct": adr_pct_20 >= MIN_ADR_PCT,
        "daily_rs_gt_80": normalized_rs is not None and normalized_rs >= MIN_DAILY_RS_RATING,
    }
    setup_reason = (
        f"10-day range {tight_flag_range_pct:.1f}% is tight"
        if tight_flag
        else f"close is {fast_ma_distance_pct:.1f}% from the nearer 10/20 SMA"
    )
    reasons = [
        f"1M return {return_1m_pct:.1f}%, 3M {return_3m_pct:.1f}%, 6M {return_6m_pct:.1f}%",
        f"close is {distance_from_recent_high_pct:.1f}% below the 3M/6M high and {distance_from_52w_high_pct:.1f}% below the 52-week high",
        f"close {current_price:.2f} above 10/20/50/200 SMA",
        f"20-day average volume {avg_volume_20:,.0f} shares and market cap ${(market_cap or 0.0) / 1_000_000:.0f}M",
        f"ADR20 {adr_pct_20:.1f}% and Daily RS {normalized_rs:.1f}" if normalized_rs is not None else f"ADR20 {adr_pct_20:.1f}% and Daily RS unavailable",
        setup_reason,
        f"prior impulse {impulse_return_pct:.1f}%; {consolidation_sessions}-session base; {setup_state} at pivot {pivot:.2f}",
    ]
    return QullamaggieSnapshot(
        matched=all(criteria.values()),
        current_price=current_price,
        market_cap=float(market_cap) if market_cap is not None else None,
        avg_volume_20=avg_volume_20,
        adr_pct_20=adr_pct_20,
        daily_rs_rating=normalized_rs,
        return_3m_pct=return_3m_pct,
        return_1m_pct=return_1m_pct,
        return_6m_pct=return_6m_pct,
        recent_return_pct=recent_return_pct,
        sma10=sma10,
        sma20=sma20,
        sma50=sma50,
        sma200=sma200,
        high_3m=high_3m,
        high_6m=high_6m,
        high_52w=high_52w,
        distance_from_recent_high_pct=distance_from_recent_high_pct,
        distance_from_52w_high_pct=distance_from_52w_high_pct,
        tight_flag_range_pct=tight_flag_range_pct,
        fast_ma_distance_pct=fast_ma_distance_pct,
        tight_flag=tight_flag,
        pullback_near_fast_ma=pullback_near_fast_ma,
        criteria_passed=sum(criteria.values()),
        criteria_total=len(criteria),
        criteria=criteria,
        reasons=reasons,
        setup_state=setup_state,
        pivot=pivot,
        breakout_day_low=float(low.iloc[-1]) if setup_state == "triggered" else None,
        impulse_return_pct=impulse_return_pct,
        consolidation_sessions=consolidation_sessions,
        higher_low=higher_low,
        range_contracting=range_contracting,
        volume_contracting=volume_contracting,
        rising_fast_mas=rising_fast_mas,
    )


def find_qullamaggie_hit(
    frame: pd.DataFrame,
    *,
    ticker: UniverseTicker,
    market_cap: float | None,
    daily_rs_rating: float | None,
    signal_date: dt.date,
) -> QullamaggieHit | None:
    snapshot = evaluate_qullamaggie(frame, market_cap=market_cap, daily_rs_rating=daily_rs_rating)
    if snapshot is None or not snapshot.matched or snapshot.market_cap is None or snapshot.daily_rs_rating is None:
        return None
    return QullamaggieHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=signal_date.isoformat(),
        current_price=snapshot.current_price,
        market_cap=snapshot.market_cap,
        avg_volume_20=snapshot.avg_volume_20,
        adr_pct_20=snapshot.adr_pct_20,
        daily_rs_rating=snapshot.daily_rs_rating,
        return_3m_pct=snapshot.return_3m_pct,
        return_1m_pct=snapshot.return_1m_pct,
        return_6m_pct=snapshot.return_6m_pct,
        recent_return_pct=snapshot.recent_return_pct,
        sma10=snapshot.sma10,
        sma20=snapshot.sma20,
        sma50=snapshot.sma50,
        sma200=snapshot.sma200,
        high_52w=snapshot.high_52w,
        distance_from_52w_high_pct=snapshot.distance_from_52w_high_pct,
        tight_flag_range_pct=snapshot.tight_flag_range_pct,
        fast_ma_distance_pct=snapshot.fast_ma_distance_pct,
        tight_flag=snapshot.tight_flag,
        pullback_near_fast_ma=snapshot.pullback_near_fast_ma,
        criteria_passed=snapshot.criteria_passed,
        criteria_total=snapshot.criteria_total,
        reasons=snapshot.reasons,
        setup_state=snapshot.setup_state,
        pivot=snapshot.pivot,
        breakout_day_low=snapshot.breakout_day_low,
        impulse_return_pct=snapshot.impulse_return_pct,
        consolidation_sessions=snapshot.consolidation_sessions,
        higher_low=snapshot.higher_low,
        range_contracting=snapshot.range_contracting,
        volume_contracting=snapshot.volume_contracting,
        rising_fast_mas=snapshot.rising_fast_mas,
    )


def run_qullamaggie_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    as_of_date: dt.date | None = None,
    database_url: str | None = None,
) -> QullamaggieScreenResult:
    run_date = as_of_date or dt.date.today()
    resolved_database_url = resolve_database_url(database_url)
    symbols = [ticker.symbol.upper() for ticker in tickers]
    frames = load_many_ticker_windows(symbols, run_date, QULLAMAGGIE_HISTORY_DAYS, database_url=resolved_database_url)
    metadata = load_ticker_metadata_map(symbols, database_url=resolved_database_url)
    ratings = RatingsRepository(resolved_database_url)
    fundamentals = ratings.load_latest_fundamentals_snapshots_for_tickers(symbols, as_of_date=run_date)
    technicals = ratings.load_latest_technical_rating_snapshots_for_tickers(
        symbols,
        as_of_date=run_date,
        allow_older_as_of_date=True,
    )
    hits: list[QullamaggieHit] = []
    failures: list[dict[str, str]] = []
    fallback: list[tuple[int, UniverseTicker]] = []

    def evaluate_ticker(position: int, ticker: UniverseTicker, frame: pd.DataFrame, source: str) -> None:
        symbol = ticker.symbol.upper()
        meta = metadata.get(symbol, {})
        fundamental = fundamentals.get(symbol, {})
        runtime_ticker = UniverseTicker(
            symbol=symbol,
            sector=ticker.sector or str(meta.get("sector") or fundamental.get("sector") or "") or None,
            industry=ticker.industry or str(meta.get("industry") or fundamental.get("industry") or "") or None,
            exchange=ticker.exchange or str(meta.get("exchange") or "") or None,
        )
        print(f"[{position}/{len(tickers)}] screening {symbol} from {source} | passed={len(hits)}", flush=True)
        hit = find_qullamaggie_hit(
            frame,
            ticker=runtime_ticker,
            market_cap=fundamental.get("market_cap"),
            daily_rs_rating=technicals.get(symbol, {}).get("daily_rs_rating"),
            signal_date=run_date,
        )
        if hit is not None:
            hits.append(hit)

    for position, ticker in enumerate(tickers, start=1):
        frame = frames.get(ticker.symbol.upper())
        if frame is None or not db_frame_has_recent_coverage(frame, run_date) or len(frame) < ONE_YEAR_SESSIONS + 1:
            fallback.append((position, ticker))
            continue
        try:
            evaluate_ticker(position, ticker, frame, "DB")
        except Exception as exc:
            failures.append({"ticker": ticker.symbol, "error": str(exc)})

    if fallback:
        cookstock = load_configured_cookstock(config)
        with freeze_cookstock_today(cookstock, as_of_date):
            for position, ticker in fallback:
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=QULLAMAGGIE_HISTORY_DAYS,
                    )
                    evaluate_ticker(position, ticker, _build_price_frame(financials), "internet fallback")
                except Exception as exc:
                    failures.append({"ticker": ticker.symbol, "error": str(exc)})

    hits.sort(key=lambda item: (-item.daily_rs_rating, -item.recent_return_pct, -item.adr_pct_20, item.ticker))
    return QullamaggieScreenResult(
        run_date=run_date.isoformat(),
        total_tickers=len(tickers),
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )


def load_qullamaggie_universe(
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
