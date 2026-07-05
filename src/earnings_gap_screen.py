from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .earnings_enrichment import EarningsAnnotation, build_earnings_annotations
from .universe import UniverseTicker


EARNINGS_GAP_HISTORY_DAYS = 5000


@dataclass(frozen=True)
class GapSignalSnapshot:
    signal_label: str
    signal_date: str
    trading_days_ago: int
    current_price: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    previous_close: float
    current_volume: float
    volume_ma_50: float
    volume_ratio: float
    volume_buzz_pct: float
    gap_pct: float
    close_gap_pct: float
    earnings_release_date: str | None
    earnings_release_session: str | None
    earnings_eps_surprise_pct: float | None
    earnings_trading_days_since_event: int | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EarningsGapHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    signal_label: str
    trading_days_ago: int
    current_price: float
    open_price: float
    high_price: float
    low_price: float
    previous_close: float
    current_volume: float
    volume_ma_50: float
    volume_ratio: float
    volume_buzz_pct: float
    gap_pct: float
    close_gap_pct: float
    earnings_release_date: str | None
    earnings_release_session: str | None
    earnings_eps_surprise_pct: float | None
    earnings_trading_days_since_event: int | None
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EarningsGapScreenResult:
    run_date: str
    profile: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[EarningsGapHit]

    def to_dict(self) -> dict[str, object]:
        return {
            'run_date': self.run_date,
            'profile': self.profile,
            'total_tickers': self.total_tickers,
            'passed_tickers': self.passed_tickers,
            'failed_tickers': self.failed_tickers,
            'hits': [item.to_dict() for item in self.hits],
        }


_PROFILE_RULES: dict[str, dict[str, object]] = {
    'peg': {
        'label': 'PEG',
        'min_gap_pct': 10.0,
        'min_volume_ratio': 3.0,
        'requires_earnings': False,
        'requires_eps_surprise': False,
        'gap_mode': 'close_vs_prior_close',
    },
    'monster-gap': {
        'label': 'Monster Gap',
        'min_gap_pct': 20.0,
        'min_volume_ratio': 4.0,
        'requires_earnings': False,
        'requires_eps_surprise': False,
        'gap_mode': 'true_gap_above_prior_high',
    },
    'monster-peg': {
        'label': 'Monster Peg',
        'min_gap_pct': 20.0,
        'min_volume_ratio': 4.0,
        'requires_earnings': True,
        'requires_eps_surprise': True,
        'gap_mode': 'close_vs_prior_close',
    },
}


def _build_price_frame(financials: object) -> pd.DataFrame:
    rows = financials._get_clean_price_data()  # type: ignore[attr-defined]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        {
            'Date': pd.to_datetime([row.get('formatted_date') for row in rows]),
            'Open': [row.get('open') for row in rows],
            'High': [row.get('high') for row in rows],
            'Low': [row.get('low') for row in rows],
            'Close': [row.get('close') for row in rows],
            'Volume': [row.get('volume') for row in rows],
        }
    )
    frame = frame.dropna(subset=['Date', 'Open', 'High', 'Low', 'Close', 'Volume']).set_index('Date').sort_index()
    if frame.empty:
        return frame
    frame['volume_ma_50'] = frame['Volume'].rolling(50).mean()
    return frame


def _find_date_index(frame: pd.DataFrame, target_date: dt.date) -> int | None:
    for idx, value in enumerate(frame.index):
        if pd.Timestamp(value).date() == target_date:
            return idx
    return None


def _resolve_earnings_anchor_index(
    frame: pd.DataFrame,
    annotation: EarningsAnnotation | None,
) -> tuple[int | None, str | None, str | None, float | None]:
    if annotation is None or not annotation.is_reported or not annotation.release_date:
        return None, None, None, None
    try:
        release_date = dt.date.fromisoformat(annotation.release_date)
    except ValueError:
        return None, annotation.release_date, annotation.release_session, annotation.eps_surprise_pct
    release_index = _find_date_index(frame, release_date)
    if release_index is None:
        return None, annotation.release_date, annotation.release_session, annotation.eps_surprise_pct
    if annotation.release_session == 'after_market' and release_index + 1 < len(frame):
        release_index += 1
    return release_index, annotation.release_date, annotation.release_session, annotation.eps_surprise_pct


def _build_gap_snapshot(
    frame: pd.DataFrame,
    *,
    idx: int,
    latest_index: int,
    signal_label: str,
    min_gap_pct: float,
    min_volume_ratio: float,
    requires_earnings: bool,
    requires_eps_surprise: bool,
    gap_mode: str,
    config: AppConfig,
    earnings_index: int | None,
    earnings_release_date: str | None,
    earnings_release_session: str | None,
    eps_surprise_pct: float | None,
) -> GapSignalSnapshot | None:
    if idx <= 0 or idx >= len(frame):
        return None
    row = frame.iloc[idx]
    volume_ma_50 = row['volume_ma_50']
    if pd.isna(volume_ma_50) or float(volume_ma_50) <= 0:
        return None
    previous_close = float(frame['Close'].iloc[idx - 1])
    previous_high = float(frame['High'].iloc[idx - 1])
    open_price = float(row['Open'])
    low_price = float(row['Low'])
    close_price = float(row['Close'])
    latest_close = float(frame['Close'].iloc[latest_index])
    if previous_close <= 0:
        return None

    close_gap_pct = ((close_price / previous_close) - 1.0) * 100.0
    current_volume = float(row['Volume'])
    volume_ratio = current_volume / float(volume_ma_50)
    if gap_mode == 'true_gap_above_prior_high':
        if previous_high <= 0 or low_price <= previous_high:
            return None
        gap_pct = ((low_price / previous_high) - 1.0) * 100.0
        if gap_pct < min_gap_pct or volume_ratio < min_volume_ratio:
            return None
        gap_midpoint = previous_high + ((low_price - previous_high) * 0.5)
        if latest_close <= gap_midpoint:
            return None
        reasons = [
            f'low above prior high {previous_high:.2f}',
            f'true gap {gap_pct:.1f}% above prior high',
            f'volume {volume_ratio:.2f}x 50D average',
            f'current close {latest_close:.2f} held above gap midpoint {gap_midpoint:.2f}',
        ]
    else:
        if open_price <= previous_close:
            return None
        gap_pct = close_gap_pct
        if gap_pct < min_gap_pct or volume_ratio < min_volume_ratio:
            return None
        reasons = [
            f'open above prior close {previous_close:.2f}',
            f'close gain {gap_pct:.1f}% vs prior close',
            f'volume {volume_ratio:.2f}x 50D average',
        ]

    earnings_days_since_event: int | None = None
    if earnings_index is not None:
        earnings_days_since_event = idx - earnings_index
    if requires_earnings and earnings_days_since_event != 0:
        return None
    if requires_eps_surprise:
        if eps_surprise_pct is None or float(eps_surprise_pct) < float(config.peg_min_eps_surprise_pct):
            return None

    volume_buzz_pct = (volume_ratio - 1.0) * 100.0
    if earnings_index is not None and earnings_release_date:
        session_label = earnings_release_session or 'unspecified session'
        reasons.append(f'most recent earnings event bar matched ({earnings_release_date}, {session_label})')
    if requires_eps_surprise and eps_surprise_pct is not None:
        reasons.append(f'EPS surprise {float(eps_surprise_pct):.1f}%')

    return GapSignalSnapshot(
        signal_label=signal_label,
        signal_date=frame.index[idx].date().isoformat(),
        trading_days_ago=latest_index - idx,
        current_price=latest_close,
        open_price=open_price,
        high_price=float(row['High']),
        low_price=float(row['Low']),
        close_price=close_price,
        previous_close=previous_close,
        current_volume=current_volume,
        volume_ma_50=float(volume_ma_50),
        volume_ratio=float(volume_ratio),
        volume_buzz_pct=float(volume_buzz_pct),
        gap_pct=float(gap_pct),
        close_gap_pct=float(close_gap_pct),
        earnings_release_date=earnings_release_date,
        earnings_release_session=earnings_release_session,
        earnings_eps_surprise_pct=float(eps_surprise_pct) if eps_surprise_pct is not None else None,
        earnings_trading_days_since_event=earnings_days_since_event,
        reasons=reasons,
    )


def find_recent_gap_signal(
    frame: pd.DataFrame,
    *,
    config: AppConfig,
    profile: str,
    lookback_days: int,
    annotation: EarningsAnnotation | None = None,
) -> GapSignalSnapshot | None:
    normalized_profile = str(profile or '').strip().lower()
    rules = _PROFILE_RULES.get(normalized_profile)
    if rules is None:
        raise ValueError(f'Unsupported gap profile: {profile}')
    if frame.empty or len(frame) < 51:
        return None

    min_gap_pct = float(rules['min_gap_pct'])
    min_volume_ratio = float(rules['min_volume_ratio'])
    requires_earnings = bool(rules['requires_earnings'])
    requires_eps_surprise = bool(rules['requires_eps_surprise'])
    gap_mode = str(rules['gap_mode'])
    signal_label = str(rules['label'])

    latest_index = len(frame) - 1
    start_index = max(1, len(frame) - max(1, int(lookback_days)))
    earnings_index, earnings_release_date, earnings_release_session, eps_surprise_pct = _resolve_earnings_anchor_index(frame, annotation)

    if earnings_index is not None and normalized_profile in {'peg', 'monster-peg'}:
        anchored_snapshot = _build_gap_snapshot(
            frame,
            idx=earnings_index,
            latest_index=latest_index,
            signal_label=signal_label,
            min_gap_pct=min_gap_pct,
            min_volume_ratio=min_volume_ratio,
            requires_earnings=requires_earnings,
            requires_eps_surprise=requires_eps_surprise,
            gap_mode=gap_mode,
            config=config,
            earnings_index=earnings_index,
            earnings_release_date=earnings_release_date,
            earnings_release_session=earnings_release_session,
            eps_surprise_pct=eps_surprise_pct,
        )
        if anchored_snapshot is not None:
            return anchored_snapshot
        if requires_earnings:
            return None

    best: GapSignalSnapshot | None = None
    for idx in range(start_index, len(frame)):
        snapshot = _build_gap_snapshot(
            frame,
            idx=idx,
            latest_index=latest_index,
            signal_label=signal_label,
            min_gap_pct=min_gap_pct,
            min_volume_ratio=min_volume_ratio,
            requires_earnings=False,
            requires_eps_surprise=False,
            gap_mode=gap_mode,
            config=config,
            earnings_index=earnings_index,
            earnings_release_date=earnings_release_date,
            earnings_release_session=earnings_release_session,
            eps_surprise_pct=eps_surprise_pct,
        )
        if snapshot is None:
            continue
        best = snapshot
    return best


def _to_hit(ticker: UniverseTicker, snapshot: GapSignalSnapshot) -> EarningsGapHit:
    return EarningsGapHit(
        ticker=ticker.symbol,
        sector=ticker.sector,
        industry=ticker.industry,
        exchange=ticker.exchange,
        signal_date=snapshot.signal_date,
        signal_label=snapshot.signal_label,
        trading_days_ago=snapshot.trading_days_ago,
        current_price=snapshot.current_price,
        open_price=snapshot.open_price,
        high_price=snapshot.high_price,
        low_price=snapshot.low_price,
        previous_close=snapshot.previous_close,
        current_volume=snapshot.current_volume,
        volume_ma_50=snapshot.volume_ma_50,
        volume_ratio=snapshot.volume_ratio,
        volume_buzz_pct=snapshot.volume_buzz_pct,
        gap_pct=snapshot.gap_pct,
        close_gap_pct=snapshot.close_gap_pct,
        earnings_release_date=snapshot.earnings_release_date,
        earnings_release_session=snapshot.earnings_release_session,
        earnings_eps_surprise_pct=snapshot.earnings_eps_surprise_pct,
        earnings_trading_days_since_event=snapshot.earnings_trading_days_since_event,
        reasons=list(snapshot.reasons),
    )


def run_earnings_gap_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    profile: str,
    as_of_date: dt.date | None = None,
) -> EarningsGapScreenResult:
    normalized_profile = str(profile or 'peg').strip().lower()
    if normalized_profile not in _PROFILE_RULES:
        raise ValueError(f'Unsupported gap profile: {profile}')

    cookstock = load_configured_cookstock(config)
    hits: list[EarningsGapHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    needs_earnings = normalized_profile in {'peg', 'monster-peg'}
    annotations: dict[str, EarningsAnnotation] = {}
    if needs_earnings:
        annotations = build_earnings_annotations(
            [ticker.symbol for ticker in tickers],
            config,
            as_of_date=run_date,
            upcoming_days=max(14, int(config.earnings_gap_signal_lookback_days)),
            provider='auto',
        )

    with freeze_cookstock_today(cookstock, as_of_date):
        position = 0
        for ticker_batch in iter_prefetched_cookstock_batches(
            config,
            tickers,
            as_of_date=as_of_date,
            history_lookback_days=EARNINGS_GAP_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f'[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}')
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=EARNINGS_GAP_HISTORY_DAYS,
                    )
                    frame = _build_price_frame(financials)
                    snapshot = find_recent_gap_signal(
                        frame,
                        config=config,
                        profile=normalized_profile,
                        lookback_days=int(config.earnings_gap_signal_lookback_days),
                        annotation=annotations.get(ticker.symbol.upper()),
                    )
                    if snapshot is None:
                        continue
                    hits.append(_to_hit(ticker, snapshot))
                except Exception as exc:
                    failures.append({'ticker': ticker.symbol, 'error': str(exc)})
                    print(f'[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}')

    hits.sort(
        key=lambda item: (
            item.trading_days_ago,
            -item.gap_pct,
            -item.volume_ratio,
            item.ticker,
        )
    )
    print(f'screen complete: profile={normalized_profile}, passed={len(hits)}, failed={len(failures)}, total={total_tickers}')

    return EarningsGapScreenResult(
        run_date=run_date.isoformat(),
        profile=normalized_profile,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
