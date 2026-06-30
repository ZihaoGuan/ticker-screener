from __future__ import annotations

from dataclasses import asdict, dataclass
import datetime as dt

import pandas as pd

from .config import AppConfig
from .cookstock_bridge import freeze_cookstock_today, iter_prefetched_cookstock_batches, load_configured_cookstock
from .earnings_enrichment import EarningsAnnotation, build_earnings_annotations
from .earnings_gap_screen import find_recent_gap_signal
from .rs_screen import _compute_latest_rs_rating
from .universe import UniverseTicker


ELITE_RS_HISTORY_DAYS = 5000


@dataclass(frozen=True)
class EliteRsHit:
    ticker: str
    sector: str | None
    industry: str | None
    exchange: str | None
    signal_date: str
    benchmark_ticker: str
    profile: str
    current_price: float
    rs_score: float
    rs_rating: float
    min_rs_rating: float
    volume_signal_kind: str | None
    volume_signal_date: str | None
    volume_signal_age_days: int | None
    current_volume: float | None
    volume_ma_20: float | None
    peg_date: str | None
    peg_gap_pct: float | None
    peg_event_age_days: int | None
    peg_actionable_now: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EliteRsScreenResult:
    run_date: str
    profile: str
    benchmark_ticker: str
    total_tickers: int
    passed_tickers: int
    failed_tickers: list[dict[str, str]]
    hits: list[EliteRsHit]

    def to_dict(self) -> dict[str, object]:
        return {
            'run_date': self.run_date,
            'profile': self.profile,
            'benchmark_ticker': self.benchmark_ticker,
            'total_tickers': self.total_tickers,
            'passed_tickers': self.passed_tickers,
            'failed_tickers': self.failed_tickers,
            'hits': [item.to_dict() for item in self.hits],
        }


@dataclass(frozen=True)
class _RecentVolumeSignal:
    signal_kind: str
    signal_date: str
    signal_age_days: int
    current_volume: float
    volume_ma_20: float | None


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
    frame['volume_ma_20'] = frame['Volume'].rolling(20).mean()
    frame['volume_ma_50'] = frame['Volume'].rolling(50).mean()
    return frame


def _find_recent_hv1_signal(
    frame: pd.DataFrame,
    *,
    lookback_days: int,
    recent_window_days: int,
) -> _RecentVolumeSignal | None:
    if frame.empty or len(frame) < max(20, lookback_days):
        return None
    start_index = max(lookback_days - 1, len(frame) - max(1, int(recent_window_days)))
    latest_index = len(frame) - 1
    candidate: _RecentVolumeSignal | None = None
    for idx in range(start_index, len(frame)):
        current_volume = float(frame['Volume'].iloc[idx])
        rolling_window = frame['Volume'].iloc[max(0, idx - lookback_days + 1) : idx + 1]
        historical_window = frame['Volume'].iloc[: idx + 1]
        if rolling_window.empty or historical_window.empty:
            continue
        is_hv1 = current_volume >= float(rolling_window.max())
        if not is_hv1:
            continue
        is_hve = current_volume >= float(historical_window.max())
        signal_kind = 'HVE' if is_hve else 'HV1'
        signal_date = frame.index[idx].date().isoformat()
        signal_age_days = latest_index - idx
        volume_ma_20 = frame['volume_ma_20'].iloc[idx]
        candidate = _RecentVolumeSignal(
            signal_kind=signal_kind,
            signal_date=signal_date,
            signal_age_days=signal_age_days,
            current_volume=current_volume,
            volume_ma_20=float(volume_ma_20) if pd.notna(volume_ma_20) else None,
        )
    return candidate


def _sort_hits(profile: str, hits: list[EliteRsHit]) -> None:
    if profile == 'recent-peg':
        hits.sort(
            key=lambda item: (
                item.peg_event_age_days if item.peg_event_age_days is not None else 9999,
                -item.rs_rating,
                -(item.peg_gap_pct or 0.0),
                item.ticker,
            )
        )
        return
    hits.sort(
        key=lambda item: (
            item.volume_signal_age_days if item.volume_signal_age_days is not None else 9999,
            0 if item.volume_signal_kind == 'HVE' else 1,
            -item.rs_rating,
            item.ticker,
        )
    )


def run_elite_rs_screen(
    config: AppConfig,
    tickers: list[UniverseTicker],
    *,
    profile: str,
    as_of_date: dt.date | None = None,
) -> EliteRsScreenResult:
    normalized_profile = str(profile or 'hv1').strip().lower()
    if normalized_profile not in {'hv1', 'recent-peg'}:
        raise ValueError(f'Unsupported elite RS profile: {profile}')

    cookstock = load_configured_cookstock(config)
    hits: list[EliteRsHit] = []
    failures: list[dict[str, str]] = []
    run_date = as_of_date or dt.date.today()
    total_tickers = len(tickers)
    peg_annotations: dict[str, EarningsAnnotation] = {}
    if normalized_profile == 'recent-peg':
        peg_annotations = build_earnings_annotations(
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
            history_lookback_days=ELITE_RS_HISTORY_DAYS,
            benchmark_ticker=config.benchmark_ticker,
        ):
            for ticker in ticker_batch:
                position += 1
                print(f'[{position}/{total_tickers}] screening {ticker.symbol} | passed={len(hits)}')
                try:
                    financials = cookstock.cookFinancials(
                        ticker.symbol,
                        benchmarkTicker=config.benchmark_ticker,
                        historyLookbackDays=ELITE_RS_HISTORY_DAYS,
                    )
                    price_data = [item for item in financials._get_clean_price_data() if isinstance(item, dict)]  # type: ignore[attr-defined]
                    benchmark_rows = [
                        item for item in financials._get_benchmark_price_data(config.benchmark_ticker) if isinstance(item, dict)
                    ]  # type: ignore[attr-defined]
                    rs_metrics = _compute_latest_rs_rating(price_data, benchmark_rows)
                    if rs_metrics is None:
                        continue
                    rs_score, rs_rating = rs_metrics
                    if rs_rating < float(config.rs_rating_min):
                        continue

                    frame = _build_price_frame(financials)
                    current_price = float(frame['Close'].iloc[-1]) if not frame.empty else 0.0

                    if normalized_profile == 'hv1':
                        signal = _find_recent_hv1_signal(
                            frame,
                            lookback_days=max(1, int(config.peg_volume_signal_lookback_days)),
                            recent_window_days=max(5, int(config.elite_rs_recent_volume_window_days)),
                        )
                        if signal is None:
                            continue
                        reasons = [
                            f'RS rating {rs_rating:.1f} is above minimum {float(config.rs_rating_min):.1f}',
                            f'{signal.signal_kind} volume signal fired {signal.signal_age_days} bar(s) ago',
                            f'signal date {signal.signal_date}',
                        ]
                        if signal.volume_ma_20 is not None and signal.volume_ma_20 > 0:
                            reasons.append(
                                f'signal volume {signal.current_volume:,.0f} vs 20D avg {signal.volume_ma_20:,.0f}'
                            )
                        hits.append(
                            EliteRsHit(
                                ticker=ticker.symbol,
                                sector=ticker.sector,
                                industry=ticker.industry,
                                exchange=ticker.exchange,
                                signal_date=signal.signal_date,
                                benchmark_ticker=config.benchmark_ticker,
                                profile=normalized_profile,
                                current_price=current_price,
                                rs_score=rs_score,
                                rs_rating=rs_rating,
                                min_rs_rating=float(config.rs_rating_min),
                                volume_signal_kind=signal.signal_kind,
                                volume_signal_date=signal.signal_date,
                                volume_signal_age_days=signal.signal_age_days,
                                current_volume=signal.current_volume,
                                volume_ma_20=signal.volume_ma_20,
                                peg_date=None,
                                peg_gap_pct=None,
                                peg_event_age_days=None,
                                peg_actionable_now=False,
                                reasons=reasons,
                            )
                        )
                        continue

                    gap_signal = find_recent_gap_signal(
                        frame,
                        config=config,
                        profile='peg',
                        lookback_days=int(config.earnings_gap_signal_lookback_days),
                        annotation=peg_annotations.get(ticker.symbol.upper()),
                    )
                    if gap_signal is None:
                        continue
                    reasons = [
                        f'RS rating {rs_rating:.1f} is above minimum {float(config.rs_rating_min):.1f}',
                        'most recent reported earnings bar is evaluated first for Pine-style PEG',
                        *gap_signal.reasons,
                    ]
                    hits.append(
                        EliteRsHit(
                            ticker=ticker.symbol,
                            sector=ticker.sector,
                            industry=ticker.industry,
                            exchange=ticker.exchange,
                            signal_date=gap_signal.signal_date,
                            benchmark_ticker=config.benchmark_ticker,
                            profile=normalized_profile,
                            current_price=current_price,
                            rs_score=rs_score,
                            rs_rating=rs_rating,
                            min_rs_rating=float(config.rs_rating_min),
                            volume_signal_kind='PEG',
                            volume_signal_date=gap_signal.signal_date,
                            volume_signal_age_days=gap_signal.trading_days_ago,
                            current_volume=gap_signal.current_volume,
                            volume_ma_20=None,
                            peg_date=gap_signal.signal_date,
                            peg_gap_pct=gap_signal.close_gap_pct,
                            peg_event_age_days=gap_signal.trading_days_ago,
                            peg_actionable_now=gap_signal.trading_days_ago == 0,
                            reasons=reasons,
                        )
                    )
                except Exception as exc:
                    failures.append({'ticker': ticker.symbol, 'error': str(exc)})
                    print(f'[{position}/{total_tickers}] {ticker.symbol} error: {exc} | passed={len(hits)}')

    _sort_hits(normalized_profile, hits)
    print(f'screen complete: profile={normalized_profile}, passed={len(hits)}, failed={len(failures)}, total={total_tickers}')

    return EliteRsScreenResult(
        run_date=run_date.isoformat(),
        profile=normalized_profile,
        benchmark_ticker=config.benchmark_ticker,
        total_tickers=total_tickers,
        passed_tickers=len(hits),
        failed_tickers=failures,
        hits=hits,
    )
