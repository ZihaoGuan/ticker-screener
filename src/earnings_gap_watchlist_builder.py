from __future__ import annotations

from .earnings_gap_screen import EarningsGapHit


def build_earnings_gap_watchlist(hits: list[EarningsGapHit], *, profile: str) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    normalized_profile = str(profile or 'peg').strip().lower()
    for hit in hits:
        if normalized_profile == 'monster-peg':
            summary = (
                f'{hit.signal_label} {hit.close_gap_pct:.1f}% on {hit.signal_date} with {hit.volume_ratio:.2f}x 50D volume. '
                f'Earnings event {hit.earnings_trading_days_since_event or 0} trading day(s) earlier and EPS surprise '
                f'{(hit.earnings_eps_surprise_pct or 0):.1f}%.'
            )
            badges = ['Monster Peg', '20% Gap', '4x Volume', 'Earnings']
            entry_style = 'monster_peg'
        elif normalized_profile == 'monster-gap':
            summary = f'{hit.signal_label} {hit.close_gap_pct:.1f}% on {hit.signal_date} with {hit.volume_ratio:.2f}x 50D volume.'
            badges = ['Monster Gap', '20% Gap', '4x Volume']
            entry_style = 'monster_gap'
        else:
            summary = f'{hit.signal_label}-type gap {hit.close_gap_pct:.1f}% on {hit.signal_date} with {hit.volume_ratio:.2f}x 50D volume.'
            badges = ['PEG Type', '10% Gap', '3x Volume']
            entry_style = 'peg_type'
        watchlist.append(
            {
                'ticker': hit.ticker,
                'sector': hit.sector,
                'industry': hit.industry,
                'exchange': hit.exchange,
                'setup_label': hit.signal_label,
                'summary': summary,
                'master_note': '. '.join(hit.reasons),
                'event_date': hit.signal_date,
                'event_label': hit.signal_label,
                'trigger_label': 'Gap day high',
                'trigger_price': round(hit.high_price, 4),
                'entry_style': entry_style,
                'entry_price': round(hit.current_price, 4),
                'entry_label': 'Gap day close',
                'entry_timeframe': 'daily',
                'secondary_entry_price': round(hit.low_price, 4),
                'secondary_entry_label': 'Gap day low',
                'secondary_entry_timeframe': 'daily',
                'stop_price': round(hit.low_price, 4),
                'stop_label': 'Gap day low',
                'stop_timeframe': 'daily',
                'current_price': round(hit.current_price, 4),
                'open_price': round(hit.open_price, 4),
                'high_price': round(hit.high_price, 4),
                'low_price': round(hit.low_price, 4),
                'previous_close': round(hit.previous_close, 4),
                'close_gap_pct': round(hit.close_gap_pct, 2),
                'volume_ratio': round(hit.volume_ratio, 2),
                'volume_buzz_pct': round(hit.volume_buzz_pct, 1),
                'earnings_release_date': hit.earnings_release_date,
                'earnings_release_session': hit.earnings_release_session,
                'earnings_eps_surprise_pct': round(hit.earnings_eps_surprise_pct, 2) if hit.earnings_eps_surprise_pct is not None else None,
                'earnings_trading_days_since_event': hit.earnings_trading_days_since_event,
                'signal_badges': badges,
            }
        )
    return watchlist
