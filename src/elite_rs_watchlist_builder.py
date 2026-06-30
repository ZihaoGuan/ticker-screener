from __future__ import annotations

from .elite_rs_screen import EliteRsHit


def build_elite_rs_watchlist(hits: list[EliteRsHit], *, profile: str) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    normalized_profile = str(profile or 'hv1').strip().lower()
    for hit in hits:
        if normalized_profile == 'recent-peg':
            summary = (
                f'RS {hit.rs_rating:.1f} with Pine-style PEG footprint from {hit.peg_date or hit.signal_date}. '
                f'Gap was {hit.peg_gap_pct or 0:.1f}% and signal age is {hit.peg_event_age_days or 0} bar(s).'
            )
            setup_label = 'Elite RS + Recent PEG'
            event_label = 'PEG Type'
            trigger_label = 'Gap day high follow-through'
            entry_style = 'elite_rs_recent_peg'
            badges = ['Elite RS', 'PEG Type', '10% Gap', '3x Volume']
        else:
            summary = (
                f'RS {hit.rs_rating:.1f} with {hit.volume_signal_kind or "HV1"} volume signal on {hit.volume_signal_date or hit.signal_date}. '
                f'Signal age is {hit.volume_signal_age_days or 0} bar(s).'
            )
            setup_label = 'Elite RS + HV1'
            event_label = hit.volume_signal_kind or 'HV1'
            trigger_label = 'Volume expansion follow-through'
            entry_style = 'elite_rs_hv1'
            badges = ['Elite RS', 'HV1', hit.volume_signal_kind or 'HV1', 'RS > 90']
        watchlist.append(
            {
                'ticker': hit.ticker,
                'sector': hit.sector,
                'industry': hit.industry,
                'exchange': hit.exchange,
                'setup_label': setup_label,
                'summary': summary,
                'master_note': '. '.join(hit.reasons),
                'event_date': hit.signal_date,
                'event_label': event_label,
                'trigger_label': trigger_label,
                'trigger_price': round(hit.current_price, 4),
                'entry_style': entry_style,
                'entry_price': round(hit.current_price, 4),
                'entry_label': 'Current close',
                'entry_timeframe': 'daily',
                'current_price': round(hit.current_price, 4),
                'rs_score': round(hit.rs_score, 2),
                'rs_rating': round(hit.rs_rating, 2),
                'min_rs_rating': round(hit.min_rs_rating, 2),
                'volume_signal_kind': hit.volume_signal_kind,
                'volume_signal_date': hit.volume_signal_date,
                'volume_signal_age_days': hit.volume_signal_age_days,
                'current_volume': round(hit.current_volume, 0) if hit.current_volume is not None else None,
                'volume_ma_20': round(hit.volume_ma_20, 0) if hit.volume_ma_20 is not None else None,
                'peg_date': hit.peg_date,
                'peg_gap_pct': round(hit.peg_gap_pct, 2) if hit.peg_gap_pct is not None else None,
                'peg_event_age_days': hit.peg_event_age_days,
                'peg_actionable_now': hit.peg_actionable_now,
                'signal_badges': badges,
            }
        )
    return watchlist
