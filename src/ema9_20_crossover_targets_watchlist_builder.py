from __future__ import annotations

from .ema9_20_crossover_targets_screen import Ema9_20CrossoverTargetHit


def _target_badge(label: str, hit_date: str | None) -> str:
    return f"{label} hit {hit_date}" if hit_date else f"{label} pending"


def build_ema9_20_crossover_targets_watchlist(hits: list[Ema9_20CrossoverTargetHit]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    for hit in hits:
        badges = [
            "Fresh Buy" if hit.is_fresh_signal else "Recent Buy",
            "Above 150 EMA" if hit.above_ema150 else "Below 150 EMA",
            "Above 500 EMA" if hit.above_ema500 else "Below 500 EMA",
            _target_badge("Bull T1", hit.bull_target_1_hit_date),
            _target_badge("Bull T2", hit.bull_target_2_hit_date),
        ]
        if hit.bear_target_1 is not None:
            badges.append(_target_badge("Bear T1", hit.bear_target_1_hit_date))
        if hit.bear_target_2 is not None:
            badges.append(_target_badge("Bear T2", hit.bear_target_2_hit_date))
        entries.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "EMA 9/20 Buy",
                "summary": f"{('Fresh' if hit.is_fresh_signal else 'Recent')} EMA 9/20 bullish crossover on {hit.signal_date}; {hit.target_status.replace('_', ' ')}.",
                "master_note": " ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Confirmed EMA 9/20 buy crossover",
                "trigger_label": "Signal-bar open",
                "trigger_price": round(hit.signal_open, 4),
                "entry_style": "ema9_20_crossover_targets",
                "entry_price": round(hit.signal_open, 4),
                "entry_label": "Signal open",
                "entry_timeframe": "daily",
                "target_price": round(hit.bull_target_1, 4),
                "target_label": "Bull T1",
                "secondary_target_price": round(hit.bull_target_2, 4),
                "secondary_target_label": "Bull T2",
                "stop_price": round(hit.bear_target_1, 4) if hit.bear_target_1 is not None else None,
                "stop_label": "Bear T1 reference",
                "stop_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "signal_open": round(hit.signal_open, 4),
                "signal_close": round(hit.signal_close, 4),
                "bull_target_1": round(hit.bull_target_1, 4),
                "bull_target_2": round(hit.bull_target_2, 4),
                "bear_target_1": round(hit.bear_target_1, 4) if hit.bear_target_1 is not None else None,
                "bear_target_2": round(hit.bear_target_2, 4) if hit.bear_target_2 is not None else None,
                "bull_target_1_hit_date": hit.bull_target_1_hit_date,
                "bull_target_2_hit_date": hit.bull_target_2_hit_date,
                "bear_target_1_hit_date": hit.bear_target_1_hit_date,
                "bear_target_2_hit_date": hit.bear_target_2_hit_date,
                "bull_target_1_bars_to_hit": hit.bull_target_1_bars_to_hit,
                "bull_target_2_bars_to_hit": hit.bull_target_2_bars_to_hit,
                "bear_target_1_bars_to_hit": hit.bear_target_1_bars_to_hit,
                "bear_target_2_bars_to_hit": hit.bear_target_2_bars_to_hit,
                "target_status": hit.target_status,
                "same_bar_target_conflict": hit.same_bar_target_conflict,
                "ema9": round(hit.ema9, 4),
                "ema20": round(hit.ema20, 4),
                "ema150": round(hit.ema150, 4),
                "ema500": round(hit.ema500, 4),
                "rsi14": round(hit.rsi14, 2) if hit.rsi14 is not None else None,
                "signal_badges": badges,
            }
        )
    return entries
