from __future__ import annotations

from .leif_high_tight_flag_screen import LeifHighTightFlagHit


def build_leif_high_tight_flag_watchlist(hits: list[LeifHighTightFlagHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Leif HTF score {hit.score:.2f}. Pole {hit.pole_gain_pct:.1f}% in {hit.pole_days} bars, "
            f"flag {hit.flag_drawdown_pct:.1f}% across {hit.flag_days} bars, "
            f"breakout {hit.breakout_volume_ratio:.2f}x volume."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Leif High Tight Flag",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Leif HTF Breakout",
                "trigger_label": "Pivot",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "leif_high_tight_flag",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Breakout close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.flag_low, 4),
                "stop_label": "Flag low",
                "stop_timeframe": "daily",
                "signal_badges": ["Leif HTF", "Breakout"],
                "rs_rating": round(hit.rs_rating, 1),
                "score": round(hit.score, 2),
                "breakout_volume_ratio": round(hit.breakout_volume_ratio, 2),
                "pole_gain_pct": round(hit.pole_gain_pct, 1),
                "flag_drawdown_pct": round(hit.flag_drawdown_pct, 1),
            }
        )
    return watchlist
