from __future__ import annotations

from .high_tight_flag_setup_screen import HighTightFlagSetupHit


def build_high_tight_flag_setup_watchlist(hits: list[HighTightFlagSetupHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        pole_gain_pct = (hit.pole_gain_ratio - 1.0) * 100.0
        distance_to_pivot_pct = hit.distance_to_pivot_pct * 100.0
        summary = (
            f"HTF setup: pole {pole_gain_pct:.1f}% in {hit.pole_days} bars, "
            f"flag {hit.flag_drawdown_pct * 100.0:.1f}% over {hit.flag_days} bars, "
            f"{distance_to_pivot_pct:.1f}% below pivot."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "High Tight Flag Setup",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "High Tight Flag Setup",
                "trigger_label": "Pivot price",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "high_tight_flag_setup",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Current close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.flag_low, 4),
                "stop_label": "Flag low",
                "stop_timeframe": "daily",
                "signal_badges": ["HTF", "Setup"],
                "atr_ratio": round(hit.atr_ratio, 4),
                "pivot_gap_pct": round(distance_to_pivot_pct, 2),
                "pole_gain_pct": round(pole_gain_pct, 2),
                "flag_drawdown_pct": round(hit.flag_drawdown_pct * 100.0, 2),
            }
        )
    return watchlist
