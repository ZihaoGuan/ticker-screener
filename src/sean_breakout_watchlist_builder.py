from __future__ import annotations

from .sean_breakout_screen import SeanBreakoutHit


def build_sean_breakout_watchlist(hits: list[SeanBreakoutHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Close {hit.current_price:.2f} above EMA21 {hit.ema21_value:.2f} and EMA50 {hit.ema50_value:.2f}. "
            f"AvgVol10 {hit.avg_volume_10:,.0f}, ADR20 {hit.adr_pct_20:.2f}%."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Sean Breakout",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Sean Breakout",
                "trigger_label": "Signal high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "sean_breakout_signal",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "signal_kind": hit.signal_kind,
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "ema21_value": round(hit.ema21_value, 4),
                "ema50_value": round(hit.ema50_value, 4),
                "avg_volume_10": round(hit.avg_volume_10, 4),
                "adr_pct_20": round(hit.adr_pct_20, 4),
                "signal_badges": ["Sean Breakout", "ADR20 >= 2%", "AvgVol10 > 500k"],
            }
        )
    return watchlist
