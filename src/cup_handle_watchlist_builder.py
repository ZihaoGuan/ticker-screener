from __future__ import annotations

from .cup_handle_screen import CupHandleHit


def _format_note(hit: CupHandleHit) -> str:
    return (
        f"{hit.pattern_direction.title()} cup width {hit.cup_width_bars} bars. "
        f"Handle {hit.handle_width_bars} bars. "
        f"Containment {hit.containment_ratio * 100:.1f}%. "
        f"Volume {hit.breakout_volume_ratio:.2f}x 50-day average. "
        f"Target {hit.target_price:.2f}."
    )


def build_cup_handle_watchlist(hits: list[CupHandleHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        setup_label = "Cup & Handle" if hit.pattern_direction == "bullish" else "Inverted Cup & Handle"
        trigger_label = "Rim breakout" if hit.pattern_direction == "bullish" else "Rim breakdown"
        entry_style = "breakout" if hit.pattern_direction == "bullish" else "breakdown"
        summary = (
            f"{setup_label} confirmed on {hit.breakout_date}. "
            f"Cup width {hit.cup_width_bars} bars, handle width {hit.handle_width_bars} bars. "
            f"Depth {hit.depth_pct * 100:.1f}%, handle retrace {hit.handle_retrace_pct * 100:.1f}%. "
            f"Containment {hit.containment_ratio * 100:.1f}%, breakout volume {hit.breakout_volume_ratio:.2f}x. "
            f"Depth projection target {hit.target_price:.2f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "setup_label": setup_label,
                "summary": summary,
                "master_note": _format_note(hit),
                "trigger_label": trigger_label,
                "trigger_price": round(hit.breakout_price, 4),
                "entry_style": entry_style,
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.stop_price, 4),
                "stop_label": "Handle pivot",
                "stop_timeframe": "daily",
            }
        )
    return watchlist
