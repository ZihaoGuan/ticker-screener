from __future__ import annotations

from .minervini_vcp_detector_screen import MinerviniVcpDetectorHit


def build_minervini_vcp_detector_watchlist(hits: list[MinerviniVcpDetectorHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Minervini VCP Detector",
            "summary": (
                f"Trend Template qualified, ${hit.market_cap / 1_000_000_000:.1f}B market cap, "
                f"and {abs(hit.distance_from_daily_100d_high_pct):.1f}% below stable 100D resistance."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": "Pre-breakout VCP setup",
            "trigger_label": "100D resistance",
            "trigger_price": round(hit.daily_100d_high, 4),
            "entry_style": "minervini_vcp_pre_breakout",
            "entry_price": round(hit.daily_100d_high, 4),
            "entry_label": "Breakout trigger",
            "entry_timeframe": "daily",
            "current_price": round(hit.current_price, 4),
            "market_cap": round(hit.market_cap, 2),
            "daily_100d_high": round(hit.daily_100d_high, 4),
            "weekly_100w_high": round(hit.weekly_100w_high, 4),
            "distance_from_daily_100d_high_pct": round(hit.distance_from_daily_100d_high_pct, 2),
            "distance_from_weekly_100w_high_pct": round(hit.distance_from_weekly_100w_high_pct, 2),
            "volume_contracting_comparisons": hit.volume_contracting_comparisons,
            "signal_badges": [
                "Trend Template", "Market Cap > $2B", "Stable 100D Resistance", "Higher Lows", "Volume Contracting",
            ],
        }
        for hit in hits
    ]
