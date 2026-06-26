from __future__ import annotations

from .stockbee_momentum_burst_screen import StockbeeMomentumBurstHit


def build_stockbee_momentum_burst_watchlist(hits: list[StockbeeMomentumBurstHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        badges = [hit.rating, hit.primary_trigger.replace("_", " ")]
        if "range_expansion" in hit.trigger_tags:
            badges.append("Range Expansion")
        summary = (
            f"{hit.rating} {hit.primary_trigger.replace('_', ' ')} setup. "
            f"Gain {hit.day_gain_pct:.2f}%, close location {hit.close_location_pct:.1f}%, "
            f"risk {hit.risk_pct_to_stop:.2f}% to trigger-day low."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Stockbee Burst",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": hit.primary_trigger.replace("_", " ").title(),
                "trigger_label": "Trigger high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "momentum_burst_close",
                "entry_price": round(hit.entry_reference, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.stop_reference, 4),
                "stop_label": "Trigger-day low",
                "stop_timeframe": "daily",
                "signal_kind": hit.primary_trigger,
                "signal_badges": badges,
                "rating": hit.rating,
                "state": hit.state,
                "score": hit.score,
                "day_gain_pct": round(hit.day_gain_pct, 2),
                "volume_ratio_20d": round(hit.volume_ratio_20d, 2),
                "close_location_pct": round(hit.close_location_pct, 2),
                "risk_pct_to_stop": round(hit.risk_pct_to_stop, 2),
            }
        )
    return watchlist
