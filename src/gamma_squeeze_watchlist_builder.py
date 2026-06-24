from __future__ import annotations

from .gamma_squeeze_screen import GammaSqueezeHit


def build_gamma_squeeze_watchlist(hits: list[GammaSqueezeHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        call_wall_text = f"{hit.call_wall:.2f}" if hit.call_wall is not None else "n/a"
        gamma_flip_text = f"{hit.gamma_flip:.2f}" if hit.gamma_flip is not None else "n/a"
        summary = (
            f"Squeeze score {hit.squeeze_score:.1f}. "
            f"{hit.gex_regime.title()} gamma, flip {gamma_flip_text}, call wall {call_wall_text}."
        )
        badges = ["Gamma Squeeze", hit.gex_regime.title()]
        if hit.distance_to_flip_pct is not None and abs(hit.distance_to_flip_pct) <= 2.5:
            badges.append("Near Flip")
        if hit.distance_to_call_wall_pct is not None and 0.0 <= hit.distance_to_call_wall_pct <= 5.0:
            badges.append("Near Call Wall")
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "Gamma Squeeze",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.as_of[:10] if hit.as_of else "",
                "event_label": "GEX snapshot",
                "trigger_label": "Call wall",
                "trigger_price": round(hit.call_wall, 4) if hit.call_wall is not None else None,
                "entry_style": "gamma_squeeze",
                "entry_price": round(hit.spot_price, 4),
                "entry_label": "Spot",
                "entry_timeframe": "daily",
                "stop_price": round(hit.put_wall, 4) if hit.put_wall is not None else None,
                "stop_label": "Put wall",
                "stop_timeframe": "daily",
                "squeeze_score": round(hit.squeeze_score, 2),
                "gex_regime": hit.gex_regime,
                "net_gex_bn": round(hit.net_gex_bn, 4),
                "gamma_flip": round(hit.gamma_flip, 4) if hit.gamma_flip is not None else None,
                "distance_to_flip_pct": hit.distance_to_flip_pct,
                "call_wall": round(hit.call_wall, 4) if hit.call_wall is not None else None,
                "put_wall": round(hit.put_wall, 4) if hit.put_wall is not None else None,
                "distance_to_call_wall_pct": hit.distance_to_call_wall_pct,
                "put_call_oi_ratio": round(hit.put_call_oi_ratio, 4) if hit.put_call_oi_ratio is not None else None,
                "call_oi_above_ratio": round(hit.call_oi_above_ratio, 4),
                "score_components": dict(hit.score_components),
                "source_url": hit.source_url,
                "signal_badges": badges,
            }
        )
    return watchlist
