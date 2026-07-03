from __future__ import annotations

from .vcp_spec_screen import VcpSpecHit


def build_vcp_spec_watchlist(hits: list[VcpSpecHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        is_pre_breakout = hit.category == "pre_breakout"
        summary = (
            f"{'Pre-breakout' if is_pre_breakout else 'Breakout'} strict VCP base. "
            f"{hit.contractions_count} contractions, prior uptrend {hit.prior_uptrend_pct:.1f}%, "
            f"pivot delta {hit.pivot_within_top_pct:+.2f}%."
        )
        badges = [
            "VCP Spec",
            "Pre-Breakout" if is_pre_breakout else "Breakout",
            f"{hit.contractions_count} Waves",
        ]
        if hit.breakout_volume_ratio is not None:
            badges.append(f"{hit.breakout_volume_ratio:.1f}x Vol")
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "VCP Spec",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Spec breakout" if not is_pre_breakout else "Spec coil",
                "trigger_label": "Pivot",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "vcp_spec_pre_breakout" if is_pre_breakout else "vcp_spec_breakout",
                "entry_price": round(hit.pivot_price, 4),
                "entry_label": "Break above pivot" if is_pre_breakout else "Hold above pivot",
                "entry_timeframe": "daily",
                "stop_price": round(hit.stop_price, 4),
                "stop_label": "Last contraction low",
                "stop_timeframe": "daily",
                "signal_kind": hit.category,
                "category": hit.category,
                "geometric_score": round(hit.geometric_score, 2),
                "contractions_count": hit.contractions_count,
                "contraction_depths": list(hit.contraction_depths),
                "prior_uptrend_pct": round(hit.prior_uptrend_pct, 2),
                "prior_uptrend_weeks": hit.prior_uptrend_weeks,
                "pivot_within_top_pct": round(hit.pivot_within_top_pct, 2),
                "breakout_observed": hit.breakout_observed,
                "breakout_volume_ratio": round(hit.breakout_volume_ratio, 2) if hit.breakout_volume_ratio is not None else None,
                "base_duration_days": hit.base_duration_days,
                "base_start_date": hit.base_start_date,
                "base_end_date": hit.base_end_date,
                "signal_badges": badges,
            }
        )
    return watchlist
