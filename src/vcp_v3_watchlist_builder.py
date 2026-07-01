from __future__ import annotations

from .vcp_v3_screen import VcpV3Hit


def build_vcp_v3_watchlist(hits: list[VcpV3Hit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        is_pre = hit.category == "pre_breakout"
        summary = (
            f"{'Pre-breakout' if is_pre else 'Recent breakout'} VCP v3 on {hit.signal_date}. "
            f"Score {hit.vcp_score:.1f}, {hit.contraction_count} contractions, RR {hit.risk_reward:.1f}."
        )
        badges = [
            "VCP v3",
            "Pre-Breakout" if is_pre else "Broken Out",
            f"Score {round(hit.vcp_score)}",
            f"{hit.contraction_count} Waves",
        ]
        if hit.is_cup:
            badges.append("Cup")
        if hit.volume_dry_up:
            badges.append("Dry-Up")
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "VCP v3",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "Pre-breakout coil" if is_pre else "Confirmed breakout",
                "trigger_label": "Pivot",
                "trigger_price": round(hit.pivot_price, 4),
                "entry_style": "vcp_v3_pre_breakout" if is_pre else "vcp_v3_broken_out",
                "entry_price": round(hit.entry_price, 4),
                "entry_label": "Break above pivot" if is_pre else "Pivot retest entry",
                "entry_timeframe": "daily",
                "stop_price": round(hit.stop_price, 4),
                "stop_label": "ATR stop" if is_pre else "Pivot failure stop",
                "stop_timeframe": "daily",
                "target_price": round(hit.target1_price, 4),
                "current_price": round(hit.current_price, 4),
                "signal_kind": hit.signal_kind,
                "category": hit.category,
                "risk_reward": round(hit.risk_reward, 2),
                "vcp_score": round(hit.vcp_score, 1),
                "contraction_count": hit.contraction_count,
                "waves": list(hit.waves),
                "contraction_ratios": list(hit.contraction_ratios),
                "rising_lows_count": hit.rising_lows_count,
                "is_cup": hit.is_cup,
                "coil_atr_percentile": round(hit.coil_atr_percentile, 2),
                "coil_volume_ratio": round(hit.coil_volume_ratio, 3),
                "rs_vs_benchmark_pct": round(hit.rs_vs_benchmark_pct, 2) if hit.rs_vs_benchmark_pct is not None else None,
                "below_52w_high_pct": round(hit.below_52w_high_pct, 2) if hit.below_52w_high_pct is not None else None,
                "signal_badges": badges,
            }
        )
    return watchlist
