from __future__ import annotations

from .vcs_screen import VcsHit


def build_vcs_watchlist(hits: list[VcsHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"VCS {hit.vcs_score:.1f} in {hit.stage_label}. "
            f"Trend factor {hit.trend_factor:.2f}, days tight {hit.days_tight}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": f"VCS {hit.stage_label}",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": f"VCS {hit.stage_label}",
                "trigger_label": "Signal high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "vcs_signal",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "signal_profile": hit.signal_profile,
                "signal_kind": hit.stage,
                "vcs_score": round(hit.vcs_score, 4),
                "signal_badges": ["VCS", hit.stage_label],
            }
        )
    return watchlist
