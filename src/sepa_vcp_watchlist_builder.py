from __future__ import annotations

from .sepa_vcp_screen import SepaVcpHit


def build_sepa_vcp_watchlist(hits: list[SepaVcpHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"Recent 5D squeeze on {hit.signal_date}. "
            f"TPR {hit.tpr_status}, {hit.buy_risk_status}, {hit.pressure_status}, RPR {hit.rpr_score:.1f}."
        )
        badges = [
            "SEPA VCP",
            f"TPR {hit.tpr_status}",
            hit.buy_risk_status,
            hit.pressure_status,
            f"RPR {round(hit.rpr_score)}",
        ]
        if hit.vcp_trigger:
            badges.append("SQUEEZE NOW")
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "exchange": hit.exchange,
                "setup_label": "SEPA VCP",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "5D squeeze",
                "trigger_label": "Squeeze high",
                "trigger_price": round(hit.trigger_price, 4),
                "entry_style": "sepa_vcp_squeeze",
                "entry_price": round(hit.trigger_price, 4),
                "entry_label": "Break above squeeze high",
                "entry_timeframe": "daily",
                "stop_price": round(hit.stop_price, 4),
                "stop_label": "Squeeze low",
                "stop_timeframe": "daily",
                "current_price": round(hit.current_price, 4),
                "signal_kind": hit.signal_kind,
                "tpr_status": hit.tpr_status,
                "buy_risk_status": hit.buy_risk_status,
                "buy_risk_distance_pct": round(hit.buy_risk_distance_pct, 2),
                "pressure_status": hit.pressure_status,
                "rpr_score": round(hit.rpr_score, 2),
                "rpr_status": hit.rpr_status,
                "vcp_status": hit.vcp_status,
                "vcp_range_pct": round(hit.vcp_range_pct, 3),
                "ma50": round(hit.ma50, 4),
                "ma150": round(hit.ma150, 4),
                "ma200": round(hit.ma200, 4),
                "signal_badges": badges,
            }
        )
    return watchlist
