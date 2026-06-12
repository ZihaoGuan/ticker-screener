from __future__ import annotations

from .bb_squeeze_screen import BbSqueezeHit


def build_bb_squeeze_watchlist(hits: list[BbSqueezeHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        cci_label = "CCI+" if hit.cci_value > 0 else "CCI-"
        summary = (
            f"BB squeeze ratio {hit.bb_squeeze_ratio:.3f} with {cci_label}. "
            f"CCI {hit.cci_value:.2f}, ATR {hit.atr_value:.3f}."
        )
        watchlist.append(
            {
                "ticker": hit.ticker,
                "sector": hit.sector,
                "industry": hit.industry,
                "setup_label": "BB Squeeze",
                "summary": summary,
                "master_note": ". ".join(hit.reasons),
                "event_date": hit.signal_date,
                "event_label": "BB Squeeze",
                "trigger_label": "Signal high",
                "trigger_price": round(hit.high_price, 4),
                "entry_style": "bb_squeeze_signal",
                "entry_price": round(hit.current_price, 4),
                "entry_label": "Signal close",
                "entry_timeframe": "daily",
                "signal_kind": hit.signal_kind,
                "stop_price": round(hit.low_price, 4),
                "stop_label": "Signal low",
                "stop_timeframe": "daily",
                "bb_squeeze_ratio": round(hit.bb_squeeze_ratio, 4),
                "cci_value": round(hit.cci_value, 4),
                "signal_badges": ["BB Squeeze", cci_label],
            }
        )
    return watchlist
