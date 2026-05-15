from __future__ import annotations

from .peg_screen import PegHit


def _format_note(hit: PegHit) -> str:
    note_parts = [
        f"{hit.setup_type.upper()} on {hit.peg_date}",
        f"Gap {hit.gap_pct * 100:.1f}%",
        f"Volume {hit.volume_ratio:.2f}x",
        f"PEG low {hit.peg_low:.2f}",
        f"Gap-day high {hit.gdh:.2f}",
    ]
    if hit.secondary_entry_low is not None and hit.secondary_entry_high is not None:
        note_parts.append(
            f"EMA zone {hit.secondary_entry_low:.2f}-{hit.secondary_entry_high:.2f}"
        )
    if hit.earnings_surprise_pct is not None:
        note_parts.append(f"EPS surprise {hit.earnings_surprise_pct:.1f}%")
    if hit.distribution_warning:
        note_parts.append(
            f"Distribution warning ({hit.distribution_days_count} days)"
        )
    return ". ".join(note_parts) + "."


def build_peg_watchlist(hits: list[PegHit]) -> list[dict[str, object]]:
    watchlist: list[dict[str, object]] = []
    for hit in hits:
        summary = (
            f"PEG event {hit.peg_date}. "
            f"Entry distance {hit.entry_distance_pct * 100:.1f}% from PEG low. "
            f"Close position {hit.close_position_ratio:.2f}. "
            f"Volume ratio {hit.volume_ratio:.2f}x."
        )
        if hit.earnings_date:
            summary = f"Earnings {hit.earnings_date}. " + summary

        entry_price = hit.primary_entry if hit.primary_entry is not None else hit.peg_low
        watchlist_entry: dict[str, object] = {
            "ticker": hit.ticker,
            "setup_label": "Power earnings gap",
            "summary": summary,
            "master_note": _format_note(hit),
            "trigger_label": "Gap-day high",
            "trigger_price": round(hit.gdh, 4),
            "entry_style": "peg_pullback",
            "entry_price": round(entry_price, 4),
            "entry_label": hit.primary_entry_label or "PEG low",
            "entry_timeframe": "daily",
            "stop_price": round(hit.hvc5, 4),
            "stop_label": "HVC -5%",
            "stop_timeframe": "daily",
        }
        if hit.secondary_entry_high is not None:
            watchlist_entry["secondary_entry_price"] = round(hit.secondary_entry_high, 4)
            watchlist_entry["secondary_entry_label"] = hit.secondary_entry_label or "EMA zone"
            watchlist_entry["secondary_entry_timeframe"] = "daily"
        watchlist.append(watchlist_entry)
    return watchlist
