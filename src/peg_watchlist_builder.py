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
    if hit.strategy_profile == "sean-peg":
        if hit.strategy_inside_day_at_ema21:
            note_parts.insert(1, "Sean inside day at 21 EMA")
        elif hit.strategy_breakout_ready:
            note_parts.insert(1, "Sean breakout-ready")
        elif hit.strategy_dema_support_ready:
            note_parts.insert(1, "Sean 8 DEMA support-ready")
        else:
            note_parts.insert(1, "Sean qualified setup")
    else:
        note_parts.insert(1, "Actionable now" if hit.actionable_now else "Event only (not actionable now)")
    if hit.strategy_profile == "sean-peg" and hit.strategy_inside_day and not hit.strategy_inside_day_at_ema21:
        note_parts.append("Inside day")
    if hit.strategy_profile == "sean-peg" and hit.strategy_ema_21 is not None:
        note_parts.append(f"21 EMA {hit.strategy_ema_21:.2f}")
    if hit.strategy_profile == "sean-peg" and hit.strategy_ema21_distance_pct is not None:
        note_parts.append(f"21 EMA distance {hit.strategy_ema21_distance_pct:+.2f}%")
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
    if hit.strategy_profile == "sean-peg":
        if hit.strategy_adr_pct_20 is not None:
            note_parts.append(f"ADR20 {hit.strategy_adr_pct_20:.2f}%")
        if hit.strategy_avg_volume_20 is not None:
            note_parts.append(f"Avg vol20 {hit.strategy_avg_volume_20:,.0f}")
    return ". ".join(note_parts) + "."


def build_peg_watchlist(hits: list[PegHit], *, strategy_profile: str = "legacy") -> list[dict[str, object]]:
    if strategy_profile == "sean-peg":
        filtered_hits = [
            hit
            for hit in hits
            if hit.strategy_profile == "sean-peg" and hit.strategy_qualifies
        ]
        filtered_hits.sort(
            key=lambda hit: (
                not (hit.strategy_inside_day and hit.strategy_price_above_ema21),
                (
                    hit.strategy_ema21_distance_pct
                    if (
                        hit.strategy_inside_day
                        and hit.strategy_price_above_ema21
                        and hit.strategy_ema21_distance_pct is not None
                        and hit.strategy_ema21_distance_pct >= 0
                    )
                    else float("inf")
                ),
                not hit.strategy_inside_day_at_ema21,
                -(hit.strategy_setup_score or 0),
                not hit.strategy_breakout_ready,
                not hit.strategy_dema_support_ready,
                -(hit.strategy_peg_age_days or 0),
            ),
        )
    else:
        filtered_hits = hits

    watchlist: list[dict[str, object]] = []
    for hit in filtered_hits:
        status_text = "Actionable now" if hit.actionable_now else "Event only"
        if strategy_profile == "sean-peg":
            setup_text = "Breakout watch" if hit.strategy_breakout_ready else "8 DEMA support watch"
            summary = (
                f"PEG event {hit.peg_date}. {setup_text}. "
                f"Age {hit.strategy_peg_age_days or 0} bars since gap. "
                f"ADR20 {hit.strategy_adr_pct_20:.2f}%. "
                f"Avg vol20 {hit.strategy_avg_volume_20:,.0f}. "
                f"Low-volume pullback {'yes' if hit.strategy_low_volume_pullback else 'no'}. "
                f"Inside day at 21 EMA {'yes' if hit.strategy_inside_day_at_ema21 else 'no'}."
            )
            if hit.strategy_ema21_distance_pct is not None:
                summary += f" 21 EMA distance {hit.strategy_ema21_distance_pct:+.2f}%."
        else:
            summary = (
                f"PEG event {hit.peg_date}. {status_text}. "
                f"Entry distance {hit.entry_distance_pct * 100:.1f}% from PEG low. "
                f"Close position {hit.close_position_ratio:.2f}. "
                f"Volume ratio {hit.volume_ratio:.2f}x."
            )
        if hit.earnings_date:
            summary = f"Earnings {hit.earnings_date}. " + summary

        entry_price = hit.primary_entry if hit.primary_entry is not None else hit.peg_low
        entry_label = hit.primary_entry_label or "PEG low"
        trigger_price = round(hit.gdh, 4)
        trigger_label = "Gap-day high"
        entry_style = "peg_pullback"
        secondary_entry_price = round(hit.secondary_entry_high, 4) if hit.secondary_entry_high is not None else None
        secondary_entry_label = hit.secondary_entry_label or "EMA zone"
        if strategy_profile == "sean-peg":
            trigger_price = round(hit.strategy_breakout_trigger or hit.gdh, 4)
            trigger_label = "Post-gap breakout trigger"
            entry_style = "post_earnings_gap_breakout" if hit.strategy_breakout_ready else "post_earnings_gap_dema_support"
            if hit.strategy_inside_day and hit.strategy_price_above_ema21 and hit.strategy_ema_21 is not None:
                entry_price = hit.strategy_ema_21
                entry_label = "Inside day at 21 EMA" if hit.strategy_inside_day_at_ema21 else "Inside day above 21 EMA"
                secondary_entry_price = round(hit.strategy_ema_21, 4)
                secondary_entry_label = "21 EMA"
            elif hit.strategy_dema_8 is not None:
                entry_price = hit.strategy_dema_8 if hit.strategy_dema_support_ready else entry_price
                entry_label = "8 DEMA support" if hit.strategy_dema_support_ready else entry_label
                secondary_entry_price = round(hit.strategy_dema_8, 4)
                secondary_entry_label = "8 DEMA"
        watchlist_entry: dict[str, object] = {
            "ticker": hit.ticker,
            "setup_label": "Post earnings gap tight flag" if strategy_profile == "sean-peg" else "Power earnings gap",
            "summary": summary,
            "master_note": _format_note(hit),
            "event_date": hit.peg_date,
            "event_label": "PEG",
            "trigger_label": trigger_label,
            "trigger_price": trigger_price,
            "entry_style": entry_style,
            "entry_price": round(entry_price, 4),
            "entry_label": entry_label,
            "entry_timeframe": "daily",
            "stop_price": round(hit.hvc5, 4),
            "stop_label": "HVC -5%",
            "stop_timeframe": "daily",
        }
        if secondary_entry_price is not None:
            watchlist_entry["secondary_entry_price"] = secondary_entry_price
            watchlist_entry["secondary_entry_label"] = secondary_entry_label
            watchlist_entry["secondary_entry_timeframe"] = "daily"
        if hit.secondary_entry_low is not None:
            watchlist_entry["secondary_entry_low"] = round(hit.secondary_entry_low, 4)
        if hit.secondary_entry_high is not None:
            watchlist_entry["secondary_entry_high"] = round(hit.secondary_entry_high, 4)
        watchlist.append(watchlist_entry)
    return watchlist
