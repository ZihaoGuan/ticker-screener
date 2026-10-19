from __future__ import annotations

from .qullamaggie_screen import QullamaggieHit


def build_qullamaggie_watchlist(hits: list[QullamaggieHit]) -> list[dict[str, object]]:
    return [
        {
            "ticker": hit.ticker,
            "sector": hit.sector,
            "industry": hit.industry,
            "exchange": hit.exchange,
            "setup_label": "Qullamaggie Breakout",
            "summary": (
                f"Recent return {hit.recent_return_pct:.1f}%, ADR20 {hit.adr_pct_20:.1f}%, "
                f"Daily RS {hit.daily_rs_rating:.0f}, and {hit.distance_from_52w_high_pct:.1f}% below the 52-week high."
            ),
            "master_note": ". ".join(hit.reasons),
            "event_date": hit.signal_date,
            "event_label": f"Qullamaggie breakout {hit.setup_state}",
            "trigger_label": "Base pivot",
            "trigger_price": round(hit.pivot, 4),
            "entry_style": "qullamaggie",
            "entry_price": round(hit.pivot, 4),
            "entry_label": "Base pivot reference",
            "entry_timeframe": "daily",
            "secondary_entry_price": round(hit.sma20, 4),
            "secondary_entry_label": "20 SMA reference",
            "secondary_entry_timeframe": "daily",
            "stop_price": round(hit.breakout_day_low, 4) if hit.breakout_day_low is not None else None,
            "stop_label": "Breakout day low" if hit.breakout_day_low is not None else None,
            "stop_timeframe": "daily" if hit.breakout_day_low is not None else None,
            "current_price": round(hit.current_price, 4),
            "market_cap": round(hit.market_cap, 2),
            "avg_volume_20": round(hit.avg_volume_20, 2),
            "adr_pct_20": round(hit.adr_pct_20, 2),
            "daily_rs_rating": round(hit.daily_rs_rating, 2),
            "return_3m_pct": round(hit.return_3m_pct, 2),
            "return_1m_pct": round(hit.return_1m_pct, 2),
            "return_6m_pct": round(hit.return_6m_pct, 2),
            "recent_return_pct": round(hit.recent_return_pct, 2),
            "distance_from_52w_high_pct": round(hit.distance_from_52w_high_pct, 2),
            "tight_flag_range_pct": round(hit.tight_flag_range_pct, 2),
            "fast_ma_distance_pct": round(hit.fast_ma_distance_pct, 2),
            "tight_flag": hit.tight_flag,
            "pullback_near_fast_ma": hit.pullback_near_fast_ma,
            "higher_low": hit.higher_low,
            "range_contracting": hit.range_contracting,
            "volume_contracting": hit.volume_contracting,
            "signal_badges": [
                "Prior Impulse >= 30%",
                "10-40 Session Base",
                "Higher Low",
                *(["Range Contracting"] if hit.range_contracting else []),
                *(["Volume Contracting"] if hit.volume_contracting else []),
                "Rising 10/20 SMA",
                "Near 52W High",
                "Above 10/20/50/200 SMA",
                "Avg Volume >= 1M",
                "Market Cap >= $100M",
                "ADR20 >= 4%",
                "Daily RS >= 80",
            ],
        }
        for hit in hits
    ]
