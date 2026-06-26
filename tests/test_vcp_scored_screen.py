from __future__ import annotations

import datetime as dt
import pandas as pd
import unittest

from src.vcp_screen import VcpHit
from src.vcp_scored_screen import score_vcp_hit


def _frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame.set_index("Date")


def _good_stock_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    price = 50.0
    start = dt.date(2025, 1, 1)
    for day in range(1, 330):
        if day < 250:
            price += 0.18
        elif day < 290:
            price += (-0.10 if day % 7 == 0 else 0.12)
        else:
            price += (-0.04 if day % 6 == 0 else 0.08)
        high = price * 1.01
        low = price * 0.99
        volume = 1_000_000 if day < 300 else 650_000
        rows.append({"Date": (start + dt.timedelta(days=day)).isoformat(), "High": high, "Low": low, "Close": price, "Volume": volume})
    rows[-1]["Close"] = 108.0
    rows[-1]["High"] = 109.0
    rows[-1]["Low"] = 107.0
    rows[-2]["Close"] = 107.4
    return _frame(rows)


def _good_benchmark_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    price = 100.0
    start = dt.date(2025, 1, 1)
    for day in range(1, 330):
        price += 0.08
        rows.append({"Date": (start + dt.timedelta(days=day)).isoformat(), "High": price * 1.01, "Low": price * 0.99, "Close": price, "Volume": 2_000_000})
    return _frame(rows)


def _hit() -> VcpHit:
    return VcpHit(
        ticker="TEST",
        sector="Technology",
        exchange="NASDAQ",
        signal_date="2026-06-26",
        benchmark_ticker="SPY",
        screen_profile="strict",
        current_price=108.0,
        support_price=102.0,
        pivot_price=107.5,
        vcp_contractions_count=3,
        vcp_record=[],
        footprint=[],
        is_vcp_structure_valid=True,
        is_good_pivot=True,
        is_deep_correction=False,
        is_demand_dry=True,
        demand_dry_start_date=None,
        demand_dry_end_date=None,
        demand_dry_volume_slope=-0.5,
        demand_dry_recent_volume_slope=-0.3,
        is_breakout_volume_confirmed=True,
        breakout_day_volume=2_000_000,
        breakout_avg_volume_50=800_000,
        is_near_year_high=True,
        year_high=109.0,
        distance_from_year_high_pct=0.01,
        is_strong_rs=True,
        stock_return_vs_rs_window_pct=25.0,
        benchmark_return_vs_rs_window_pct=8.0,
        current_rs_line=1.1,
        rs_line_high=1.12,
        is_sector_etf_strong=True,
        sector_etf="XLK",
        sector_etf_near_year_high=True,
        sector_etf_distance_from_year_high_pct=0.03,
        sector_etf_return_vs_rs_window_pct=12.0,
        sector_benchmark_return_vs_rs_window_pct=5.0,
        reasons=["3 contractions", "tightening VCP structure"],
    )


class VcpScoredScreenTests(unittest.TestCase):
    def test_score_vcp_hit_returns_strong_or_better_for_good_setup(self) -> None:
        scored = score_vcp_hit(_hit(), bars=_good_stock_frame(), benchmark_bars=_good_benchmark_frame())

        self.assertIsNotNone(scored)
        assert scored is not None
        self.assertGreater(scored.composite_score, 80.0)
        self.assertIn(scored.rating, {"Strong VCP", "Textbook VCP"})
        self.assertIn(scored.execution_state, {"Pre-breakout", "Breakout", "Early-post-breakout"})


if __name__ == "__main__":
    unittest.main()
