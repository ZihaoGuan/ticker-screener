from __future__ import annotations

import unittest

from src.universe import UniverseTicker
from src.weekly_sepa_vcp_screen import find_weekly_sepa_vcp_hit
from src.weekly_vcp_scored_screen import score_weekly_vcp_hit
from src.weekly_vcp_spec_screen import find_weekly_vcp_spec_hit

from tests.test_sepa_vcp_screen import _sepa_vcp_benchmark_frame, _sepa_vcp_stock_frame
from tests.test_vcp_scored_screen import _good_benchmark_frame, _good_stock_frame, _hit
from tests.test_vcp_spec_screen import _good_frame


class WeeklyVcpScreenersTests(unittest.TestCase):
    def test_find_weekly_vcp_spec_hit_returns_candidate(self) -> None:
        hit = find_weekly_vcp_spec_hit(
            _good_frame(),
            ticker=UniverseTicker(symbol="TEST", sector="Technology", industry="Software"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertIn(hit.category, {"pre_breakout", "breakout"})
        self.assertGreaterEqual(hit.contractions_count, 2)

    def test_score_weekly_vcp_hit_returns_scored_payload(self) -> None:
        scored = score_weekly_vcp_hit(
            _hit(),
            bars=_good_stock_frame(),
            benchmark_bars=_good_benchmark_frame(),
        )

        self.assertIsNotNone(scored)
        assert scored is not None
        self.assertGreater(scored.composite_score, 50.0)
        self.assertIn(scored.execution_state, {"Pre-breakout", "Breakout", "Early-post-breakout", "Extended"})

    def test_find_weekly_sepa_vcp_hit_returns_recent_signal(self) -> None:
        hit = find_weekly_sepa_vcp_hit(
            _sepa_vcp_stock_frame(),
            _sepa_vcp_benchmark_frame(),
            ticker=UniverseTicker(symbol="AAPL"),
            benchmark_ticker="SPY",
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertGreater(hit.rpr_score, 70.0)
        self.assertIn(hit.buy_risk_status, {"Low Risk", "Caution", "Extended"})


if __name__ == "__main__":
    unittest.main()
