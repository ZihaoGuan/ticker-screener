from __future__ import annotations

import unittest

from src.gamma_squeeze_screen import build_gamma_squeeze_hit
from src.universe import UniverseTicker


def _report(*, net_gex: float, gamma_flip: float, call_wall: float, put_call_oi_ratio: float, call_oi_above: float) -> dict[str, object]:
    return {
        "symbol": "NVDA",
        "as_of": "2026-06-24T20:00:00+00:00",
        "underlying_price": 150.0,
        "net_gex": net_gex,
        "gamma_flip": gamma_flip,
        "call_wall": call_wall,
        "put_wall": 140.0,
        "top_net_gex_strike": 155.0,
        "put_call_oi_ratio": put_call_oi_ratio,
        "source_url": "https://example.test/nvda.json",
        "strikes": [
            {"strike": 140.0, "call_oi": 900.0, "put_oi": 1400.0, "net_gex": -400_000_000.0},
            {"strike": 150.0, "call_oi": 1200.0, "put_oi": 900.0, "net_gex": -250_000_000.0},
            {"strike": 155.0, "call_oi": call_oi_above, "put_oi": 300.0, "net_gex": 350_000_000.0},
            {"strike": 160.0, "call_oi": 1800.0, "put_oi": 250.0, "net_gex": 500_000_000.0},
        ],
    }


class GammaSqueezeScreenTests(unittest.TestCase):
    def test_build_gamma_squeeze_hit_returns_hit_for_high_score_setup(self) -> None:
        hit = build_gamma_squeeze_hit(
            _report(
                net_gex=-1_200_000_000.0,
                gamma_flip=149.0,
                call_wall=153.0,
                put_call_oi_ratio=0.62,
                call_oi_above=2400.0,
            ),
            ticker=UniverseTicker(symbol="NVDA", sector="Technology", industry="Semiconductors", exchange="NASDAQ"),
        )

        self.assertIsNotNone(hit)
        assert hit is not None
        self.assertEqual(hit.gex_regime, "negative")
        self.assertGreaterEqual(hit.squeeze_score, 65.0)
        self.assertLess(hit.distance_to_call_wall_pct or 99.0, 5.0)
        self.assertTrue(any("Net GEX" in reason for reason in hit.reasons))

    def test_build_gamma_squeeze_hit_filters_low_conviction_setup(self) -> None:
        hit = build_gamma_squeeze_hit(
            _report(
                net_gex=1_800_000_000.0,
                gamma_flip=132.0,
                call_wall=175.0,
                put_call_oi_ratio=1.8,
                call_oi_above=300.0,
            ),
            ticker=UniverseTicker(symbol="NVDA"),
        )

        self.assertIsNone(hit)


if __name__ == "__main__":
    unittest.main()
