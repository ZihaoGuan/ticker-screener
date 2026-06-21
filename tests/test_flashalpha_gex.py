from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from src.flashalpha_gex import fetch_gex_snapshot, summarize_gex_payload


class FlashalphaGexTests(unittest.TestCase):
    def test_summarize_gex_payload_extracts_walls_and_regime(self) -> None:
        payload = {
            "symbol": "SPY",
            "underlying_price": 597.505,
            "as_of": "2026-02-28T16:30:45Z",
            "gamma_flip": 595.25,
            "net_gex": 2_850_000_000,
            "net_gex_label": "positive",
            "strikes": [
                {"strike": 590.0, "call_gex": 12_000_000, "put_gex": 8_000_000, "net_gex": 20_000_000, "call_oi": 10_000, "put_oi": 9_000},
                {"strike": 595.0, "call_gex": 18_000_000, "put_gex": 14_000_000, "net_gex": 32_000_000, "call_oi": 15_000, "put_oi": 12_000},
                {"strike": 600.0, "call_gex": 11_000_000, "put_gex": 16_000_000, "net_gex": 27_000_000, "call_oi": 11_000, "put_oi": 13_000},
            ],
        }

        summary = summarize_gex_payload(payload)

        self.assertEqual(summary["ticker"], "SPY")
        self.assertEqual(summary["gex_regime"], "positive")
        self.assertEqual(summary["call_wall"], 595.0)
        self.assertEqual(summary["put_wall"], 600.0)
        self.assertEqual(summary["top_net_gex_strike"], 595.0)
        self.assertEqual(summary["put_call_oi_ratio"], 0.94)
        self.assertIn("gamma flip 595.25", summary["summary"])

    def test_fetch_gex_snapshot_reads_json_payload(self) -> None:
        response_payload = {"symbol": "SPY", "net_gex": 1}
        response = MagicMock()
        response.read.return_value = json.dumps(response_payload).encode("utf-8")
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with patch("src.flashalpha_gex.request.urlopen", return_value=response) as mocked_urlopen:
            payload = fetch_gex_snapshot(symbol="SPY", api_key="test-key")

        self.assertEqual(payload, response_payload)
        called_request = mocked_urlopen.call_args.args[0]
        self.assertEqual(called_request.headers["X-api-key"], "test-key")


if __name__ == "__main__":
    unittest.main()
