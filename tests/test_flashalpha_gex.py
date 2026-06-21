from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from src.flashalpha_gex import build_gamma_exposure_report, fetch_gex_snapshot, summarize_gex_payload


class FlashalphaGexTests(unittest.TestCase):
    def test_summarize_gex_payload_extracts_walls_and_regime(self) -> None:
        payload = {
            "symbol": "SPY",
            "underlying_price": 597.505,
            "as_of": "2026-02-28T16:30:45Z",
            "expiration": "2026-02-28",
            "expiration_mode": "0dte",
            "gamma_flip": 595.25,
            "net_gex": 2_850_000_000,
            "strikes": [
                {"strike": 590.0, "call_gex": 12_000_000, "put_gex": -8_000_000, "net_gex": 4_000_000, "call_oi": 10_000, "put_oi": 9_000},
                {"strike": 595.0, "call_gex": 18_000_000, "put_gex": -14_000_000, "net_gex": 4_000_000, "call_oi": 15_000, "put_oi": 12_000},
                {"strike": 600.0, "call_gex": 11_000_000, "put_gex": -16_000_000, "net_gex": -5_000_000, "call_oi": 11_000, "put_oi": 13_000},
            ],
        }

        summary = summarize_gex_payload(payload)

        self.assertEqual(summary["ticker"], "SPY")
        self.assertEqual(summary["gex_regime"], "positive")
        self.assertEqual(summary["call_wall"], 595.0)
        self.assertEqual(summary["put_wall"], 600.0)
        self.assertEqual(summary["front_expiry"], "2026-02-28")
        self.assertEqual(summary["put_call_oi_ratio"], 0.94)
        self.assertIn("gamma flip 595.25", summary["summary"])
        self.assertIn("0dte expiry 2026-02-28", summary["summary"])

    def test_fetch_gex_snapshot_reads_json_payload(self) -> None:
        response_payload = {
            "timestamp": "2026-02-28T16:30:45Z",
            "data": {
                "current_price": 597.505,
                "options": [
                    {
                        "option": "SPY260228C00595000",
                        "iv": 0.2,
                        "gamma": 0.01,
                        "open_interest": 1000,
                        "volume": 1500,
                    },
                    {
                        "option": "SPY260228P00595000",
                        "iv": 0.21,
                        "gamma": 0.012,
                        "open_interest": 900,
                        "volume": 1400,
                    },
                    {
                        "option": "SPY260303C00600000",
                        "iv": 0.19,
                        "gamma": 0.008,
                        "open_interest": 800,
                        "volume": 0,
                    },
                ],
            },
        }
        response = MagicMock()
        response.read.return_value = json.dumps(response_payload).encode("utf-8")
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with patch("src.flashalpha_gex.request.urlopen", return_value=response) as mocked_urlopen:
            payload = fetch_gex_snapshot(symbol="SPY")

        self.assertEqual(payload["symbol"], "SPY")
        self.assertEqual(payload["expiration"], "2026-02-28")
        self.assertEqual(payload["expiration_mode"], "0dte")
        self.assertEqual(len(payload["strikes"]), 1)
        self.assertIsInstance(payload["net_gex"], float)
        called_request = mocked_urlopen.call_args.args[0]
        self.assertEqual(called_request.headers["Accept"], "application/json")

    def test_build_gamma_exposure_report_normalizes_spx_and_builds_profiles(self) -> None:
        response_payload = {
            "timestamp": "2026-02-28T16:30:45Z",
            "data": {
                "current_price": 5975.05,
                "options": [
                    {
                        "option": "_SPX260228C05950000",
                        "iv": 0.2,
                        "gamma": 0.01,
                        "open_interest": 1000,
                        "volume": 1500,
                    },
                    {
                        "option": "_SPX260228P05950000",
                        "iv": 0.21,
                        "gamma": 0.012,
                        "open_interest": 900,
                        "volume": 1400,
                    },
                    {
                        "option": "_SPX260303C06000000",
                        "iv": 0.19,
                        "gamma": 0.008,
                        "open_interest": 800,
                        "volume": 0,
                    },
                    {
                        "option": "_SPX260320P05800000",
                        "iv": 0.23,
                        "gamma": 0.009,
                        "open_interest": 700,
                        "volume": 0,
                    },
                ],
            },
        }
        response = MagicMock()
        response.read.return_value = json.dumps(response_payload).encode("utf-8")
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with patch("src.flashalpha_gex.request.urlopen", return_value=response) as mocked_urlopen:
            payload = build_gamma_exposure_report(symbol="SPX")

        self.assertEqual(payload["symbol"], "SPX")
        self.assertEqual(payload["source_symbol"], "_SPX")
        self.assertEqual(payload["next_expiry"], "2026-02-28")
        self.assertEqual(payload["next_monthly_expiry"], "2026-03-20")
        self.assertEqual(len(payload["profile"]["levels"]), 60)
        self.assertEqual(len(payload["profile"]["all"]), 60)
        self.assertEqual(len(payload["strikes"]), 3)
        called_request = mocked_urlopen.call_args.args[0]
        self.assertIn("_SPX.json", called_request.full_url)


if __name__ == "__main__":
    unittest.main()
