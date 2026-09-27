from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.momentum_etf_holdings import MomentumEtf, parse_momentum_etf_holdings_html, refresh_momentum_etf_holdings_cache


class _Response:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None


class MomentumEtfHoldingsTests(unittest.TestCase):
    def test_parses_and_filters_issuer_holdings_table(self) -> None:
        etf = MomentumEtf("TEST", "Test ETF", "Test", "https://example.test")
        payload = parse_momentum_etf_holdings_html("""
          <p>Holdings As of September 25, 2026</p>
          <table><tr><th>Name</th><th>Ticker</th><th>Weight</th><th>Sector</th></tr>
          <tr><td>Example Corp</td><td>EXM</td><td>4.25%</td><td>Technology</td></tr>
          <tr><td>Cash & Other</td><td>CASH&OTHER</td><td>1.5%</td><td></td></tr></table>
        """, etf=etf)
        self.assertEqual(payload["as_of_date"], "2026-09-25")
        self.assertEqual(payload["holdings"], [{"ticker": "EXM", "name": "Example Corp", "weight": 4.25, "sector": "Technology"}])
        self.assertTrue(payload["is_complete"])

    def test_total_refresh_failure_preserves_existing_latest_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            etf = MomentumEtf("TEST", "Test ETF", "Test", "https://example.test")
            first = refresh_momentum_etf_holdings_cache(
                artifacts_dir=root,
                etfs=(etf,),
                get=lambda *args, **kwargs: _Response("<table><tr><th>Ticker</th><th>Weight</th></tr><tr><td>EXM</td><td>3.2</td></tr></table>"),
            )
            existing = Path(first["output_file"]).read_text(encoding="utf-8")
            failed = refresh_momentum_etf_holdings_cache(
                artifacts_dir=root,
                etfs=(etf,),
                get=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
            )
            self.assertFalse(failed["updated_cache"])
            self.assertEqual(Path(first["output_file"]).read_text(encoding="utf-8"), existing)


if __name__ == "__main__":
    unittest.main()
