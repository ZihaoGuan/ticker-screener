from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.momentum_etf_holdings import (
    MomentumEtf,
    parse_etf_channel_holdings_html,
    parse_invesco_holdings_json,
    parse_momentum_etf_holdings_html,
    refresh_momentum_etf_holdings_cache,
)


class _Response:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None


class MomentumEtfHoldingsTests(unittest.TestCase):
    def test_parses_complete_invesco_json_holdings(self) -> None:
        etf = MomentumEtf("TEST", "Test ETF", "Invesco", "https://example.test")
        payload = parse_invesco_holdings_json(
            '{"effectiveDate":"2026-09-25","totalNumberOfHoldings":3,"holdings":['
            '{"ticker":"EXM","issuerName":"Example Corp","percentageOfTotalNetAssets":4.25},'
            '{"ticker":"ABC","issuerName":"ABC Corp","percentageOfTotalNetAssets":3.1},'
            '{"ticker":"USD","issuerName":"Cash","percentageOfTotalNetAssets":0.2}]}' ,
            etf=etf,
        )
        self.assertEqual(payload["as_of_date"], "2026-09-25")
        self.assertEqual(payload["holding_count"], 2)
        self.assertEqual(payload["reported_holding_count"], 3)
        self.assertTrue(payload["is_complete"])

    def test_parses_paginated_full_holdings_table(self) -> None:
        rows, pages = parse_etf_channel_holdings_html("""
          <span>TEST — Stock Holdings Page 1 of 2</span>
          <tr><td><a href="/symbol/exm/">Example Corp</a></td><td><span>4.25%</span></td></tr>
        """)
        self.assertEqual(pages, 2)
        self.assertEqual(rows, [{"ticker": "EXM", "name": "Example Corp", "weight": 4.25, "sector": ""}])

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

    def test_refresh_uses_all_pages_instead_of_partial_table(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            etf = MomentumEtf(
                "TEST",
                "Test ETF",
                "Test",
                "https://issuer.test",
                "https://partial.test",
                full_fallback_url="https://full.test?page={page}",
            )

            def get(url: str, **_kwargs: object) -> _Response:
                if url == "https://issuer.test":
                    raise RuntimeError("client-rendered")
                if url == "https://partial.test":
                    return _Response("""
                      <p>Showing 1 of 3 holdings</p><table><tr><th>Ticker</th><th>Weight</th></tr>
                      <tr><td>EXM</td><td>4.25%</td></tr></table>
                    """)
                page = int(url.rsplit("=", 1)[-1])
                ticker = "EXM" if page == 0 else "ABC"
                return _Response(f"""
                  <span>TEST — Stock Holdings Page {page + 1} of 2</span>
                  <tr><td><a href="/symbol/{ticker.lower()}/">{ticker} Corp</a></td><td><span>{4.25 - page}%</span></td></tr>
                """)

            payload = refresh_momentum_etf_holdings_cache(artifacts_dir=Path(directory), etfs=(etf,), get=get)
            result = payload["results"]["TEST"]
            self.assertEqual(result["source_kind"], "full_fallback")
            self.assertEqual(result["holding_count"], 2)
            self.assertTrue(result["is_complete"])


if __name__ == "__main__":
    unittest.main()
