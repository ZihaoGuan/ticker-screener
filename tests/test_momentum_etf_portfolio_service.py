from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.momentum_etf_holdings import momentum_etf_holdings_cache_path
from src.webapp.services.momentum_etf_portfolio_service import MomentumEtfPortfolioService, clear_momentum_etf_portfolio_cache


class _WatchlistService:
    def get_scanner_top_hits_snapshot_payload(self):
        return {"snapshot": {"source_data_as_of": "2026-09-25"}, "rows": [{
            "ticker": "EXM", "company": "Example Corp", "sector": "Technology", "scanner_count": 3,
            "scanner_labels": ["Qullamaggie"], "daily_rs_rating": 98,
            "stage_analysis": {"alias": "2A"}, "strike_zone": {"state": "active", "label": "Active", "score": 84, "reason": "fresh"},
            "atr_to_sma50": 2.1,
        }]}


class MomentumEtfPortfolioServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_momentum_etf_portfolio_cache()

    def test_combines_etf_overlap_and_top_hits_context(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifacts = Path(directory)
            path = momentum_etf_holdings_cache_path(artifacts)
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"generated_at": "2026-09-26T00:00:00Z", "results": {
                "FMTM": {"holdings": [{"ticker": "EXM", "name": "Example Corp", "weight": 4.0, "sector": "Technology"}]},
                "SPMO": {"holdings": [{"ticker": "EXM", "name": "Example Corp", "weight": 2.5, "sector": "Technology"}]},
            }}), encoding="utf-8")
            payload = MomentumEtfPortfolioService(artifacts_dir=artifacts, watchlist_service=_WatchlistService()).get_payload()
            row = payload["rows"][0]
            self.assertEqual(row["ticker"], "EXM")
            self.assertEqual(row["etf_count"], 2)
            self.assertEqual(row["combined_weight"], 6.5)
            self.assertTrue(row["top_hit"])
            self.assertEqual(row["scanner_count"], 3)


if __name__ == "__main__":
    unittest.main()
