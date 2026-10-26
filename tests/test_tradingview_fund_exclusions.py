from __future__ import annotations

import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.tradingview_fund_exclusions import fetch_classified_funds, refresh


def page(flag, offset, limit):
    all_rows = {
        "Leveraged": [["INTW", "2x", "fund", "Leveraged", 2], ["KORU", "3x", "fund", "Leveraged", 3],
                      ["MVLL", "2x", "fund", "Leveraged", 2]],
        "Inverse": [["SOXS", "-3x", "fund", "Inverse", -3]],
    }[flag]
    chunk = all_rows[offset:offset + limit]
    return {"totalCount": len(all_rows), "data": [{"s": f"NASDAQ:{r[0]}", "d": r} for r in chunk]}


class TradingViewFundExclusionsTests(unittest.TestCase):
    def test_replay_hash_tracks_effective_symbols_not_refresh_timestamp(self):
        import src.ticker_filters as filters
        from src.ticker_filters import leveraged_or_inverse_provenance
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir()
            listing = config / "tradingview_leveraged_inverse_etfs.txt"
            listing.write_text("# refreshed_at_utc=2026-01-01T00:00:00+00:00\nABC\n")
            with patch("src.ticker_filters.project_root", return_value=root), \
                 patch("src.etf_matcher.load_etf_catalog", return_value=[]), \
                 patch.object(filters, "_FUNDS_CACHE_KEY", None):
                first = leveraged_or_inverse_provenance()
                listing.write_text("# refreshed_at_utc=2026-01-08T00:00:00+00:00\nABC\n")
                second = leveraged_or_inverse_provenance()
            self.assertEqual(first["sha256"], second["sha256"])
            self.assertTrue({"KORU", "SOXS", "INTW", "MVLL"}.issubset(first["symbols"]))

    def test_replay_hash_includes_catalog_classifications(self):
        import src.ticker_filters as filters
        from src.ticker_filters import leveraged_or_inverse_provenance
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            (root / "config" / "tradingview_leveraged_inverse_etfs.txt").write_text("ABC\n")
            with patch("src.ticker_filters.project_root", return_value=root), \
                 patch("src.etf_matcher.load_etf_catalog", return_value=[]), \
                 patch.object(filters, "_FUNDS_CACHE_KEY", None):
                baseline = leveraged_or_inverse_provenance()
            catalog = [{"ticker": "CATX", "name": "Direxion Daily Bull 2X ETF"}]
            with patch("src.ticker_filters.project_root", return_value=root), \
                 patch("src.etf_matcher.load_etf_catalog", return_value=catalog), \
                 patch.object(filters, "_FUNDS_CACHE_KEY", None):
                classified = leveraged_or_inverse_provenance()
            self.assertIn("CATX", classified["symbols"])
            self.assertNotEqual(baseline["sha256"], classified["sha256"])

    def test_classifies_both_flags_and_paginates(self):
        calls = []
        def fetch(flag, offset, limit):
            calls.append((flag, offset))
            return page(flag, offset, 1)
        rows = fetch_classified_funds(fetch_page=fetch, page_size=1)
        self.assertEqual({r["ticker"] for r in rows}, {"INTW", "KORU", "SOXS", "MVLL"})
        self.assertEqual({r["exchange"] for r in rows}, {"NASDAQ"})
        self.assertIn(("Leveraged", 1), calls)

    def test_default_page_size_covers_current_universe_in_one_page(self):
        calls = []
        def fetch(flag, offset, limit):
            calls.append((flag, offset, limit))
            return page(flag, offset, limit)
        rows = fetch_classified_funds(fetch_page=fetch)
        self.assertEqual(len(rows), 4)
        self.assertEqual(calls, [("Leveraged", 0, 1000), ("Inverse", 0, 1000)])

    def test_incomplete_or_mismatched_responses_fail(self):
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            fetch_classified_funds(fetch_page=lambda f, o, n: {"totalCount": 2, "data": []}, page_size=1)
        def wrong(flag, offset, limit):
            return {"totalCount": 1, "data": [{"s": "NYSE:SPY", "d": ["SPY", "", "fund", "Non-leveraged", 1]}]}
        with self.assertRaisesRegex(ValueError, "Mismatched"):
            fetch_classified_funds(fetch_page=wrong, page_size=1)
        def zero_inverse(flag, offset, limit):
            return page(flag, offset, limit) if flag == "Leveraged" else {"totalCount": 0, "data": []}
        with self.assertRaisesRegex(ValueError, "zero Inverse"):
            fetch_classified_funds(fetch_page=zero_inverse, page_size=10)

    def test_exchange_qualified_duplicates_and_ambiguous_tickers_fail(self):
        def colliding(flag, offset, limit):
            rows = {
                "Leveraged": [["KORU", "3x", "fund", "Leveraged", 3], ["KORU", "3x", "fund", "Leveraged", 3],
                              ["INTW", "2x", "fund", "Leveraged", 2], ["MVLL", "2x", "fund", "Leveraged", 2]],
                "Inverse": [["SOXS", "-3x", "fund", "Inverse", -3]],
            }[flag]
            exchanges = ["NYSE", "NASDAQ", "NASDAQ", "NASDAQ"]
            selected = rows[offset:offset + limit]
            return {"totalCount": len(rows), "data": [{"s": f"{exchanges[offset+i]}:{row[0]}", "d": row} for i, row in enumerate(selected)]}
        with self.assertRaisesRegex(ValueError, "Ambiguous"):
            fetch_classified_funds(fetch_page=colliding, page_size=10)

    def test_refresh_writes_review_artifact_and_promotes_complete_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            out = refresh(root=root, fetch_page=page)
            target = root / "config" / "tradingview_leveraged_inverse_etfs.txt"
            self.assertEqual({line for line in target.read_text().splitlines() if line and not line.startswith("#")},
                             {"INTW", "KORU", "SOXS", "MVLL"})
            self.assertEqual(out["count"], 4)
            self.assertEqual(len(list((root / "artifacts" / "tradingview_fund_exclusions").glob("*.json"))), 1)
            old = target.read_text()
            with self.assertRaises(ValueError):
                refresh(root=root, fetch_page=lambda f, o, n: {"totalCount": 0, "data": []})
            self.assertEqual(target.read_text(), old)

    def test_refresh_promotes_denylist_with_deploy_readable_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            refresh(root=root, fetch_page=page)
            target = root / "config" / "tradingview_leveraged_inverse_etfs.txt"
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o644)

    def test_process_reads_updated_denylist_and_manual_include_cannot_override(self):
        from src.config import AppConfig
        from src.ticker_filters import (is_leveraged_or_inverse_ticker, load_excluded_tickers,
                                        set_replay_fund_exclusions, reset_replay_fund_exclusions)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config"
            config.mkdir()
            (config / "tradingview_leveraged_inverse_etfs.txt").write_text("ABC\n")
            (config / "manual_include_tickers.txt").write_text("ABC\nSPY\n")
            (config / "smallcap_exclude_tickers.txt").write_text("")
            (config / "manual_exclude_tickers.txt").write_text("")
            (config / "auto_exclude_tickers").mkdir()
            app_config = AppConfig(excluded_tickers_file=str(config / "smallcap_exclude_tickers.txt"),
                manual_excluded_tickers_file=str(config / "manual_exclude_tickers.txt"),
                manual_included_tickers_file=str(config / "manual_include_tickers.txt"),
                auto_excluded_tickers_dir=str(config / "auto_exclude_tickers"))
            with patch("src.ticker_filters.project_root", return_value=root), patch("src.etf_matcher.load_etf_catalog", return_value=[]):
                self.assertTrue(is_leveraged_or_inverse_ticker("ABC"))
                self.assertIn("ABC", load_excluded_tickers(app_config))
                self.assertNotIn("SPY", load_excluded_tickers(app_config))
                (config / "tradingview_leveraged_inverse_etfs.txt").write_text("XYZ\n")
                self.assertFalse(is_leveraged_or_inverse_ticker("ABC"))
                self.assertTrue(is_leveraged_or_inverse_ticker("XYZ"))
                token = set_replay_fund_exclusions({"ABC"})
                try:
                    self.assertIn("ABC", load_excluded_tickers(app_config))
                    self.assertNotIn("XYZ", load_excluded_tickers(app_config))
                finally:
                    reset_replay_fund_exclusions(token)

    def test_refresh_retains_prior_symbols_and_uses_configured_artifact_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "project"
            artifacts = Path(directory) / "durable-artifacts"
            target = root / "config" / "tradingview_leveraged_inverse_etfs.txt"
            target.parent.mkdir(parents=True)
            target.write_text("OLD_ETF\n")
            with patch.dict("os.environ", {"TICKER_SCREENER_ARTIFACTS_DIR": str(artifacts)}):
                result = refresh(root=root, fetch_page=page)
            self.assertIn("OLD_ETF", target.read_text())
            self.assertEqual(result["removed"], [])
            self.assertEqual(result["fresh_count"], 4)
            self.assertEqual(len(list((artifacts / "tradingview_fund_exclusions").glob("*.json"))), 1)


if __name__ == "__main__":
    unittest.main()
