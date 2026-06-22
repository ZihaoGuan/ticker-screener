from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import requests

from src.ratings.finviz_insider import _normalize_parsed_rows, fetch_finviz_insider_trades, load_finviz_insider_signal_map
from src.ratings.finviz_missing_tickers import load_missing_finviz_tickers, record_missing_finviz_ticker


_SAMPLE_HTML = """
<html><body>
<table class="styled-table-new">
  <thead>
    <tr>
      <th>Insider Trading</th><th>Relationship</th><th>Date</th><th>Transaction</th><th>Cost</th>
      <th>#Shares</th><th>Value ($)</th><th>#Shares Total</th><th>SEC Form 4</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><a href="insidertrading?oc=1">WELLS DAVID B</a></td><td>Director</td><td>May 26 '26</td><td>Buy</td><td>24.23</td>
      <td>48,400</td><td>1,172,974</td><td>224,417</td><td><a href="http://www.sec.gov/form4a.xml">May 26 04:48 PM</a></td>
    </tr>
    <tr>
      <td><a href="insidertrading?oc=2">COOK TIMOTHY D</a></td><td>CEO</td><td>Jun 17 '26</td><td>Sale</td><td>31.50</td>
      <td>14,027</td><td>441,850</td><td>432,124</td><td><a href="http://www.sec.gov/form4b.xml">Jun 17 06:50 PM</a></td>
    </tr>
    <tr>
      <td><a href="insidertrading?oc=3">ELSHENAWY MOHAMED</a></td><td>Officer</td><td>Jun 17 '26</td><td>Proposed Sale</td><td>31.50</td>
      <td>30,040</td><td>946,260</td><td></td><td><a href="http://www.sec.gov/form4c.xml">Jun 17 11:10 AM</a></td>
    </tr>
  </tbody>
</table>
</body></html>
"""


class _FakeResponse:
    def __init__(self, text: str, url: str = "https://finviz.com/quote.ashx?t=RKT&p=d") -> None:
        self.text = text
        self.url = url

    def raise_for_status(self) -> None:
        return None


class _FakeSession:
    def __init__(self, text: str) -> None:
        self.text = text
        self.headers: dict[str, str] = {}

    def get(self, url: str, params: dict[str, str], timeout: tuple[int, int]) -> _FakeResponse:
        return _FakeResponse(self.text, f"{url}?t={params['t']}&p={params['p']}")


class FinvizInsiderTests(unittest.TestCase):
    def test_fetch_finviz_insider_trades_parses_buy_and_sale_only(self) -> None:
        rows = fetch_finviz_insider_trades("RKT", session=_FakeSession(_SAMPLE_HTML))

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["type"], "BUY")
        self.assertEqual(rows[0]["gross_amount"], 1_172_974.0)
        self.assertEqual(rows[0]["transaction_date"], "2026-05-26")
        self.assertEqual(rows[1]["type"], "SELL")
        self.assertEqual(rows[1]["shares"], 14_027)

    def test_normalize_rows_skips_proposed_sale(self) -> None:
        rows = _normalize_parsed_rows(
            "RKT",
            [
                "Insider Trading",
                "Relationship",
                "Date",
                "Transaction",
                "Cost",
                "#Shares",
                "Value ($)",
                "#Shares Total",
                "SEC Form 4",
                "SEC Form 4 URL",
            ],
            [
                ["A", "Officer", "Jun 17 '26", "Proposed Sale", "31.50", "10", "315", "", "Jun 17 11:10 AM", "http://sec"],
            ],
        )

        self.assertEqual(rows, [])

    def test_load_signal_map_refreshes_finviz_cache_and_summarizes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifacts_dir = Path(temp_dir)
            with patch(
                "src.ratings.finviz_insider.fetch_finviz_insider_trades",
                return_value=[
                    {
                        "ticker": "RKT",
                        "transaction_date": "2026-06-18",
                        "filing_date": None,
                        "owner_name": "Buyer",
                        "position": "Director",
                        "type": "BUY",
                        "shares": 1000,
                        "price": 30.0,
                        "gross_amount": 750_000.0,
                        "net_amount": 750_000.0,
                        "shares_owned_after": 1000,
                        "is_10b5_1": False,
                        "source_url": "",
                    },
                    {
                        "ticker": "RKT",
                        "transaction_date": "2026-06-19",
                        "filing_date": None,
                        "owner_name": "Seller",
                        "position": "Officer",
                        "type": "SELL",
                        "shares": 1000,
                        "price": 30.0,
                        "gross_amount": 250_000.0,
                        "net_amount": -250_000.0,
                        "shares_owned_after": 1000,
                        "is_10b5_1": False,
                        "source_url": "",
                    },
                ],
            ):
                signal_map = load_finviz_insider_signal_map(
                    ["RKT"],
                    as_of_date=dt.date(2026, 6, 22),
                    lookback_days=30,
                    artifacts_dir=artifacts_dir,
                    ttl_hours=12,
                )

            self.assertEqual(signal_map["RKT"]["buy_count"], 1)
            self.assertEqual(signal_map["RKT"]["sell_count"], 1)
            self.assertEqual(signal_map["RKT"]["buy_amount"], 750_000.0)
            self.assertEqual(signal_map["RKT"]["discretionary_sell_amount"], 250_000.0)
            self.assertEqual(signal_map["RKT"]["net_amount_excl_10b5_1"], 500_000.0)
            self.assertTrue((artifacts_dir / "raw" / "insider" / "finviz_insider_trades_latest.json").exists())

    def test_load_signal_map_ignores_http_error_and_keeps_batch_running(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifacts_dir = Path(temp_dir)
            with patch(
                "src.ratings.finviz_insider.fetch_finviz_insider_trades",
                side_effect=requests.HTTPError("404 Client Error"),
            ):
                signal_map = load_finviz_insider_signal_map(
                    ["AEFC"],
                    as_of_date=dt.date(2026, 6, 22),
                    lookback_days=30,
                    artifacts_dir=artifacts_dir,
                    ttl_hours=12,
                )

            self.assertEqual(signal_map, {})
            self.assertFalse((artifacts_dir / "raw" / "insider" / "finviz_insider_trades_latest.json").exists())
            missing_registry = load_missing_finviz_tickers(artifacts_dir)
            self.assertIn("AEFC", missing_registry)
            self.assertEqual(missing_registry["AEFC"]["source"], "insider")

    def test_load_signal_map_skips_known_missing_ticker_without_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifacts_dir = Path(temp_dir)
            record_missing_finviz_ticker(
                "SOJD",
                artifacts_dir=artifacts_dir,
                reason="404 Client Error: Not Found",
                source="insider",
            )
            with patch("src.ratings.finviz_insider.fetch_finviz_insider_trades") as fetch_mock:
                signal_map = load_finviz_insider_signal_map(
                    ["SOJD"],
                    as_of_date=dt.date(2026, 6, 22),
                    lookback_days=30,
                    artifacts_dir=artifacts_dir,
                    ttl_hours=12,
                )

            self.assertEqual(signal_map, {})
            fetch_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
