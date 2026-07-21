from __future__ import annotations

import unittest

from lxml import html

from src.finviz_screener_rows import _extract_rows, normalize_finviz_ticker, repair_shifted_finviz_row, sanitize_finviz_company_name


class FinvizScreenerRowsTests(unittest.TestCase):
    def test_sanitize_company_name_keeps_real_company_names(self) -> None:
        row = {"Ticker": "PLTR", "Company": "Palantir"}

        self.assertEqual(normalize_finviz_ticker(row), "PLTR")
        self.assertEqual(sanitize_finviz_company_name(row, ticker="PLTR"), "Palantir")

    def test_sanitize_company_name_drops_adjacent_ticker_values(self) -> None:
        row = {"Ticker": "S", "Company": "SGHC"}

        self.assertEqual(normalize_finviz_ticker(row), "S")
        self.assertEqual(sanitize_finviz_company_name(row, ticker="S"), "")


    def test_repair_shifted_finviz_row_moves_symbol_and_company_back_into_place(self) -> None:
        row = {
            "No.": "9",
            "Ticker": "D",
            "Company": "DRH",
            "Sector": "Diamondrock Hospitality Co",
            "Industry": "Real Estate",
            "Country": "REIT - Hotel & Motel",
            "Market Cap": "USA",
            "P/E": "2.62B",
            "Price": "27.31",
            "Change": "12.75",
            "Volume": "1.59%",
            "ticker": "D",
            "company_name": "DRH",
            "source": "finviz",
        }

        repaired = repair_shifted_finviz_row(row)

        self.assertEqual(repaired["Ticker"], "DRH")
        self.assertEqual(repaired["Company"], "Diamondrock Hospitality Co")
        self.assertEqual(repaired["ticker"], "DRH")
        self.assertEqual(repaired["company_name"], "Diamondrock Hospitality Co")

    def test_extract_rows_uses_ticker_link_instead_of_logo_initial(self) -> None:
        tree = html.fromstring(
            """
            <table>
              <tr valign="top">
                <td>1</td>
                <td><a href="stock?t=IOVA&amp;ty=c&amp;p=d&amp;b=1">I</a><a href="stock?t=IOVA&amp;ty=c&amp;p=d&amp;b=1">IOVA</a></td>
                <td><a href="stock?t=IOVA&amp;ty=c&amp;p=d&amp;b=1">Iovance Biotherapeutics Inc</a></td>
                <td>Healthcare</td>
                <td>Biotechnology</td>
                <td>USA</td>
                <td>2.24B</td>
                <td>-</td>
                <td>5.02</td>
                <td>0.40%</td>
                <td>16,361,516</td>
              </tr>
            </table>
            """
        )

        rows = _extract_rows(tree, ["No.", "Ticker", "Company", "Sector", "Industry", "Country", "Market Cap", "P/E", "Price", "Change", "Volume"])

        self.assertEqual(rows[0]["Ticker"], "IOVA")
        self.assertEqual(rows[0]["Company"], "Iovance Biotherapeutics Inc")
        self.assertEqual(rows[0]["Price"], "5.02")
        self.assertEqual(rows[0]["Volume"], "16,361,516")


if __name__ == "__main__":
    unittest.main()
