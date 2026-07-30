import unittest
from io import BytesIO
from zipfile import ZipFile

from src.ssga_holdings import parse_ssga_holdings_xlsx


def _xlsx_fixture() -> bytes:
    shared = [
        "Fund Name:",
        "State Street Communication Services Select Sector SPDR ETF",
        "Ticker Symbol:",
        "XLC",
        "Holdings:",
        "As of 29-Jul-2026",
        "Name",
        "Ticker",
        "Identifier",
        "SEDOL",
        "Weight",
        "Sector",
        "Shares Held",
        "Local Currency",
        "META PLATFORMS INC CLASS A",
        "META",
        "30303M102",
        "B7TL820",
        "-",
        "USD",
        "US DOLLAR",
        "999USDZ92",
        "S+P EMINI COM SER SEP26",
        "XASU6",
    ]
    shared_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        + "".join(f"<si><t>{value}</t></si>" for value in shared)
        + "</sst>"
    )
    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
    <row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2" t="s"><v>3</v></c></row>
    <row r="3"><c r="A3" t="s"><v>4</v></c><c r="B3" t="s"><v>5</v></c></row>
    <row r="5">
      <c r="A5" t="s"><v>6</v></c><c r="B5" t="s"><v>7</v></c><c r="C5" t="s"><v>8</v></c><c r="D5" t="s"><v>9</v></c>
      <c r="E5" t="s"><v>10</v></c><c r="F5" t="s"><v>11</v></c><c r="G5" t="s"><v>12</v></c><c r="H5" t="s"><v>13</v></c>
    </row>
    <row r="6">
      <c r="A6" t="s"><v>14</v></c><c r="B6" t="s"><v>15</v></c><c r="C6" t="s"><v>16</v></c><c r="D6" t="s"><v>17</v></c>
      <c r="E6"><v>17.220425</v></c><c r="F6" t="s"><v>18</v></c><c r="G6"><v>6483808.0</v></c><c r="H6" t="s"><v>19</v></c>
    </row>
    <row r="7">
      <c r="A7" t="s"><v>20</v></c><c r="B7" t="s"><v>18</v></c><c r="C7" t="s"><v>21</v></c><c r="D7" t="s"><v>18</v></c>
      <c r="E7"><v>0.017885</v></c><c r="F7" t="s"><v>18</v></c><c r="G7"><v>3943498.18</v></c><c r="H7" t="s"><v>19</v></c>
    </row>
    <row r="8">
      <c r="A8" t="s"><v>22</v></c><c r="B8" t="s"><v>23</v></c><c r="C8" t="s"><v>18</v></c><c r="D8" t="s"><v>18</v></c>
      <c r="E8"><v>-0.004681</v></c><c r="F8" t="s"><v>18</v></c><c r="G8"><v>64250.0</v></c><c r="H8" t="s"><v>19</v></c>
    </row>
  </sheetData>
</worksheet>"""
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("xl/sharedStrings.xml", shared_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()


class SsgaHoldingsTests(unittest.TestCase):
    def test_parse_filters_cash_and_futures_rows(self):
        payload = parse_ssga_holdings_xlsx(_xlsx_fixture(), etf_ticker="XLC", source_url="https://example.test/xlc.xlsx")

        self.assertEqual(payload["etf_ticker"], "XLC")
        self.assertEqual(payload["as_of_date"], "2026-07-29")
        self.assertEqual(payload["holding_count"], 1)
        self.assertEqual(payload["holdings"][0]["ticker"], "META")
        self.assertEqual(payload["holdings"][0]["weight"], 17.220425)
        self.assertEqual(payload["holdings"][0]["shares_held"], 6483808.0)


if __name__ == "__main__":
    unittest.main()
