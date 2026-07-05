from __future__ import annotations

import datetime as dt
import unittest

from src.ratings.finviz_parser import parse_finviz_probe
from src.ratings.models import FinvizProbeResult


class FinvizParserTests(unittest.TestCase):
    def test_parse_probe_maps_growth_and_volatility_fields(self) -> None:
        probe = FinvizProbeResult(
            ticker="NVDA",
            source_url="https://finviz.com/quote.ashx?t=NVDA&p=d",
            status_code=200,
            final_url="https://finviz.com/stock?t=NVDA&p=d",
            title="NVDA - NVIDIA Corp Stock Price and Quote",
            body_excerpt="NVDA\nNVIDIA Corp\nTechnology • Semiconductors • USA • Mega • NASD",
            sector="Technology",
            industry="Semiconductors",
            metric_pairs=(
                ("Forward P/E", "16.44"),
                ("PEG", "0.36"),
                ("P/S", "19.59"),
                ("P/B", "25.42"),
                ("P/FCF", "46.81"),
                ("Profit Margin", "62.97%"),
                ("Oper. Margin", "64.02%"),
                ("Gross Margin", "74.15%"),
                ("ROA", "82.97%"),
                ("ROE", "114.29%"),
                ("EPS this Y", "87.17%"),
                ("EPS next Y", "12.48"),
                ("EPS next Y", "39.83%"),
                ("EPS next 5Y", "45.51%"),
                ("Sales Q/Q", "85.23%"),
                ("EPS Q/Q", "213.42%"),
                ("Perf Month", "-9.14%"),
                ("Perf Quarter", "13.84%"),
                ("Perf Half Y", "11.65%"),
                ("Perf Year", "41.51%"),
                ("Perf YTD", "10.02%"),
                ("Volatility", "3.34% 3.58%"),
            ),
        )

        snapshot = parse_finviz_probe(probe, as_of_date=dt.date(2026, 6, 13))

        self.assertEqual(snapshot.sector, "Technology")
        self.assertEqual(snapshot.industry, "Semiconductors")
        self.assertEqual(snapshot.eps_next_y_pct, 39.83)
        self.assertEqual(snapshot.volatility_week_pct, 3.34)
        self.assertEqual(snapshot.volatility_month_pct, 3.58)
        self.assertEqual(snapshot.price_to_fcf, 46.81)
        self.assertEqual(snapshot.perf_month_pct, -9.14)

    def test_parse_probe_maps_ipo_date(self) -> None:
        probe = FinvizProbeResult(
            ticker="A",
            source_url="https://finviz.com/quote.ashx?t=A&p=d",
            status_code=200,
            final_url="https://finviz.com/stock?t=A&p=d",
            title="A - Agilent Technologies Inc Stock Price and Quote",
            body_excerpt="A\nAgilent Technologies Inc\nHealthcare • Diagnostics & Research • USA • Large • NYSE",
            sector="Healthcare",
            industry="Diagnostics & Research",
            metric_pairs=(
                ("IPO", "Nov 18, 1999"),
            ),
        )

        snapshot = parse_finviz_probe(probe, as_of_date=dt.date(2026, 7, 5))

        self.assertEqual(snapshot.ipo_date, dt.date(1999, 11, 18))


if __name__ == "__main__":
    unittest.main()
