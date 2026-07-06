from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch


class SyncPostgresMarketDataFallbackTests(unittest.TestCase):
    def test_diagnose_missing_ticker_falls_back_to_nasdaq_when_yahoo_is_empty(self) -> None:
        import pandas as pd
        import scripts.sync_postgres_market_data as script

        nasdaq_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-01-25"), "Open": 11.1, "High": 11.1, "Low": 10.2, "Close": 10.88, "Adj Close": 10.88, "Volume": 345221},
                {"Date": pd.Timestamp("2021-01-26"), "Open": 11.3, "High": 11.3, "Low": 10.5901, "Close": 10.63, "Adj Close": 10.63, "Volume": 270742},
            ]
        ).set_index("Date")

        with patch.object(script, "_download_single_history", return_value=(pd.DataFrame(), "yahoo empty")), patch.object(
            script,
            "_download_nasdaq_history",
            return_value=(nasdaq_frame, None),
        ):
            history, outcome = script._diagnose_missing_ticker(
                "NVTS",
                dt.date(2021, 1, 25),
                dt.date(2021, 1, 26),
                30,
                max_retries=1,
                retry_base_seconds=0.0,
                single_ticker_sleep_seconds=0.0,
            )

        self.assertFalse(history.empty)
        self.assertEqual(outcome.status, "synced_full_window")
        self.assertEqual(outcome.source, "nasdaq")

    def test_existing_yahoo_history_is_augmented_with_nasdaq_for_leading_gap(self) -> None:
        import pandas as pd
        import scripts.sync_postgres_market_data as script

        yahoo_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-10-20"), "Open": 13.98, "High": 14.08, "Low": 11.73, "Close": 12.8, "Adj Close": 12.8, "Volume": 1220376},
                {"Date": pd.Timestamp("2021-10-21"), "Open": 13.2, "High": 13.2, "Low": 12.635, "Close": 12.78, "Adj Close": 12.78, "Volume": 714979},
            ]
        ).set_index("Date")
        nasdaq_gap_frame = pd.DataFrame(
            [
                {"Date": pd.Timestamp("2021-01-25"), "Open": 11.1, "High": 11.1, "Low": 10.2, "Close": 10.88, "Adj Close": 10.88, "Volume": 345221},
                {"Date": pd.Timestamp("2021-01-26"), "Open": 11.3, "High": 11.3, "Low": 10.5901, "Close": 10.63, "Adj Close": 10.63, "Volume": 270742},
            ]
        ).set_index("Date")

        with patch.object(script, "_download_nasdaq_history", return_value=(nasdaq_gap_frame, None)):
            merged, source, errors = script._augment_history_with_nasdaq_fallback(
                "NVTS",
                yahoo_frame,
                dt.date(2021, 1, 25),
                dt.date(2021, 10, 21),
                max_retries=1,
                retry_base_seconds=0.0,
            )

        self.assertFalse(merged.empty)
        self.assertEqual(source, "yfinance+nasdaq")
        self.assertEqual(merged.index.min().date().isoformat(), "2021-01-25")
        self.assertEqual(merged.index.max().date().isoformat(), "2021-10-21")
        self.assertEqual(errors, [])

