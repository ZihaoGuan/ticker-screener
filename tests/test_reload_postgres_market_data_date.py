from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call, patch

from scripts.reload_postgres_market_data_date import _auto_exclude_delisted_tickers, _should_auto_exclude_delisted
from scripts.sync_postgres_market_data import TickerSyncOutcome


class ReloadPostgresMarketDataDateTests(unittest.TestCase):
    def test_should_auto_exclude_delisted_only_for_delisted_like_outcomes(self) -> None:
        self.assertTrue(_should_auto_exclude_delisted(TickerSyncOutcome(ticker="AAPL", status="failed_no_history_available", reason="none")))
        self.assertTrue(_should_auto_exclude_delisted(TickerSyncOutcome(ticker="MSFT", status="skipped_delisted_before_window", reason="old")))
        self.assertFalse(_should_auto_exclude_delisted(TickerSyncOutcome(ticker="NVDA", status="failed_rate_limited", reason="429")))
        self.assertFalse(_should_auto_exclude_delisted(TickerSyncOutcome(ticker="ARM", status="skipped_listed_after_requested_end", reason="new")))

    def test_auto_exclude_delisted_tickers_dedups_and_sets_delisted_reason(self) -> None:
        config = MagicMock()
        outcomes = [
            TickerSyncOutcome(ticker="AAPL", status="failed_no_history_available", reason="none"),
            TickerSyncOutcome(ticker="AAPL", status="skipped_delisted_before_window", reason="old"),
            TickerSyncOutcome(ticker="MSFT", status="skipped_delisted_before_window", reason="old"),
            TickerSyncOutcome(ticker="NVDA", status="failed_rate_limited", reason="429"),
        ]

        with patch("scripts.reload_postgres_market_data_date.load_app_config", return_value=config), patch(
            "scripts.reload_postgres_market_data_date.add_manual_exclusion"
        ) as add_manual_exclusion:
            added = _auto_exclude_delisted_tickers(outcomes)

        self.assertEqual(added, 2)
        add_manual_exclusion.assert_has_calls(
            [
                call(config, ticker="AAPL", reason="delisted"),
                call(config, ticker="MSFT", reason="delisted"),
            ]
        )
        self.assertEqual(add_manual_exclusion.call_count, 2)


if __name__ == "__main__":
    unittest.main()
