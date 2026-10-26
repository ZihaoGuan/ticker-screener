import datetime as dt
from decimal import Decimal
import unittest

from scripts.fill_tradingview_missing_sessions import plan_inserts, select_repair_items


class TradingViewMissingSessionTests(unittest.TestCase):
    def setUp(self):
        self.day = "2026-06-26"
        self.dates = ("2026-06-23", "2026-06-24", "2026-06-25", self.day,
                      "2026-06-29", "2026-06-30", "2026-07-01")
        self.candles = {
            day: {"open": Decimal("10.000000"), "high": Decimal("11.000000"),
                  "low": Decimal("9.000000"), "close": Decimal("10.500000"), "volume": 1000}
            for day in self.dates
        }
        self.rows = [
            {"trade_date": dt.date.fromisoformat(day), "source": "tradingview",
             **self.candles[day]}
            for day in self.dates if day != self.day
        ]

    def test_fills_only_absent_session_with_matching_tradingview_anchors(self):
        planned, blocked = plan_inserts(self.rows, self.candles, [self.day])
        self.assertEqual(planned, {self.day: self.candles[self.day]})
        self.assertEqual(blocked, {})

    def test_never_overwrites_existing_bar(self):
        existing = self.rows + [{"trade_date": dt.date.fromisoformat(self.day),
                                 "source": "massive", **self.candles[self.day]}]
        planned, blocked = plan_inserts(existing, self.candles, [self.day])
        self.assertEqual(planned, {})
        self.assertEqual(blocked, {self.day: "already_present"})

    def test_blocks_price_basis_mismatch_and_missing_anchors(self):
        rows = [dict(row) for row in self.rows]
        next(row for row in rows if str(row["trade_date"]) == "2026-06-25")["close"] = Decimal("15")
        planned, blocked = plan_inserts(rows, self.candles, [self.day])
        self.assertEqual(planned, {})
        self.assertEqual(blocked[self.day], "adjustment_basis_mismatch")
        rows = [row for row in self.rows if str(row["trade_date"]) not in
                {"2026-06-30", "2026-07-01"}]
        planned, blocked = plan_inserts(rows, self.candles, [self.day])
        self.assertEqual(planned, {})
        self.assertEqual(blocked[self.day], "insufficient_matching_basis_anchors")

    def test_accepts_matching_yfinance_anchors_within_nine_calendar_days(self):
        rows = [dict(row, source="yfinance") for row in self.rows]
        planned, blocked = plan_inserts(rows, self.candles, [self.day])
        self.assertEqual(planned, {self.day: self.candles[self.day]})
        self.assertEqual(blocked, {})

    def test_requires_all_ohlc_anchor_fields_to_match(self):
        rows = [dict(row) for row in self.rows]
        next(row for row in rows if str(row["trade_date"]) == "2026-06-25")["high"] = Decimal("11.1")
        planned, blocked = plan_inserts(rows, self.candles, [self.day])
        self.assertEqual(planned, {})
        self.assertEqual(blocked[self.day], "adjustment_basis_mismatch")

    def test_explicit_selection_requires_active_universe_and_cached_scope(self):
        cache = [{"ticker": ticker} for ticker in ("AAA", "QQQ")]
        items, outside = select_repair_items(
            cache, {"AAA", "QQQ", "ZZZ"}, set(), {"AAA"}, explicit=True)
        self.assertEqual(items, [{"ticker": "AAA"}])
        self.assertEqual(outside, 2)

    def test_raw_result_selection_keeps_blocked_ticker_intersection(self):
        cache = [{"ticker": ticker} for ticker in ("AAA", "BBB")]
        items, outside = select_repair_items(
            cache, {"AAA", "BBB"}, {"AAA"}, {"AAA", "BBB"}, explicit=False)
        self.assertEqual(items, [{"ticker": "AAA"}])
        self.assertEqual(outside, 0)

    def test_blocks_provider_missing_day_and_known_split_event(self):
        planned, blocked = plan_inserts(self.rows, {k: v for k, v in self.candles.items()
                                                   if k != self.day}, [self.day])
        self.assertEqual(planned, {})
        self.assertEqual(blocked[self.day], "provider_missing_session")
        planned, blocked = plan_inserts(self.rows, self.candles, [self.day],
                                        known_split_dates={self.day})
        self.assertEqual(planned, {})
        self.assertEqual(blocked[self.day], "known_split_event")


if __name__ == "__main__":
    unittest.main()
