from datetime import date
import sqlite3
import unittest
from unittest.mock import MagicMock

from scripts.reload_postgres_market_data_date import _replace_trade_date_atomically
from scripts.sync_postgres_market_data import _DAILY_BAR_UPSERT_SQL, _replace_daily_bars, _upsert_daily_bars


class DailyBarSourcePriorityTests(unittest.TestCase):
    def test_upsert_sql_allows_only_equal_or_higher_source_rank(self):
        sql = " ".join(_DAILY_BAR_UPSERT_SQL.lower().split())
        self.assertIn("when 'tradingview' then 3", sql)
        self.assertIn("when 'massive' then 2", sql)
        self.assertIn("when 'yfinance' then 1", sql)
        self.assertIn("<= case lower(coalesce(excluded.source, ''))", sql)

    def test_lower_rank_conflict_is_not_counted_as_persisted(self):
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value.rowcount = 0
        self.assertEqual(_upsert_daily_bars(connection, [("XYZ", date(2026, 10, 23))], 100), (0, 0))
        self.assertIn("when 'massive' then 2", connection.cursor.return_value.__enter__.return_value.executemany.call_args.args[0].lower())

    def test_ranked_conflicts_persist_massive_over_yahoo_and_tradingview_over_massive(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("""CREATE TABLE daily_bars (
            ticker TEXT, trade_date TEXT, open REAL, high REAL, low REAL, close REAL,
            adj_close REAL, volume INTEGER, dividend REAL, split_factor REAL,
            source TEXT, updated_at TEXT, PRIMARY KEY (ticker, trade_date))""")
        sql = _DAILY_BAR_UPSERT_SQL.replace("%s", "?")
        def save(source, close):
            row = ("XYZ", "2026-10-23", close, close + 1, close - 1, close,
                   close, 1000, 0, 1, source, "now")
            return connection.execute(sql, row).rowcount

        self.assertEqual(save("yfinance", 100), 1)
        self.assertEqual(save("massive", 101), 1)
        self.assertEqual(save("yahoo", 102), 0)
        self.assertEqual(connection.execute("SELECT source,close FROM daily_bars").fetchone(), ("massive", 101))
        self.assertEqual(save("tradingview", 103), 1)
        self.assertEqual(save("massive", 104), 0)
        self.assertEqual(connection.execute("SELECT source,close FROM daily_bars").fetchone(), ("tradingview", 103))
        connection.close()

    def test_full_history_replacement_leaves_conflicts_to_ranked_upsert(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(date(2026, 10, 23),)]
        _replace_daily_bars(connection, [("XYZ", date(2026, 10, 23))])
        self.assertEqual(cursor.execute.call_count, 1)
        upsert_sql = cursor.executemany.call_args.args[0].lower()
        self.assertIn("when 'massive' then 2", upsert_sql)
        self.assertIn("when 'yfinance' then 1", upsert_sql)

    def test_date_reload_deletes_missing_tickers_but_keeps_upsert_conflicts_for_rank_check(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        _replace_trade_date_atomically(connection, date(2026, 10, 23), [("XYZ", date(2026, 10, 23))], 100)
        delete_sql = cursor.execute.call_args.args[0].lower()
        self.assertIn("tradingview_screener", delete_sql)
        self.assertIn("'massive'", delete_sql)
        self.assertIn("ticker <> all(%s::text[])", delete_sql)
        self.assertEqual(cursor.execute.call_args.args[1], (date(2026, 10, 23), ["XYZ"]))
        self.assertIn("when 'massive' then 2", cursor.executemany.call_args.args[0].lower())


if __name__ == "__main__":
    unittest.main()
