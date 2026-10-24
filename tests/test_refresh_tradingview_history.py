import datetime as dt
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from scripts import refresh_tradingview_history as repair


class TradingViewHistoryTests(unittest.TestCase):
    def payload(self):
        return {'metadata':{'pro_name':'NYSE:A','name':'A','type':'stock','currency_code':'USD',
                            'timezone':'America/New_York','subsession_id':'regular','full_name':'BATS:A'},
                'query':{'symbol':'NYSE:A','session':'regular','adjustment':'splits',
                         'from':'2026-10-23T00:00:00.000Z','to':'2026-10-23T23:59:59.000Z'},
                'candles':[{'time':1792762200,'open':10,'high':12,'low':9,'close':11,'volume':100}]}

    def validate(self, payload):
        return repair.validate_provider(payload,'NYSE:A','2026-10-23','2026-10-23',{'2026-10-23'})

    def test_cboe_feed_keeps_tradingview_identity_and_decimal_prices(self):
        bars=self.validate(self.payload())
        self.assertEqual(bars['2026-10-23']['close'],Decimal('11.000000'))

    def test_utc_daily_label_does_not_move_to_previous_ny_session(self):
        payload=self.payload();payload['candles'][0]['time']=int(dt.datetime(2026,10,23,tzinfo=dt.timezone.utc).timestamp())
        self.assertEqual(list(self.validate(payload)),['2026-10-23'])

    def test_wrong_symbol_session_adjustment_and_invalid_candles_rejected(self):
        for section,key,value in [('metadata','pro_name','NYSE:AA'),('metadata','subsession_id','extended'),
                                  ('query','adjustment','dividends'),('metadata','currency_code','EUR')]:
            payload=self.payload();payload[section][key]=value
            with self.subTest(key=key),self.assertRaises(ValueError): self.validate(payload)
        for field,value in [('open',13),('low',12),('close',float('nan')),('open',0),('open',0.0000001),('volume',-1)]:
            payload=self.payload();payload['candles'][0][field]=value
            if field=='open' and value==0.0000001: payload['candles'][0]['low']=value
            with self.subTest(field=field,value=value),self.assertRaises(ValueError): self.validate(payload)
        payload=self.payload();payload['candles']*=2
        with self.assertRaisesRegex(ValueError,'duplicate_session'): self.validate(payload)
        payload=self.payload();payload['candles']=[]
        with self.assertRaisesRegex(ValueError,'no_data'): self.validate(payload)

    def test_fractional_historical_volume_rounded_for_integer_storage(self):
        payload=self.payload();payload['candles'][0]['volume']=555.33333333
        self.assertEqual(self.validate(payload)['2026-10-23']['volume'],555)
        self.assertEqual(payload['candles'][0]['volume'],555.33333333)
        payload['candles'][0]['volume']=555.5
        self.assertEqual(self.validate(payload)['2026-10-23']['volume'],556)

    def test_partial_provider_result_preserves_unavailable_dates_and_non_yahoo(self):
        rows=[{'trade_date':'2026-10-22','source':'yfinance','close':10},
              {'trade_date':'2026-10-23','source':'yfinance','close':10},
              {'trade_date':'2026-10-21','source':'tradingview','close':10}]
        covered,missing=repair.plan_updates(rows,{'2026-10-23':{'close':Decimal(10)}})
        self.assertEqual((len(covered),missing),(1,1))
        with self.assertRaisesRegex(ValueError,'partial_history_adjustment_mismatch'):
            repair.plan_updates(rows,{'2026-10-23':{'close':Decimal(5)}})

    def test_universe_excludes_inactive_special_and_all_exclusion_lists(self):
        universe=[SimpleNamespace(symbol=t,exchange='NYSE') for t in ('A','AA','OLD','BAD','AACBR')]
        self.assertEqual(repair.select_universe(universe,{'A':True,'AA':True,'OLD':False,'BAD':True,'AACBR':True},{'BAD'}),
                         [{'ticker':'A','symbol':'NYSE:A'},{'ticker':'AA','symbol':'NYSE:AA'}])
        with TemporaryDirectory() as folder:
            path=Path(folder);(path/'manual.txt').write_text('A\n');(path/'auto').mkdir();(path/'auto'/'delisted.txt').write_text('AA\n')
            with patch.object(repair,'load_excluded_tickers',return_value=set()),patch.object(repair,'excluded_tickers_path',return_value=path/'absent'),patch.object(repair,'manual_excluded_tickers_path',return_value=path/'manual.txt'),patch.object(repair,'auto_excluded_tickers_dir',return_value=path/'auto'):
                self.assertTrue({'A','AA'} <= repair.exclusions(None))

    def test_cache_roundtrip_preserves_fingerprint(self):
        rows=[{'close':Decimal('10.000000'),'trade_date':dt.date(2026,10,23),'updated_at':dt.datetime.now(dt.timezone.utc)}]
        with TemporaryDirectory() as folder:
            path=Path(folder)/'backup.json.gz';repair.write_json(path,rows)
            self.assertEqual(repair.fingerprint(rows),repair.fingerprint(repair.read_json(path)))

    def test_missing_symbol_never_retries_or_falls_back(self):
        with TemporaryDirectory() as folder,patch.object(repair.subprocess,'run',return_value=SimpleNamespace(stdout='{"error_code":"SYMBOL_ERROR"}')) as fetch:
            args=SimpleNamespace(node='node',client_dir='/tmp/client',end_date='2026-10-23')
            with self.assertRaisesRegex(ValueError,'SYMBOL_ERROR'):
                repair.fetch_provider(args,{'symbol':'NYSE:ABSENT'},'2020-01-01',Path(folder)/'cache.gz')
            self.assertEqual(fetch.call_count,1)

    def test_retries_transient_provider_error(self):
        calls=[SimpleNamespace(stdout='{"error_code":"TIMEOUT"}'),SimpleNamespace(stdout='{"candles":[]}')]
        with TemporaryDirectory() as folder,patch.object(repair.subprocess,'run',side_effect=calls) as fetch,patch.object(repair.time,'sleep'):
            args=SimpleNamespace(node='node',client_dir='/tmp/client',end_date='2026-10-23')
            repair.fetch_provider(args,{'symbol':'NYSE:A'},'2020-01-01',Path(folder)/'cache.gz')
            self.assertEqual(fetch.call_count,2)

    def test_concurrent_update_stops_before_any_write(self):
        from contextlib import nullcontext
        cursor=SimpleNamespace(execute=lambda *a:None,fetchone=lambda:{'is_active':True})
        connection=SimpleNamespace(transaction=nullcontext,cursor=lambda:nullcontext(cursor))
        with patch.object(repair,'exclusions',return_value=set()),patch.object(repair,'load_rows',return_value=[{'close':11}]),patch.object(repair,'write_json') as save:
            with self.assertRaisesRegex(ValueError,'concurrent_history_change'):
                repair.apply_updates(connection,'A','2026-10-23',[{'close':10}],{},Path('/tmp/no-write'),None)
            save.assert_not_called()

    def test_protected_rows_are_not_counted_as_persisted(self):
        from scripts.sync_postgres_market_data import _upsert_daily_bars, _replace_daily_bars
        from scripts.reload_postgres_market_data_date import _replace_trade_date_atomically
        connection=MagicMock()
        cursor=connection.cursor.return_value.__enter__.return_value
        cursor.rowcount=0
        cursor.fetchall.side_effect=[[(dt.date(2026,10,23),)],[]]
        rows=[('A',dt.date(2026,10,23))]
        self.assertEqual(_upsert_daily_bars(connection,rows,100),(0,0))
        self.assertEqual(_replace_daily_bars(connection,rows),(0,0))
        self.assertEqual(_replace_trade_date_atomically(connection,dt.date(2026,10,23),rows,100),(0,0))


if __name__=='__main__': unittest.main()
