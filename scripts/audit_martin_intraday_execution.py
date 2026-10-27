#!/usr/bin/env python3
"""Freeze and audit same-day Martin limit fills against TradingView intraday bars.

Read-only: database access is forced read-only; output is an immutable evidence bundle.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
NY = ZoneInfo('America/New_York')
SUPPORTED_US_EXCHANGES = ('NYSE', 'NASDAQ', 'NYSEARCA', 'AMEX', 'NYSEAMERICAN', 'ARCA')


def normalize_exchange(exchange, default='NASDAQ'):
    value = str(exchange or '').strip().upper().replace(' ', '')
    aliases = {'NYSE': 'NYSE', 'NEWYORKSTOCKEXCHANGE': 'NYSE', 'NASDAQ': 'NASDAQ',
               'NASDAQGS': 'NASDAQ', 'NASDAQGM': 'NASDAQ', 'NASDAQCM': 'NASDAQ',
               'NYSEARCA': 'NYSEARCA', 'ARCA': 'ARCA', 'AMEX': 'AMEX',
               'NYSEAMERICAN': 'NYSEAMERICAN', 'AMERICAN': 'NYSEAMERICAN'}
    return aliases.get(value, default)


def fetch_with_exchange_fallback(client_dir, ticker, primary_exchange, day, tf, *, fetcher=None,
                                 resolve_symbols=False):
    """Try metadata's exchange first; only SYMBOL_ERROR permits exchange retries."""
    fetcher = fetcher or fetch_safely
    primary = f'{normalize_exchange(primary_exchange)}:{ticker}'
    attempted = []
    symbols = [primary] + [f'{exchange}:{ticker}' for exchange in SUPPORTED_US_EXCHANGES
                           if f'{exchange}:{ticker}' != primary]
    last = None
    for symbol in symbols:
        attempted.append(symbol)
        try:
            data = fetcher(client_dir, symbol, day, tf, resolve_symbols=resolve_symbols)
        except TypeError:  # Keep simple injected fetchers usable in tests/callers.
            data = fetcher(client_dir, symbol, day, tf)
        last = data
        if data and not data.get('error_code'):
            data = dict(data)
            data['exchange_lookup'] = {'attempted_symbols': attempted, 'selected_symbol':
                data.get('resolved_symbol') or symbol, 'primary_symbol': primary,
                'fallback_used': symbol != primary}
            return data
        code = (data or {}).get('error_code')
        if code not in {'SYMBOL_ERROR', 'LOOKUP_FAILED'}:
            break
    if last is None:
        last = {'error_code': 'provider_fetch_failed'}
    last = dict(last)
    last['exchange_lookup'] = {'attempted_symbols': attempted, 'selected_symbol': None,
                               'primary_symbol': primary, 'fallback_used': len(attempted) > 1}
    return last


def digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def session_starts(day, interval=15):
    import exchange_calendars as xcals
    cal = xcals.get_calendar('XNYS')
    if not cal.is_session(day):
        return []
    session = cal.date_to_session(day, direction='none')
    opened = cal.session_open(session).to_pydatetime().astimezone(NY)
    closed = cal.session_close(session).to_pydatetime().astimezone(NY)
    out = []
    t = opened
    while t < closed:
        out.append(int(t.astimezone(dt.timezone.utc).timestamp()))
        t += dt.timedelta(minutes=interval)
    return out


def aggregate(candles):
    if not candles: return None
    return {'open': candles[0]['open'], 'high': max(x['high'] for x in candles),
            'low': min(x['low'] for x in candles), 'close': candles[-1]['close'],
            'volume': sum(x.get('volume', 0) for x in candles)}


def classify(*, candles, metadata, requested_symbol, day, entry, stop, daily, timeframe='15', execution_version='martin_bar_retest_execution_v2'):
    actual = (metadata or {}).get('full_name') or (metadata or {}).get('pro_name')
    aggregate_bar = aggregate(candles)
    if daily is None:
        return {'classification':'missing_bars', 'actual_feed':actual, 'requested_feed':requested_symbol,
                'intraday_ohlc':aggregate_bar, 'daily_ohlc':None, 'expected_bar_count':0,
                'received_bar_count':len(candles), 'feed_mismatch':False,
                'reason':'stored_daily_bar_missing'}
    starts = session_starts(day, 1 if timeframe == '1' else 15)
    mismatch = actual != requested_symbol
    if aggregate_bar and daily:
        # Intraday volume and daily volume can differ between feeds. Price disagreement is decisive.
        mismatch |= any(abs(float(aggregate_bar[k])-float(daily[k])) > max(0.02, abs(float(daily[k]))*0.0005)
                        for k in ('open','high','low','close'))
    feed_conflict = bool(mismatch)
    by_time = {int(x['time']): x for x in candles}
    complete = bool(starts) and len(candles) == len(starts) and set(by_time) == set(starts)
    if mismatch and actual and actual.startswith('BATS:') and daily and complete and aggregate_bar:
        # Accept consolidated BATS only when its range is contained in stored daily prices
        # and its opening cannot change whether this order filled at the open.
        tol = max(0.02, abs(float(daily['close'])) * 0.0005)
        range_ok = (aggregate_bar['high'] <= float(daily['high']) + tol and
                    aggregate_bar['low'] >= float(daily['low']) - tol)
        open_fill_same = (float(aggregate_bar['open']) <= entry) == (float(daily['open']) <= entry)
        if range_ok and (open_fill_same or execution_version in {'martin_bar_retest_execution_v3', 'martin_bar_retest_execution_v4'}):
            mismatch = False
    if mismatch: status = 'feed_mismatch'
    elif not complete: status = 'missing_bars'
    else:
        from src.webapp.services.martin_intraday_execution import evaluate_sequence
        sequence = evaluate_sequence([by_time[t] for t in starts], entry=entry, stop=stop)
        status = sequence['status']
        if status == 'entry_before_stop' and sequence['bar_index'] == 0 and float(by_time[starts[0]]['open']) <= entry:
            status = 'opening_fill'
        elif status == 'no_fill':
            status = 'no_fill'
    return {'classification':status, 'actual_feed':actual, 'requested_feed':requested_symbol,
            'intraday_ohlc':aggregate_bar, 'daily_ohlc':daily, 'expected_bar_count':len(starts),
            'received_bar_count':len(candles), 'feed_mismatch':bool(mismatch),
            'feed_conflict':feed_conflict,
            **({'feed_conflict_note':'Complete regular-session BATS lower-timeframe bars inside the daily envelope determine limit touch and stop order.'}
               if feed_conflict and not mismatch and execution_version in {'martin_bar_retest_execution_v3', 'martin_bar_retest_execution_v4'} else {})}


def fetch(client_dir, symbol, day, tf, *, resolve_symbols=False):
    command = ['node', str(ROOT/'scripts/fetch_tradingview_intraday.mjs'), client_dir, symbol, day, tf,
               'resolve' if resolve_symbols else 'legacy']
    p = subprocess.run(command,
                       text=True, capture_output=True, check=False)
    data = json.loads(p.stdout)
    return data


def fetch_safely(client_dir, symbol, day, tf, *, resolve_symbols=False):
    try:
        return fetch(client_dir, symbol, day, tf, resolve_symbols=resolve_symbols)
    except Exception:
        # Provider and process errors may contain account or response details.
        return None


def prepare_output_dir(path):
    path = Path(path)
    if path.exists() and (not path.is_dir() or any(path.iterdir())):
        raise RuntimeError('output_directory_not_empty')
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_fills(conn, book, start, end):
    with conn.cursor() as cur:
        cur.execute("""SELECT e.id,e.ticker,e.event_date,e.payload_json,p.payload_json
            FROM martin_forward_events e JOIN martin_forward_positions p ON p.id=e.position_id
            LEFT JOIN daily_bars d ON d.ticker=e.ticker AND d.trade_date=e.event_date
            WHERE e.strategy_id=%s AND e.event_type='order_filled' AND e.event_date BETWEEN %s AND %s
              AND p.status='closed' AND p.opened_on=e.event_date AND p.closed_on=e.event_date
              AND e.payload_json->>'same_day_stop'='true'
              AND (e.payload_json->>'price')::numeric =
                  COALESCE(NULLIF(p.payload_json->>'limit_price','')::numeric,
                           NULLIF(p.payload_json->>'max_entry','')::numeric)
              AND (d.open IS NULL OR d.open > COALESCE(NULLIF(p.payload_json->>'limit_price','')::numeric,
                                                       NULLIF(p.payload_json->>'max_entry','')::numeric))
            ORDER BY e.event_date,e.ticker,e.id""", (book,start,end))
        return cur.fetchall()


def load_daily(conn, ticker, day):
    with conn.cursor() as cur:
        cur.execute("SELECT open,high,low,close,volume,source FROM daily_bars WHERE ticker=%s AND trade_date=%s",
                    (ticker,day))
        row = cur.fetchone()
        return ({k: float(v) for k,v in zip(('open','high','low','close','volume'), row[:5])} | {'source':row[5]}) if row else None


def load_ticker_exchange(conn, ticker):
    with conn.cursor() as cur:
        cur.execute('SELECT exchange FROM ticker_metadata WHERE ticker=%s', (ticker,))
        row = cur.fetchone()
        return row[0] if row else None


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--book-id', required=True); ap.add_argument('--from-date', required=True); ap.add_argument('--to-date', required=True)
    ap.add_argument('--client-dir', required=True); ap.add_argument('--output-dir', required=True)
    ap.add_argument('--exchange', default='NASDAQ', help='requested TradingView exchange prefix')
    ap.add_argument('--one-minute', action='store_true')
    ap.add_argument('--execution-version', choices=('v2','v3','v4'), default='v2')
    ap.add_argument('--freeze-tickers', help='Comma-separated symbols to freeze for every XNYS session in range; creates replay bundles without requiring prior fills.')
    a=ap.parse_args(argv)
    if not a.book_id.startswith('martin_range_bar_retest_'): ap.error('--book-id must start with martin_range_bar_retest_')
    start,end=dt.date.fromisoformat(a.from_date),dt.date.fromisoformat(a.to_date)
    if start>end: ap.error('--from-date must be <= --to-date')
    out=prepare_output_dir(a.output_dir)
    try:
        import psycopg
        from src.webapp.config import load_webapp_config
        conn=psycopg.connect(load_webapp_config().database_url, options='-c default_transaction_read_only=on -c statement_timeout=60000')
    except Exception as exc:
        raise RuntimeError('read_only_database_unavailable') from exc
    audit=[]
    try:
        if a.freeze_tickers:
            tickers = sorted({value.strip().upper() for value in a.freeze_tickers.split(',') if value.strip()})
            fills = [(None, ticker, day, {}, {}) for day in __import__('scripts.replay_martin_bar_retest_range', fromlist=['xnys_sessions']).xnys_sessions(start, end)
                     for ticker in tickers]
        else:
            fills=load_fills(conn,a.book_id,start,end)
        for eid,ticker,day,event,position in fills:
            ticker=str(ticker).upper(); day=day.isoformat()
            if a.freeze_tickers:
                # Sentinel levels make this a feed/coverage freeze, not an execution outcome.
                entry, stop, limit = 1e100, .01, None
            else:
                entry=float(event.get('price') or position.get('entry_price'))
                stop=float(position.get('stop') or position.get('initial_stop'))
                limit=float(position.get('limit_price') or position.get('max_entry') or entry)
                if abs(entry-limit) > 0.011: continue
            exchange = load_ticker_exchange(conn, ticker)
            symbol=f'{normalize_exchange(exchange, a.exchange)}:{ticker}'; resolve_symbols=a.execution_version in {'v3','v4'}
            data=fetch_with_exchange_fallback(a.client_dir,ticker,exchange or a.exchange,day,'15',resolve_symbols=resolve_symbols)
            if data is None or data.get('error_code'):
                resolution = (data or {}).get('symbol_resolution') or {'requested_symbol':symbol,
                    'resolved_symbol':(data or {}).get('resolved_symbol'),
                    'error_code':(data or {}).get('error_code') or 'provider_fetch_failed'}
                audit.append({'event_id':eid,'book_id':a.book_id,'ticker':ticker,'date':day,'entry':entry,
                              'limit':limit,'stop':stop,'classification':'symbol_lookup_failed' if (data or {}).get('error_code') == 'SYMBOL_ERROR' else 'provider_unavailable',
                              'requested_symbol':symbol,'resolved_symbol':(data or {}).get('resolved_symbol'),
                              'symbol_resolution':resolution, 'exchange_lookup':(data or {}).get('exchange_lookup'),
                              'provider_error_code':(data or {}).get('error_code') or 'provider_fetch_failed',
                              'timeframe':'15m','adjustment':'splits','session':'regular',
                              'candle_file':None,'candle_fingerprint':None,
                              'daily_source':(load_daily(conn,ticker,day) or {}).get('source')})
                continue
            candles=data.get('candles') or []; daily=load_daily(conn,ticker,day)
            resolved_symbol=data.get('resolved_symbol') or symbol
            result=classify(candles=candles,metadata=data.get('metadata'),requested_symbol=resolved_symbol,day=day,
                            entry=entry,stop=stop,daily=daily,
                            execution_version=f'martin_bar_retest_execution_{a.execution_version}')
            query=data.get('query') or {}; meta=data.get('metadata') or {}
            settings_ok=(query.get('session')=='regular' and query.get('adjustment')=='splits' and
                         query.get('timezone')=='America/New_York' and meta.get('subsession_id')=='regular' and
                         meta.get('timezone')=='America/New_York' and meta.get('currency_code')=='USD')
            if not settings_ok:
                result['classification']='feed_mismatch'; result['feed_mismatch']=True
            if a.one_minute and (a.freeze_tickers or result['classification']=='same_bar_ambiguous'):
                fine=fetch_safely(a.client_dir,symbol,day,'1',resolve_symbols=resolve_symbols)
                if fine is None:
                    result['classification']='provider_unavailable'
                    result['one_minute_refinement']={'classification':'provider_unavailable'}
                    fine_candles=[]
                else:
                    fine_candles=fine.get('candles') or []
                refined=classify(candles=fine_candles,metadata=fine.get('metadata'),requested_symbol=fine.get('resolved_symbol') or resolved_symbol,day=day,
                                 entry=entry,stop=stop,daily=daily,timeframe='1',
                                 execution_version=f'martin_bar_retest_execution_{a.execution_version}') if fine is not None else None
                if refined:
                    result['one_minute_refinement']=refined
                    if refined['classification'] in ('entry_before_stop','stop_after_entry','opening_fill','stop_before_entry','feed_mismatch','missing_bars'):
                        result['classification']=refined['classification']
                    data['one_minute']=fine
            stem=f'{ticker}_{day}'
            candle_path=out/f'{stem}.json'; candle_path.write_text(json.dumps(data,sort_keys=True,indent=2,default=str)+'\n')
            audit.append({'event_id':eid,'book_id':a.book_id,'ticker':ticker,'date':day,
                          **({'intraday_bundle_only': True} if a.freeze_tickers else {'entry':entry,'limit':limit,'stop':stop}),
                          **result,'requested_symbol':symbol,'resolved_symbol':data.get('resolved_symbol'),
                          'symbol_resolution':data.get('symbol_resolution'), 'exchange_lookup':data.get('exchange_lookup'),
                          'timeframe':'15m','adjustment':'splits','session':'regular',
                          'candle_file':candle_path.name,'candle_fingerprint':digest(data), 'daily_source':daily.get('source') if daily else None})
    finally: conn.close()
    document={'schema':'martin_intraday_execution_audit_v1','created_at':dt.datetime.now(dt.timezone.utc).isoformat(),
              'read_only':True,'book_id':a.book_id,'from_date':start.isoformat(),'to_date':end.isoformat(),
              'provider':'TradingView','execution_version':f'martin_bar_retest_execution_{a.execution_version}',
              'records':audit}
    document['fingerprint']=digest(document)
    path=out/'audit.json'; path.write_text(json.dumps(document,sort_keys=True,indent=2,default=str)+'\n')
    print(json.dumps({'audit_file':str(path),'records':len(audit),'fingerprint':document['fingerprint']}))

if __name__=='__main__':
    try: main()
    except RuntimeError as exc:
        print(json.dumps({'error_code':str(exc)}),file=sys.stderr); raise SystemExit(2)
