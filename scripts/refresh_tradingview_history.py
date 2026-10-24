#!/usr/bin/env python3
"""Refresh existing Yahoo OHLCV from TradingView, preserving coverage and actions.

Dry-run by default. Run inside the application runtime with its production config.
Each ticker has durable provider cache, before/after backup, and outcome files.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
import gzip
import hashlib
import json
import math
import os
import re
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_app_config
from src.ticker_filters import (load_excluded_tickers, is_excluded_ticker, _load_ticker_file,
                                excluded_tickers_path, manual_excluded_tickers_path, auto_excluded_tickers_dir,
                                _load_special_security_tickers, special_security_tickers_path,
                                leveraged_or_inverse_catalog_symbols)
from src.universe import load_universe
from src.webapp.config import load_webapp_config

YAHOO_SOURCES = ('yfinance', 'yfinance+nasdaq', 'yfinance_historical_repair', 'yahoo', 'yahoo_finance')
COLUMNS = 'ticker,trade_date,open,high,low,close,adj_close,volume,dividend,split_factor,source,updated_at'
FIELDS = ('open', 'high', 'low', 'close', 'volume')
NY = ZoneInfo('America/New_York')


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value, default=str, sort_keys=True, separators=(',', ':')).encode()
    tmp = path.with_suffix(path.suffix+'.tmp')
    with tmp.open('wb') as handle:
        handle.write(gzip.compress(raw) if path.suffix == '.gz' else raw)
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)


def read_json(path):
    raw = path.read_bytes()
    return json.loads(gzip.decompress(raw) if path.suffix == '.gz' else raw)


def fingerprint(rows):
    return hashlib.sha256(json.dumps(rows, default=str, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def exclusions(config):
    # For a bulk repair, every exclusion list wins, including overlaps with includes.
    result = load_excluded_tickers(config)
    paths = [excluded_tickers_path(config), manual_excluded_tickers_path(config),
             *auto_excluded_tickers_dir(config).glob('*.txt')]
    for path in paths:
        result.update(_load_ticker_file(path))
    from src.etf_matcher import load_etf_catalog
    result.update(_load_special_security_tickers(special_security_tickers_path(config)))
    result.update(leveraged_or_inverse_catalog_symbols(load_etf_catalog()))
    return result


def select_universe(universe, metadata, excluded):
    exchanges = {'NYSE':'NYSE', 'NASDAQ':'NASDAQ', 'AMEX':'AMEX', 'NYSEAMERICAN':'AMEX'}
    selected = []
    for item in universe:
        symbol = item.symbol
        if is_excluded_ticker(symbol, excluded) or not metadata.get(symbol, False):
            continue
        exchange = exchanges.get(str(item.exchange or '').upper())
        if exchange:
            selected.append({'ticker':symbol, 'symbol':f'{exchange}:{symbol}'})
    return sorted({row['ticker']: row for row in selected}.values(), key=lambda r:r['ticker'])


def calendar_sessions(start, end):
    import exchange_calendars as xcals
    calendar = xcals.get_calendar('XNYS', start=str(dt.date.fromisoformat(start)-dt.timedelta(days=7)),
                                 end=str(dt.date.fromisoformat(end)+dt.timedelta(days=7)))
    return {v.date().isoformat() for v in calendar.sessions_in_range(start, end)}


def validate_provider(payload, symbol, start, end, sessions):
    if payload.get('error_code'):
        raise ValueError(payload['error_code'])
    meta, query = payload.get('metadata') or {}, payload.get('query') or {}
    if query.get('symbol') != symbol or meta.get('pro_name') != symbol:
        raise ValueError('wrong_symbol')
    if meta.get('currency_code') != 'USD' or meta.get('timezone') != 'America/New_York':
        raise ValueError('wrong_currency_or_timezone')
    if query.get('adjustment') != 'splits' or query.get('session') != 'regular' or meta.get('subsession_id') != 'regular':
        raise ValueError('wrong_session_or_adjustment')
    if query.get('from','')[:10] != start or query.get('to','')[:10] != end:
        raise ValueError('cached_range_mismatch')
    if meta.get('name') != symbol.split(':',1)[1] or meta.get('type') not in ('stock','fund','dr'):
        raise ValueError('wrong_instrument')
    candles, previous = {}, None
    for row in payload.get('candles', []):
        utc = dt.datetime.fromtimestamp(row['time'],dt.timezone.utc)
        local = utc.astimezone(NY)
        if local.time() == dt.time(9,30):
            date = local.date().isoformat()
        elif utc.time() == dt.time(0):
            date = utc.date().isoformat()  # Daily UTC labels must not shift to yesterday.
        else:
            raise ValueError('unexpected_daily_timestamp')
        if date not in sessions or date in candles or (previous and date <= previous):
            raise ValueError('invalid_or_duplicate_session')
        values = [float(row[k]) for k in FIELDS]
        o,h,l,c,v = values
        if (not all(math.isfinite(x) for x in values) or min(o,h,l,c)<=0 or
                h<max(o,c,l) or l>min(o,c,h) or v<0):
            raise ValueError('invalid_ohlcv')
        candles[date] = {k:Decimal(str(row[k])).quantize(Decimal('0.000001')) for k in FIELDS[:4]}
        if min(candles[date].values()) <= 0:
            raise ValueError('price_below_database_precision')
        # Split-adjusted history can have fractional volume; raw precision stays in the cache.
        candles[date]['volume'] = int(Decimal(str(row['volume'])).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        previous = date
    if not candles:
        raise ValueError('no_data')
    return candles


def plan_updates(rows, candles):
    targets = [r for r in rows if r['source'] in YAHOO_SOURCES]
    covered = [r for r in targets if str(r['trade_date']) in candles]
    if not covered:
        raise ValueError('no_overlapping_yahoo_dates')
    # A partial rebase can mix split regimes. Leave that ticker for a full-series review.
    if len(covered)<len(targets) and any(r['close'] and abs(float(candles[str(r['trade_date'])]['close'])/float(r['close'])-1)>0.02 for r in covered):
        raise ValueError('partial_history_adjustment_mismatch')
    return covered, len(targets)-len(covered)


def connect(read_only=False):
    import psycopg
    from psycopg.rows import dict_row
    options = '-c statement_timeout=60000 -c lock_timeout=5000'
    if read_only:
        options += ' -c default_transaction_read_only=on'
    return psycopg.connect(load_webapp_config().database_url, row_factory=dict_row, options=options)


def load_rows(connection, ticker, end, *, lock=False):
    with connection.cursor() as cur:
        cur.execute(f'SELECT {COLUMNS} FROM daily_bars WHERE ticker=%s AND trade_date<=%s ORDER BY trade_date'+(' FOR UPDATE' if lock else ''), (ticker,end))
        return cur.fetchall()


def apply_updates(connection, ticker, end, baseline, candles, backup_path, config):
    with connection.transaction():
        # Serialize repair processes; row locks and exact snapshots catch other writers.
        with connection.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ('tradingview-history:'+ticker,))
            cur.execute('SELECT is_active FROM ticker_metadata WHERE ticker=%s FOR SHARE', (ticker,))
            metadata = cur.fetchone()
        if not metadata or metadata['is_active'] is False or is_excluded_ticker(ticker,exclusions(config)):
            raise ValueError('no_longer_scan_eligible')
        current = load_rows(connection,ticker,end,lock=True)
        if fingerprint(current) != fingerprint(baseline):
            raise ValueError('concurrent_history_change')
        covered, missing = plan_updates(current,candles)
        updated_at = dt.datetime.now(dt.timezone.utc)
        write_json(backup_path, {'ticker':ticker,'before':current,'after':None})
        with connection.cursor() as cur:
            cur.executemany('UPDATE daily_bars SET open=%s,high=%s,low=%s,close=%s,volume=%s,source=%s,updated_at=%s WHERE ticker=%s AND trade_date=%s AND source=ANY(%s)',
                            [(*(candles[str(r['trade_date'])][k] for k in FIELDS),'tradingview',updated_at,ticker,r['trade_date'],list(YAHOO_SOURCES)) for r in covered])
        after = load_rows(connection,ticker,end)
        if len(after) != len(current):
            raise RuntimeError('coverage_changed')
        before_by_date = {r['trade_date']:r for r in current}
        changed_dates = {r['trade_date'] for r in covered}
        for row in after:
            old = before_by_date[row['trade_date']]
            if any(row[k]!=old[k] for k in ('adj_close','dividend','split_factor')):
                raise RuntimeError('corporate_actions_changed')
            if row['trade_date'] in changed_dates:
                candidate = candles[str(row['trade_date'])]
                if row['source'] != 'tradingview' or any(row[k] != candidate[k] for k in FIELDS):
                    raise RuntimeError('persisted_candle_mismatch')
            elif row != old:
                raise RuntimeError('untargeted_row_changed')
        write_json(backup_path, {'ticker':ticker,'before':current,'after':after})
    return len(covered),missing


def rollback_updates(connection, ticker, end, backup):
    if not backup.get('after'):
        raise ValueError('backup_has_no_applied_snapshot')
    with connection.transaction():
        with connection.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(hashtext(%s))', ('tradingview-history:'+ticker,))
        current=load_rows(connection,ticker,end,lock=True)
        if fingerprint(current)==fingerprint(backup['before']):
            return 0
        if fingerprint(current)!=fingerprint(backup['after']):
            raise ValueError('concurrent_history_change')
        targets=[r for r in backup['before'] if r['source'] in YAHOO_SOURCES]
        with connection.cursor() as cur:
            cur.executemany('UPDATE daily_bars SET open=%s,high=%s,low=%s,close=%s,adj_close=%s,volume=%s,dividend=%s,split_factor=%s,source=%s,updated_at=%s WHERE ticker=%s AND trade_date=%s',
                            [tuple(r[k] for k in ('open','high','low','close','adj_close','volume','dividend','split_factor','source','updated_at','ticker','trade_date')) for r in targets])
        if fingerprint(load_rows(connection,ticker,end))!=fingerprint(backup['before']):
            raise RuntimeError('rollback_verification_failed')
    return len(targets)


def fetch_provider(args, item, start, cache):
    if cache.exists():
        return read_json(cache)
    for attempt in range(3):
        proc = subprocess.run([args.node,str(ROOT/'scripts/fetch_tradingview_history.mjs'),args.client_dir,item['symbol'],start,args.end_date],
                              capture_output=True,text=True,timeout=65)
        try:
            payload=json.loads(proc.stdout)
        except ValueError:
            payload={'error_code':'CLIENT_FAILED'}
        code=payload.get('error_code')
        if not code:
            write_json(cache,payload)
            return payload
        if code in ('SYMBOL_ERROR','NO_DATA','INVALID_SYMBOL'):
            raise ValueError(code)
        if attempt<2:
            time.sleep(2**(attempt+1))
    raise ValueError(code)


def process_ticker(args, item):
    ticker=item['ticker']
    folder=Path(args.artifacts_dir)/ticker
    outcome_path=folder/('rollback.json' if args.rollback else 'apply.json' if args.apply else 'dry_run.json')
    if outcome_path.exists():
        return read_json(outcome_path)
    result={'ticker':ticker,'requested_symbol':item['symbol'],'mode':'apply' if args.apply else 'dry-run'}
    try:
        if args.rollback:
            with connect() as connection:
                restored=rollback_updates(connection,ticker,args.end_date,read_json(folder/'backup.json.gz'))
            result.update(mode='rollback',status='rolled_back',rows_restored=restored)
            write_json(outcome_path,result)
            return result
        with connect(read_only=True) as connection:
            baseline=load_rows(connection,ticker,args.end_date)
        baseline_path=folder/'baseline.json.gz'
        if baseline_path.exists():
            frozen=read_json(baseline_path)
            if fingerprint(baseline)!=fingerprint(frozen):
                backup_path=folder/'backup.json.gz'
                if args.apply and backup_path.exists() and fingerprint(baseline)==fingerprint(read_json(backup_path).get('after')):
                    result.update(status='applied',reason='recovered_committed_backup')
                    write_json(outcome_path,result)
                    return result
                raise ValueError('changed_since_dry_run')
        else:
            write_json(baseline_path,baseline)
        targets=[r for r in baseline if r['source'] in YAHOO_SOURCES]
        if not targets:
            result.update(status='skipped',reason='no_yahoo_rows')
        else:
            start=min(str(r['trade_date']) for r in targets)
            payload=fetch_provider(args,item,start,folder/'provider.json.gz')
            candles=validate_provider(payload,item['symbol'],start,args.end_date,calendar_sessions(start,args.end_date))
            covered,missing=plan_updates(baseline,candles)
            result.update(status='ready',available_candles=len(candles),replaceable_rows=len(covered),uncovered_yahoo_rows=missing,
                          rounded_volume_bars=sum(not float(c['volume']).is_integer() for c in payload['candles']),
                          resolved_symbol=payload['metadata']['full_name'],retrieved_at=payload['retrieved_at'],
                          first_date=min(candles),last_date=max(candles),baseline_sha256=fingerprint(baseline))
            if args.apply:
                with connect() as connection:
                    applied,missing=apply_updates(connection,ticker,args.end_date,baseline,candles,folder/'backup.json.gz',load_app_config(args.config or None))
                result.update(status='applied',rows_persisted=applied,uncovered_yahoo_rows=missing)
    except Exception as exc:
        # Provider or data-quality failures are isolated; connection strings never reach logs.
        reason=str(exc) if isinstance(exc,ValueError) and re.fullmatch(r'[A-Za-z0-9_]+',str(exc)) else type(exc).__name__
        result.update(status='skipped',reason=reason)
    write_json(outcome_path,result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',default='')
    parser.add_argument('--client-dir',default='',help='Built local @mathieuc/tradingview checkout')
    parser.add_argument('--node',default='node')
    parser.add_argument('--artifacts-dir',required=True)
    parser.add_argument('--end-date',required=True)
    parser.add_argument('--tickers',nargs='+')
    parser.add_argument('--workers',type=int,default=2,choices=(1,2,3,4))
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--rollback',action='store_true',help='Restore this run only if rows still match its applied backup')
    args=parser.parse_args()
    if args.apply and args.rollback:
        parser.error('--apply and --rollback are mutually exclusive')
    if not args.rollback and not args.client_dir:
        parser.error('--client-dir is required for historical downloads')
    # Never include a live or incomplete daily session.
    import exchange_calendars as xcals
    calendar=xcals.get_calendar('XNYS')
    end=dt.date.fromisoformat(args.end_date)
    if end.isoformat() not in calendar_sessions(args.end_date,args.end_date) or calendar.session_close(args.end_date).to_pydatetime()>dt.datetime.now(dt.timezone.utc):
        parser.error('--end-date must be a completed XNYS session')
    root=Path(args.artifacts_dir);root.mkdir(parents=True,exist_ok=True)
    with (root/'run.lock').open('w') as handle:
        import fcntl
        fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        scope_path=root/'scope.json'
        config=load_app_config(args.config or None)
        if scope_path.exists():
            scope=read_json(scope_path)
            if scope['end_date']!=args.end_date or scope.get('requested_tickers')!=args.tickers:
                parser.error('artifact directory belongs to a different date or ticker scope')
            items=scope['tickers']
        else:
            with connect(read_only=True) as connection:
                with connection.cursor() as cur:
                    cur.execute('SELECT ticker,COALESCE(is_active,TRUE) AS active FROM ticker_metadata')
                    metadata={r['ticker']:r['active'] for r in cur.fetchall()}
            items=select_universe(load_universe(config),metadata,exclusions(config))
            if args.tickers:
                items=[r for r in items if r['ticker'] in args.tickers]
            write_json(scope_path,{'created_at':dt.datetime.now(dt.timezone.utc),'end_date':args.end_date,'requested_tickers':args.tickers,'tickers':items})
        print(f'universe_tickers={len(items)} mode={"apply" if args.apply else "dry-run"}',flush=True)
        results=[]
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures=[pool.submit(process_ticker,args,item) for item in items]
            try:
                for future in as_completed(futures):
                    result=future.result();results.append(result)
                    print(json.dumps(result,default=str),flush=True)
                    mode='rollback' if args.rollback else 'apply' if args.apply else 'dry_run'
                    write_json(root/(mode+'_summary.json'),{'mode':mode,'completed':len(results),'universe':len(items),'results':results})
            except KeyboardInterrupt:
                for future in futures:
                    future.cancel()
                raise
    return 0


if __name__=='__main__':
    raise SystemExit(main())
