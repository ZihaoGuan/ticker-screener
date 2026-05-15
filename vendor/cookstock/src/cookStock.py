#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  9 00:10:18 2021

@author: sxu
"""
import numpy as np
import pandas as pd
import json as js
import datetime as dt
import os.path
import multiprocessing as mp
from time import sleep
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty
import requests
from requests.adapters import HTTPAdapter

try:
    # urllib3 >= 1.26
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover - very old urllib3
    from requests.packages.urllib3.util.retry import Retry

import matplotlib.pyplot as plt

# Cache the result of find_path so we don't walk the home directory on every
# import or instance creation.
_BASE_PATH_CACHE = None

def find_path():
    """Locate the 'cookstock' directory on disk. Result is cached after first call."""
    global _BASE_PATH_CACHE
    if _BASE_PATH_CACHE is not None:
        return _BASE_PATH_CACHE

    # Prefer a path relative to this file: src/.. is the cookstock root.
    here = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.dirname(here)
    if os.path.basename(candidate).lower() == 'cookstock' and os.path.isdir(candidate):
        _BASE_PATH_CACHE = candidate
        return _BASE_PATH_CACHE

    # Fallback: walk the home directory (legacy behavior).
    home_dir = os.path.expanduser("~")
    for root, dirs, _files in os.walk(home_dir):
        if 'cookstock' in dirs:
            _BASE_PATH_CACHE = os.path.join(root, 'cookstock')
            return _BASE_PATH_CACHE
    return None

basePath = find_path()
yhPath = os.path.join(basePath, 'yahoofinancials')
sys.path.insert(0, yhPath)
from yahoofinancials import YahooFinancials


def _build_yahoo_session():
    """Create a requests.Session with sensible retry/backoff for Yahoo."""
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET']),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


_YAHOO_SESSION = _build_yahoo_session()
_EARNINGS_SURPRISE_CACHE = {}
_YAHOO_CHART_HOSTS = (
    "https://query1.finance.yahoo.com",
    "https://query2.finance.yahoo.com",
)


def _fetch_yahoo_chart_json(symbol, period1, period2, interval="1d", events="div,splits,earn"):
    last_error = None
    timeout_seconds = max(1, int(algoParas.REQUEST_TIMEOUT_SECONDS))
    for host in _YAHOO_CHART_HOSTS:
        url = f"{host}/v8/finance/chart/{symbol}"
        params = {
            "period1": period1,
            "period2": period2,
            "interval": interval,
            "includePrePost": "false",
            "events": events,
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        }
        for attempt in range(1, 4):
            try:
                response = _YAHOO_SESSION.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                result = payload.get("chart", {}).get("result", [])
                if result:
                    return payload
                error_payload = payload.get("chart", {}).get("error")
                if error_payload:
                    raise RuntimeError(f"Yahoo chart error for {symbol}: {error_payload}")
                raise RuntimeError(f"Yahoo returned empty chart result for {symbol}")
            except Exception as exc:
                last_error = exc
                print(
                    f"Yahoo fetch retry {attempt}/3 failed for {symbol} via {host}: {exc}"
                )
                if attempt < 3:
                    sleep(attempt)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Yahoo fetch failed for {symbol}")


def _parse_ics_date(date_text):
    try:
        return dt.datetime.strptime(date_text.strip(), "%Y%m%d").date()
    except (TypeError, ValueError):
        return None


def _iter_ics_events(ics_text):
    current_event = None
    previous_line = None
    for raw_line in ics_text.splitlines():
        if raw_line.startswith((" ", "\t")) and previous_line is not None:
            previous_line += raw_line[1:]
            if current_event is not None:
                current_event[-1] = previous_line
            continue

        line = raw_line.strip()
        previous_line = line
        if line == "BEGIN:VEVENT":
            current_event = []
            continue
        if line == "END:VEVENT":
            if current_event is not None:
                yield current_event
            current_event = None
            previous_line = None
            continue
        if current_event is not None:
            current_event.append(line)


def _fetch_recent_ics_tickers(lookback_days, source_url):
    response = requests.get(
        source_url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "text/calendar,text/plain,*/*"},
        timeout=max(1, int(algoParas.REQUEST_TIMEOUT_SECONDS)),
    )
    response.raise_for_status()

    today = dt.date.today()
    start_date = today - dt.timedelta(days=lookback_days)
    tickers = []
    seen = set()

    for event_lines in _iter_ics_events(response.text):
        event_date = None
        ticker = None
        for line in event_lines:
            if line.startswith("DTSTART"):
                parts = line.split(":", 1)
                if len(parts) == 2:
                    event_date = _parse_ics_date(parts[1])
            elif line.startswith("SUMMARY:"):
                summary = line.split(":", 1)[1].strip()
                if summary:
                    ticker = summary.split()[0].upper()

        if event_date is None or ticker is None:
            continue
        if event_date < start_date or event_date > today:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)

    return tickers


def _fetch_ics_events_in_range(start_date, end_date, source_url):
    response = requests.get(
        source_url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "text/calendar,text/plain,*/*"},
        timeout=max(1, int(algoParas.REQUEST_TIMEOUT_SECONDS)),
    )
    response.raise_for_status()

    events = []
    seen = set()
    for event_lines in _iter_ics_events(response.text):
        event_date = None
        ticker = None
        summary = ''
        for line in event_lines:
            if line.startswith("DTSTART"):
                parts = line.split(":", 1)
                if len(parts) == 2:
                    event_date = _parse_ics_date(parts[1])
            elif line.startswith("SUMMARY:"):
                summary = line.split(":", 1)[1].strip()
                if summary:
                    ticker = summary.split()[0].upper()

        if event_date is None or ticker is None:
            continue
        if event_date < start_date or event_date > end_date:
            continue
        key = (ticker, event_date)
        if key in seen:
            continue
        seen.add(key)
        events.append({
            'ticker': ticker,
            'event_date': event_date,
            'summary': summary,
        })
    return events


def fetch_recent_earnings_watchlist(lookback_days=None, source_url=None, exclude_urls=None):
    lookback_days = max(1, int(lookback_days or algoParas.EARNINGS_WATCHLIST_LOOKBACK_DAYS))
    source_url = source_url or algoParas.EARNINGS_WATCHLIST_ICS_URL
    tickers = _fetch_recent_ics_tickers(lookback_days, source_url)

    configured_excludes = exclude_urls
    if configured_excludes is None:
        configured_excludes = getattr(algoParas, 'EARNINGS_WATCHLIST_EXCLUDE_ICS_URLS', [])

    excluded = set()
    for exclude_url in configured_excludes:
        if not isinstance(exclude_url, str) or not exclude_url.strip():
            continue
        excluded.update(_fetch_recent_ics_tickers(lookback_days, exclude_url.strip()))

    if not excluded:
        return tickers

    return [ticker for ticker in tickers if ticker not in excluded]


def get_next_week_date_range(reference_date=None):
    reference_date = reference_date or dt.date.today()
    days_until_monday = (7 - reference_date.weekday()) % 7
    if days_until_monday == 0:
        days_until_monday = 7
    start_date = reference_date + dt.timedelta(days=days_until_monday)
    end_date = start_date + dt.timedelta(days=4)
    return start_date, end_date


def fetch_earnings_calendar_watchlist(start_date, end_date, source_url=None, exclude_urls=None):
    source_url = source_url or algoParas.EARNINGS_WATCHLIST_ICS_URL
    events = _fetch_ics_events_in_range(start_date, end_date, source_url)

    configured_excludes = exclude_urls
    if configured_excludes is None:
        configured_excludes = getattr(algoParas, 'EARNINGS_WATCHLIST_EXCLUDE_ICS_URLS', [])

    excluded = set()
    for exclude_url in configured_excludes:
        if not isinstance(exclude_url, str) or not exclude_url.strip():
            continue
        for item in _fetch_ics_events_in_range(start_date, end_date, exclude_url.strip()):
            excluded.add(item['ticker'])

    filtered = []
    seen = set()
    for event in sorted(events, key=lambda item: (item['event_date'], item['ticker'])):
        ticker = event['ticker']
        if ticker in excluded or ticker in seen:
            continue
        filtered.append(event)
        seen.add(ticker)
    return filtered


def fetch_next_week_earnings_watchlist(reference_date=None, source_url=None, exclude_urls=None):
    start_date, end_date = get_next_week_date_range(reference_date)
    return fetch_earnings_calendar_watchlist(start_date, end_date, source_url, exclude_urls)


def _get_fmp_earnings_surprise_map(lookback_days):
    cache_key = ('fmp', int(lookback_days))
    if cache_key in _EARNINGS_SURPRISE_CACHE:
        return _EARNINGS_SURPRISE_CACHE[cache_key]

    api_key = os.getenv('FMP_API_KEY')
    if not api_key:
        _EARNINGS_SURPRISE_CACHE[cache_key] = {}
        return {}

    today = dt.date.today()
    start_date = today - dt.timedelta(days=max(1, int(lookback_days)))
    response = requests.get(
        'https://financialmodelingprep.com/stable/earnings-calendar',
        params={
            'apikey': api_key,
            'from': str(start_date),
            'to': str(today),
        },
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=max(1, int(algoParas.REQUEST_TIMEOUT_SECONDS)),
    )
    response.raise_for_status()

    surprise_map = {}
    for item in response.json():
        if not isinstance(item, dict):
            continue
        symbol = (item.get('symbol') or '').upper()
        date_text = item.get('date')
        actual_eps = item.get('epsActual')
        estimated_eps = item.get('epsEstimated')
        if not symbol or not date_text or actual_eps is None or estimated_eps in (None, 0):
            continue
        try:
            surprise_pct = ((float(actual_eps) - float(estimated_eps)) / abs(float(estimated_eps))) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        surprise_map.setdefault(symbol, []).append({
            'date': date_text,
            'actual_eps': float(actual_eps),
            'estimated_eps': float(estimated_eps),
            'surprise_pct': surprise_pct,
        })

    _EARNINGS_SURPRISE_CACHE[cache_key] = surprise_map
    return surprise_map


def get_recent_earnings_surprise_map(lookback_days=None):
    lookback_days = max(1, int(lookback_days or algoParas.EARNINGS_WATCHLIST_LOOKBACK_DAYS))
    provider = getattr(algoParas, 'EARNINGS_SURPRISE_PROVIDER', 'auto').lower()
    if provider in ('none', 'disabled', ''):
        return {}
    if provider in ('auto', 'fmp'):
        return _get_fmp_earnings_surprise_map(lookback_days)
    return {}

#define some constants
class algoParas:
    PIVOT_PRICE_PERC = 0.2
    VOLUME_DROP_THRESHOLD_HIGH = 0.8
    VOLUME_DROP_THRESHOLD_LOW = 0.4
    REGRESSION_DAYS = 100
    PEAK_VOL_RATIO = 1.3
    PRICE_POSITION_LOW = 0.66
    VOLUME_THRESHOLD = 100000
    BREAKOUT_VOLUME_RATIO = 1.4
    FINAL_CONTRACTION_MAX = 0.1
    PIVOT_EXTENSION_RATIO = 0.05
    MIN_VCP_CONTRACTIONS = 2
    BENCHMARK_TICKER = 'SPY'
    RS_LOOKBACK_DAYS = 90
    RS_LINE_NEAR_HIGH_RATIO = 0.95
    RS_NEW_HIGH_DAILY_LOOKBACK_DAYS = 250
    RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS = 52
    RS_NEW_HIGH_HISTORY_DAYS = 400
    RS_NEW_HIGH_REQUIRE_BEFORE_PRICE = True
    YEAR_HIGH_PROXIMITY = 0.15
    PARALLEL_ENABLED = False
    MAX_WORKERS = 4
    SCREEN_PROFILE = 'strict'
    REQUEST_TIMEOUT_SECONDS = 20
    TICKER_TIMEOUT_SECONDS = 180
    # Engine version controls whether the corrected algorithms are used:
    #   'v1' -> original/legacy behavior (calendar-day MAs, close-based swings, etc.)
    #   'v2' -> corrected behavior (trading-day MAs, high/low swings, VCP structure
    #           validation, up-day breakout filter, short-circuited screener, ...)
    # Default is 'v1' to preserve historical results bit-for-bit.
    ENGINE_VERSION = 'v1'
    # v2-only: max allowed depth for the FIRST (oldest) contraction in a valid VCP.
    FIRST_CONTRACTION_MAX = 0.35
    # v2-only: number of recent trading bars used for the recent-volume regression
    # inside is_demand_dry. Must be >= 5 for a meaningful slope.
    DEMAND_DRY_RECENT_DAYS = 7
    EARNINGS_WATCHLIST_LOOKBACK_DAYS = 20
    EARNINGS_WATCHLIST_ICS_URL = 'https://earnings.beavern.com/ics/all.ics'
    EARNINGS_WATCHLIST_EXCLUDE_ICS_URLS = []
    EARNINGS_SURPRISE_PROVIDER = 'auto'
    PEG_LOOKBACK_DAYS = 20
    PEG_EARNINGS_TOLERANCE_DAYS = 3
    PEG_MIN_GAP_PCT = 0.10
    PEG_MIN_VOLUME_RATIO = 3.0
    PEG_MONSTER_GAP_PCT = 0.20
    PEG_MONSTER_VOLUME_RATIO = 4.0
    PEG_MIN_EPS_SURPRISE_PCT = 20.0
    PEG_MAX_ENTRY_DISTANCE_PCT = 0.03
    PEG_MIN_CLOSE_POSITION_RATIO = 0.6
    PEG_REQUIRE_EARNINGS_EVENT = True
    PEG_REQUIRE_GREEN_CANDLE = True
    PEG_PRIMARY_ENTRY_MODE = 'peg_low'
    PEG_SECONDARY_ENTRY_FAST_EMA = 9
    PEG_SECONDARY_ENTRY_SLOW_EMA = 21
    PEG_DISTRIBUTION_LOOKBACK_DAYS = 10
    PEG_DISTRIBUTION_VOLUME_RATIO = 1.5
    PRE_EARNINGS_FAST_EMA = 9
    PRE_EARNINGS_SLOW_EMA = 21
    PRE_EARNINGS_LONG_EMA = 50
    PRE_EARNINGS_COMPRESSION_LOOKBACK_DAYS = 10
    PRE_EARNINGS_LIQUIDITY_LOOKBACK_DAYS = 20
    PRE_EARNINGS_A_SCORE_THRESHOLD = 75
    PRE_EARNINGS_B_SCORE_THRESHOLD = 55
    PRE_EARNINGS_HISTORY_DAYS = 180
    PRE_EARNINGS_USE_MARKET_MEMORY = False
    HTF_RUNUP_WINDOW_DAYS = 40
    HTF_MIN_RUNUP_PCT = 100.0
    HTF_MAX_CORRECTION_PCT = 25.0
    HTF_FAST_EMA = 9
    HTF_SLOW_EMA = 21
    HTF_LONG_EMA = 50
    HTF_LIQUIDITY_LOOKBACK_DAYS = 20
    HTF_A_SCORE_THRESHOLD = 75
    HTF_B_SCORE_THRESHOLD = 55
    HTF_HISTORY_DAYS = 180
    SECTOR_ETF_MAP = {
        'Technology': 'XLK',
        'Health Care': 'XLV',
        'Finance': 'XLF',
        'Energy': 'XLE',
        'Consumer Non-Durables': 'XLP',
        'Capital Goods': 'XLI',
        'Basic Industries': 'XLB',
        'Consumer Services': 'XLY',
        'Public Utilities': 'XLU',
        'Consumer Durables': 'XLY',
        'Transportation': 'IYT',
    }
    
    
def load_market_config():
    config_path = os.path.join(os.path.dirname(__file__), 'market_config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = js.load(f)
    except FileNotFoundError:
        return
    except ValueError as exc:
        print(f"Failed to parse market config {config_path}: {exc}")
        return

    benchmark_ticker = config.get('benchmark_ticker')
    if isinstance(benchmark_ticker, str) and benchmark_ticker.strip():
        algoParas.BENCHMARK_TICKER = benchmark_ticker.strip().upper()

    screen_profile = config.get('screen_profile')
    if isinstance(screen_profile, str) and screen_profile.strip():
        algoParas.SCREEN_PROFILE = screen_profile.strip().lower()

    engine_version = config.get('engine_version')
    if isinstance(engine_version, str) and engine_version.strip():
        normalized_version = engine_version.strip().lower()
        if normalized_version in ('v1', 'v2'):
            algoParas.ENGINE_VERSION = normalized_version
        else:
            print(
                f"Skipping invalid engine_version '{engine_version}'; "
                f"keeping '{algoParas.ENGINE_VERSION}'"
            )

    numeric_overrides = {
        'rs_lookback_days': ('RS_LOOKBACK_DAYS', int),
        'rs_line_near_high_ratio': ('RS_LINE_NEAR_HIGH_RATIO', float),
        'rs_new_high_daily_lookback_days': ('RS_NEW_HIGH_DAILY_LOOKBACK_DAYS', int),
        'rs_new_high_weekly_lookback_weeks': ('RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS', int),
        'rs_new_high_history_days': ('RS_NEW_HIGH_HISTORY_DAYS', int),
        'year_high_proximity': ('YEAR_HIGH_PROXIMITY', float),
        'breakout_volume_ratio': ('BREAKOUT_VOLUME_RATIO', float),
        'final_contraction_max': ('FINAL_CONTRACTION_MAX', float),
        'first_contraction_max': ('FIRST_CONTRACTION_MAX', float),
        'demand_dry_recent_days': ('DEMAND_DRY_RECENT_DAYS', int),
        'earnings_watchlist_lookback_days': ('EARNINGS_WATCHLIST_LOOKBACK_DAYS', int),
        'peg_lookback_days': ('PEG_LOOKBACK_DAYS', int),
        'peg_earnings_tolerance_days': ('PEG_EARNINGS_TOLERANCE_DAYS', int),
        'peg_min_gap_pct': ('PEG_MIN_GAP_PCT', float),
        'peg_min_volume_ratio': ('PEG_MIN_VOLUME_RATIO', float),
        'peg_monster_gap_pct': ('PEG_MONSTER_GAP_PCT', float),
        'peg_monster_volume_ratio': ('PEG_MONSTER_VOLUME_RATIO', float),
        'peg_min_eps_surprise_pct': ('PEG_MIN_EPS_SURPRISE_PCT', float),
        'peg_max_entry_distance_pct': ('PEG_MAX_ENTRY_DISTANCE_PCT', float),
        'peg_min_close_position_ratio': ('PEG_MIN_CLOSE_POSITION_RATIO', float),
        'peg_secondary_entry_fast_ema': ('PEG_SECONDARY_ENTRY_FAST_EMA', int),
        'peg_secondary_entry_slow_ema': ('PEG_SECONDARY_ENTRY_SLOW_EMA', int),
        'peg_distribution_lookback_days': ('PEG_DISTRIBUTION_LOOKBACK_DAYS', int),
        'peg_distribution_volume_ratio': ('PEG_DISTRIBUTION_VOLUME_RATIO', float),
        'pre_earnings_fast_ema': ('PRE_EARNINGS_FAST_EMA', int),
        'pre_earnings_slow_ema': ('PRE_EARNINGS_SLOW_EMA', int),
        'pre_earnings_long_ema': ('PRE_EARNINGS_LONG_EMA', int),
        'pre_earnings_compression_lookback_days': ('PRE_EARNINGS_COMPRESSION_LOOKBACK_DAYS', int),
        'pre_earnings_liquidity_lookback_days': ('PRE_EARNINGS_LIQUIDITY_LOOKBACK_DAYS', int),
        'pre_earnings_a_score_threshold': ('PRE_EARNINGS_A_SCORE_THRESHOLD', int),
        'pre_earnings_b_score_threshold': ('PRE_EARNINGS_B_SCORE_THRESHOLD', int),
        'pre_earnings_history_days': ('PRE_EARNINGS_HISTORY_DAYS', int),
        'htf_runup_window_days': ('HTF_RUNUP_WINDOW_DAYS', int),
        'htf_min_runup_pct': ('HTF_MIN_RUNUP_PCT', float),
        'htf_max_correction_pct': ('HTF_MAX_CORRECTION_PCT', float),
        'htf_fast_ema': ('HTF_FAST_EMA', int),
        'htf_slow_ema': ('HTF_SLOW_EMA', int),
        'htf_long_ema': ('HTF_LONG_EMA', int),
        'htf_liquidity_lookback_days': ('HTF_LIQUIDITY_LOOKBACK_DAYS', int),
        'htf_a_score_threshold': ('HTF_A_SCORE_THRESHOLD', int),
        'htf_b_score_threshold': ('HTF_B_SCORE_THRESHOLD', int),
        'htf_history_days': ('HTF_HISTORY_DAYS', int),
        'pivot_extension_ratio': ('PIVOT_EXTENSION_RATIO', float),
        'min_vcp_contractions': ('MIN_VCP_CONTRACTIONS', int),
        'volume_threshold': ('VOLUME_THRESHOLD', int),
        'max_workers': ('MAX_WORKERS', int),
        'request_timeout_seconds': ('REQUEST_TIMEOUT_SECONDS', int),
        'ticker_timeout_seconds': ('TICKER_TIMEOUT_SECONDS', int),
    }
    for config_key, (attr_name, caster) in numeric_overrides.items():
        value = config.get(config_key)
        if value is None:
            continue
        try:
            setattr(algoParas, attr_name, caster(value))
        except (TypeError, ValueError):
            print(f"Skipping invalid market config value for {config_key}: {value}")

    parallel_enabled = config.get('parallel_enabled')
    if isinstance(parallel_enabled, bool):
        algoParas.PARALLEL_ENABLED = parallel_enabled

    rs_new_high_require_before_price = config.get('rs_new_high_require_before_price')
    if isinstance(rs_new_high_require_before_price, bool):
        algoParas.RS_NEW_HIGH_REQUIRE_BEFORE_PRICE = rs_new_high_require_before_price

    pre_earnings_use_market_memory = config.get('pre_earnings_use_market_memory')
    if isinstance(pre_earnings_use_market_memory, bool):
        algoParas.PRE_EARNINGS_USE_MARKET_MEMORY = pre_earnings_use_market_memory

    peg_require_earnings_event = config.get('peg_require_earnings_event')
    if isinstance(peg_require_earnings_event, bool):
        algoParas.PEG_REQUIRE_EARNINGS_EVENT = peg_require_earnings_event

    peg_require_green_candle = config.get('peg_require_green_candle')
    if isinstance(peg_require_green_candle, bool):
        algoParas.PEG_REQUIRE_GREEN_CANDLE = peg_require_green_candle

    peg_primary_entry_mode = config.get('peg_primary_entry_mode')
    if isinstance(peg_primary_entry_mode, str) and peg_primary_entry_mode.strip():
        algoParas.PEG_PRIMARY_ENTRY_MODE = peg_primary_entry_mode.strip().lower()

    earnings_watchlist_ics_url = config.get('earnings_watchlist_ics_url')
    if isinstance(earnings_watchlist_ics_url, str) and earnings_watchlist_ics_url.strip():
        algoParas.EARNINGS_WATCHLIST_ICS_URL = earnings_watchlist_ics_url.strip()

    earnings_watchlist_exclude_ics_urls = config.get('earnings_watchlist_exclude_ics_urls')
    if isinstance(earnings_watchlist_exclude_ics_urls, list):
        normalized_urls = []
        for raw_url in earnings_watchlist_exclude_ics_urls:
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue
            normalized_urls.append(raw_url.strip())
        algoParas.EARNINGS_WATCHLIST_EXCLUDE_ICS_URLS = normalized_urls

    earnings_surprise_provider = config.get('earnings_surprise_provider')
    if isinstance(earnings_surprise_provider, str) and earnings_surprise_provider.strip():
        algoParas.EARNINGS_SURPRISE_PROVIDER = earnings_surprise_provider.strip().lower()

    sector_etf_map = config.get('sector_etf_map')
    if isinstance(sector_etf_map, dict):
        normalized_map = {}
        for sector_name, etf_ticker in sector_etf_map.items():
            if not isinstance(sector_name, str) or not isinstance(etf_ticker, str):
                continue
            if not sector_name.strip() or not etf_ticker.strip():
                continue
            normalized_map[sector_name.strip()] = etf_ticker.strip().upper()
        if normalized_map:
            algoParas.SECTOR_ETF_MAP = normalized_map


load_market_config()


def _is_v2():
    """True when the corrected algorithm path should be used."""
    return getattr(algoParas, 'ENGINE_VERSION', 'v1') == 'v2'


class cookFinancials(YahooFinancials):
    ticker = ''
    bshData = []
    bshData_quarter = []
    ish = []
    ish_quarter = []
    cfsh = []
    cfsh_quarter = []
    summaryData = []
    priceData = []
    m_recordVCP = []
    m_footPrint = []
    current_stickerPrice = []
    benchmark_price_cache = {}
    #define some parameters
    
    def __init__(self, ticker, benchmarkTicker=None, historyLookbackDays=365):
        super().__init__(ticker)  # Calls the parent class's initializer
        if isinstance(ticker, str):
            self.ticker = ticker.upper()
        else:
            self.ticker = [t.upper() for t in ticker]
        self.benchmark_ticker = (benchmarkTicker or algoParas.BENCHMARK_TICKER).upper()
        self.history_lookback_days = max(60, int(historyLookbackDays))
        self._cache = {}
        date = dt.date.today()
        self.priceData = self.get_historical_price_data(
            str(date - dt.timedelta(days=self.history_lookback_days)),
            str(date),
            'daily',
        )
        #get current_stickerPrice from self.priceData
        self.current_stickerPrice = self.priceData[self.ticker]['prices'][-1]['close']

    def get_historical_price_data(self, start_date, end_date, time_interval):
        if time_interval != 'daily':
            return super().get_historical_price_data(start_date, end_date, time_interval)

        start_dt = dt.datetime.strptime(str(start_date), "%Y-%m-%d")
        end_dt = dt.datetime.strptime(str(end_date), "%Y-%m-%d") + dt.timedelta(days=1)
        period1 = int(start_dt.replace(tzinfo=dt.timezone.utc).timestamp())
        period2 = int(end_dt.replace(tzinfo=dt.timezone.utc).timestamp())
        payload = _fetch_yahoo_chart_json(
            symbol=self.ticker,
            period1=period1,
            period2=period2,
            interval="1d",
            events="div,splits,earn",
        )
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return {self.ticker: {"eventsData": [], "firstTradeDate": None, "currency": None, "instrumentType": None, "timeZone": None, "prices": []}}

        chart = result[0]
        meta = chart.get("meta", {})
        raw_events = chart.get("events", {}) or {}
        timestamps = chart.get("timestamp", []) or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        prices = []
        for idx, ts in enumerate(timestamps):
            close_value = closes[idx] if idx < len(closes) else None
            if close_value is None:
                continue
            prices.append({
                "date": ts,
                "formatted_date": dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d"),
                "open": opens[idx] if idx < len(opens) else None,
                "high": highs[idx] if idx < len(highs) else None,
                "low": lows[idx] if idx < len(lows) else None,
                "close": close_value,
                "adjclose": close_value,
                "volume": volumes[idx] if idx < len(volumes) and volumes[idx] is not None else 0,
            })

        return {
            self.ticker: {
                "eventsData": self._normalize_chart_events(raw_events),
                "firstTradeDate": meta.get("firstTradeDate"),
                "currency": meta.get("currency"),
                "instrumentType": meta.get("instrumentType"),
                "timeZone": meta.get("exchangeTimezoneName"),
                "prices": prices,
            }
        }

    def _normalize_chart_events(self, raw_events):
        if not isinstance(raw_events, dict):
            return {}
        normalized = {}
        for event_type, event_map in raw_events.items():
            if not isinstance(event_map, dict):
                continue
            normalized_map = {}
            for event_key, event_payload in event_map.items():
                payload = dict(event_payload) if isinstance(event_payload, dict) else {}
                try:
                    event_ts = int(payload.get('date', event_key))
                    payload['date'] = event_ts
                    payload['formatted_date'] = dt.datetime.fromtimestamp(
                        event_ts,
                        dt.timezone.utc,
                    ).strftime("%Y-%m-%d")
                except (TypeError, ValueError, OSError):
                    payload['formatted_date'] = str(event_key)
                normalized_map[str(event_key)] = payload
            normalized[event_type] = normalized_map
        return normalized
        
    def get_balanceSheetHistory(self):
        self.bshData = self.get_financial_stmts('annual', 'balance')['balanceSheetHistory']
        return self.bshData
    
    def get_balanceSheetHistory_quarter(self):
        self.bshData_quarter = self.get_financial_stmts('quarterly', 'balance')['balanceSheetHistoryQuarterly']
        return self.bshData_quarter
    
    def get_incomeStatementHistory(self):
        self.ish = self.get_financial_stmts('annual', 'income')['incomeStatementHistory']
        return self.ish
    
    def get_incomeStatementHistory_quarter(self):
        self.ish_quarter = self.get_financial_stmts('quarterly', 'income')['incomeStatementHistoryQuarterly']
        return self.ish_quarter
    
    def get_cashflowStatementHistory(self):
        self.cfsh = self.get_financial_stmts('annual','cash')['cashflowStatementHistory']
        return self.cfsh
    def get_cashflowStatementHistory_quarter(self):
        self.cfsh_quarter = self.get_financial_stmts('quarterly','cash')['cashflowStatementHistoryQuarterly']
        return self.cfsh_quarter
    
    def get_BV(self, numofYears=20):
        bv = []
        if not(self.bshData):
            self.get_balanceSheetHistory()
        for i in range(min(np.size(self.bshData[self.ticker]), numofYears)):
            date_key = list(self.bshData[self.ticker][i].keys())[0]
            if not(self.bshData[self.ticker][i][date_key]):    
                break
            #check if the key is in the dictionary
            if not(self.bshData[self.ticker][i][date_key].get('stockholdersEquity')):
                #warning
                print('stockholdersEquity is not in the dictionary')
                break
            bv.append(self.bshData[self.ticker][i][date_key]['stockholdersEquity'])
        return bv
    
    def get_BV_quarter(self, numofQuarter=20):
        bv = []
        if not(self.bshData_quarter):
            self.get_balanceSheetHistory_quarter()
        for i in range(min(np.size(self.bshData_quarter[self.ticker]), numofQuarter)):
            date_key = list(self.bshData_quarter[self.ticker][i].keys())[0]
            if not(self.bshData_quarter[self.ticker][i][date_key]):    
                break
            if not(self.bshData_quarter[self.ticker][i][date_key].get('stockholdersEquity')):
                #warning
                print('stockholdersEquity is not in the dictionary')
                break
            bv.append(self.bshData_quarter[self.ticker][i][date_key]['stockholdersEquity'])
        return bv   
    
    def get_ROIC(self, numofYears=20):
        roic = []
        if not(self.cfsh):
            self.get_cashflowStatementHistory()
        if not(self.bshData):
            self.get_balanceSheetHistory()
        for i in range(min(np.size(self.bshData[self.ticker]), numofYears)):
            date_key = list(self.bshData[self.ticker][i].keys())[0]
            if not(self.bshData[self.ticker][i][date_key]):    
                break
            #check if the key is in the dictionary
            if not(self.bshData[self.ticker][i][date_key].get('stockholdersEquity')):
                #warning
                print('stockholdersEquity is not in the dictionary')
                break
            equity = self.bshData[self.ticker][i][date_key]['stockholdersEquity']
            if self.bshData[self.ticker][i][date_key].get('shortLongTermDebt') is None or not(self.bshData[self.ticker][i][date_key]['shortLongTermDebt']):
                debt_short = 0
            else:
                debt_short = self.bshData[self.ticker][i][date_key].get('shortLongTermDebt')
            if self.bshData[self.ticker][i][date_key].get('longTermDebt') is None or not(self.bshData[self.ticker][i][date_key]['longTermDebt']) :
                debt_long = 0
            else:
                debt_long = self.bshData[self.ticker][i][date_key]['longTermDebt']
            debt = debt_short + debt_long
            date_key = list(self.cfsh[self.ticker][i].keys())[0]
            if not(self.cfsh[self.ticker][i][date_key]):    
                break
            netincome = self.cfsh[self.ticker][i][date_key]['netIncome']
            roic_year = netincome/(equity + debt)
            roic.append(roic_year)
        return roic 
    
    def get_totalCashFromOperatingActivities(self, numofYears=20):
        totalCash = []
        if not(self.cfsh):
            self.get_cashflowStatementHistory()        
        for i in range(min(np.size(self.cfsh[self.ticker]), numofYears)):
            date_key = list(self.cfsh[self.ticker][i].keys())[0]
            if not(self.cfsh[self.ticker][i][date_key]):    
                break
            #check if the key is in the dictionary
            if not(self.cfsh[self.ticker][i][date_key].get('operatingCashFlow')):
                #warning
                print('operatingCashFlow is not in the dictionary')
                break
            totalCash.append(self.cfsh[self.ticker][i][date_key]['operatingCashFlow'])  
        return totalCash
    
    def get_pricetoSales(self):
        if not(self.summaryData):
            self.summaryData = self.get_summary_data()
        if not(self.summaryData[self.ticker]):
            return 'na'
        return self.summaryData[self.ticker]['priceToSalesTrailing12Months']
    
    def get_marketCap_B(self):
        if not(self.summaryData):
            self.summaryData = self.get_summary_data()
        if not(self.summaryData[self.ticker]):
            return 'na'
        return self.summaryData[self.ticker]['marketCap']/1000000000
    
    def get_CF_GR_median(self, totalCash):
        gr = []
        for v in range(np.size(totalCash)-1):
            gr.append((totalCash[v]-totalCash[v+1])/abs(totalCash[v+1]))
        #print(gr)
        return np.size(totalCash)-1, np.median(gr) 
    
    #use mean of each year    
    def get_BV_GR_median(self, bv):
        # Filter out None values from bv
        bv_filtered = [value for value in bv if value is not None]
        
        gr = []
        for v in range(np.size(bv_filtered) - 1):
            # Calculate growth rate between consecutive years
            gr.append((bv_filtered[v] - bv_filtered[v + 1]) / abs(bv_filtered[v + 1]))

        return np.size(bv_filtered) - 1, np.median(gr) if gr else None
    
    def get_GR_median(self, bv):
        gr = []
        for v in range(np.size(bv)-1):
            gr.append((bv[v]-bv[v+1])/abs(bv[v+1]))
        #print(gr)
        return np.size(bv)-1, np.median(gr)
    
    #use mean of each year    
    def get_ROIC_median(self, roic):
        return np.size(roic), np.median(roic)
    
    def get_BV_GR_max(self, bv):
        gr = []
        for v in range(np.size(bv)-1):
            gr.append((bv[v]-bv[v+1])/abs(bv[v+1]))
        #print(gr)
        return np.size(bv)-1, np.max(gr)
    
    def growthRate(self, cur,init, years):
        if cur <=0 or init<=0:
            return -1
        return (cur/init)**(1/years)-1
    
    def get_BV_GR_mean(self, bv):
        gr = []
        BV_GR = self.growthRate(bv[0], bv[np.size(bv)-1], np.size(bv)-1)
        if BV_GR==-1:
            for v in range(np.size(bv)-1):
                gr.append((bv[v]-bv[v+1])/abs(bv[v+1]))
            BV_GR = np.mean(gr)
        return np.size(bv)-1, BV_GR
    
    def get_suggest_price(self, cEPS, growth, years, rRate, PE, safty):
        if not(cEPS) or not(growth) or not(PE):
            return 'NA'
        fEPS = cEPS*(1+growth)**years
        fPrice = fEPS*PE;
        stickerPrice = fPrice/(1+rRate)**years
        return stickerPrice, stickerPrice*safty
    
    def payBackTime(self, price, cEPS, growth):
        tmp = 0
        i = 0
        if cEPS < 0:
            return 0
        while(growth>0):
            i+=1
            tmp = tmp + cEPS*(1+growth)**i
            if (tmp>price):
                break
        return i
    
    def get_earningsperShare(self):
        eps = self.get_earnings_per_share()
        if not(eps):
            eps = self.get_key_statistics_data()[self.ticker]['trailingEps']
        print(eps)
        return eps
    
    def get_PE(self):
        #print(self._stock_summary_data('trailingPE'))
        #print(self._stock_summary_data('forwardPE'))
        if not(self._stock_summary_data('trailingPE')):
            return self._stock_summary_data('forwardPE')
        if not(self._stock_summary_data('forwardPE')):
            return self._stock_summary_data('trailingPE')
        return (self._stock_summary_data('trailingPE')+self._stock_summary_data('forwardPE'))/2
    
    def get_decision(self,suggestPrice, stockprice):
        #print('suggested price:', suggestPrice)
        #print('stock price:', stockprice)
        if isinstance(suggestPrice, str):
            return 'skip due to negative eps'
        elif suggestPrice>stockprice:
            return 'strong buy' 
        else:
            return 'do not buy'   
    def get_ma_ref(self, date_from, date_to):
        data = self.get_historical_price_data(str(date_from),str(date_to), 'daily')
        tmp = 0
        if not(data[self.ticker]['prices']):
            return -1
        for i in range(len(data[self.ticker]['prices'])):
            #print(data[self.ticker]['prices'][i]['formatted_date'])
            if not(data[self.ticker]['prices'][i]['close']):
                data[self.ticker]['prices'][i]['close'] = data[self.ticker]['prices'][i-1]['close']
            tmp = tmp + data[self.ticker]['prices'][i]['close']
        return tmp/(i+1)
    
    def get_ma(self, date_from, date_to):
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        #don't need to pull data from remote, use local
        priceDataStruct = self.priceData[self.ticker]['prices']
        selectedPriceDataStruct = self.get_price_from_buffer_start_end(priceDataStruct, date_from, date_to)
        #data = self.get_historical_price_data(date_from,date_to, 'daily')
        tmp = 0
        if not(selectedPriceDataStruct):
            return -1
        for i in range(len(selectedPriceDataStruct)):
            #print(data[self.ticker]['prices'][i]['formatted_date'])
            if not(selectedPriceDataStruct[i]['close']):
                selectedPriceDataStruct[i]['close'] = selectedPriceDataStruct[i-1]['close']
            tmp = tmp + selectedPriceDataStruct[i]['close']
        return tmp/(i+1)
    
    def _get_ma_trading_days(self, n, end_date=None):
        """v2 helper: simple moving average over the last `n` trading bars
        (i.e., actual rows in priceData) ending at or before `end_date`.
        Falls back to using all available bars if there are fewer than n."""
        if not self.priceData:
            today = dt.date.today()
            self.priceData = self.get_historical_price_data(
                str(today - dt.timedelta(days=365)), str(today), 'daily')
        prices = [p for p in self.priceData[self.ticker]['prices']
                  if p.get('close') is not None]
        if not prices:
            return -1
        if end_date is not None:
            end_str = str(end_date)
            prices = [p for p in prices if p.get('formatted_date', '') <= end_str]
            if not prices:
                return -1
        window = prices[-n:] if len(prices) >= n else prices
        closes = [p['close'] for p in window if p.get('close') is not None]
        if not closes:
            return -1
        return float(np.mean(closes))

    def get_ma_50(self, date):
        if _is_v2():
            return self._get_ma_trading_days(50, end_date=date)
        date_from = (date - dt.timedelta(days=50))
        date_to = (date)
        return self.get_ma(date_from, date_to)
    def get_ma_200(self, date):
        if _is_v2():
            return self._get_ma_trading_days(200, end_date=date)
        date_from = (date - dt.timedelta(days=200))
        date_to = (date)
        return self.get_ma(date_from, date_to)
    def get_ma_150(self, date):
        if _is_v2():
            return self._get_ma_trading_days(150, end_date=date)
        date_from = (date - dt.timedelta(days=150))
        date_to = (date)
        return self.get_ma(date_from, date_to)
    def get_30day_trend_ma200(self):
        ###no need to look at everyday, just check last, mid, current
        current = self.get_ma_200((dt.date.today()))
        #print(dt.date.today())
        #print(current)
        mid = self.get_ma_200((dt.date.today()-dt.timedelta(days=15)))
        #print(dt.date.today()-dt.timedelta(days=15))
        #print(mid)
        last = self.get_ma_200((dt.date.today()-dt.timedelta(days=30)))
        #print(dt.date.today()-dt.timedelta(days=30))
        #print(last)
        if current - mid > 0 and mid -last > 0:
            return 1
        return -1
    def get_30day_trend(self):
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        length = len(self.priceData[self.ticker]['prices'])
        #get 30 days data
        price30Structure = self.get_price_from_buffer(self.priceData[self.ticker]['prices'], dt.date.today()-dt.timedelta(days=30), 30)
        price30 = [item['close'] for item in price30Structure]
        #find the trend
        trend, _ = self._calculate_volume_trend(price30)
        flag = 1 if trend > 0 else -1
        return flag
    
    def mv_strategy(self):
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        if not self.current_stickerPrice:
            self.current_stickerPrice = self.get_current_price()
        currentPrice = self.current_stickerPrice
        price50 = self.get_ma_50(dt.date.today())
        price150 = self.get_ma_150(dt.date.today())
        price200 = self.get_ma_200(dt.date.today())
        #print(currentPrice, price50, price150, price200, self.get_30day_trend_ma200())
        if (
            currentPrice > price50 > price150 > price200
            and self.get_30day_trend() == 1
            and self.get_30day_trend_ma200() == 1
        ):
            return 1
        return -1  
        
    def get_vol(self, checkDays, avrgDays):
        date = dt.date.today()
        vol3day = []
        vol50day = []
        if not self.priceData:
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        length = len(self.priceData[self.ticker]['prices'])
        for i in range(checkDays):
            if not(self.priceData[self.ticker]['prices'][length-1-i]['volume']):
                self.priceData[self.ticker]['prices'][length-1-i]['volume'] = self.priceData[self.ticker]['prices'][length-1-i+1]['volume']
            vol3day.append(self.priceData[self.ticker]['prices'][length-1-i]['volume'])
        #print(vol3day)
        for i in range(np.min([avrgDays, length])):
            if not(self.priceData[self.ticker]['prices'][length-1-checkDays-i]['volume']):
                self.priceData[self.ticker]['prices'][length-1-checkDays-i]['volume'] = self.priceData[self.ticker]['prices'][length-1-checkDays-i+1]['volume']
        #    print(self.priceData[self.ticker]['prices'][length-1-checkDays-i]['volume'])
            vol50day.append(self.priceData[self.ticker]['prices'][length-1-checkDays-i]['volume'])
        return vol3day, np.sum(vol3day)/checkDays, vol50day, np.sum(vol50day)/avrgDays
    
    def vol_strategy(self):
        avgVol50day = self._get_average_volume(50)
        if avgVol50day >= algoParas.VOLUME_THRESHOLD:
            return 1
        return -1

    def is_breakout_volume_confirmed(self):
        recentBar = self._get_recent_bar()
        if recentBar is None or recentBar.get('volume') is None:
            return False, 0, 0
        avgVolume50 = self._get_average_volume(50, exclude_recent=1)
        if avgVolume50 <= 0:
            return False, recentBar['volume'], avgVolume50
        flag = recentBar['volume'] >= algoParas.BREAKOUT_VOLUME_RATIO * avgVolume50
        if _is_v2():
            # A high-volume DOWN day is distribution, not breakout. Require an
            # up day on the breakout candle.
            open_p = recentBar.get('open')
            close_p = recentBar.get('close')
            if open_p is not None and close_p is not None:
                flag = flag and (close_p > open_p)
            # Require the close to actually clear the most-recent pivot.
            if self.m_recordVCP and close_p is not None:
                try:
                    pivot = float(self.m_recordVCP[-1][1])
                    flag = flag and (close_p > pivot)
                except (TypeError, ValueError, IndexError):
                    pass
        return flag, recentBar['volume'], avgVolume50
        
    def price_strategy(self):
        closePrice = []
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        length = len(self.priceData[self.ticker]['prices'])
        for i in range(length):
            if not(self.priceData[self.ticker]['prices'][i]['close']):
                self.priceData[self.ticker]['prices'][i]['close'] = self.priceData[self.ticker]['prices'][i-1]['close']
            closePrice.append(self.priceData[self.ticker]['prices'][i]['close'])
        lowestPrice = np.min(closePrice)
        if not self.current_stickerPrice:
            self.current_stickerPrice = self.get_current_price()
        currentPrice = self.current_stickerPrice
        highestPrice = np.max(closePrice)
    # Calculate range position as a percentage
        range_position = (currentPrice - lowestPrice) / (highestPrice - lowestPrice)

        # Conditions: within the upper third but below 90% of the 1-year high
        if algoParas.PRICE_POSITION_LOW <= range_position: #if it is larger than 1, it means it break out
            return 1  # Passes price positioning criteria
        return -1  # Fails price strategy
        
    def get_price_from_buffer(self, priceDataStruct, startDate, frame):
        selectedPriceDataStruct = []
        ##for each date
        currentDate = dt.date.today()
        dateList = []
        i = 0
        while(True):
            dateList.append(startDate + dt.timedelta(i))
            i = i + 1
            if dateList[-1] == currentDate:
                break
        for dd in dateList:
            for item in priceDataStruct:
                if item['formatted_date'] == str(dd):
                    selectedPriceDataStruct.append(item)
                    frame = frame - 1
                if frame <= 0:
                    break
        return selectedPriceDataStruct
    
    def get_price_from_buffer_start_end(self, priceDataStruct, startDate, endDate):
        selectedPriceDataStruct = []
        ##for each date
        currentDate = dt.date.today()
        dateList = []
        i = 0
        while(True):
            dateList.append(startDate + dt.timedelta(i))
            i = i + 1
            if dateList[-1] == endDate:
                break
        for dd in dateList:
            for item in priceDataStruct:
                if item['formatted_date'] == str(dd):
                    selectedPriceDataStruct.append(item)
        return selectedPriceDataStruct
        
#given start date and a time frame, if no price on that day, just move to next day
    def get_price(self, startDate, frame):
        to_date = startDate + dt.timedelta(frame)
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        #don't need to pull data from remote, use local
        priceDataStruct = self.priceData[self.ticker]['prices']
        selectedPriceDataStruct = self.get_price_from_buffer(priceDataStruct, startDate, frame)
        
                
        return selectedPriceDataStruct
    
    def get_price_ref(self, startDate, frame):
        to_date = startDate + dt.timedelta(frame)
        if not(self.priceData):
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date -  dt.timedelta(days=365)), str(date), 'daily')
        #don't need to pull data from remote, use local
        priceData = self.get_historical_price_data(str(startDate), str(to_date), 'daily')
        priceDataStruct = priceData[self.ticker]['prices']    
        return priceDataStruct

    def _get_clean_price_data(self):
        if not self.priceData:
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date - dt.timedelta(days=365)), str(date), 'daily')
        return [item for item in self.priceData[self.ticker]['prices'] if item.get('close') is not None]

    def _get_average_volume(self, lookback=50, exclude_recent=0):
        priceDataStruct = self._get_clean_price_data()
        if not priceDataStruct:
            return 0
        endIndex = len(priceDataStruct) - exclude_recent if exclude_recent > 0 else len(priceDataStruct)
        startIndex = max(0, endIndex - lookback)
        selected = priceDataStruct[startIndex:endIndex]
        if not selected:
            return 0
        volumeList = [item['volume'] for item in selected if item.get('volume') is not None]
        return np.mean(volumeList) if volumeList else 0

    def _get_average_volume_before_index(self, index, lookback=50):
        priceDataStruct = self._get_clean_price_data()
        if not priceDataStruct or index <= 0:
            return 0
        endIndex = max(0, index)
        startIndex = max(0, endIndex - lookback)
        selected = priceDataStruct[startIndex:endIndex]
        if not selected:
            return 0
        volumeList = [item['volume'] for item in selected if item.get('volume') is not None]
        return np.mean(volumeList) if volumeList else 0

    def _get_recent_bar(self, offset=0):
        priceDataStruct = self._get_clean_price_data()
        if len(priceDataStruct) <= offset:
            return None
        return priceDataStruct[-1-offset]

    def _get_earnings_event_dates(self):
        if not self.priceData:
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date - dt.timedelta(days=365)), str(date), 'daily')
        eventsData = self.priceData.get(self.ticker, {}).get('eventsData', {}) or {}
        earningsEvents = eventsData.get('earn', {}) or {}
        dates = set()
        for event_payload in earningsEvents.values():
            formatted_date = event_payload.get('formatted_date') if isinstance(event_payload, dict) else None
            if formatted_date:
                dates.add(formatted_date)
        return dates

    def _is_near_earnings_event(self, bar_date, earnings_dates, tolerance_days):
        if not bar_date or not earnings_dates:
            return False
        try:
            bar_dt = dt.datetime.strptime(bar_date, "%Y-%m-%d").date()
        except ValueError:
            return False
        for earnings_date in earnings_dates:
            try:
                event_dt = dt.datetime.strptime(earnings_date, "%Y-%m-%d").date()
            except ValueError:
                continue
            if abs((bar_dt - event_dt).days) <= tolerance_days:
                return True
        return False

    def _get_matching_earnings_surprise(self, bar_date, tolerance_days):
        surprise_map = get_recent_earnings_surprise_map(algoParas.PEG_LOOKBACK_DAYS + tolerance_days + 5)
        ticker_records = surprise_map.get(self.ticker, [])
        if not bar_date or not ticker_records:
            return None
        try:
            bar_dt = dt.datetime.strptime(bar_date, "%Y-%m-%d").date()
        except ValueError:
            return None

        closest = None
        for record in ticker_records:
            try:
                event_dt = dt.datetime.strptime(record['date'], "%Y-%m-%d").date()
            except (ValueError, KeyError, TypeError):
                continue
            delta_days = abs((bar_dt - event_dt).days)
            if delta_days > tolerance_days:
                continue
            if closest is None or delta_days < closest['delta_days']:
                closest = {**record, 'delta_days': delta_days}
        return closest

    def _compute_roc_series(self, values, length):
        series = np.full(len(values), np.nan, dtype=float)
        if length <= 0 or len(values) <= length:
            return series
        for idx in range(length, len(values)):
            base = values[idx - length]
            if base:
                series[idx] = (values[idx] - base) / base * 100.0
        return series

    def _compute_sma_series(self, values, length):
        series = np.full(len(values), np.nan, dtype=float)
        if length <= 0 or len(values) < length:
            return series
        rolling_sum = 0.0
        for idx, value in enumerate(values):
            rolling_sum += value
            if idx >= length:
                rolling_sum -= values[idx - length]
            if idx >= length - 1:
                series[idx] = rolling_sum / float(length)
        return series

    def _compute_ema_series(self, values, length):
        series = np.full(len(values), np.nan, dtype=float)
        if length <= 0 or len(values) == 0:
            return series
        alpha = 2.0 / (float(length) + 1.0)
        ema_value = np.nan
        for idx, value in enumerate(values):
            if np.isnan(value):
                series[idx] = ema_value
                continue
            if np.isnan(ema_value):
                ema_value = value
            else:
                ema_value = alpha * value + (1.0 - alpha) * ema_value
            series[idx] = ema_value
        return series

    def _compute_rsi_series(self, closes, length):
        series = np.full(len(closes), np.nan, dtype=float)
        if length <= 0 or len(closes) <= length:
            return series
        deltas = np.diff(closes)
        gains = np.maximum(deltas, 0.0)
        losses = np.maximum(-deltas, 0.0)
        avg_gain = np.mean(gains[:length])
        avg_loss = np.mean(losses[:length])

        if avg_loss == 0:
            series[length] = 100.0
        else:
            rs = avg_gain / avg_loss
            series[length] = 100.0 - (100.0 / (1.0 + rs))

        for idx in range(length + 1, len(closes)):
            gain = gains[idx - 1]
            loss = losses[idx - 1]
            avg_gain = ((avg_gain * (length - 1)) + gain) / float(length)
            avg_loss = ((avg_loss * (length - 1)) + loss) / float(length)
            if avg_loss == 0:
                series[idx] = 100.0
            else:
                rs = avg_gain / avg_loss
                series[idx] = 100.0 - (100.0 / (1.0 + rs))
        return series

    def _compute_atr_pct_series(self, highs, lows, closes, length):
        series = np.full(len(closes), np.nan, dtype=float)
        if length <= 1 or len(closes) <= length:
            return series
        true_ranges = np.full(len(closes), np.nan, dtype=float)
        for idx in range(1, len(closes)):
            tr_1 = highs[idx] - lows[idx]
            tr_2 = abs(highs[idx] - closes[idx - 1])
            tr_3 = abs(lows[idx] - closes[idx - 1])
            true_ranges[idx] = max(tr_1, tr_2, tr_3)

        atr_value = np.nanmean(true_ranges[1:length + 1])
        if not np.isnan(atr_value) and closes[length]:
            series[length] = atr_value / closes[length] * 100.0

        for idx in range(length + 1, len(closes)):
            atr_value = ((atr_value * (length - 1)) + true_ranges[idx]) / float(length)
            if closes[idx]:
                series[idx] = atr_value / closes[idx] * 100.0
        return series

    def _get_latest_ema_value(self, length):
        priceDataStruct = self._get_clean_price_data()
        if not priceDataStruct:
            return None
        closes = np.asarray([item.get('close') or np.nan for item in priceDataStruct], dtype=float)
        ema_series = self._compute_ema_series(closes, int(length))
        latest = ema_series[-1]
        if np.isnan(latest):
            return None
        return float(latest)

    def _get_average_dollar_volume(self, lookback=20):
        priceDataStruct = self._get_clean_price_data()
        if not priceDataStruct:
            return 0.0
        selected = priceDataStruct[-max(1, int(lookback)):]
        dollar_volumes = []
        for item in selected:
            close_p = item.get('close')
            volume = item.get('volume')
            if close_p is None or volume is None:
                continue
            dollar_volumes.append(float(close_p) * float(volume))
        return float(np.mean(dollar_volumes)) if dollar_volumes else 0.0

    def _get_recent_range_pct(self, lookback=10):
        priceDataStruct = self._get_clean_price_data()
        if not priceDataStruct:
            return None
        selected = priceDataStruct[-max(2, int(lookback)):]
        highs = [item.get('high') for item in selected if item.get('high') is not None]
        lows = [item.get('low') for item in selected if item.get('low') is not None]
        current_price = selected[-1].get('close')
        if not highs or not lows or not current_price:
            return None
        return float((max(highs) - min(lows)) / current_price)

    def _get_distribution_warning(self):
        priceDataStruct = self._get_clean_price_data()
        lookback_days = max(1, int(getattr(algoParas, 'PEG_DISTRIBUTION_LOOKBACK_DAYS', 10)))
        volume_ratio_threshold = float(getattr(algoParas, 'PEG_DISTRIBUTION_VOLUME_RATIO', 1.5))
        if len(priceDataStruct) < 35:
            return {
                'distribution_warning': False,
                'distribution_days_count': 0,
                'latest_distribution_date': None,
                'latest_distribution_volume_ratio': None,
            }

        distribution_days = []
        start_index = max(1, len(priceDataStruct) - lookback_days)
        for idx in range(start_index, len(priceDataStruct)):
            bar = priceDataStruct[idx]
            prev_bar = priceDataStruct[idx - 1]
            close_p = bar.get('close')
            prev_close = prev_bar.get('close')
            open_p = bar.get('open')
            volume = bar.get('volume')
            if None in (close_p, prev_close, open_p, volume):
                continue
            avg_volume_30 = self._get_average_volume_before_index(idx, 30)
            if avg_volume_30 <= 0:
                continue
            volume_ratio = volume / avg_volume_30
            if close_p < prev_close and close_p < open_p and volume_ratio >= volume_ratio_threshold:
                distribution_days.append({
                    'date': bar.get('formatted_date'),
                    'volume_ratio': volume_ratio,
                })

        latest_distribution = distribution_days[-1] if distribution_days else None
        return {
            'distribution_warning': bool(distribution_days),
            'distribution_days_count': len(distribution_days),
            'latest_distribution_date': latest_distribution['date'] if latest_distribution else None,
            'latest_distribution_volume_ratio': latest_distribution['volume_ratio'] if latest_distribution else None,
        }

    def _get_htf_runup_summary(self, window_days=40):
        priceDataStruct = self._get_clean_price_data()
        window_days = max(10, int(window_days))
        if len(priceDataStruct) < window_days:
            return None

        selected = priceDataStruct[-window_days:]
        lows = [item.get('low') for item in selected if item.get('low') is not None]
        highs = [item.get('high') for item in selected if item.get('high') is not None]
        closes = [item.get('close') for item in selected if item.get('close') is not None]
        if not lows or not highs or not closes:
            return None

        runup_low = float(min(lows))
        runup_high = float(max(highs))
        current_price = float(closes[-1])
        if runup_low <= 0 or runup_high <= 0:
            return None

        runup_pct = ((runup_high - runup_low) / runup_low) * 100.0
        pullback_from_high_pct = ((runup_high - current_price) / runup_high) * 100.0
        priceDataByDate = {item.get('formatted_date'): item for item in selected if item.get('formatted_date')}
        high_dates = [
            item.get('formatted_date')
            for item in selected
            if item.get('high') is not None and float(item.get('high')) == runup_high
        ]
        low_dates = [
            item.get('formatted_date')
            for item in selected
            if item.get('low') is not None and float(item.get('low')) == runup_low
        ]

        return {
            'window_days': window_days,
            'runup_low': runup_low,
            'runup_high': runup_high,
            'runup_pct': runup_pct,
            'pullback_from_high_pct': pullback_from_high_pct,
            'current_price': current_price,
            'runup_low_date': low_dates[0] if low_dates else None,
            'runup_high_date': high_dates[-1] if high_dates else None,
        }

    def get_peg_trade_plan(self, peg_setup):
        if not peg_setup:
            return None

        fast_ema_length = max(1, int(getattr(algoParas, 'PEG_SECONDARY_ENTRY_FAST_EMA', 9)))
        slow_ema_length = max(fast_ema_length, int(getattr(algoParas, 'PEG_SECONDARY_ENTRY_SLOW_EMA', 21)))
        primary_entry_mode = getattr(algoParas, 'PEG_PRIMARY_ENTRY_MODE', 'peg_low')
        primary_entry = float(peg_setup['peg_low'])
        ema_fast = self._get_latest_ema_value(fast_ema_length)
        ema_slow = self._get_latest_ema_value(slow_ema_length)
        distribution = self._get_distribution_warning()

        if ema_fast is not None and ema_slow is not None:
            secondary_low = min(ema_fast, ema_slow)
            secondary_high = max(ema_fast, ema_slow)
            secondary_label = f'ema{fast_ema_length}/ema{slow_ema_length}_zone'
        else:
            secondary_low = None
            secondary_high = None
            secondary_label = 'ema_zone'

        return {
            'primary_entry_mode': str(primary_entry_mode),
            'primary_entry_label': 'peg_low',
            'primary_entry': primary_entry,
            'secondary_entry_label': secondary_label,
            'secondary_entry_fast_ema_length': fast_ema_length,
            'secondary_entry_slow_ema_length': slow_ema_length,
            'secondary_entry_fast_ema': ema_fast,
            'secondary_entry_slow_ema': ema_slow,
            'secondary_entry_low': secondary_low,
            'secondary_entry_high': secondary_high,
            'distribution_warning': distribution['distribution_warning'],
            'distribution_days_count': distribution['distribution_days_count'],
            'latest_distribution_date': distribution['latest_distribution_date'],
            'latest_distribution_volume_ratio': distribution['latest_distribution_volume_ratio'],
            'distribution_volume_ratio_threshold': float(getattr(algoParas, 'PEG_DISTRIBUTION_VOLUME_RATIO', 1.5)),
        }

    def get_market_memory_summary(self):
        priceDataStruct = self._get_clean_price_data()
        if len(priceDataStruct) < 120:
            return None

        closes = np.asarray([item.get('close') or np.nan for item in priceDataStruct], dtype=float)
        highs = np.asarray([item.get('high') or np.nan for item in priceDataStruct], dtype=float)
        lows = np.asarray([item.get('low') or np.nan for item in priceDataStruct], dtype=float)
        volumes = np.asarray([item.get('volume') or 0.0 for item in priceDataStruct], dtype=float)

        scan_depth = min(300, max(20, len(closes) - 30))
        top_matches = 5
        pattern_bars = 10
        sensitivity = 1.5
        rsi_length = 14
        atr_length = 14
        vol_length = 20
        smooth_length = 50
        trend_length = 20

        roc1 = self._compute_roc_series(closes, 1)
        roc3 = self._compute_roc_series(closes, 3)
        roc5 = self._compute_roc_series(closes, 5)
        rsi = self._compute_rsi_series(closes, rsi_length) / 100.0
        atr_pct = self._compute_atr_pct_series(highs, lows, closes, atr_length)
        vol_sma = self._compute_sma_series(volumes, vol_length)
        vol_rel = np.ones(len(closes), dtype=float)
        valid_vol = ~np.isnan(vol_sma) & (vol_sma != 0)
        vol_rel[valid_vol] = volumes[valid_vol] / vol_sma[valid_vol]

        raw_memory = np.full(len(closes), np.nan, dtype=float)
        last_avg_similarity = np.nan
        last_weighted_momentum = np.nan
        last_match_count = 0

        min_index = pattern_bars + 6
        for current_idx in range(min_index, len(closes)):
            if any(
                np.isnan(feature[current_idx])
                for feature in (roc1, roc3, roc5, rsi, atr_pct, vol_rel)
            ):
                continue

            max_scan = min(scan_depth, current_idx - pattern_bars - 1)
            if max_scan < pattern_bars + 1:
                continue

            matches = []
            for bars_ago in range(pattern_bars + 1, max_scan + 1):
                hist_idx = current_idx - bars_ago
                hist_features = (
                    roc1[hist_idx],
                    roc3[hist_idx],
                    roc5[hist_idx],
                    rsi[hist_idx],
                    atr_pct[hist_idx],
                    vol_rel[hist_idx],
                )
                if any(np.isnan(value) for value in hist_features):
                    continue

                d1 = abs(roc1[current_idx] - roc1[hist_idx]) / 2.0
                d2 = abs(roc3[current_idx] - roc3[hist_idx]) / 4.0
                d3 = abs(roc5[current_idx] - roc5[hist_idx]) / 6.0
                d4 = abs(rsi[current_idx] - rsi[hist_idx]) * 2.0
                d5 = abs(atr_pct[current_idx] - atr_pct[hist_idx]) / 2.0
                d6 = abs(vol_rel[current_idx] - vol_rel[hist_idx]) / 1.5
                distance = d1 + d2 + d3 + d4 + d5 + d6
                similarity = 100.0 * np.exp(-distance * sensitivity)
                matches.append((similarity, roc5[hist_idx]))

            if not matches:
                continue

            matches.sort(key=lambda item: item[0], reverse=True)
            selected = matches[:top_matches]
            total_weight = sum(item[0] for item in selected)
            if total_weight <= 0:
                continue

            weighted_momentum = sum(item[0] * item[1] for item in selected) / total_weight
            raw_memory[current_idx] = closes[current_idx] * (1.0 + weighted_momentum / 100.0)

            if current_idx == len(closes) - 1:
                last_avg_similarity = sum(item[0] for item in selected) / float(len(selected))
                last_weighted_momentum = weighted_momentum
                last_match_count = len(selected)

        smoothed_memory = self._compute_ema_series(raw_memory, smooth_length)
        latest_memory = smoothed_memory[-1]
        if np.isnan(latest_memory):
            return None

        slope_anchor_idx = max(0, len(smoothed_memory) - trend_length - 1)
        slope_anchor = smoothed_memory[slope_anchor_idx]
        if np.isnan(slope_anchor) or slope_anchor == 0:
            slope_pct = 0.0
        else:
            slope_pct = (latest_memory - slope_anchor) / slope_anchor * 100.0

        if slope_pct > 1.0:
            trend = 'bullish'
        elif slope_pct < -1.0:
            trend = 'bearish'
        else:
            trend = 'neutral'

        current_close = closes[-1]
        price_vs_memory_pct = ((current_close - latest_memory) / latest_memory * 100.0) if latest_memory else 0.0
        if price_vs_memory_pct > 1.0:
            price_position = 'above'
        elif price_vs_memory_pct < -1.0:
            price_position = 'below'
        else:
            price_position = 'near'

        similarity_score = 0.0 if np.isnan(last_avg_similarity) else last_avg_similarity
        weighted_momentum_score = 0.0 if np.isnan(last_weighted_momentum) else abs(last_weighted_momentum)
        strength_score = min(
            100.0,
            (abs(slope_pct) * 6.0) + (weighted_momentum_score * 5.0) + (similarity_score * 0.2),
        )

        if strength_score >= 75:
            strength_label = 'very strong'
        elif strength_score >= 55:
            strength_label = 'strong'
        elif strength_score >= 35:
            strength_label = 'moderate'
        else:
            strength_label = 'weak'

        return {
            'memory_average': latest_memory,
            'memory_slope_pct': slope_pct,
            'memory_trend': trend,
            'memory_strength_score': strength_score,
            'memory_strength_label': strength_label,
            'memory_price_position': price_position,
            'memory_price_vs_average_pct': price_vs_memory_pct,
            'memory_similarity_score': similarity_score,
            'memory_weighted_momentum_pct': 0.0 if np.isnan(last_weighted_momentum) else last_weighted_momentum,
            'memory_match_count': last_match_count,
        }

    def get_pre_earnings_focus_summary(self, event_date=None, sectorName=None, benchmarkTicker=None):
        priceDataStruct = self._get_clean_price_data()
        if len(priceDataStruct) < 60:
            return None

        closes = np.asarray([item.get('close') or np.nan for item in priceDataStruct], dtype=float)
        current_price = float(closes[-1])
        ema_fast_length = max(1, int(getattr(algoParas, 'PRE_EARNINGS_FAST_EMA', 9)))
        ema_slow_length = max(ema_fast_length, int(getattr(algoParas, 'PRE_EARNINGS_SLOW_EMA', 21)))
        ema_long_length = max(ema_slow_length, int(getattr(algoParas, 'PRE_EARNINGS_LONG_EMA', 50)))
        compression_lookback = max(5, int(getattr(algoParas, 'PRE_EARNINGS_COMPRESSION_LOOKBACK_DAYS', 10)))
        liquidity_lookback = max(5, int(getattr(algoParas, 'PRE_EARNINGS_LIQUIDITY_LOOKBACK_DAYS', 20)))
        a_threshold = int(getattr(algoParas, 'PRE_EARNINGS_A_SCORE_THRESHOLD', 75))
        b_threshold = int(getattr(algoParas, 'PRE_EARNINGS_B_SCORE_THRESHOLD', 55))

        ema_fast = self._get_latest_ema_value(ema_fast_length)
        ema_slow = self._get_latest_ema_value(ema_slow_length)
        ema_long = self._get_latest_ema_value(ema_long_length)
        if None in (ema_fast, ema_slow, ema_long):
            return None

        benchmarkTicker = benchmarkTicker or algoParas.BENCHMARK_TICKER
        use_market_memory = bool(getattr(algoParas, 'PRE_EARNINGS_USE_MARKET_MEMORY', False))
        memorySummary = self.get_market_memory_summary() if use_market_memory else None
        distribution = self._get_distribution_warning()
        isNearYearHigh, _, yearHigh, distanceFromHigh = self.is_near_year_high()
        isStrongRs, stockReturn, benchmarkReturn, currentRsLine, rsLineHigh = self.is_relative_strength_strong(benchmarkTicker)
        isSectorStrong, sectorEtf, sectorEtfNearHigh, _, _, sectorEtfDistance, sectorEtfReturn, sectorBenchmarkReturn = self.is_sector_etf_strong(sectorName, benchmarkTicker)
        avg_dollar_volume = self._get_average_dollar_volume(liquidity_lookback)
        recent_range_pct = self._get_recent_range_pct(compression_lookback)
        market_cap_b = self.get_marketCap_B()
        try:
            market_cap_b = float(market_cap_b)
        except (TypeError, ValueError):
            market_cap_b = None

        liquidity_score = 0
        if avg_dollar_volume >= 100_000_000:
            liquidity_score = 20
        elif avg_dollar_volume >= 50_000_000:
            liquidity_score = 16
        elif avg_dollar_volume >= 20_000_000:
            liquidity_score = 12
        elif avg_dollar_volume >= 10_000_000:
            liquidity_score = 8
        elif avg_dollar_volume >= 3_000_000:
            liquidity_score = 4

        trend_score = 0
        if current_price > ema_fast:
            trend_score += 4
        if current_price > ema_slow:
            trend_score += 6
        if current_price > ema_long:
            trend_score += 4
        if ema_fast > ema_slow:
            trend_score += 3
        if ema_slow > ema_long:
            trend_score += 3

        rs_score = 0
        if isStrongRs:
            rs_score = 20
        elif stockReturn > benchmarkReturn:
            rs_score = 12
        elif stockReturn > 0:
            rs_score = 6

        year_high_score = 0
        if isNearYearHigh:
            year_high_score = 15
        elif distanceFromHigh <= 0.10:
            year_high_score = 10
        elif distanceFromHigh <= 0.18:
            year_high_score = 5

        sector_score = 0
        if isSectorStrong:
            sector_score = 10
        elif sectorEtfNearHigh:
            sector_score = 5

        compression_score = 0
        if recent_range_pct is not None:
            if recent_range_pct <= 0.08:
                compression_score = 10
            elif recent_range_pct <= 0.12:
                compression_score = 7
            elif recent_range_pct <= 0.18:
                compression_score = 4

        mm_score = 0
        mm_trend = 'NA'
        mm_strength = 'NA'
        mm_position = 'NA'
        mm_raw_score = 0.0
        if memorySummary:
            mm_trend = memorySummary['memory_trend']
            mm_strength = memorySummary['memory_strength_label']
            mm_position = memorySummary['memory_price_position']
            mm_raw_score = float(memorySummary['memory_strength_score'])
            mm_score = min(20, max(0, mm_raw_score * 0.2))
            if mm_trend == 'bearish':
                mm_score = max(0, mm_score - 6)
            elif mm_trend == 'neutral':
                mm_score = max(0, mm_score - 2)
            if mm_position == 'below':
                mm_score = max(0, mm_score - 4)

        penalty = 0
        if distribution['distribution_warning']:
            penalty += min(15, 6 + int(distribution['distribution_days_count']) * 3)
        if market_cap_b is not None and market_cap_b < 2.0:
            penalty += 8

        focus_score = max(
            0,
            min(
                100,
                round(
                    liquidity_score + trend_score + rs_score + year_high_score +
                    sector_score + compression_score + mm_score - penalty,
                    2,
                ),
            ),
        )

        if focus_score >= a_threshold and not distribution['distribution_warning'] and mm_trend == 'bullish':
            focus_grade = 'A'
            trade_plan = 'pre_earnings_candidate'
        elif focus_score >= b_threshold:
            focus_grade = 'B'
            trade_plan = 'watch_only'
        elif mm_trend == 'bullish':
            focus_grade = 'C'
            trade_plan = 'post_earnings_only'
        else:
            focus_grade = 'C'
            trade_plan = 'avoid'

        reasons = []
        if isStrongRs:
            reasons.append('strong RS vs benchmark')
        elif stockReturn > benchmarkReturn:
            reasons.append('outperforming benchmark')
        if isNearYearHigh:
            reasons.append('near 52-week high')
        if current_price > ema_slow and ema_slow > ema_long:
            reasons.append('healthy 9/21/50 EMA stack')
        if recent_range_pct is not None and recent_range_pct <= 0.12:
            reasons.append('tight pre-earnings compression')
        if avg_dollar_volume >= 20_000_000:
            reasons.append('good liquidity')
        if distribution['distribution_warning']:
            reasons.append('recent distribution warning')
        if mm_trend == 'bearish':
            reasons.append('market memory bearish')
        elif mm_trend == 'bullish':
            reasons.append('market memory bullish')

        return {
            'event_date': str(event_date) if event_date else 'NA',
            'focus_score': focus_score,
            'focus_grade': focus_grade,
            'trade_plan': trade_plan,
            'current_price': current_price,
            'market_cap_b': market_cap_b,
            'avg_dollar_volume': avg_dollar_volume,
            'ema_fast_length': ema_fast_length,
            'ema_slow_length': ema_slow_length,
            'ema_long_length': ema_long_length,
            'ema_fast': float(ema_fast),
            'ema_slow': float(ema_slow),
            'ema_long': float(ema_long),
            'is_near_year_high': bool(isNearYearHigh),
            'year_high': float(yearHigh),
            'distance_from_year_high_pct': float(distanceFromHigh * 100.0),
            'is_strong_rs': bool(isStrongRs),
            'stock_return_vs_rs_window_pct': float(stockReturn * 100.0),
            'benchmark_return_vs_rs_window_pct': float(benchmarkReturn * 100.0),
            'current_rs_line': float(currentRsLine),
            'rs_line_high': float(rsLineHigh),
            'is_sector_etf_strong': bool(isSectorStrong),
            'sector_etf': sectorEtf or 'NA',
            'sector_etf_near_year_high': bool(sectorEtfNearHigh),
            'sector_etf_distance_from_year_high_pct': float(sectorEtfDistance * 100.0) if isinstance(sectorEtfDistance, (int, float)) else 'NA',
            'sector_etf_return_vs_rs_window_pct': float(sectorEtfReturn * 100.0) if isinstance(sectorEtfReturn, (int, float)) else 'NA',
            'sector_benchmark_return_vs_rs_window_pct': float(sectorBenchmarkReturn * 100.0) if isinstance(sectorBenchmarkReturn, (int, float)) else 'NA',
            'recent_range_pct': float(recent_range_pct * 100.0) if recent_range_pct is not None else 'NA',
            'distribution_warning': distribution['distribution_warning'],
            'distribution_days_count': distribution['distribution_days_count'],
            'latest_distribution_date': distribution['latest_distribution_date'],
            'latest_distribution_volume_ratio': distribution['latest_distribution_volume_ratio'],
            'market_memory_trend': mm_trend,
            'market_memory_strength_label': mm_strength,
            'market_memory_strength_score': mm_raw_score,
            'market_memory_price_position': mm_position,
            'reasons': reasons,
        }

    def get_htf_leader_summary(self, event_date=None, sectorName=None, benchmarkTicker=None):
        priceDataStruct = self._get_clean_price_data()
        if len(priceDataStruct) < 60:
            return None

        closes = np.asarray([item.get('close') or np.nan for item in priceDataStruct], dtype=float)
        current_price = float(closes[-1])
        runup_window = max(10, int(getattr(algoParas, 'HTF_RUNUP_WINDOW_DAYS', 40)))
        min_runup_pct = float(getattr(algoParas, 'HTF_MIN_RUNUP_PCT', 100.0))
        max_correction_pct = float(getattr(algoParas, 'HTF_MAX_CORRECTION_PCT', 25.0))
        ema_fast_length = max(1, int(getattr(algoParas, 'HTF_FAST_EMA', 9)))
        ema_slow_length = max(ema_fast_length, int(getattr(algoParas, 'HTF_SLOW_EMA', 21)))
        ema_long_length = max(ema_slow_length, int(getattr(algoParas, 'HTF_LONG_EMA', 50)))
        liquidity_lookback = max(5, int(getattr(algoParas, 'HTF_LIQUIDITY_LOOKBACK_DAYS', 20)))
        a_threshold = int(getattr(algoParas, 'HTF_A_SCORE_THRESHOLD', 75))
        b_threshold = int(getattr(algoParas, 'HTF_B_SCORE_THRESHOLD', 55))

        runup_summary = self._get_htf_runup_summary(runup_window)
        if not runup_summary:
            return None

        ema_fast = self._get_latest_ema_value(ema_fast_length)
        ema_slow = self._get_latest_ema_value(ema_slow_length)
        ema_long = self._get_latest_ema_value(ema_long_length)
        if None in (ema_fast, ema_slow, ema_long):
            return None

        benchmarkTicker = benchmarkTicker or algoParas.BENCHMARK_TICKER
        distribution = self._get_distribution_warning()
        isNearYearHigh, _, yearHigh, distanceFromHigh = self.is_near_year_high()
        isStrongRs, stockReturn, benchmarkReturn, currentRsLine, rsLineHigh = self.is_relative_strength_strong(benchmarkTicker)
        isSectorStrong, sectorEtf, sectorEtfNearHigh, _, _, sectorEtfDistance, sectorEtfReturn, sectorBenchmarkReturn = self.is_sector_etf_strong(sectorName, benchmarkTicker)
        avg_dollar_volume = self._get_average_dollar_volume(liquidity_lookback)
        market_cap_b = self.get_marketCap_B()
        try:
            market_cap_b = float(market_cap_b)
        except (TypeError, ValueError):
            market_cap_b = None

        runup_pct = float(runup_summary['runup_pct'])
        pullback_pct = float(runup_summary['pullback_from_high_pct'])
        hard_gate = runup_pct >= min_runup_pct and pullback_pct <= max_correction_pct
        if not hard_gate:
            return None

        liquidity_score = 0
        if avg_dollar_volume >= 100_000_000:
            liquidity_score = 15
        elif avg_dollar_volume >= 50_000_000:
            liquidity_score = 12
        elif avg_dollar_volume >= 20_000_000:
            liquidity_score = 9
        elif avg_dollar_volume >= 10_000_000:
            liquidity_score = 6
        elif avg_dollar_volume >= 3_000_000:
            liquidity_score = 3

        runup_score = 0
        if runup_pct >= 150:
            runup_score = 30
        elif runup_pct >= 125:
            runup_score = 25
        elif runup_pct >= 100:
            runup_score = 20

        correction_score = 0
        if pullback_pct <= 10:
            correction_score = 20
        elif pullback_pct <= 15:
            correction_score = 16
        elif pullback_pct <= 20:
            correction_score = 12
        elif pullback_pct <= 25:
            correction_score = 8

        trend_score = 0
        if current_price > ema_fast:
            trend_score += 5
        if current_price > ema_slow:
            trend_score += 7
        if current_price > ema_long:
            trend_score += 4
        if ema_fast > ema_slow:
            trend_score += 3
        if ema_slow > ema_long:
            trend_score += 3

        rs_score = 0
        if isStrongRs:
            rs_score = 15
        elif stockReturn > benchmarkReturn:
            rs_score = 8

        year_high_score = 0
        if isNearYearHigh:
            year_high_score = 10
        elif distanceFromHigh <= 0.10:
            year_high_score = 6
        elif distanceFromHigh <= 0.18:
            year_high_score = 3

        sector_score = 0
        if isSectorStrong:
            sector_score = 5
        elif sectorEtfNearHigh:
            sector_score = 3

        penalty = 0
        if distribution['distribution_warning']:
            penalty += min(12, 4 + int(distribution['distribution_days_count']) * 2)
        if market_cap_b is not None and market_cap_b < 2.0:
            penalty += 6
        if current_price < ema_slow:
            penalty += 6

        htf_score = max(
            0,
            min(
                100,
                round(
                    liquidity_score + runup_score + correction_score +
                    trend_score + rs_score + year_high_score +
                    sector_score - penalty,
                    2,
                ),
            ),
        )

        if htf_score >= a_threshold and pullback_pct <= 15 and not distribution['distribution_warning']:
            htf_grade = 'A'
            trade_plan = 'htf_good_entry'
        elif htf_score >= b_threshold:
            htf_grade = 'B'
            trade_plan = 'htf_wait_pullback'
        else:
            htf_grade = 'C'
            trade_plan = 'htf_watch_only'

        reasons = []
        reasons.append(f'8w runup {runup_pct:.1f}%')
        reasons.append(f'pullback {pullback_pct:.1f}% from 8w high')
        if isStrongRs:
            reasons.append('strong RS vs benchmark')
        if isNearYearHigh:
            reasons.append('near 52-week high')
        if current_price > ema_slow and ema_slow > ema_long:
            reasons.append('trend above key EMAs')
        if distribution['distribution_warning']:
            reasons.append('recent distribution warning')
        if sectorEtf:
            reasons.append(f'sector ETF {sectorEtf}')

        return {
            'event_date': str(event_date) if event_date else 'NA',
            'htf_score': htf_score,
            'htf_grade': htf_grade,
            'trade_plan': trade_plan,
            'htf_runup_window_days': runup_window,
            'htf_min_runup_pct': min_runup_pct,
            'htf_max_correction_pct': max_correction_pct,
            'htf_runup_pct': runup_pct,
            'htf_pullback_from_high_pct': pullback_pct,
            'htf_runup_low': float(runup_summary['runup_low']),
            'htf_runup_high': float(runup_summary['runup_high']),
            'htf_runup_low_date': runup_summary['runup_low_date'] or 'NA',
            'htf_runup_high_date': runup_summary['runup_high_date'] or 'NA',
            'current_price': current_price,
            'market_cap_b': market_cap_b,
            'avg_dollar_volume': avg_dollar_volume,
            'ema_fast_length': ema_fast_length,
            'ema_slow_length': ema_slow_length,
            'ema_long_length': ema_long_length,
            'ema_fast': float(ema_fast),
            'ema_slow': float(ema_slow),
            'ema_long': float(ema_long),
            'is_near_year_high': bool(isNearYearHigh),
            'year_high': float(yearHigh),
            'distance_from_year_high_pct': float(distanceFromHigh * 100.0),
            'is_strong_rs': bool(isStrongRs),
            'stock_return_vs_rs_window_pct': float(stockReturn * 100.0),
            'benchmark_return_vs_rs_window_pct': float(benchmarkReturn * 100.0),
            'current_rs_line': float(currentRsLine),
            'rs_line_high': float(rsLineHigh),
            'is_sector_etf_strong': bool(isSectorStrong),
            'sector_etf': sectorEtf or 'NA',
            'sector_etf_near_year_high': bool(sectorEtfNearHigh),
            'sector_etf_distance_from_year_high_pct': float(sectorEtfDistance * 100.0) if isinstance(sectorEtfDistance, (int, float)) else 'NA',
            'sector_etf_return_vs_rs_window_pct': float(sectorEtfReturn * 100.0) if isinstance(sectorEtfReturn, (int, float)) else 'NA',
            'sector_benchmark_return_vs_rs_window_pct': float(sectorBenchmarkReturn * 100.0) if isinstance(sectorBenchmarkReturn, (int, float)) else 'NA',
            'distribution_warning': distribution['distribution_warning'],
            'distribution_days_count': distribution['distribution_days_count'],
            'latest_distribution_date': distribution['latest_distribution_date'],
            'latest_distribution_volume_ratio': distribution['latest_distribution_volume_ratio'],
            'reasons': reasons,
        }

    def find_recent_power_earnings_gap(self):
        priceDataStruct = self._get_clean_price_data()
        if len(priceDataStruct) < 60:
            return None

        require_earnings_event = bool(getattr(algoParas, 'PEG_REQUIRE_EARNINGS_EVENT', True))
        earningsDates = self._get_earnings_event_dates() if require_earnings_event else []

        lookback = max(1, int(algoParas.PEG_LOOKBACK_DAYS))
        tolerance_days = max(0, int(algoParas.PEG_EARNINGS_TOLERANCE_DAYS))
        currentPrice = priceDataStruct[-1]['close']
        startIndex = max(1, len(priceDataStruct) - lookback)
        surprise_map = get_recent_earnings_surprise_map(lookback + tolerance_days + 5)
        provider_name = getattr(algoParas, 'EARNINGS_SURPRISE_PROVIDER', 'auto').lower()
        provider_enabled = provider_name not in ('none', 'disabled', '') and bool(surprise_map)

        candidates = []
        for idx in range(startIndex, len(priceDataStruct)):
            bar = priceDataStruct[idx]
            prevBar = priceDataStruct[idx - 1]
            open_p = bar.get('open')
            high_p = bar.get('high')
            low_p = bar.get('low')
            close_p = bar.get('close')
            prev_close = prevBar.get('close')
            volume = bar.get('volume')
            bar_date = bar.get('formatted_date')

            if None in (open_p, high_p, low_p, close_p, prev_close, volume):
                continue
            if prev_close <= 0:
                continue
            if require_earnings_event and earningsDates and not self._is_near_earnings_event(bar_date, earningsDates, tolerance_days):
                continue

            earnings_surprise = None
            if provider_enabled:
                earnings_surprise = self._get_matching_earnings_surprise(bar_date, tolerance_days)
                if earnings_surprise is None:
                    continue
                if float(earnings_surprise['surprise_pct']) < float(algoParas.PEG_MIN_EPS_SURPRISE_PCT):
                    continue
            if algoParas.PEG_REQUIRE_GREEN_CANDLE and close_p <= open_p:
                continue

            open_gap_pct = (open_p - prev_close) / prev_close
            close_gap_pct = (close_p - prev_close) / prev_close
            if open_gap_pct <= 0:
                continue
            if close_gap_pct < float(algoParas.PEG_MIN_GAP_PCT):
                continue

            # For a valid PEG, the up-gap should remain open intraday.
            if low_p <= prev_close:
                continue

            candle_range = high_p - low_p
            if candle_range <= 0:
                continue
            close_position_ratio = (close_p - low_p) / candle_range
            if close_position_ratio < float(algoParas.PEG_MIN_CLOSE_POSITION_RATIO):
                continue

            avg_volume = self._get_average_volume_before_index(idx, 50)
            if avg_volume <= 0:
                continue
            volume_ratio = volume / avg_volume
            if volume_ratio < float(algoParas.PEG_MIN_VOLUME_RATIO):
                continue

            setup_type = 'peg'
            if (
                close_gap_pct >= float(algoParas.PEG_MONSTER_GAP_PCT)
                and volume_ratio >= float(algoParas.PEG_MONSTER_VOLUME_RATIO)
            ):
                setup_type = 'monster_peg' if earnings_surprise is not None else 'monster'

            peg_low = low_p
            if currentPrice is None or currentPrice < peg_low:
                continue
            entry_distance_pct = (currentPrice - peg_low) / peg_low if peg_low > 0 else None
            if entry_distance_pct is None:
                continue
            max_entry_distance_pct = float(algoParas.PEG_MAX_ENTRY_DISTANCE_PCT)
            if max_entry_distance_pct > 0 and entry_distance_pct > max_entry_distance_pct:
                continue

            candidates.append({
                'setup_type': setup_type,
                'peg_date': bar_date,
                'peg_open': open_p,
                'peg_high': high_p,
                'peg_low': peg_low,
                'peg_close': close_p,
                'previous_close': prev_close,
                'gap_pct': close_gap_pct,
                'open_gap_pct': open_gap_pct,
                'volume_ratio': volume_ratio,
                'close_position_ratio': close_position_ratio,
                'entry_distance_pct': entry_distance_pct,
                'current_price': currentPrice,
                'hvc': close_p,
                'hvc5': close_p * 0.95,
                'gdh': high_p,
                'gdl': low_p,
                'earnings_actual_eps': earnings_surprise['actual_eps'] if earnings_surprise else None,
                'earnings_estimated_eps': earnings_surprise['estimated_eps'] if earnings_surprise else None,
                'earnings_surprise_pct': earnings_surprise['surprise_pct'] if earnings_surprise else None,
            })

        if not candidates:
            return None

        candidates.sort(key=lambda item: item['peg_date'], reverse=True)
        return candidates[0]

    def _resolve_benchmark_ticker(self, benchmarkTicker=None):
        return (benchmarkTicker or self.benchmark_ticker or algoParas.BENCHMARK_TICKER).upper()

    def _get_benchmark_price_data(self, benchmarkTicker=None):
        ticker = self._resolve_benchmark_ticker(benchmarkTicker)
        cache_key = (ticker, int(getattr(self, 'history_lookback_days', 365)))
        if cache_key in self.benchmark_price_cache:
            return self.benchmark_price_cache[cache_key]
        date = dt.date.today()
        start_dt = date - dt.timedelta(days=getattr(self, 'history_lookback_days', 365))
        period1 = int(dt.datetime.combine(start_dt, dt.time.min).replace(tzinfo=dt.timezone.utc).timestamp())
        period2 = int((dt.datetime.combine(date, dt.time.min) + dt.timedelta(days=1)).replace(tzinfo=dt.timezone.utc).timestamp())
        payload = _fetch_yahoo_chart_json(
            symbol=ticker,
            period1=period1,
            period2=period2,
            interval="1d",
            events="div,splits,earn",
        )
        result = payload.get("chart", {}).get("result", [])
        chart = result[0] if result else {}
        timestamps = chart.get("timestamp", []) or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        prices = []
        for idx, ts in enumerate(timestamps):
            close_value = closes[idx] if idx < len(closes) else None
            if close_value is None:
                continue
            prices.append({
                "date": ts,
                "formatted_date": dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d"),
                "open": opens[idx] if idx < len(opens) else None,
                "high": highs[idx] if idx < len(highs) else None,
                "low": lows[idx] if idx < len(lows) else None,
                "close": close_value,
                "adjclose": close_value,
                "volume": volumes[idx] if idx < len(volumes) and volumes[idx] is not None else 0,
            })
        cleanPrices = [item for item in prices if item.get('close') is not None]
        self.benchmark_price_cache[cache_key] = cleanPrices
        return cleanPrices

    def _evaluate_year_high(self, priceDataStruct):
        if not priceDataStruct:
            return False, 0, 0, 0
        trailingPrices = priceDataStruct[-252:] if len(priceDataStruct) >= 252 else priceDataStruct
        closePrices = [item['close'] for item in trailingPrices]
        yearHigh = np.max(closePrices)
        currentPrice = trailingPrices[-1]['close']
        distanceFromHigh = 0 if yearHigh == 0 else (yearHigh - currentPrice) / yearHigh
        flag = currentPrice >= yearHigh * (1 - algoParas.YEAR_HIGH_PROXIMITY)
        return flag, currentPrice, yearHigh, distanceFromHigh

    def _evaluate_relative_strength(self, assetData, benchmarkData):
        if not assetData or not benchmarkData:
            return False, 0, 0, 0, 0

        assetByDate = {item['formatted_date']: item['close'] for item in assetData}
        benchmarkByDate = {item['formatted_date']: item['close'] for item in benchmarkData}
        sharedDates = sorted(set(assetByDate).intersection(benchmarkByDate))
        if len(sharedDates) < 2:
            return False, 0, 0, 0, 0

        lookbackWindow = min(algoParas.RS_LOOKBACK_DAYS + 1, len(sharedDates))
        windowDates = sharedDates[-lookbackWindow:]
        assetWindow = [assetByDate[item] for item in windowDates]
        benchmarkWindow = [benchmarkByDate[item] for item in windowDates]
        if assetWindow[0] == 0 or benchmarkWindow[0] == 0 or benchmarkWindow[-1] == 0:
            return False, 0, 0, 0, 0

        assetReturn = assetWindow[-1] / assetWindow[0] - 1
        benchmarkReturn = benchmarkWindow[-1] / benchmarkWindow[0] - 1
        rsLineSeries = [assetClose / benchmarkClose for assetClose, benchmarkClose in zip(assetWindow, benchmarkWindow) if benchmarkClose]
        if not rsLineSeries:
            return False, assetReturn, benchmarkReturn, 0, 0

        currentRsLine = rsLineSeries[-1]
        rsLineHigh = np.max(rsLineSeries)
        flag = (
            assetReturn > 0
            and assetReturn > benchmarkReturn
            and currentRsLine >= rsLineHigh * algoParas.RS_LINE_NEAR_HIGH_RATIO
        )
        return flag, assetReturn, benchmarkReturn, currentRsLine, rsLineHigh

    def _build_price_dataframe(self, priceDataStruct):
        rows = []
        for item in priceDataStruct:
            formatted_date = item.get('formatted_date')
            close_p = item.get('close')
            if not formatted_date or close_p is None:
                continue
            try:
                bar_date = pd.to_datetime(formatted_date)
            except Exception:
                continue
            rows.append({
                'date': bar_date,
                'close': float(close_p),
                'high': float(item.get('high')) if item.get('high') is not None else float(close_p),
            })
        if not rows:
            return pd.DataFrame(columns=['close', 'high'])
        frame = pd.DataFrame(rows).drop_duplicates(subset=['date']).set_index('date').sort_index()
        return frame[['close', 'high']]

    def _compute_rs_new_high_flags(self, rs_line, price_reference, lookback):
        if rs_line.empty or price_reference.empty:
            empty = pd.Series(dtype=bool)
            return empty, empty
        aligned = pd.concat([rs_line, price_reference], axis=1, join='inner').dropna()
        if aligned.empty:
            empty = pd.Series(dtype=bool)
            return empty, empty
        aligned.columns = ['rs_line', 'price_reference']
        rolling_rs_high = aligned['rs_line'].rolling(window=max(1, int(lookback)), min_periods=1).max()
        rolling_price_high = aligned['price_reference'].rolling(window=max(1, int(lookback)), min_periods=1).max()
        tolerance = 1e-12
        new_high = aligned['rs_line'] >= (rolling_rs_high - tolerance)
        new_high_before_price = new_high & (aligned['price_reference'] < (rolling_price_high - tolerance))
        return (
            new_high.reindex(rs_line.index, fill_value=False),
            new_high_before_price.reindex(rs_line.index, fill_value=False),
        )

    def get_rs_new_high_before_price_summary(self, sectorName=None, benchmarkTicker=None):
        stockData = self._get_clean_price_data()
        benchmarkData = self._get_benchmark_price_data(benchmarkTicker)
        if len(stockData) < 60 or len(benchmarkData) < 60:
            return None

        stockFrame = self._build_price_dataframe(stockData)
        benchmarkFrame = self._build_price_dataframe(benchmarkData)
        if stockFrame.empty or benchmarkFrame.empty:
            return None

        aligned = stockFrame.join(
            benchmarkFrame[['close']].rename(columns={'close': 'benchmark_close'}),
            how='inner',
        ).dropna()
        if len(aligned) < 60:
            return None

        aligned['rs_line'] = aligned['close'] / aligned['benchmark_close']
        daily_lookback = max(20, int(getattr(algoParas, 'RS_NEW_HIGH_DAILY_LOOKBACK_DAYS', 250)))
        weekly_lookback = max(10, int(getattr(algoParas, 'RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS', 52)))
        daily_new_high, daily_new_high_before_price = self._compute_rs_new_high_flags(
            rs_line=aligned['rs_line'],
            price_reference=aligned['high'],
            lookback=daily_lookback,
        )

        weekly_stock = aligned[['close', 'high']].resample('W-FRI').agg({'close': 'last', 'high': 'max'}).dropna()
        weekly_benchmark = aligned[['benchmark_close']].resample('W-FRI').agg({'benchmark_close': 'last'}).dropna()
        weekly_aligned = weekly_stock.join(weekly_benchmark, how='inner').dropna()
        weekly_rs_line = weekly_aligned['close'] / weekly_aligned['benchmark_close'] if not weekly_aligned.empty else pd.Series(dtype=float)
        weekly_new_high, weekly_new_high_before_price = self._compute_rs_new_high_flags(
            rs_line=weekly_rs_line,
            price_reference=weekly_aligned['high'] if not weekly_aligned.empty else pd.Series(dtype=float),
            lookback=weekly_lookback,
        )

        latest_date = aligned.index[-1]
        latest_daily_new_high = bool(daily_new_high.iloc[-1]) if not daily_new_high.empty else False
        latest_daily_new_high_before_price = bool(daily_new_high_before_price.iloc[-1]) if not daily_new_high_before_price.empty else False
        latest_weekly_new_high = bool(weekly_new_high.iloc[-1]) if not weekly_new_high.empty else False
        latest_weekly_new_high_before_price = bool(weekly_new_high_before_price.iloc[-1]) if not weekly_new_high_before_price.empty else False
        require_before_price = bool(getattr(algoParas, 'RS_NEW_HIGH_REQUIRE_BEFORE_PRICE', True))
        triggered = latest_daily_new_high_before_price if require_before_price else latest_daily_new_high
        if not triggered:
            return None

        isNearYearHigh, currentPrice, yearHigh, distanceFromHigh = self._evaluate_year_high(stockData)
        isStrongRs, stockReturn, benchmarkReturn, currentRsLine, rsLineHigh = self._evaluate_relative_strength(stockData, benchmarkData)
        isSectorStrong, sectorEtf, sectorEtfNearHigh, _, _, sectorEtfDistance, sectorEtfReturn, sectorBenchmarkReturn = self.is_sector_etf_strong(sectorName, benchmarkTicker)

        reasons = []
        reasons.append('daily RS new high before price' if latest_daily_new_high_before_price else 'daily RS new high')
        if latest_weekly_new_high_before_price:
            reasons.append('weekly RS new high before price')
        elif latest_weekly_new_high:
            reasons.append('weekly RS new high')
        if isNearYearHigh:
            reasons.append('near 52-week high')
        if isStrongRs:
            reasons.append('strong RS window performance')
        if isSectorStrong:
            reasons.append(f'sector ETF {sectorEtf} strong')

        return {
            'signal_date': latest_date.strftime('%Y-%m-%d'),
            'benchmark_ticker': self._resolve_benchmark_ticker(benchmarkTicker),
            'current_price': float(aligned['close'].iloc[-1]),
            'current_high': float(aligned['high'].iloc[-1]),
            'current_rs_line': float(aligned['rs_line'].iloc[-1]),
            'daily_rs_line_high': float(aligned['rs_line'].rolling(window=daily_lookback, min_periods=1).max().iloc[-1]),
            'daily_price_high': float(aligned['high'].rolling(window=daily_lookback, min_periods=1).max().iloc[-1]),
            'daily_lookback_days': daily_lookback,
            'weekly_lookback_weeks': weekly_lookback,
            'daily_rs_new_high': latest_daily_new_high,
            'daily_rs_new_high_before_price': latest_daily_new_high_before_price,
            'weekly_rs_new_high': latest_weekly_new_high,
            'weekly_rs_new_high_before_price': latest_weekly_new_high_before_price,
            'require_before_price': require_before_price,
            'is_near_year_high': bool(isNearYearHigh),
            'year_high': float(yearHigh),
            'distance_from_year_high_pct': float(distanceFromHigh * 100.0),
            'is_strong_rs': bool(isStrongRs),
            'stock_return_vs_rs_window_pct': float(stockReturn * 100.0),
            'benchmark_return_vs_rs_window_pct': float(benchmarkReturn * 100.0),
            'rs_line_high': float(rsLineHigh),
            'is_sector_etf_strong': bool(isSectorStrong),
            'sector_etf': sectorEtf or 'NA',
            'sector_etf_near_year_high': bool(sectorEtfNearHigh),
            'sector_etf_distance_from_year_high_pct': float(sectorEtfDistance * 100.0) if isinstance(sectorEtfDistance, (int, float)) else 'NA',
            'sector_etf_return_vs_rs_window_pct': float(sectorEtfReturn * 100.0) if isinstance(sectorEtfReturn, (int, float)) else 'NA',
            'sector_benchmark_return_vs_rs_window_pct': float(sectorBenchmarkReturn * 100.0) if isinstance(sectorBenchmarkReturn, (int, float)) else 'NA',
            'reasons': reasons,
        }

    def get_sector_etf(self, sectorName):
        if not sectorName:
            return None
        return algoParas.SECTOR_ETF_MAP.get(sectorName)

    def is_near_year_high(self):
        priceDataStruct = self._get_clean_price_data()
        return self._evaluate_year_high(priceDataStruct)

    def is_relative_strength_strong(self, benchmarkTicker=None):
        stockData = self._get_clean_price_data()
        benchmarkData = self._get_benchmark_price_data(benchmarkTicker)
        return self._evaluate_relative_strength(stockData, benchmarkData)

    def is_sector_etf_strong(self, sectorName=None, benchmarkTicker=None):
        sectorEtf = self.get_sector_etf(sectorName)
        if not sectorEtf:
            return True, '', False, 0, 0, 0, 0, 0

        sectorData = self._get_benchmark_price_data(sectorEtf)
        benchmarkData = self._get_benchmark_price_data(benchmarkTicker)
        isStrongRs, sectorReturn, benchmarkReturn, currentRsLine, rsLineHigh = self._evaluate_relative_strength(sectorData, benchmarkData)
        isNearHigh, currentPrice, yearHigh, distanceFromHigh = self._evaluate_year_high(sectorData)
        flag = isStrongRs and isNearHigh
        return flag, sectorEtf, isNearHigh, currentPrice, yearHigh, distanceFromHigh, sectorReturn, benchmarkReturn
    
    def get_highest_in5days(self, startDate):
        priceData = []
        priceDataStruct = self.get_price(startDate, 5)
        tmpLen = len(priceDataStruct)
        # v2 uses bar 'high' (true intraday high) instead of close.
        use_high = _is_v2()
        for i in range(tmpLen):
            value = None
            if use_high:
                value = priceDataStruct[i].get('high')
            if value is None:
                value = priceDataStruct[i].get('close')
            priceData.append(value)
        if not(priceData):
            return [-1, -1]
        else:
            highestPrice = np.max(priceData)
            ind = np.argmax(priceData)
        return highestPrice, priceDataStruct[ind]['formatted_date']

    def get_lowest_in5days(self, startDate):
        priceData = []
        priceDataStruct = self.get_price(startDate, 5)
        tmpLen = len(priceDataStruct)
        # v2 uses bar 'low' (true intraday low) instead of close.
        use_low = _is_v2()
        for i in range(tmpLen):
            value = None
            if use_low:
                value = priceDataStruct[i].get('low')
            if value is None:
                value = priceDataStruct[i].get('close')
            priceData.append(value)
        if not(priceData):
            return [-1, -1]
        else:
            lowestPrice = np.min(priceData)
            ind = np.argmin(priceData)
        return lowestPrice, priceDataStruct[ind]['formatted_date']
        
    def _find_one_contraction_v2(self, startDate):
        """Trading-bar implementation of find_one_contraction.

        Walks the actual price bars (no calendar-day arithmetic) and uses bar
        'high' for the local-high search and bar 'low' for the local-low
        search. Lock-in threshold counts trading days, not calendar days.
        """
        if isinstance(startDate, str):
            try:
                startDate = dt.datetime.strptime(startDate, "%Y-%m-%d").date()
            except ValueError:
                return False, -1, -1, -1, -1

        if not self.priceData:
            today = dt.date.today()
            self.priceData = self.get_historical_price_data(
                str(today - dt.timedelta(days=365)), str(today), 'daily')

        bars = [
            b for b in self.priceData[self.ticker]['prices']
            if b.get('close') is not None
        ]
        if not bars:
            return False, -1, -1, -1, -1

        # First bar at or after startDate.
        start_str = str(startDate)
        start_idx = None
        for i, b in enumerate(bars):
            if b.get('formatted_date', '') >= start_str:
                start_idx = i
                break
        if start_idx is None or start_idx >= len(bars):
            return False, -1, -1, -1, -1

        counterThr = 5

        # Locate local high using bar 'high'.
        localHighestPrice = -float('inf')
        localHighestIdx = -1
        counter = 0
        last_idx = len(bars) - 1
        for i in range(start_idx, len(bars)):
            price = bars[i].get('high')
            if price is None:
                price = bars[i].get('close')
            if price is None:
                continue
            if price > localHighestPrice:
                localHighestPrice = price
                localHighestIdx = i
                counter = 0
            else:
                counter += 1
            if counter >= counterThr:
                break
        # If we never confirmed a local high (no 5-bar streak below it),
        # the contraction has not resolved.
        if counter < counterThr or localHighestIdx < 0:
            return False, -1, -1, -1, -1

        # Locate local low after the high using bar 'low'.
        localLowestPrice = float('inf')
        localLowestIdx = -1
        counter2 = 0
        for j in range(localHighestIdx, len(bars)):
            price = bars[j].get('low')
            if price is None:
                price = bars[j].get('close')
            if price is None:
                continue
            if price < localLowestPrice:
                localLowestPrice = price
                localLowestIdx = j
                counter2 = 0
            else:
                counter2 += 1
            if counter2 >= counterThr or j == last_idx:
                break

        if localLowestIdx < 0 or localHighestPrice == localLowestPrice:
            return False, -1, -1, -1, -1

        return (
            True,
            bars[localHighestIdx]['formatted_date'],
            localHighestPrice,
            bars[localLowestIdx]['formatted_date'],
            localLowestPrice,
        )

    def find_one_contraction(self, startDate):
        if _is_v2():
            return self._find_one_contraction_v2(startDate)
        print('start searching date')
        print(startDate)
        date = dt.date.today()
        tmp = date - startDate
        numOfDate = tmp.days
        localHighestPrice = -float('inf')
        localHighestDate = -1
        counter = 0
        counterThr = 5
        flag = True
        for i in range(numOfDate):
            movingDate = startDate + dt.timedelta(i)
            #print(movingDate)
            price, priceDate = self.get_highest_in5days(movingDate)
            if price == -1 and priceDate == -1:
                flag = False
                return flag, -1, -1, -1, -1
            if price > localHighestPrice:
                localHighestPrice = price
                localHighestDate = priceDate
                counter = 0
            else:
                counter = counter + 1
                print('start lock the date')
                print(priceDate)
            if counter >= counterThr or i == numOfDate-1:
                #get local high
                print('find the local highest price')
                print(localHighestPrice)
                print('date is')
                print(localHighestDate)
                break
        if counter < counterThr:
            flag = False
            return flag, -1, -1, -1, -1
            
        #search for local low
        if counter >= counterThr:
            print('start search for lowest price')
            tmp_dt = dt.datetime.strptime(localHighestDate, "%Y-%m-%d")
            localHighestDate_dt = tmp_dt.date()
            tmp = date - localHighestDate_dt
            numOfDate2 = tmp.days
            startDate2 = localHighestDate_dt
            localLowestPrice = float('inf')
            localLowestDate = -1
            counter2 = 0
            for j in range(numOfDate2):
                movingDate2 = startDate2 + dt.timedelta(j)
                price, priceDate = self.get_lowest_in5days(movingDate2)
                if price == -1 and priceDate == -1:
                    break
                if price < localLowestPrice:
                    localLowestPrice = price
                    localLowestDate = priceDate
                    counter2 = 0
                else:
                    counter2 = counter2 + 1
                    print('start lock the date')
                    print(priceDate)
                if counter2 >= counterThr or j == numOfDate2-1:
                    #get local high
                    print('find the local lowest price')
                    print(localLowestPrice)
                    print('date is')
                    print(localLowestDate)
                    break
                
        #
        if localHighestPrice == localLowestPrice:
            return False, -1, -1, -1, -1
        return flag, localHighestDate, localHighestPrice, localLowestDate, localLowestPrice
                
    def find_volatility_contraction_pattern(self, startDate):
        """
        Finds all contraction patterns starting from the given date.
        Returns a tuple: (count, recordVCP).
        """
        MAX_ITERATIONS = 1000
        recordVCP = []
        self.m_recordVCP = []
        counterForVCP = 0

        while counterForVCP < MAX_ITERATIONS:
            flagForOneContraction, hD, hP, lD, lP = self.find_one_contraction(startDate)

            if not flagForOneContraction:
                break

            recordVCP.append([hD, hP, lD, lP])
            counterForVCP += 1
            startDate = dt.datetime.strptime(lD, "%Y-%m-%d").date()

        self.m_recordVCP = recordVCP
        return counterForVCP, recordVCP
            
    
    def get_footPrint(self):
        flag = False
        if not(self.m_recordVCP):
            date_from = (dt.date.today() - dt.timedelta(days=60))
            self.find_volatility_contraction_pattern(date_from)
        length = len(self.m_recordVCP)
        self.m_footPrint=[]
        for i in range(length):            
            self.m_footPrint.append([self.m_recordVCP[i][0], self.m_recordVCP[i][2], (self.m_recordVCP[i][1]-self.m_recordVCP[i][3])/self.m_recordVCP[i][1]])
        return self.m_footPrint
    
    def is_pivot_good(self):
        flag = False
        if not(self.m_footPrint):
            self.get_footPrint()
        if not self.m_footPrint or not self.m_recordVCP:
            return flag, -1, -1, -1
        #correction within 10% of max price and current price higher then lower boundary
        if not self.current_stickerPrice:
            current = self.get_current_price()
        else:
            current = self.current_stickerPrice
        finalContraction = self.m_footPrint[-1][2]
        support = self.m_recordVCP[-1][3]
        pivot = self.m_recordVCP[-1][1]
        isTight = finalContraction <= algoParas.FINAL_CONTRACTION_MAX
        isAbovePivot = current > pivot
        isNotExtended = current <= pivot * (1 + algoParas.PIVOT_EXTENSION_RATIO)
        flag = isTight and isAbovePivot and isNotExtended and current > support
        #report support and pressure
        print(self.ticker + ' current price: ' + str(current))
        print(self.ticker + ' support price: ' + str(support))
        print(self.ticker + ' pressure price: ' + str(pivot))
        return flag, current, support, pivot
    
    def is_correction_deep(self):
        flag = False
        if not(self.m_footPrint):
            self.get_footPrint()
        if not self.m_footPrint:
            return False
        tmp = np.asarray(self.m_footPrint)
        tmpcorrection = tmp[:, 2]
        correction = tmpcorrection.astype(float)
        if _is_v2():
            # In a healthy VCP the FIRST contraction defines how much damage
            # the stock took; later contractions should be tighter, not deeper.
            # Flag the base as too deep when the first contraction exceeds the
            # configured maximum (default 35%).
            return float(correction[0]) > algoParas.FIRST_CONTRACTION_MAX
        return correction.max() >= 0.5

    def is_vcp_structure_valid(self):
        """v2 structural check: contraction depths must monotonically decrease,
        and the first (oldest) contraction must not exceed FIRST_CONTRACTION_MAX.

        This enforces the core Minervini-style VCP rule that each successive
        base is tighter than the previous one. A small tolerance allows for
        minor noise in the depth measurement."""
        if not self.m_footPrint:
            self.get_footPrint()
        if not self.m_footPrint:
            return False
        depths = [float(fp[2]) for fp in self.m_footPrint]
        if not depths:
            return False
        if depths[0] > algoParas.FIRST_CONTRACTION_MAX:
            return False
        # Allow ~1% slack so floating point / measurement noise doesn't kill
        # an otherwise clean structure.
        slack = 0.01
        for i in range(1, len(depths)):
            if depths[i] >= depths[i - 1] - slack:
                return False
        return True

    #check the last contraction, is the demand dry

    def is_demand_dry(self):
        if not self.m_footPrint:
            self.get_footPrint()
        if not self.m_footPrint:
            return False, -1, -1, [], 0, 0, -1, -1, [], 0, 0

        # Get the date range from the last footprint entry
        startDate = self.m_footPrint[-1][0]
        endDate = self.m_footPrint[-1][1]
        startDate_dt = dt.datetime.strptime(startDate, "%Y-%m-%d")
        endDate_dt = dt.datetime.strptime(endDate, "%Y-%m-%d")

        # Load price data if not already loaded
        if not self.priceData:
            date = dt.date.today()
            self.priceData = self.get_historical_price_data(str(date - dt.timedelta(days=365)), str(date), 'daily')

        # Fetch volumes for the specific period in the footprint
        priceDataStruct = self.priceData[self.ticker]['prices']
        footprintVolume = self._extract_volume_for_period(priceDataStruct, startDate_dt.date(), endDate_dt.date())
        if len(footprintVolume) < 2:
            return False, startDate, endDate, footprintVolume, 0, 0, -1, -1, [], 0, 0

        # Calculate the volume trend using linear regression
        slope, intercept = self._calculate_volume_trend(footprintVolume)
        
        # Recent-volume regression window: 4 bars in v1 (legacy), bumped to
        # DEMAND_DRY_RECENT_DAYS (default 7) in v2 so the slope is meaningful.
        recent_window = (
            max(5, int(algoParas.DEMAND_DRY_RECENT_DAYS)) if _is_v2() else 4
        )
        recentData = priceDataStruct[-recent_window:]
        if len(recentData) < 2:
            return False, startDate, endDate, footprintVolume, slope, intercept, -1, -1, [], 0, 0
        recentStartDate = recentData[0]['formatted_date']
        recentEndDate = recentData[-1]['formatted_date']
        recentVolume = [item['volume'] for item in recentData]
        slopeRecent, interceptRecent = self._calculate_volume_trend(recentVolume)
        slopeRecentPrice, _ = self._calculate_volume_trend([item['close'] for item in recentData])
        recentAvgVolume = np.mean(recentVolume) if recentVolume else 0
        baseAvgVolume = np.mean(footprintVolume) if footprintVolume else 0
        historicalAvgVolume = self._get_average_volume(50)

        # Determine if demand is dry based on slope and volume comparison
        isDry = (
            slope <= 0
            and recentAvgVolume <= baseAvgVolume
            and recentAvgVolume <= historicalAvgVolume
        )
        # exclude the case that the volume is going up slopeRecent is going up and price is going down, which means selling pressure is increasing
        if slopeRecent > 0 and slopeRecentPrice < 0:
            isDry = False
        return isDry, startDate, endDate, footprintVolume, slope, intercept, recentStartDate, recentEndDate, recentVolume, slopeRecent, interceptRecent

    def _extract_volume_for_period(self, priceDataStruct, start_date, end_date):
        """Extracts volume data for a specified period from price data."""
        selected_data = self.get_price_from_buffer_start_end(priceDataStruct, start_date, end_date)
        return [item['volume'] for item in selected_data]

    def _calculate_volume_trend(self, volume_list):
        """Performs linear regression to determine volume trend."""
        x = np.arange(len(volume_list))
        y = np.array(volume_list)
        slope, intercept = np.polyfit(x, y, 1)
        return slope, intercept

    def _calculate_historical_average_volume(self, priceDataStruct, days):
        """Calculates the average volume over the last 'days' period."""
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=days)
        historical_data = self.get_price_from_buffer_start_end(priceDataStruct, start_date, end_date)
        volume_list = [item['volume'] for item in historical_data]
        return np.mean(volume_list) if volume_list else 0
    
    
    def combined_best_strategy(self, sectorName=None, benchmarkTicker=None, screenProfile=None):
        profile = (screenProfile or algoParas.SCREEN_PROFILE or 'strict').lower()

        if _is_v2():
            return self._combined_best_strategy_v2(sectorName, benchmarkTicker, profile)

        # ----- v1 (legacy) path: preserved bit-for-bit -----
        # Check moving average strategy
        s = True
        if self.mv_strategy() != 1:
            s = s and False

        # Check volume strategy
        if self.vol_strategy() != 1:
            s = s and False

        # Check price strategy
        if self.price_strategy() != 1:
            s = s and False

        # Original/default profile used before the stricter RS, year-high, sector ETF,
        # contraction-count, and breakout-volume confirmations were added.
        if profile == 'legacy':
            isGoodPivot, _, _, _ = self.is_pivot_good()
            if not isGoodPivot:
                s = s and False

            if self.is_correction_deep():
                s = s and False

            isDemandDry, _, _, _, _, _, _, _, _, _, _ = self.is_demand_dry()
            if not isDemandDry:
                s = s and False

            return s

        isNearYearHigh, _, _, _ = self.is_near_year_high()
        if not isNearYearHigh:
            s = s and False

        isStrongRs, _, _, _, _ = self.is_relative_strength_strong(benchmarkTicker)
        if not isStrongRs:
            s = s and False

        isSectorStrong, _, _, _, _, _, _, _ = self.is_sector_etf_strong(sectorName, benchmarkTicker)
        if not isSectorStrong:
            s = s and False

        counterForVCP, _ = self.find_volatility_contraction_pattern(dt.date.today() - dt.timedelta(days=100))
        if counterForVCP < algoParas.MIN_VCP_CONTRACTIONS:
            s = s and False

        # Check if the stock is near a good pivot point
        isGoodPivot, currentPrice, supportPrice, resistancePrice = self.is_pivot_good()
        if not isGoodPivot:
            s = s and False

        # Check if recent correction is not too deep
        if self.is_correction_deep():
            s = s and False

        # Check if demand is drying up (selling pressure has decreased)
        isDemandDry, startDate, endDate, volume_ls, slope, intercept, _, _, _, _, _ = self.is_demand_dry()
        if not isDemandDry:
            s = s and False

        isBreakoutVolume, _, _ = self.is_breakout_volume_confirmed()
        if not isBreakoutVolume:
            s = s and False

        # All criteria met, return True for a strong buy signal
        return s

    def _combined_best_strategy_v2(self, sectorName, benchmarkTicker, profile):
        """v2: short-circuited screener that bails on first failed check.

        Order is cheapest-first so we avoid expensive HTTP / regression work
        when an earlier check has already disqualified the ticker.
        """
        if self.mv_strategy() != 1:
            return False
        if self.vol_strategy() != 1:
            return False
        if self.price_strategy() != 1:
            return False

        if profile == 'legacy':
            isGoodPivot, _, _, _ = self.is_pivot_good()
            if not isGoodPivot:
                return False
            if self.is_correction_deep():
                return False
            isDemandDry = self.is_demand_dry()[0]
            if not isDemandDry:
                return False
            return True

        isNearYearHigh = self.is_near_year_high()[0]
        if not isNearYearHigh:
            return False

        isStrongRs = self.is_relative_strength_strong(benchmarkTicker)[0]
        if not isStrongRs:
            return False

        isSectorStrong = self.is_sector_etf_strong(sectorName, benchmarkTicker)[0]
        if not isSectorStrong:
            return False

        counterForVCP, _ = self.find_volatility_contraction_pattern(
            dt.date.today() - dt.timedelta(days=100)
        )
        if counterForVCP < algoParas.MIN_VCP_CONTRACTIONS:
            return False

        # v2-only: enforce the structural VCP rule (monotonically tightening
        # contractions; first contraction not too deep).
        if not self.is_vcp_structure_valid():
            return False

        isGoodPivot = self.is_pivot_good()[0]
        if not isGoodPivot:
            return False

        if self.is_correction_deep():
            return False

        isDemandDry = self.is_demand_dry()[0]
        if not isDemandDry:
            return False

        isBreakoutVolume = self.is_breakout_volume_confirmed()[0]
        if not isBreakoutVolume:
            return False

        return True



class batch_process:
    tickers = []
    resultsPath = ''
    result_file = ''
    sector_by_ticker = {}
    earnings_event_by_ticker = {}
    benchmark_ticker = algoParas.BENCHMARK_TICKER
    screen_profile = algoParas.SCREEN_PROFILE
    shared_benchmark_cache_warmed = False
    
    def __init__(self, tickers, sectors, sector_by_ticker=None, benchmark_ticker=None, screen_profile=None, earnings_event_by_ticker=None):
        self.tickers = tickers
        self.sector_by_ticker = sector_by_ticker or {}
        self.earnings_event_by_ticker = earnings_event_by_ticker or {}
        self.benchmark_ticker = benchmark_ticker or algoParas.BENCHMARK_TICKER
        self.screen_profile = (screen_profile or algoParas.SCREEN_PROFILE or 'strict').lower()
        self.shared_benchmark_cache_warmed = False
        basePath = find_path()
        current_date = dt.date.today().strftime('%Y-%m-%d')
        self.resultsPath = os.path.join(basePath, 'results', current_date)
        file = sectors + '.json'
        self.result_file = setup_result_file(self.resultsPath, file)

    def _get_parallel_workers(self):
        return max(1, int(algoParas.MAX_WORKERS))

    def _get_ticker_timeout_seconds(self):
        return max(0, int(algoParas.TICKER_TIMEOUT_SECONDS))

    def _format_progress_bar(self, completed, total, width=24):
        total = max(1, int(total))
        completed = max(0, min(int(completed), total))
        filled = int(width * completed / total)
        return "[" + "#" * filled + "-" * (width - filled) + "]"

    def _print_progress(self, completed, total, ticker, status, passed_count):
        percent = (max(0, min(completed, total)) / max(1, total)) * 100
        bar = self._format_progress_bar(completed, total)
        print(
            f"PROGRESS {bar} {completed}/{total} ({percent:5.1f}%) "
            f"| passed={passed_count} | {ticker} | {status}"
        )

    def _warm_shared_benchmark_cache(self, history_days):
        cache_key = (self.benchmark_ticker.upper(), int(history_days))
        if cache_key in cookFinancials.benchmark_price_cache:
            self.shared_benchmark_cache_warmed = True
            return

        print(
            f"warming benchmark cache for {self.benchmark_ticker.upper()} "
            f"({int(history_days)}d history)"
        )
        date = dt.date.today()
        start_dt = date - dt.timedelta(days=int(history_days))
        period1 = int(dt.datetime.combine(start_dt, dt.time.min).replace(tzinfo=dt.timezone.utc).timestamp())
        period2 = int((dt.datetime.combine(date, dt.time.min) + dt.timedelta(days=1)).replace(tzinfo=dt.timezone.utc).timestamp())
        payload = _fetch_yahoo_chart_json(
            symbol=self.benchmark_ticker.upper(),
            period1=period1,
            period2=period2,
            interval="1d",
            events="div,splits,earn",
        )
        result = payload.get("chart", {}).get("result", [])
        chart = result[0] if result else {}
        timestamps = chart.get("timestamp", []) or []
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])
        prices = []
        for idx, ts in enumerate(timestamps):
            close_value = closes[idx] if idx < len(closes) else None
            if close_value is None:
                continue
            prices.append({
                "date": ts,
                "formatted_date": dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d"),
                "open": opens[idx] if idx < len(opens) else None,
                "high": highs[idx] if idx < len(highs) else None,
                "low": lows[idx] if idx < len(lows) else None,
                "close": close_value,
                "adjclose": close_value,
                "volume": volumes[idx] if idx < len(volumes) and volumes[idx] is not None else 0,
            })
        cleanPrices = [item for item in prices if item.get('close') is not None]
        cookFinancials.benchmark_price_cache[cache_key] = cleanPrices
        self.shared_benchmark_cache_warmed = True
        print(f"benchmark cache ready: {self.benchmark_ticker.upper()} bars={len(cleanPrices)}")

    def _analyze_vcp_ticker_child(self, result_queue, ticker, date_from):
        try:
            ticker_data = self._analyze_vcp_ticker(ticker, date_from)
            result_queue.put({
                'status': 'ok',
                'ticker_data': ticker_data,
            })
        except Exception as exc:
            result_queue.put({
                'status': 'error',
                'error': repr(exc),
            })

    def _analyze_peg_ticker_child(self, result_queue, ticker, date_from):
        try:
            ticker_data = self._analyze_peg_ticker(ticker, date_from)
            result_queue.put({
                'status': 'ok',
                'ticker_data': ticker_data,
            })
        except Exception as exc:
            result_queue.put({
                'status': 'error',
                'error': repr(exc),
            })

    def _analyze_pre_earnings_ticker_child(self, result_queue, ticker, date_from):
        try:
            ticker_data = self._analyze_pre_earnings_ticker(ticker, date_from)
            result_queue.put({
                'status': 'ok',
                'ticker_data': ticker_data,
            })
        except Exception as exc:
            result_queue.put({
                'status': 'error',
                'error': repr(exc),
            })

    def _analyze_rsnhbp_ticker_child(self, result_queue, ticker, date_from):
        try:
            ticker_data = self._analyze_rsnhbp_ticker(ticker, date_from)
            result_queue.put({
                'status': 'ok',
                'ticker_data': ticker_data,
            })
        except Exception as exc:
            result_queue.put({
                'status': 'error',
                'error': repr(exc),
            })

    def _run_ticker_with_timeout(self, ticker, date_from):
        timeout_seconds = self._get_ticker_timeout_seconds()
        if timeout_seconds <= 0:
            return self._analyze_vcp_ticker(ticker, date_from)

        ctx = mp.get_context('spawn')
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=self._analyze_vcp_ticker_child,
            args=(result_queue, ticker, date_from),
        )
        process.start()
        process.join(timeout_seconds)

        if process.is_alive():
            print(f"timeout analyzing {ticker} after {timeout_seconds}s, skipping")
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            result_queue.close()
            result_queue.join_thread()
            return None

        try:
            payload = result_queue.get_nowait()
        except Empty:
            result_queue.close()
            result_queue.join_thread()
            if process.exitcode not in (0, None):
                raise RuntimeError(f"worker exited with code {process.exitcode}")
            return None

        result_queue.close()
        result_queue.join_thread()

        if payload.get('status') == 'error':
            raise RuntimeError(payload.get('error', 'unknown worker error'))
        return payload.get('ticker_data')

    def _run_peg_ticker_with_timeout(self, ticker, date_from):
        timeout_seconds = self._get_ticker_timeout_seconds()
        if timeout_seconds <= 0:
            return self._analyze_peg_ticker(ticker, date_from)

        ctx = mp.get_context('spawn')
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=self._analyze_peg_ticker_child,
            args=(result_queue, ticker, date_from),
        )
        process.start()
        process.join(timeout_seconds)

        if process.is_alive():
            print(f"timeout analyzing {ticker} after {timeout_seconds}s, skipping")
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            result_queue.close()
            result_queue.join_thread()
            return None

        try:
            payload = result_queue.get_nowait()
        except Empty:
            result_queue.close()
            result_queue.join_thread()
            if process.exitcode not in (0, None):
                raise RuntimeError(f"worker exited with code {process.exitcode}")
            return None

        result_queue.close()
        result_queue.join_thread()

        if payload.get('status') == 'error':
            raise RuntimeError(payload.get('error', 'unknown worker error'))
        return payload.get('ticker_data')

    def _run_pre_earnings_ticker_with_timeout(self, ticker, date_from):
        timeout_seconds = self._get_ticker_timeout_seconds()
        if timeout_seconds <= 0:
            return self._analyze_pre_earnings_ticker(ticker, date_from)

        ctx = mp.get_context('spawn')
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=self._analyze_pre_earnings_ticker_child,
            args=(result_queue, ticker, date_from),
        )
        process.start()
        process.join(timeout_seconds)

        if process.is_alive():
            print(f"timeout analyzing {ticker} after {timeout_seconds}s, skipping")
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            result_queue.close()
            result_queue.join_thread()
            return None

        try:
            payload = result_queue.get_nowait()
        except Empty:
            result_queue.close()
            result_queue.join_thread()
            if process.exitcode not in (0, None):
                raise RuntimeError(f"worker exited with code {process.exitcode}")
            return None

        result_queue.close()
        result_queue.join_thread()

        if payload.get('status') == 'error':
            raise RuntimeError(payload.get('error', 'unknown worker error'))
        return payload.get('ticker_data')

    def _run_rsnhbp_ticker_with_timeout(self, ticker, date_from):
        timeout_seconds = self._get_ticker_timeout_seconds()
        if timeout_seconds <= 0:
            return self._analyze_rsnhbp_ticker(ticker, date_from)

        ctx = mp.get_context('spawn')
        result_queue = ctx.Queue()
        process = ctx.Process(
            target=self._analyze_rsnhbp_ticker_child,
            args=(result_queue, ticker, date_from),
        )
        process.start()
        process.join(timeout_seconds)

        if process.is_alive():
            print(f"timeout analyzing {ticker} after {timeout_seconds}s, skipping")
            process.terminate()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            result_queue.close()
            result_queue.join_thread()
            return None

        try:
            payload = result_queue.get_nowait()
        except Empty:
            result_queue.close()
            result_queue.join_thread()
            if process.exitcode not in (0, None):
                raise RuntimeError(f"worker exited with code {process.exitcode}")
            return None

        result_queue.close()
        result_queue.join_thread()

        if payload.get('status') == 'error':
            raise RuntimeError(payload.get('error', 'unknown worker error'))
        return payload.get('ticker_data')

    def _analyze_vcp_ticker(self, ticker, date_from):
        print(ticker)
        tickerSector = self.sector_by_ticker.get(ticker)
        x = cookFinancials(
            ticker,
            benchmarkTicker=self.benchmark_ticker,
            historyLookbackDays=getattr(algoParas, 'PRE_EARNINGS_HISTORY_DAYS', 180),
        )
        flag = x.combined_best_strategy(
            sectorName=tickerSector,
            benchmarkTicker=self.benchmark_ticker,
            screenProfile=self.screen_profile,
        )
        if flag != True:
            return None

        print("congrats, this stock passes all strategys")
        sp = x.get_price(date_from, 100)
        if not sp:
            return None

        date = []
        price = []
        volume = []
        for item in sp:
            date.append(item['formatted_date'])
            price.append(item['close'])
            volume.append(item['volume'])

        fig, ax = plt.subplots(2)
        try:
            fig.suptitle(x.ticker)
            ax[0].plot(date, price, color="blue", marker="o")
            ax[0].set_xlabel("date", fontsize=14)
            ax[0].set_ylabel("stock price", color="blue", fontsize=14)
            ax[1].bar(date, np.asarray(volume) / 10**6, color="green")
            ax[1].set_ylabel("volume (m)", color="green", fontsize=14)

            xticks = np.arange(0, len(date), 10).tolist()
            if len(date) - 1 not in xticks:
                xticks.append(len(date) - 1)
            ax[0].set_xticks(xticks)
            ax[1].set_xticks(xticks)
            fig.autofmt_xdate(rotation=45)

            print(x.get_highest_in5days(date_from))
            counter, record = x.find_volatility_contraction_pattern(date_from)
            if counter <= 0:
                return None

            for idx in range(counter):
                ax[0].plot([record[idx][0], record[idx][2]], [record[idx][1], record[idx][3]], 'r')

            print('footprint:')
            footprint = x.get_footPrint()
            print(footprint)
            print('is a good pivot?')
            isGoodPivot, currentPrice, supportPrice, pressurePrice = x.is_pivot_good()
            print(isGoodPivot)
            print('is a deep correction?')
            isDeepCor = x.is_correction_deep()
            print(isDeepCor)
            print('is demand dried?')
            isDemandDry, startDate, endDate, volume_ls, slope, interY, recentStart, recentEnd, volume_re, slopeRecet, interYRecent = x.is_demand_dry()
            print(isDemandDry)
            isNearYearHigh = 'N/A'
            yearHigh = 'N/A'
            yearHighDistance = 'N/A'
            isStrongRs = 'N/A'
            stockReturn = 'N/A'
            benchmarkReturn = 'N/A'
            currentRsLine = 'N/A'
            rsLineHigh = 'N/A'
            isSectorStrong = 'N/A'
            sectorEtf = 'N/A'
            sectorEtfNearHigh = 'N/A'
            sectorEtfCurrent = 'N/A'
            sectorEtfYearHigh = 'N/A'
            sectorEtfDistance = 'N/A'
            sectorEtfReturn = 'N/A'
            sectorBenchmarkReturn = 'N/A'

            if self.screen_profile != 'legacy':
                print('is near 52-week high?')
                isNearYearHigh, _, yearHigh, yearHighDistance = x.is_near_year_high()
                print(isNearYearHigh)
                print('is relative strength strong?')
                isStrongRs, stockReturn, benchmarkReturn, currentRsLine, rsLineHigh = x.is_relative_strength_strong(self.benchmark_ticker)
                print(isStrongRs)
                print('is sector ETF strong?')
                isSectorStrong, sectorEtf, sectorEtfNearHigh, sectorEtfCurrent, sectorEtfYearHigh, sectorEtfDistance, sectorEtfReturn, sectorBenchmarkReturn = x.is_sector_etf_strong(tickerSector, self.benchmark_ticker)
                print(isSectorStrong)

            ticker_data = {
                ticker: {
                    'current price': str(currentPrice),
                    'support price': str(supportPrice),
                    'pressure price': str(pressurePrice),
                    'is_good_pivot': str(isGoodPivot),
                    'is_deep_correction': str(isDeepCor),
                    'is_demand_dry': str(isDemandDry),
                    'is_near_year_high': str(isNearYearHigh),
                    'year_high': str(yearHigh),
                    'distance_from_year_high': str(yearHighDistance),
                    'is_strong_rs': str(isStrongRs),
                    'benchmark_ticker': self.benchmark_ticker,
                    'stock_return_vs_rs_window': str(stockReturn),
                    'benchmark_return_vs_rs_window': str(benchmarkReturn),
                    'current_rs_line': str(currentRsLine),
                    'rs_line_high': str(rsLineHigh),
                    'sector_name': str(tickerSector),
                    'sector_etf': str(sectorEtf),
                    'is_sector_etf_strong': str(isSectorStrong),
                    'sector_etf_near_year_high': str(sectorEtfNearHigh),
                    'sector_etf_current_price': str(sectorEtfCurrent),
                    'sector_etf_year_high': str(sectorEtfYearHigh),
                    'sector_etf_distance_from_year_high': str(sectorEtfDistance),
                    'sector_etf_return_vs_rs_window': str(sectorEtfReturn),
                    'sector_benchmark_return_vs_rs_window': str(sectorBenchmarkReturn),
                    'screen_profile': self.screen_profile,
                }
            }

            for ind, item in enumerate(date):
                if item == startDate:
                    print(ind)
                    break
            x_axis = []
            for idx in range(len(volume_ls)):
                x_axis.append(ind + idx)
            x_axis = np.array(x_axis)
            y = slope * x_axis - slope * ind + volume_ls[0]
            ax[1].plot(np.asarray(date)[x_axis], y / 10**6, color="red", linewidth=4)

            for ind, item in enumerate(date):
                if item == recentStart:
                    print(ind)
                    break
            x_axis = []
            for idx in range(len(volume_re)):
                x_axis.append(ind + idx)
            x_axis = np.array(x_axis)
            yRecent = slopeRecet * x_axis - slopeRecet * ind + volume_re[0]
            ax[1].plot(np.asarray(date)[x_axis], yRecent / 10**6, color="red", linewidth=4)

            figName = os.path.join(self.resultsPath, ticker + '.jpg')
            fig.savefig(figName, format='jpeg', dpi=100, bbox_inches='tight')
            ticker_data[ticker]['fig'] = figName

            return ticker_data
        finally:
            plt.close(fig)

    def _analyze_peg_ticker(self, ticker, date_from):
        print(ticker)
        tickerSector = self.sector_by_ticker.get(ticker)
        x = cookFinancials(ticker, benchmarkTicker=self.benchmark_ticker)
        pegSetup = x.find_recent_power_earnings_gap()
        if not pegSetup:
            return None
        memorySummary = x.get_market_memory_summary()
        tradePlan = x.get_peg_trade_plan(pegSetup)

        sp = x.get_price(date_from, 100)
        if not sp:
            return None

        date = []
        price = []
        volume = []
        for item in sp:
            date.append(item['formatted_date'])
            price.append(item['close'])
            volume.append(item['volume'])

        fig, ax = plt.subplots(2)
        try:
            title = f"{x.ticker} {pegSetup['setup_type'].upper()} near entry"
            if memorySummary:
                title += f" | MM {memorySummary['memory_trend']} {memorySummary['memory_strength_label']}"
            fig.suptitle(title)
            ax[0].plot(date, price, color="blue", marker="o")
            ax[0].axhline(pegSetup['peg_low'], color="orange", linestyle="--", linewidth=2, label="PEG low")
            ax[0].axhline(pegSetup['hvc'], color="purple", linestyle="--", linewidth=1.5, label="HVC")
            ax[0].axhline(pegSetup['hvc5'], color="red", linestyle=":", linewidth=1.5, label="HVC -5%")
            ax[0].axhline(pegSetup['gdh'], color="teal", linestyle="-.", linewidth=1.2, label="GDH")
            if memorySummary:
                ax[0].axhline(
                    memorySummary['memory_average'],
                    color="black",
                    linestyle="-",
                    linewidth=1.2,
                    alpha=0.7,
                    label="Market Memory Avg",
                )
            if tradePlan:
                if tradePlan.get('secondary_entry_fast_ema') is not None:
                    ax[0].axhline(
                        tradePlan['secondary_entry_fast_ema'],
                        color="cyan",
                        linestyle="--",
                        linewidth=1.0,
                        alpha=0.7,
                        label=f"EMA {tradePlan['secondary_entry_fast_ema_length']}",
                    )
                if tradePlan.get('secondary_entry_slow_ema') is not None:
                    ax[0].axhline(
                        tradePlan['secondary_entry_slow_ema'],
                        color="gold",
                        linestyle="--",
                        linewidth=1.0,
                        alpha=0.7,
                        label=f"EMA {tradePlan['secondary_entry_slow_ema_length']}",
                    )
            if pegSetup['peg_date'] in date:
                peg_idx = date.index(pegSetup['peg_date'])
                ax[0].scatter(
                    [date[peg_idx]],
                    [price[peg_idx]],
                    color="red",
                    s=100,
                    zorder=5,
                    label="PEG candle",
                )
            if memorySummary:
                memory_text = (
                    f"MM trend: {memorySummary['memory_trend']}\n"
                    f"Strength: {memorySummary['memory_strength_label']} ({memorySummary['memory_strength_score']:.1f})\n"
                    f"Price vs MM: {memorySummary['memory_price_position']} ({memorySummary['memory_price_vs_average_pct']:.2f}%)"
                )
                if tradePlan:
                    secondary_zone = "n/a"
                    if tradePlan.get('secondary_entry_low') is not None and tradePlan.get('secondary_entry_high') is not None:
                        secondary_zone = (
                            f"{tradePlan['secondary_entry_low']:.2f}-{tradePlan['secondary_entry_high']:.2f}"
                        )
                    distribution_text = (
                        f"Dist warn: {'yes' if tradePlan['distribution_warning'] else 'no'}"
                    )
                    if tradePlan.get('latest_distribution_volume_ratio') is not None:
                        distribution_text += (
                            f" ({tradePlan['latest_distribution_volume_ratio']:.2f}x on "
                            f"{tradePlan['latest_distribution_date']})"
                        )
                    memory_text += (
                        f"\nPrimary: PEG low {tradePlan['primary_entry']:.2f}"
                        f"\nSecondary: {secondary_zone}"
                        f"\n{distribution_text}"
                    )
                ax[0].text(
                    0.02,
                    0.98,
                    memory_text,
                    transform=ax[0].transAxes,
                    va="top",
                    ha="left",
                    fontsize=10,
                    bbox=dict(boxstyle="round", facecolor="white", alpha=0.75),
                )
            ax[0].set_xlabel("date", fontsize=14)
            ax[0].set_ylabel("stock price", color="blue", fontsize=14)
            ax[0].legend(loc="best")
            ax[1].bar(date, np.asarray(volume) / 10**6, color="green")
            ax[1].set_ylabel("volume (m)", color="green", fontsize=14)

            xticks = np.arange(0, len(date), 10).tolist()
            if len(date) - 1 not in xticks:
                xticks.append(len(date) - 1)
            ax[0].set_xticks(xticks)
            ax[1].set_xticks(xticks)
            fig.autofmt_xdate(rotation=45)

            ticker_data = {
                ticker: {
                    'setup_type': str(pegSetup['setup_type']),
                    'peg_date': pegSetup['peg_date'],
                    'peg_low': str(round(float(pegSetup['peg_low']), 4)),
                    'peg_open': str(round(float(pegSetup['peg_open']), 4)),
                    'peg_high': str(round(float(pegSetup['peg_high']), 4)),
                    'peg_close': str(round(float(pegSetup['peg_close']), 4)),
                    'previous_close': str(round(float(pegSetup['previous_close']), 4)),
                    'gap_pct': str(round(float(pegSetup['gap_pct']), 6)),
                    'open_gap_pct': str(round(float(pegSetup['open_gap_pct']), 6)),
                    'volume_ratio': str(round(float(pegSetup['volume_ratio']), 4)),
                    'close_position_ratio': str(round(float(pegSetup['close_position_ratio']), 4)),
                    'entry_distance_pct': str(round(float(pegSetup['entry_distance_pct']), 6)),
                    'hvc': str(round(float(pegSetup['hvc']), 4)),
                    'hvc5': str(round(float(pegSetup['hvc5']), 4)),
                    'gdh': str(round(float(pegSetup['gdh']), 4)),
                    'gdl': str(round(float(pegSetup['gdl']), 4)),
                    'earnings_actual_eps': (
                        str(round(float(pegSetup['earnings_actual_eps']), 4))
                        if pegSetup.get('earnings_actual_eps') is not None else 'NA'
                    ),
                    'earnings_estimated_eps': (
                        str(round(float(pegSetup['earnings_estimated_eps']), 4))
                        if pegSetup.get('earnings_estimated_eps') is not None else 'NA'
                    ),
                    'earnings_surprise_pct': (
                        str(round(float(pegSetup['earnings_surprise_pct']), 4))
                        if pegSetup.get('earnings_surprise_pct') is not None else 'NA'
                    ),
                    'market_memory_trend': (
                        str(memorySummary['memory_trend']) if memorySummary else 'NA'
                    ),
                    'market_memory_strength_label': (
                        str(memorySummary['memory_strength_label']) if memorySummary else 'NA'
                    ),
                    'market_memory_strength_score': (
                        str(round(float(memorySummary['memory_strength_score']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_average': (
                        str(round(float(memorySummary['memory_average']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_slope_pct': (
                        str(round(float(memorySummary['memory_slope_pct']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_price_position': (
                        str(memorySummary['memory_price_position']) if memorySummary else 'NA'
                    ),
                    'market_memory_price_vs_average_pct': (
                        str(round(float(memorySummary['memory_price_vs_average_pct']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_similarity_score': (
                        str(round(float(memorySummary['memory_similarity_score']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_weighted_momentum_pct': (
                        str(round(float(memorySummary['memory_weighted_momentum_pct']), 4))
                        if memorySummary else 'NA'
                    ),
                    'market_memory_match_count': (
                        str(int(memorySummary['memory_match_count'])) if memorySummary else 'NA'
                    ),
                    'primary_entry_label': (
                        str(tradePlan['primary_entry_label']) if tradePlan else 'NA'
                    ),
                    'primary_entry': (
                        str(round(float(tradePlan['primary_entry']), 4)) if tradePlan else 'NA'
                    ),
                    'secondary_entry_label': (
                        str(tradePlan['secondary_entry_label']) if tradePlan else 'NA'
                    ),
                    'secondary_entry_fast_ema_length': (
                        str(int(tradePlan['secondary_entry_fast_ema_length'])) if tradePlan else 'NA'
                    ),
                    'secondary_entry_slow_ema_length': (
                        str(int(tradePlan['secondary_entry_slow_ema_length'])) if tradePlan else 'NA'
                    ),
                    'secondary_entry_fast_ema': (
                        str(round(float(tradePlan['secondary_entry_fast_ema']), 4))
                        if tradePlan and tradePlan.get('secondary_entry_fast_ema') is not None else 'NA'
                    ),
                    'secondary_entry_slow_ema': (
                        str(round(float(tradePlan['secondary_entry_slow_ema']), 4))
                        if tradePlan and tradePlan.get('secondary_entry_slow_ema') is not None else 'NA'
                    ),
                    'secondary_entry_low': (
                        str(round(float(tradePlan['secondary_entry_low']), 4))
                        if tradePlan and tradePlan.get('secondary_entry_low') is not None else 'NA'
                    ),
                    'secondary_entry_high': (
                        str(round(float(tradePlan['secondary_entry_high']), 4))
                        if tradePlan and tradePlan.get('secondary_entry_high') is not None else 'NA'
                    ),
                    'distribution_warning': (
                        str(tradePlan['distribution_warning']) if tradePlan else 'NA'
                    ),
                    'distribution_days_count': (
                        str(int(tradePlan['distribution_days_count'])) if tradePlan else 'NA'
                    ),
                    'latest_distribution_date': (
                        str(tradePlan['latest_distribution_date']) if tradePlan and tradePlan.get('latest_distribution_date') else 'NA'
                    ),
                    'latest_distribution_volume_ratio': (
                        str(round(float(tradePlan['latest_distribution_volume_ratio']), 4))
                        if tradePlan and tradePlan.get('latest_distribution_volume_ratio') is not None else 'NA'
                    ),
                    'distribution_volume_ratio_threshold': (
                        str(round(float(tradePlan['distribution_volume_ratio_threshold']), 4)) if tradePlan else 'NA'
                    ),
                    'current price': str(round(float(pegSetup['current_price']), 4)),
                    'benchmark_ticker': self.benchmark_ticker,
                    'sector_name': str(tickerSector),
                }
            }

            figName = os.path.join(self.resultsPath, ticker + '_peg.jpg')
            fig.savefig(figName, format='jpeg', dpi=100, bbox_inches='tight')
            ticker_data[ticker]['fig'] = figName
            return ticker_data
        finally:
            plt.close(fig)

    def _analyze_pre_earnings_ticker(self, ticker, date_from):
        print(ticker)
        tickerSector = self.sector_by_ticker.get(ticker)
        event_date = self.earnings_event_by_ticker.get(ticker)
        x = cookFinancials(ticker, benchmarkTicker=self.benchmark_ticker)
        summary = x.get_pre_earnings_focus_summary(
            event_date=event_date,
            sectorName=tickerSector,
            benchmarkTicker=self.benchmark_ticker,
        )
        if not summary:
            return None
        return {
            ticker: {
                'earnings_date': str(summary['event_date']),
                'earnings_focus_score': str(round(float(summary['focus_score']), 4)),
                'earnings_focus_grade': str(summary['focus_grade']),
                'earnings_trade_plan': str(summary['trade_plan']),
                'current_price': str(round(float(summary['current_price']), 4)),
                'market_cap_b': str(round(float(summary['market_cap_b']), 4)) if summary.get('market_cap_b') is not None else 'NA',
                'avg_dollar_volume': str(round(float(summary['avg_dollar_volume']), 4)),
                'ema_fast_length': str(int(summary['ema_fast_length'])),
                'ema_slow_length': str(int(summary['ema_slow_length'])),
                'ema_long_length': str(int(summary['ema_long_length'])),
                'ema_fast': str(round(float(summary['ema_fast']), 4)),
                'ema_slow': str(round(float(summary['ema_slow']), 4)),
                'ema_long': str(round(float(summary['ema_long']), 4)),
                'is_near_year_high': str(summary['is_near_year_high']),
                'year_high': str(round(float(summary['year_high']), 4)),
                'distance_from_year_high_pct': str(round(float(summary['distance_from_year_high_pct']), 4)),
                'is_strong_rs': str(summary['is_strong_rs']),
                'stock_return_vs_rs_window_pct': str(round(float(summary['stock_return_vs_rs_window_pct']), 4)),
                'benchmark_return_vs_rs_window_pct': str(round(float(summary['benchmark_return_vs_rs_window_pct']), 4)),
                'current_rs_line': str(round(float(summary['current_rs_line']), 4)),
                'rs_line_high': str(round(float(summary['rs_line_high']), 4)),
                'is_sector_etf_strong': str(summary['is_sector_etf_strong']),
                'sector_etf': str(summary['sector_etf']),
                'sector_etf_near_year_high': str(summary['sector_etf_near_year_high']),
                'sector_etf_distance_from_year_high_pct': str(summary['sector_etf_distance_from_year_high_pct']),
                'sector_etf_return_vs_rs_window_pct': str(summary['sector_etf_return_vs_rs_window_pct']),
                'sector_benchmark_return_vs_rs_window_pct': str(summary['sector_benchmark_return_vs_rs_window_pct']),
                'recent_range_pct': str(summary['recent_range_pct']),
                'distribution_warning': str(summary['distribution_warning']),
                'distribution_days_count': str(int(summary['distribution_days_count'])),
                'latest_distribution_date': str(summary['latest_distribution_date']) if summary.get('latest_distribution_date') else 'NA',
                'latest_distribution_volume_ratio': str(round(float(summary['latest_distribution_volume_ratio']), 4)) if summary.get('latest_distribution_volume_ratio') is not None else 'NA',
                'market_memory_trend': str(summary['market_memory_trend']),
                'market_memory_strength_label': str(summary['market_memory_strength_label']),
                'market_memory_strength_score': str(round(float(summary['market_memory_strength_score']), 4)),
                'market_memory_price_position': str(summary['market_memory_price_position']),
                'focus_reasons': " | ".join(summary.get('reasons', [])),
                'benchmark_ticker': self.benchmark_ticker,
                'sector_name': str(tickerSector),
            }
        }

    def _analyze_htf_ticker(self, ticker, date_from):
        print(ticker)
        tickerSector = self.sector_by_ticker.get(ticker)
        event_date = self.earnings_event_by_ticker.get(ticker)
        history_days = max(90, int(getattr(algoParas, 'HTF_HISTORY_DAYS', 180)))
        x = cookFinancials(
            ticker,
            benchmarkTicker=self.benchmark_ticker,
            historyLookbackDays=history_days,
        )
        summary = x.get_htf_leader_summary(
            event_date=event_date,
            sectorName=tickerSector,
            benchmarkTicker=self.benchmark_ticker,
        )
        if not summary:
            return None
        return {
            ticker: {
                'earnings_date': str(summary['event_date']),
                'htf_score': str(round(float(summary['htf_score']), 4)),
                'htf_grade': str(summary['htf_grade']),
                'htf_trade_plan': str(summary['trade_plan']),
                'htf_runup_window_days': str(int(summary['htf_runup_window_days'])),
                'htf_min_runup_pct': str(round(float(summary['htf_min_runup_pct']), 4)),
                'htf_max_correction_pct': str(round(float(summary['htf_max_correction_pct']), 4)),
                'htf_runup_pct': str(round(float(summary['htf_runup_pct']), 4)),
                'htf_pullback_from_high_pct': str(round(float(summary['htf_pullback_from_high_pct']), 4)),
                'htf_runup_low': str(round(float(summary['htf_runup_low']), 4)),
                'htf_runup_high': str(round(float(summary['htf_runup_high']), 4)),
                'htf_runup_low_date': str(summary['htf_runup_low_date']),
                'htf_runup_high_date': str(summary['htf_runup_high_date']),
                'current_price': str(round(float(summary['current_price']), 4)),
                'market_cap_b': str(round(float(summary['market_cap_b']), 4)) if summary.get('market_cap_b') is not None else 'NA',
                'avg_dollar_volume': str(round(float(summary['avg_dollar_volume']), 4)),
                'ema_fast_length': str(int(summary['ema_fast_length'])),
                'ema_slow_length': str(int(summary['ema_slow_length'])),
                'ema_long_length': str(int(summary['ema_long_length'])),
                'ema_fast': str(round(float(summary['ema_fast']), 4)),
                'ema_slow': str(round(float(summary['ema_slow']), 4)),
                'ema_long': str(round(float(summary['ema_long']), 4)),
                'is_near_year_high': str(summary['is_near_year_high']),
                'year_high': str(round(float(summary['year_high']), 4)),
                'distance_from_year_high_pct': str(round(float(summary['distance_from_year_high_pct']), 4)),
                'is_strong_rs': str(summary['is_strong_rs']),
                'stock_return_vs_rs_window_pct': str(round(float(summary['stock_return_vs_rs_window_pct']), 4)),
                'benchmark_return_vs_rs_window_pct': str(round(float(summary['benchmark_return_vs_rs_window_pct']), 4)),
                'current_rs_line': str(round(float(summary['current_rs_line']), 4)),
                'rs_line_high': str(round(float(summary['rs_line_high']), 4)),
                'is_sector_etf_strong': str(summary['is_sector_etf_strong']),
                'sector_etf': str(summary['sector_etf']),
                'sector_etf_near_year_high': str(summary['sector_etf_near_year_high']),
                'sector_etf_distance_from_year_high_pct': str(summary['sector_etf_distance_from_year_high_pct']),
                'sector_etf_return_vs_rs_window_pct': str(summary['sector_etf_return_vs_rs_window_pct']),
                'sector_benchmark_return_vs_rs_window_pct': str(summary['sector_benchmark_return_vs_rs_window_pct']),
                'distribution_warning': str(summary['distribution_warning']),
                'distribution_days_count': str(int(summary['distribution_days_count'])),
                'latest_distribution_date': str(summary['latest_distribution_date']) if summary.get('latest_distribution_date') else 'NA',
                'latest_distribution_volume_ratio': str(round(float(summary['latest_distribution_volume_ratio']), 4)) if summary.get('latest_distribution_volume_ratio') is not None else 'NA',
                'htf_reasons': " | ".join(summary.get('reasons', [])),
                'benchmark_ticker': self.benchmark_ticker,
                'sector_name': str(tickerSector),
            }
        }

    def _analyze_rsnhbp_ticker(self, ticker, date_from):
        print(ticker)
        tickerSector = self.sector_by_ticker.get(ticker)
        history_days = max(260, int(getattr(algoParas, 'RS_NEW_HIGH_HISTORY_DAYS', 400)))
        x = cookFinancials(
            ticker,
            benchmarkTicker=self.benchmark_ticker,
            historyLookbackDays=history_days,
        )
        summary = x.get_rs_new_high_before_price_summary(
            sectorName=tickerSector,
            benchmarkTicker=self.benchmark_ticker,
        )
        if not summary:
            return None
        return {
            ticker: {
                'signal_date': str(summary['signal_date']),
                'benchmark_ticker': str(summary['benchmark_ticker']),
                'current_price': str(round(float(summary['current_price']), 4)),
                'current_high': str(round(float(summary['current_high']), 4)),
                'current_rs_line': str(round(float(summary['current_rs_line']), 6)),
                'daily_rs_line_high': str(round(float(summary['daily_rs_line_high']), 6)),
                'daily_price_high': str(round(float(summary['daily_price_high']), 4)),
                'daily_lookback_days': str(int(summary['daily_lookback_days'])),
                'weekly_lookback_weeks': str(int(summary['weekly_lookback_weeks'])),
                'daily_rs_new_high': str(summary['daily_rs_new_high']),
                'daily_rs_new_high_before_price': str(summary['daily_rs_new_high_before_price']),
                'weekly_rs_new_high': str(summary['weekly_rs_new_high']),
                'weekly_rs_new_high_before_price': str(summary['weekly_rs_new_high_before_price']),
                'require_before_price': str(summary['require_before_price']),
                'is_near_year_high': str(summary['is_near_year_high']),
                'year_high': str(round(float(summary['year_high']), 4)),
                'distance_from_year_high_pct': str(round(float(summary['distance_from_year_high_pct']), 4)),
                'is_strong_rs': str(summary['is_strong_rs']),
                'stock_return_vs_rs_window_pct': str(round(float(summary['stock_return_vs_rs_window_pct']), 4)),
                'benchmark_return_vs_rs_window_pct': str(round(float(summary['benchmark_return_vs_rs_window_pct']), 4)),
                'rs_line_high': str(round(float(summary['rs_line_high']), 6)),
                'is_sector_etf_strong': str(summary['is_sector_etf_strong']),
                'sector_etf': str(summary['sector_etf']),
                'sector_etf_near_year_high': str(summary['sector_etf_near_year_high']),
                'sector_etf_distance_from_year_high_pct': str(summary['sector_etf_distance_from_year_high_pct']),
                'sector_etf_return_vs_rs_window_pct': str(summary['sector_etf_return_vs_rs_window_pct']),
                'sector_benchmark_return_vs_rs_window_pct': str(summary['sector_benchmark_return_vs_rs_window_pct']),
                'signal_reasons': " | ".join(summary.get('reasons', [])),
                'sector_name': str(tickerSector),
            }
        }

    def _persist_analysis_results(self, ticker_data):
        if ticker_data:
            append_to_json(self.result_file, ticker_data)
            
    def batch_strategy(self):
        superStock=[]
        for i in range(np.size(self.tickers)):
            try:
                print(self.tickers[i])
                x = cookFinancials(self.tickers[i])
                s1=0
                s2=0
                s3=0
                if x.mv_strategy()==1:
                    s1 = 1
                    print("passing moving average strategy")
                if x.vol_strategy() == 1: #not from original book, not working
                    s2 = 1
                    print("passing 3 day volume strategy")
                if x.price_strategy() == 1:
                    s3 = 1
                    print("passing price strategy")
                #if s1==1 and s2==1 and s3==1:
                if s1==1 and s3==1 and s2:
                    print("congrats, this stock passes all strategys, run volatility contraction pattern")
                    superStock.append(self.tickers[i])    
                append_to_json(self.result_file, self.tickers[i])
            except Exception:
                print("error!")
                pass
        
            
    def batch_pipeline_full(self):
        date_from = (dt.date.today() - dt.timedelta(days=100))
        total_tickers = len(self.tickers)
        completed = 0
        passed_count = 0
        ticker_timeout_seconds = self._get_ticker_timeout_seconds()
        use_parallel = (
            algoParas.PARALLEL_ENABLED
            and len(self.tickers) > 1
            and self._get_parallel_workers() > 1
            and ticker_timeout_seconds <= 0
        )

        print(
            f"starting batch pipeline: total={total_tickers}, "
            f"mode={'parallel' if use_parallel else 'sequential'}, "
            f"screen_profile={self.screen_profile}"
        )

        if algoParas.PARALLEL_ENABLED and ticker_timeout_seconds > 0:
            print(
                "ticker timeout is enabled; running sequential isolated workers "
                "instead of thread pool"
            )

        if use_parallel:
            with ThreadPoolExecutor(max_workers=self._get_parallel_workers()) as executor:
                future_map = {
                    executor.submit(self._analyze_vcp_ticker, ticker, date_from): ticker
                    for ticker in self.tickers
                }
                for future in as_completed(future_map):
                    ticker = future_map[future]
                    try:
                        ticker_data = future.result()
                        self._persist_analysis_results(ticker_data)
                        completed += 1
                        if ticker_data:
                            passed_count += 1
                        status = "passed" if ticker_data else "filtered"
                        self._print_progress(completed, total_tickers, ticker, status, passed_count)
                    except Exception as exc:
                        completed += 1
                        self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                        print(f"error analyzing {ticker}: {exc}")
        else:
            for ticker in self.tickers:
                try:
                    self._print_progress(completed, total_tickers, ticker, "running", passed_count)
                    ticker_data = self._run_ticker_with_timeout(ticker, date_from)
                    self._persist_analysis_results(ticker_data)
                    completed += 1
                    if ticker_data:
                        passed_count += 1
                    status = "passed" if ticker_data else "filtered"
                    self._print_progress(completed, total_tickers, ticker, status, passed_count)
                except Exception as exc:
                    completed += 1
                    self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                    print(f"error analyzing {ticker}: {exc}")

    def batch_pipeline_peg(self):
        date_from = (dt.date.today() - dt.timedelta(days=120))
        total_tickers = len(self.tickers)
        completed = 0
        passed_count = 0

        print(
            f"starting PEG batch pipeline: total={total_tickers}, "
            f"lookback={algoParas.PEG_LOOKBACK_DAYS}, "
            f"entry_distance_max={algoParas.PEG_MAX_ENTRY_DISTANCE_PCT}"
        )

        for ticker in self.tickers:
            try:
                self._print_progress(completed, total_tickers, ticker, "running", passed_count)
                ticker_data = self._run_peg_ticker_with_timeout(ticker, date_from)
                self._persist_analysis_results(ticker_data)
                completed += 1
                if ticker_data:
                    passed_count += 1
                status = "passed" if ticker_data else "filtered"
                self._print_progress(completed, total_tickers, ticker, status, passed_count)
            except Exception as exc:
                completed += 1
                self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                print(f"error analyzing {ticker}: {exc}")

    def batch_pipeline_pre_earnings(self):
        date_from = (dt.date.today() - dt.timedelta(days=180))
        total_tickers = len(self.tickers)
        completed = 0
        passed_count = 0

        print(
            f"starting pre-earnings batch pipeline: total={total_tickers}, "
            f"next_week={min(self.earnings_event_by_ticker.values()) if self.earnings_event_by_ticker else 'NA'}"
        )

        for ticker in self.tickers:
            try:
                self._print_progress(completed, total_tickers, ticker, "running", passed_count)
                ticker_data = self._analyze_pre_earnings_ticker(ticker, date_from)
                self._persist_analysis_results(ticker_data)
                completed += 1
                if ticker_data:
                    passed_count += 1
                status = "scored" if ticker_data else "filtered"
                self._print_progress(completed, total_tickers, ticker, status, passed_count)
            except Exception as exc:
                completed += 1
                self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                print(f"error analyzing {ticker}: {exc}")

    def batch_pipeline_htf(self):
        history_days = max(90, int(getattr(algoParas, 'HTF_HISTORY_DAYS', 180)))
        date_from = (dt.date.today() - dt.timedelta(days=history_days))
        total_tickers = len(self.tickers)
        completed = 0
        passed_count = 0

        print(
            f"starting HTF leader pipeline: total={total_tickers}, "
            f"window={algoParas.HTF_RUNUP_WINDOW_DAYS}, "
            f"min_runup={algoParas.HTF_MIN_RUNUP_PCT}%, "
            f"max_pullback={algoParas.HTF_MAX_CORRECTION_PCT}%"
        )

        for ticker in self.tickers:
            try:
                self._print_progress(completed, total_tickers, ticker, "running", passed_count)
                ticker_data = self._analyze_htf_ticker(ticker, date_from)
                self._persist_analysis_results(ticker_data)
                completed += 1
                if ticker_data:
                    passed_count += 1
                status = "passed" if ticker_data else "filtered"
                self._print_progress(completed, total_tickers, ticker, status, passed_count)
            except Exception as exc:
                completed += 1
                self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                print(f"error analyzing {ticker}: {exc}")

    def batch_pipeline_rsnhbp(self):
        history_days = max(260, int(getattr(algoParas, 'RS_NEW_HIGH_HISTORY_DAYS', 400)))
        date_from = (dt.date.today() - dt.timedelta(days=history_days))
        total_tickers = len(self.tickers)
        completed = 0
        passed_count = 0

        print(
            f"starting RS new-high-before-price pipeline: total={total_tickers}, "
            f"daily_lookback={algoParas.RS_NEW_HIGH_DAILY_LOOKBACK_DAYS}, "
            f"weekly_lookback={algoParas.RS_NEW_HIGH_WEEKLY_LOOKBACK_WEEKS}, "
            f"require_before_price={algoParas.RS_NEW_HIGH_REQUIRE_BEFORE_PRICE}"
        )

        self._warm_shared_benchmark_cache(history_days)

        for ticker in self.tickers:
            try:
                self._print_progress(completed, total_tickers, ticker, "running", passed_count)
                ticker_data = self._analyze_rsnhbp_ticker(ticker, date_from)
                self._persist_analysis_results(ticker_data)
                completed += 1
                if ticker_data:
                    passed_count += 1
                status = "passed" if ticker_data else "filtered"
                self._print_progress(completed, total_tickers, ticker, status, passed_count)
            except Exception as exc:
                completed += 1
                self._print_progress(completed, total_tickers, ticker, f"error: {exc}", passed_count)
                print(f"error analyzing {ticker}: {exc}")

            
    def batch_financial(self):       
        for i in range(np.size(self.tickers)):
            try:
                print(self.tickers[i])
                x = cookFinancials(self.tickers[i])
                bv = x.get_BV(20)
                bv.insert(0, x.get_book_value())
                print(bv)
                bvgr = x.get_BV_GR_median(bv)
                print(bvgr)
                growth = bvgr[1]
                cEPS = x.get_earnings_per_share()
                print(cEPS)
                years = 3;
                rRate = 0.25;
                safty = 0.5
                PE = x.get_PE()
                price=x.get_suggest_price(cEPS, growth, years, rRate, PE, safty)
                print(price)
                stickerPrice = x.current_stickerPrice
                decision = x.get_decision(price[1],stickerPrice)
                print(decision)
                y2pb = 0
                roic = 0
                mcap = 0
                cashflow = 0
                priceSales = 0
                if decision == 'strong buy':
                    y2pb = x.payBackTime(stickerPrice, cEPS, growth)
                    roic = x.get_ROIC()
                    mcap = x.get_marketCap_B()
                    cashflow = (x.get_totalCashFromOperatingActivities())
                    priceSales = x.get_pricetoSales()               
                s = {
                    self.tickers[i]:{
                        "decision":decision,
                        "suggested price":price[1],
                        "stock price":stickerPrice,                     
                        "Payback years": y2pb,
                        "Book Value": bv,
                        "ROIC": roic,
                        "market cap (b)": mcap,
                        "cashflow": cashflow,
                        "priceSalesRatio":priceSales,
                        "PE": PE
                    }
                }
                print(s)
                with open(self.jsfile, "r") as f:
                    data = js.load(f)
                    cont = data['data']
                    cont.append(s)
                with open(self.jsfile, "w") as f:
                    js.dump(data, f, indent=4) 
                print('=====================================')
            except Exception:
                print("error!")
                pass
            
def load_json(filepath):
    with open(filepath, "r") as f:
        return js.load(f)

def save_json(filepath, data):
    with open(filepath, "w") as f:
        js.dump(data, f, indent=4)

def append_to_json(filepath, ticker_data):
    data = load_json(filepath)
    data['data'].append(ticker_data)
    save_json(filepath, data)

def setup_result_file(basePath, file):
    # check if each level directory exists
    if not os.path.exists(basePath):
        os.makedirs(basePath)
    filepath = os.path.join(basePath, file)
    save_json(filepath, {"data": []})
    return filepath
