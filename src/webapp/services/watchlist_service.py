from __future__ import annotations

import copy
import datetime as dt
import html
from io import StringIO
import json
import math
from pathlib import Path
import re
import subprocess
import logging
import threading
import time
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import yfinance as yf

from ...config import AppConfig
from ...canslim_screen import CANSLIM_HISTORY_DAYS, CANSLIM_INSIDER_LOOKBACK_DAYS, compute_canslim_frame_metrics, evaluate_canslim_ticker
from ...etf_matcher import infer_theme_tags_for_ticker, load_etf_catalog, load_ticker_theme_overrides
from ...ftd_sweep_screen import find_recent_ftd_sweep_hit
from ...flashalpha_gex import build_gamma_exposure_report, render_gamma_exposure_report_svgs
from ...market_extension import compute_extension_frame, resample_to_weekly
from ...market_data_access import (
    db_frame_has_recent_coverage,
    load_many_ticker_windows,
    load_many_ticker_windows_for_range,
    resolve_database_url,
    resolve_market_data_source,
)
from ...ratings.finviz_insider import load_finviz_insider_signal_map
from ...ratings.repository import RatingsRepository
from ...rs_rating_screen import approximate_rs_rating, compute_weighted_rs_score
from ...sepa_vcp_screen import build_sepa_dashboard_snapshot
from ...ticker_filters import is_excluded_ticker, load_excluded_tickers, normalize_ticker_symbol
from ...trend_template_screen import evaluate_trend_template
from ...universe import UniverseTicker, load_universe
from ...vcs_indicator import latest_vcs_snapshot
from ...wyckoff_analysis import compute_wyckoff_markers
from ...config import load_app_config
from ..repositories.insider_repository import InsiderRepository
from ..repositories.watchlist_repository import WatchlistRepository
from .insider_fetcher import fetch_insider_trades_window
from .screener_history_service import ScreenerHistoryService


logger = logging.getLogger(__name__)
TREND_TEMPLATE_DESCRIPTION = (
    'This screen is based on the Trend Template (TTP) by 2 times US Investing Champion Mark Minervini. '
    'He uses the Trend Template as the first step for his stock selection. The criteria are described in his book '
    '"Think and trade like a stock market wizard" : The current stock price is above both the 150-day (30-week) '
    "and the 200-day (40-week) moving average price lines. The 150-day moving average is above the 200-day moving average. "
    "The 200-day moving average line is trending up for at least 1 month (preferably 4–5 months minimum in most cases). "
    "The 50-day (10-week) moving average is above both the 150-day and 200-day moving averages. "
    "The current stock price is trading above the 50-day moving average. "
    "The current stock price is at least 30% above its 52-week low. "
    "The current stock price is within at least 25% of its 52-week high (the closer to a new high the better). "
    "The Relative Strength ranking (RS ranking) is no less than 70."
)
_YAHOO_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
_YAHOO_SCRAPE_TIMEOUT = (3.05, 4.0)
_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS = 40
_YAHOO_BLOCK_MARKERS = (
    "Too Many Requests",
    "Edge: Too Many Requests",
    "Will be right back",
)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT = _REPO_ROOT / "frontend" / "scripts" / "probe_yahoo_playwright.mjs"
_YAHOO_OPTIONS_PROBE_SCRIPT = _REPO_ROOT / "frontend" / "scripts" / "probe_yahoo_options_playwright.mjs"
_INSIDER_CACHE_TTL_HOURS = 12
_CHART_PAYLOAD_CACHE_TTL_SECONDS = 5 * 60
_CHART_GEX_CACHE_TTL_SECONDS = 5 * 60
_SCANNER_TOP_HITS_CACHE_TTL_SECONDS = 3 * 60
_SECTOR_MOMENTUM_CACHE_TTL_SECONDS = 10 * 60
_NEW_YORK_TZ = ZoneInfo("America/New_York")
_SCANNER_BOARD_CUTOFF_HOUR = 20
_SCANNER_BOARD_CUTOFF_MINUTE = 30
_chart_payload_cache: dict[tuple[str, str, str, str, str, str], tuple[float, dict[str, Any]]] = {}
_chart_payload_cache_lock = threading.Lock()
_chart_gex_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_chart_gex_cache_lock = threading.Lock()
_chart_overlay_cache: dict[tuple[str, str, str, str, str, str], tuple[float, dict[str, Any]]] = {}
_chart_overlay_cache_lock = threading.Lock()
_scanner_top_hits_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_scanner_top_hits_cache_lock = threading.Lock()
_sector_momentum_cache: dict[tuple[str, str, str, str], tuple[float, dict[str, dict[str, Any]]]] = {}
_sector_momentum_cache_lock = threading.Lock()
_SCANNER_TOP_HITS_SNAPSHOT_STRATEGY_ID = "scanner_top_hits_snapshot"
_SCANNER_BOARD_CONFIG: tuple[dict[str, str], ...] = (
    {
        "id": "weekly_rs_new_high",
        "strategy_id": "weekly_rs_new_high",
        "label": "Weekly RS New High",
        "description": "Weekly relative-strength leaders printing fresh RS highs, even if price has already pushed to new highs too.",
        "timeframe": "Weekly",
        "accent": "violet",
    },
    {
        "id": "weekly_rs_before_price",
        "strategy_id": "weekly_rs",
        "label": "Weekly RS New High Before Price",
        "description": "Relative-strength leaders holding leadership while price still has room to catch up.",
        "timeframe": "Weekly",
        "accent": "violet",
    },
    {
        "id": "daily_rs_new_high",
        "strategy_id": "daily_rs_new_high",
        "label": "Daily RS New High",
        "description": "Daily RS line clears its lookback high, including names where price has already matched that leadership.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "rs",
        "strategy_id": "rs",
        "label": "RS New High Before Price",
        "description": "Daily RS line clears its lookback high before price itself breaks to a new high.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "vcp_scored",
        "strategy_id": "vcp_scored",
        "label": "VCP Scored",
        "description": "Additional Minervini-style VCP scanner with composite score, execution state, and only score-above-80 entries shown on the scanner card.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "vcp_v3",
        "strategy_id": "vcp_v3",
        "label": "VCP v3",
        "description": "Swing-based VCP scanner split between pre-breakout coils and fresh breakouts, with contraction quality, dry-up, RS, and risk-reward context.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "vcp_spec",
        "strategy_id": "vcp_spec",
        "label": "VCP Spec",
        "description": "Strict geometric VCP scan adapted from the attached spec: Stage 2 trend, prior uptrend, shrinking contractions, declining volume, and pivot proximity.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "sean_gap_up",
        "strategy_id": "sean_gap_up",
        "label": "Sean Gap Up",
        "description": "Post-earnings gap leaders with HVE or HV1 volume, tight structure, and continuation context.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "elite_rs_hv1",
        "strategy_id": "elite_rs_hv1",
        "label": "Elite RS + HV1",
        "description": "Leaders with RS above the current threshold and a fresh HV1 or HVE volume signature inside the recent 5 to 10 bar window.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "elite_rs_recent_peg",
        "strategy_id": "elite_rs_recent_peg",
        "label": "Elite RS + Recent PEG",
        "description": "Elite RS names with a recent Pine-style PEG footprint: open above prior close, 10% or more close-vs-prior-close gap, and at least 3x 50D volume. Earnings event is not required.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "monster_gap",
        "strategy_id": "monster_gap",
        "label": "Monster Gap",
        "description": "Recent Pine-style monster gaps in the last 15 trading days: open above prior close, 20% or more close-vs-prior-close gap, and at least 4x 50D volume.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "monster_peg",
        "strategy_id": "monster_peg",
        "label": "Monster Peg",
        "description": "Recent Pine-style monster gaps tied to a reported earnings event inside the last 15 trading days, with EPS surprise at or above the PEG threshold.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "gap_fill",
        "strategy_id": "gap_fill",
        "label": "Gap Fill",
        "description": "Open overhead gap setups nearing reclaim or already trading back into the fill zone.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "canslim",
        "strategy_id": "canslim",
        "label": "CANSLIM High Score",
        "description": "Finviz plus local technical composite leaders ranked by CANSLIM-style growth, supply, leadership, and market context.",
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "canslim_v2",
        "strategy_id": "canslim_v2",
        "label": "CANSLIM V2",
        "description": "Weighted 0-100 CANSLIM composite using local fundamentals, cached price history, leadership, and market context. Scanner card only shows score-above-80 entries.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "fundamental_quality",
        "strategy_id": "fundamental_quality",
        "label": "Fundamental Quality",
        "description": "Finviz-prefiltered quality compounders with mid-cap-and-up size, strong margins and ROE, then local annual revenue CAGR and diluted EPS growth confirmation.",
        "timeframe": "Daily",
        "accent": "emerald",
        "bias_group": "bullish",
    },
    {
        "id": "minervini_growth_acceleration",
        "strategy_id": "minervini_growth_acceleration",
        "label": "Minervini Growth Accel",
        "description": "Names passing annual and quarterly EPS plus revenue growth-and-acceleration checks in one combined Minervini-style growth screen.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "industry_group_rs_rank",
        "strategy_id": "industry_group_rs_rank",
        "label": "Industry Group RS Rank",
        "description": "Daily technical leaders with persisted industry-group RS rank above 90 on the 0-99 scale.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "venu_scanner",
        "strategy_id": "venu_scanner",
        "label": "Venu Scanner",
        "description": "Live Finviz mid-cap stock scan for liquid names above the 20, 50, and 200 day moving averages with strong current participation.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "finviz_target_price_50",
        "strategy_id": "finviz_target_price_50",
        "label": "Finviz Target +50%",
        "description": "Live Finviz scan for names where analyst target price is at least 50% above current price.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_horizontal",
        "strategy_id": "finviz_pattern_horizontal",
        "label": "Finviz Horizontal S/R",
        "description": "Live Finviz chart-pattern scan for range-bound names pressing well-defined horizontal support and resistance.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_horizontal2",
        "strategy_id": "finviz_pattern_horizontal2",
        "label": "Finviz Horizontal S/R Strong",
        "description": "Live Finviz chart-pattern scan for stronger horizontal support and resistance ranges nearing breakout resolution.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_tlsupport",
        "strategy_id": "finviz_pattern_tlsupport",
        "label": "Finviz TL Support",
        "description": "Live Finviz chart-pattern scan for names pulling back into rising trendline support.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_tlsupport2",
        "strategy_id": "finviz_pattern_tlsupport2",
        "label": "Finviz TL Support Strong",
        "description": "Live Finviz chart-pattern scan for stronger rising-trendline support setups with cleaner structure.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_wedgedown",
        "strategy_id": "finviz_pattern_wedgedown",
        "label": "Finviz Wedge Down",
        "description": "Live Finviz chart-pattern scan for falling-wedge contractions that can break upward.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_wedgedown2",
        "strategy_id": "finviz_pattern_wedgedown2",
        "label": "Finviz Wedge Down Strong",
        "description": "Live Finviz chart-pattern scan for stronger falling-wedge structures with cleaner bullish reversal potential.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_wedgeresistance",
        "strategy_id": "finviz_pattern_wedgeresistance",
        "label": "Finviz Triangle Ascending",
        "description": "Live Finviz chart-pattern scan for ascending-triangle setups leaning into a bullish continuation breakout.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_wedgeresistance2",
        "strategy_id": "finviz_pattern_wedgeresistance2",
        "label": "Finviz Triangle Ascending Strong",
        "description": "Live Finviz chart-pattern scan for stronger ascending-triangle continuation setups.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_channelup",
        "strategy_id": "finviz_pattern_channelup",
        "label": "Finviz Channel Up",
        "description": "Live Finviz chart-pattern scan for names riding an orderly upward price channel.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_channelup2",
        "strategy_id": "finviz_pattern_channelup2",
        "label": "Finviz Channel Up Strong",
        "description": "Live Finviz chart-pattern scan for stronger upward-channel leaders with cleaner continuation structure.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_doublebottom",
        "strategy_id": "finviz_pattern_doublebottom",
        "label": "Finviz Double Bottom",
        "description": "Live Finviz chart-pattern scan for names currently tagged as Double Bottom.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_multiplebottom",
        "strategy_id": "finviz_pattern_multiplebottom",
        "label": "Finviz Multiple Bottom",
        "description": "Live Finviz chart-pattern scan for names building repeated-bottom reversal structures.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "finviz_pattern_headandshouldersinv",
        "strategy_id": "finviz_pattern_headandshouldersinv",
        "label": "Finviz Inverse H&S",
        "description": "Live Finviz chart-pattern scan for inverse head-and-shoulders reversal setups.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "stockbee_momentum_burst",
        "strategy_id": "stockbee_momentum_burst",
        "label": "Stockbee Momentum Burst",
        "description": "Short-term burst candidates with 4% breakouts, dollar breakouts, or range expansions after contraction and manageable trigger-day risk.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "macd_golden_cross",
        "strategy_id": "macd_golden_cross",
        "label": "MACD Golden Cross",
        "description": "Fresh bullish MACD crossovers where momentum flips positive with MACD moving above its signal line.",
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "inside_dryup_v2",
        "strategy_id": "inside_dryup_v2",
        "label": "Inside Day + Extreme Dry-Up",
        "description": "Latest inside-day setups where current price-volume has collapsed into an extreme dry-up state.",
        "timeframe": "Daily",
        "accent": "violet",
    },
    {
        "id": "wyckoff_buy_signal",
        "strategy_id": "wyckoff_buy_signal",
        "label": "Wyckoff Buy Signal",
        "description": "Wyckoff accumulation names where the indicator flips into a fresh BUY state after spring, LPS, or phase progression.",
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "ftd_sweep",
        "strategy_id": "ftd_sweep",
        "label": "FTD Successful Sweep",
        "description": "Bullish follow-through day sweep names reclaiming pivot structure after shakeout and readying for continuation.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
    {
        "id": "sepa_vcp",
        "strategy_id": "sepa_vcp",
        "label": "SEPA",
        "description": "SEPA names passing the Minervini trend template with risk, pressure, and RS dashboard context persisted together.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "cup_detection",
        "strategy_id": "cup_detection",
        "label": "Cup Detection",
        "description": "Active cup bases still building below breakout, before a confirmed pivot breakout removes setup-stage edge.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "eight_week_100_runup",
        "strategy_id": "eight_week_100_runup",
        "label": "8 Week Run Up (Doubler)",
        "description": "Momentum leaders already up 100% or more inside eight weeks, useful for spotting true high-velocity leadership early.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "three_weeks_tight",
        "strategy_id": "three_weeks_tight",
        "label": "Three Weeks Tight",
        "description": "Names compressing into classic three-weeks-tight structure, useful for spotting quiet continuation pivots before breakout.",
        "timeframe": "Weekly",
        "accent": "violet",
    },
    {
        "id": "rti",
        "strategy_id": "rti",
        "label": "Range Tightness Index",
        "description": "Daily range-compression names where RTI is below 20, stacking tight bars, or expanding right after a tight reset.",
        "timeframe": "Daily",
        "accent": "cyan",
    },
    {
        "id": "double_bottom_detection",
        "strategy_id": "double_bottom_detection",
        "label": "Double Bottom",
        "description": "Base-building names with an active double-bottom structure and pivot-ready reversal context.",
        "timeframe": "Daily",
        "accent": "amber",
        "bias_group": "bullish",
    },
    {
        "id": "weekly_tight_close",
        "strategy_id": "weekly_tight_close",
        "label": "Weekly Tight Close",
        "description": "Three weekly bars with ATR-scaled tight closes plus tight highs or lows, while first bar still passes wick and range filter.",
        "timeframe": "Weekly",
        "accent": "violet",
    },
    {
        "id": "weinstein_stage2_early",
        "strategy_id": "weinstein_stage2_early",
        "label": "Weinstein Stage 2 Early",
        "description": "Weekly regime names that just transitioned from Stage 1 base into early Stage 2 advance, with 30-week EMA slope and price band already aligned.",
        "timeframe": "Weekly",
        "accent": "lime",
    },
    {
        "id": "ema21_pullback_buy",
        "strategy_id": "ema21_pullback_buy",
        "label": "EMA21 Pullback Buy",
        "description": "Uptrend leaders that tested the 21 EMA, held the close, then triggered a first bullish breakout over the test-candle high.",
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "sma200_pullback_buy",
        "strategy_id": "sma200_pullback_buy",
        "label": "200 SMA Pullback Buy",
        "description": "Long-trend leaders that tested the 200 SMA from above, held the close, then triggered a bullish breakout over the test-candle high.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "trend_template",
        "strategy_id": "trend_template",
        "label": "Trend Template",
        "description": TREND_TEMPLATE_DESCRIPTION,
        "timeframe": "Daily",
        "accent": "lime",
    },
    {
        "id": "vcs_critical_tightness",
        "strategy_id": "vcs_critical_tightness",
        "label": "VCS Critical Tightness",
        "description": "Advanced volatility contraction names where price, spread, and volume have all compressed into a high-readiness state.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "sean_breakout",
        "strategy_id": "sean_breakout",
        "label": "Sean Breakout",
        "description": "Daily leaders closing above 21 and 50 EMA with price, volume, and ADR thresholds already cleared.",
        "timeframe": "Daily",
        "accent": "amber",
    },
    {
        "id": "fearzone",
        "strategy_id": "fearzone",
        "label": "Fearzone",
        "description": "High-velocity dislocation setups where panic can create asymmetric snapback entries.",
        "timeframe": "Daily",
        "accent": "cyan",
        "bias_group": "bullish",
    },
    {
        "id": "td9_bullish",
        "strategy_id": "td9_bullish",
        "label": "TD9 Bullish",
        "description": "Bullish TD Sequential exhaustion names where downside pressure may be finishing.",
        "timeframe": "Daily",
        "accent": "lime",
        "bias_group": "bullish",
    },
)


class WatchlistService:
    def __init__(
        self,
        artifacts_dir: Path,
        *,
        database_url: str | None = None,
        market_data_source: str | None = None,
        benchmark_ticker: str = "SPY",
    ) -> None:
        self.repository = WatchlistRepository(artifacts_dir=artifacts_dir, database_url=database_url or "")
        self.insider_repository = InsiderRepository(artifacts_dir=artifacts_dir)
        self._universe_index: dict[str, UniverseTicker] | None = None
        self._theme_catalog: list[dict[str, object]] | None = None
        self.database_url = resolve_database_url(database_url)
        self.market_data_source = resolve_market_data_source(market_data_source)
        self.benchmark_ticker = str(benchmark_ticker or "SPY").strip().upper() or "SPY"
        self._excluded_tickers: set[str] | None = None
        self._scanner_board_override_path = self.repository.artifacts_dir / "status" / "scanner_board_override.json"
        self.screener_history_service = ScreenerHistoryService(
            database_url=self.database_url or "",
            artifacts_dir=artifacts_dir,
            repository=self.repository.history_repository,
        )

    def list_recent(self, *, include_deprecated: bool = True) -> list[dict[str, Any]]:
        return self.repository.list_recent_watchlists(limit=50, include_deprecated=include_deprecated)

    def get_scanner_board(self, *, now: dt.datetime | None = None) -> dict[str, Any]:
        reference_now = _normalize_scanner_now(now)
        default_target_trading_date = _latest_completed_trading_day(reference_now)
        latest_visible_trading_day = _latest_visible_trading_day(reference_now)
        override_payload = self._load_scanner_board_override()
        override_target_date_text = _coerce_iso_date(override_payload.get("target_trading_date"))
        override_target_date = dt.date.fromisoformat(override_target_date_text) if override_target_date_text else None
        target_trading_date = default_target_trading_date
        manual_override_active = False
        if (
            override_target_date is not None
            and override_target_date > default_target_trading_date
            and override_target_date <= latest_visible_trading_day
        ):
            target_trading_date = override_target_date
            manual_override_active = True
        recent_watchlists = self.repository.list_recent_watchlists(limit=400)
        cards: list[dict[str, Any]] = []

        for config in _SCANNER_BOARD_CONFIG:
            selected_meta = _select_scanner_board_watchlist(
                recent_watchlists,
                strategy_id=config["strategy_id"],
                target_date=target_trading_date,
            )
            entries = self._filter_excluded_entries(
                self.repository.load_watchlist(str(selected_meta.get("stem") or "")) if selected_meta else []
            )
            preview_tickers = [
                str(item.get("ticker") or "").strip().upper()
                for item in entries
                if isinstance(item, dict) and str(item.get("ticker") or "").strip()
            ][:6]
            cards.append(
                {
                    "id": config["id"],
                    "strategy_id": config["strategy_id"],
                    "label": config["label"],
                    "description": config["description"],
                    "timeframe": config["timeframe"],
                    "accent": config["accent"],
                    "available": selected_meta is not None and len(entries) > 0,
                    "stem": str(selected_meta.get("stem") or "") if selected_meta else "",
                    "group_label": str(selected_meta.get("group_label") or "") if selected_meta else "",
                    "captured_at": str(selected_meta.get("captured_at") or "") if selected_meta else "",
                    "sort_date": str(selected_meta.get("sort_date") or "") if selected_meta else "",
                    "entry_count": len(entries),
                    "preview_tickers": preview_tickers,
                    "list_href": (
                        f"/watchlists?stem={selected_meta.get('stem')}"
                        if selected_meta and selected_meta.get("stem")
                        else None
                    ),
                }
            )

        available_cards = [item for item in cards if item["available"]]
        latest_update_at = max((str(item["captured_at"] or "") for item in available_cards), default="")
        latest_signal_date = max((str(item["sort_date"] or "") for item in available_cards), default="")
        return {
            "generated_at": reference_now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "reference_now_new_york": reference_now.astimezone(_NEW_YORK_TZ).isoformat(),
            "target_trading_date": target_trading_date.isoformat(),
            "cutoff_time_label": "20:30 America/New_York",
            "latest_update_at": latest_update_at,
            "latest_signal_date": latest_signal_date,
            "manual_override_active": manual_override_active,
            "manual_override_target_date": override_target_date.isoformat() if override_target_date is not None else "",
            "manual_override_requested_at": str(override_payload.get("requested_at") or ""),
            "cards": cards,
        }

    def force_scanner_board_refresh(
        self,
        *,
        now: dt.datetime | None = None,
        requested_by: str = "",
    ) -> dict[str, Any]:
        reference_now = _normalize_scanner_now(now)
        target_trading_date = _latest_visible_trading_day(reference_now)
        payload = {
            "target_trading_date": target_trading_date.isoformat(),
            "requested_at": reference_now.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "requested_by": str(requested_by or "").strip(),
        }
        self._scanner_board_override_path.parent.mkdir(parents=True, exist_ok=True)
        self._scanner_board_override_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        board_payload = self.get_scanner_board(now=reference_now)
        try:
            self.persist_scanner_top_hits_snapshot(now=reference_now, board_payload=board_payload)
        except Exception as exc:
            logger.warning("Scanner top hits snapshot persistence failed during manual refresh: %s", exc)
        return board_payload

    def _load_scanner_board_override(self) -> dict[str, Any]:
        if not self._scanner_board_override_path.exists():
            return {}
        try:
            payload = json.loads(self._scanner_board_override_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def get_scanner_top_hits_payload(self, *, rrg_service: Any | None = None, now: dt.datetime | None = None) -> dict[str, Any]:
        board_payload = self.get_scanner_board(now=now)
        cache_key = (
            str(board_payload.get("target_trading_date") or ""),
            str(board_payload.get("manual_override_target_date") or ""),
        )
        cached_payload = _read_scanner_top_hits_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        persisted_payload = self._load_persisted_scanner_top_hits_payload(board_payload=board_payload)
        if persisted_payload is not None:
            _write_scanner_top_hits_cache(cache_key, persisted_payload)
            return persisted_payload
        payload = self._build_scanner_top_hits_payload_live(board_payload=board_payload, rrg_service=rrg_service)
        if self.screener_history_service.is_configured():
            try:
                self.persist_scanner_top_hits_snapshot(
                    now=now,
                    board_payload=board_payload,
                    rrg_service=rrg_service,
                    precomputed_payload=payload,
                )
            except Exception as exc:
                logger.warning("Scanner top hits snapshot persistence failed during live rebuild: %s", exc)
        _write_scanner_top_hits_cache(cache_key, payload)
        return payload

    def persist_scanner_top_hits_snapshot(
        self,
        *,
        now: dt.datetime | None = None,
        board_payload: dict[str, Any] | None = None,
        rrg_service: Any | None = None,
        precomputed_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not self.screener_history_service.is_configured():
            return precomputed_payload
        resolved_board_payload = board_payload or self.get_scanner_board(now=now)
        payload = precomputed_payload or self._build_scanner_top_hits_payload_live(
            board_payload=resolved_board_payload,
            rrg_service=rrg_service,
        )
        target_trading_date_text = str(resolved_board_payload.get("target_trading_date") or "").strip()
        if not target_trading_date_text:
            return payload
        run_date = dt.date.fromisoformat(target_trading_date_text)
        summary_payload = {
            "generated_at": payload.get("generated_at"),
            "reference_now_new_york": payload.get("reference_now_new_york"),
            "target_trading_date": payload.get("target_trading_date"),
            "cutoff_time_label": payload.get("cutoff_time_label"),
            "latest_update_at": payload.get("latest_update_at"),
            "latest_signal_date": payload.get("latest_signal_date"),
            "manual_override_active": bool(payload.get("manual_override_active")),
            "manual_override_target_date": payload.get("manual_override_target_date"),
            "manual_override_requested_at": payload.get("manual_override_requested_at"),
            "total_live_scanners": int(payload.get("total_live_scanners") or 0),
            "total_unique_tickers": int(payload.get("total_unique_tickers") or 0),
            "overlapping_ticker_count": int(payload.get("overlapping_ticker_count") or 0),
        }
        config_json = {
            "kind": "scanner_top_hits_snapshot",
            "benchmark_ticker": self.benchmark_ticker,
        }
        scope_json = {
            "target_trading_date": target_trading_date_text,
            "manual_override_target_date": str(resolved_board_payload.get("manual_override_target_date") or ""),
        }
        hit_rows = []
        for index, row in enumerate(payload.get("rows", []), start=1):
            if not isinstance(row, dict):
                continue
            ticker = normalize_ticker_symbol(str(row.get("ticker") or ""))
            if not ticker:
                continue
            hit_rows.append(
                {
                    "strategy_id": _SCANNER_TOP_HITS_SNAPSHOT_STRATEGY_ID,
                    "signal_date": run_date,
                    "ticker": ticker,
                    "passed": True,
                    "rank": index,
                    "metrics_json": {
                        "scanner_count": int(row.get("scanner_count") or 0),
                    },
                    "reasons_json": list(row.get("scanner_labels") or []),
                    "hit_payload_json": dict(row),
                }
            )
        self.screener_history_service.persist_snapshot_run(
            strategy_id=_SCANNER_TOP_HITS_SNAPSHOT_STRATEGY_ID,
            run_date=run_date,
            summary_payload=summary_payload,
            hit_rows=hit_rows,
            config_json=config_json,
            scope_json=scope_json,
            market_data_mode="derived",
            source_kind="scanner-top-hits",
            notes="Scanner top hits snapshot",
        )
        return payload

    def _load_persisted_scanner_top_hits_payload(self, *, board_payload: dict[str, Any]) -> dict[str, Any] | None:
        if not self.screener_history_service.is_configured():
            return None
        target_trading_date_text = str(board_payload.get("target_trading_date") or "").strip()
        if not target_trading_date_text:
            return None
        try:
            run_date = dt.date.fromisoformat(target_trading_date_text)
        except ValueError:
            return None
        rows = self.screener_history_service.list_runs(
            strategy_id=_SCANNER_TOP_HITS_SNAPSHOT_STRATEGY_ID,
            start_date=run_date,
            end_date=run_date,
            limit=5,
        )
        if not rows:
            return None
        manual_override_target_date = str(board_payload.get("manual_override_target_date") or "")
        for row in rows:
            run_id = int(row.get("id") or 0)
            if run_id <= 0:
                continue
            payload = self.screener_history_service.get_run(run_id, include_hits=True, hit_limit=5000)
            if not isinstance(payload, dict):
                continue
            summary_payload = payload.get("result_summary_json")
            if not isinstance(summary_payload, dict):
                continue
            if str(summary_payload.get("manual_override_target_date") or "") != manual_override_target_date:
                continue
            hit_rows = payload.get("hits") if isinstance(payload.get("hits"), list) else []
            rows_payload = []
            for hit in hit_rows:
                if not isinstance(hit, dict):
                    continue
                row_payload = hit.get("hit_payload_json")
                if isinstance(row_payload, dict):
                    rows_payload.append(copy.deepcopy(row_payload))
            rows_by_ticker = {
                normalize_ticker_symbol(str(item.get("ticker") or "")): item
                for item in rows_payload
                if isinstance(item, dict) and normalize_ticker_symbol(str(item.get("ticker") or ""))
            }
            if rows_by_ticker:
                self._attach_latest_rating_snapshots(rows_by_ticker, sorted(rows_by_ticker))
            return {
                "generated_at": str(summary_payload.get("generated_at") or ""),
                "reference_now_new_york": str(summary_payload.get("reference_now_new_york") or ""),
                "target_trading_date": str(summary_payload.get("target_trading_date") or target_trading_date_text),
                "cutoff_time_label": str(summary_payload.get("cutoff_time_label") or "20:30 America/New_York"),
                "latest_update_at": str(summary_payload.get("latest_update_at") or ""),
                "latest_signal_date": str(summary_payload.get("latest_signal_date") or ""),
                "manual_override_active": bool(summary_payload.get("manual_override_active")),
                "manual_override_target_date": str(summary_payload.get("manual_override_target_date") or ""),
                "manual_override_requested_at": str(summary_payload.get("manual_override_requested_at") or ""),
                "cards": copy.deepcopy(board_payload.get("cards") or []),
                "total_live_scanners": int(summary_payload.get("total_live_scanners") or 0),
                "total_unique_tickers": int(summary_payload.get("total_unique_tickers") or 0),
                "overlapping_ticker_count": int(summary_payload.get("overlapping_ticker_count") or 0),
                "rows": rows_payload,
            }
        return None

    def _build_scanner_top_hits_payload_live(self, *, board_payload: dict[str, Any], rrg_service: Any | None = None) -> dict[str, Any]:
        live_cards = self._select_scanner_top_hit_live_cards(board_payload)
        aggregated: dict[str, dict[str, Any]] = {}

        for card in live_cards:
            stem = str(card.get("stem") or "").strip()
            if not stem:
                continue
            entries = self._prepare_scanner_top_hit_entries(
                self._filter_excluded_entries(self.repository.load_watchlist(stem))
            )
            scanner_meta = {
                "id": str(card.get("id") or ""),
                "strategy_id": str(card.get("strategy_id") or ""),
                "label": str(card.get("label") or ""),
                "timeframe": str(card.get("timeframe") or ""),
                "stem": stem,
                "sort_date": str(card.get("sort_date") or ""),
            }
            for entry in entries:
                ticker = normalize_ticker_symbol(str(entry.get("ticker") or ""))
                if not ticker:
                    continue
                bucket = aggregated.setdefault(
                    ticker,
                    {
                        "ticker": ticker,
                        "company": "",
                        "sector": "",
                        "industry": "",
                        "day_close": None,
                        "change_pct": None,
                        "perf_year_pct": None,
                        "perf_ytd_pct": None,
                        "rs_rating": None,
                        "ta_rating": None,
                        "fa_rating": None,
                        "fa_current_rank": None,
                        "technical_indicator_ratings": {},
                        "scanner_count": 0,
                        "scanners": [],
                    },
                )
                self._merge_scanner_top_hit_entry(bucket, entry)
                scanners = bucket["scanners"]
                if scanner_meta["id"] and not any(str(item.get("id") or "") == scanner_meta["id"] for item in scanners):
                    scanners.append(dict(scanner_meta))

        total_unique_tickers = len(aggregated)
        for row in aggregated.values():
            row["scanner_count"] = len(row["scanners"])

        top_hit_tickers = sorted(
            ticker
            for ticker, row in aggregated.items()
            if int(row.get("scanner_count") or 0) >= 2
        )
        if top_hit_tickers:
            self._attach_latest_market_snapshots(aggregated, top_hit_tickers)
            self._attach_latest_rating_snapshots(aggregated, top_hit_tickers)
        sector_momentum_map = self._load_sector_momentum_map(rrg_service) if top_hit_tickers else {}

        rows = []
        for ticker in top_hit_tickers:
            row = aggregated[ticker]
            sector_key = _coalesce_text(row.get("sector"))
            if sector_key:
                row["sector_momentum"] = copy.deepcopy(sector_momentum_map.get(sector_key) or None)
            else:
                row["sector_momentum"] = None
            row["scanner_labels"] = [str(item.get("label") or "") for item in row["scanners"] if str(item.get("label") or "").strip()]
            rows.append(row)

        rows.sort(key=lambda item: (-int(item.get("scanner_count") or 0), str(item.get("ticker") or "")))
        overlapping_count = len(rows)
        payload = {
            **board_payload,
            "total_live_scanners": len(live_cards),
            "total_unique_tickers": total_unique_tickers,
            "overlapping_ticker_count": overlapping_count,
            "rows": rows,
        }
        return payload

    def get_weekly_watchlist_board(self, stem: str | None = None) -> dict[str, Any]:
        weekly_files = [item for item in self.repository.list_recent_watchlists(limit=200, include_deprecated=False) if item.get("group_key") == "weekly_rs"]
        selected_meta = None
        normalized_stem = str(stem or "").strip()
        if normalized_stem:
            selected_meta = next((item for item in weekly_files if str(item.get("stem") or "") == normalized_stem), None)
        if selected_meta is None and weekly_files:
            selected_meta = weekly_files[0]
        selected_stem = str(selected_meta.get("stem") or "") if isinstance(selected_meta, dict) else ""
        entries = self._enrich_entries(self._filter_excluded_entries(self.repository.load_watchlist(selected_stem))) if selected_stem else []
        board_entries: list[dict[str, Any]] = []
        for raw_entry in entries:
            entry = dict(raw_entry)
            signal_badges = _build_weekly_signal_badges(entry)
            if signal_badges:
                entry["signal_badges"] = signal_badges
            board_entries.append(entry)
        return {
            "source_stem": selected_stem,
            "source_name": str(selected_meta.get("name") or "") if isinstance(selected_meta, dict) else "",
            "captured_at": str(selected_meta.get("captured_at") or "") if isinstance(selected_meta, dict) else "",
            "sort_date": str(selected_meta.get("sort_date") or "") if isinstance(selected_meta, dict) else "",
            "group_label": "Weekly RS",
            "entry_count": len(board_entries),
            "entries": board_entries,
            "available_files": weekly_files[:24],
        }

    def get_watchlist_detail(self, stem: str, *, allow_deprecated: bool = True) -> dict[str, Any]:
        metadata = self.repository.get_watchlist_metadata(stem)
        if metadata and bool(metadata.get("is_deprecated")) and not allow_deprecated:
            raise ValueError(f"Deprecated watchlist is admin-only: {stem}")
        recent_watchlists = self.repository.list_recent_watchlists(limit=400, include_deprecated=allow_deprecated)
        current_meta = next((item for item in recent_watchlists if str(item.get("stem") or "") == stem), None)
        current_strategy_id = _strategy_id_for_watchlist_meta(current_meta) if current_meta else _normalize_scanner_strategy_id(_stem_strategy_id(stem))
        previous_meta = _find_previous_watchlist_meta(
            recent_watchlists,
            stem=stem,
            strategy_id=current_strategy_id,
        )
        previous_tickers = _watchlist_ticker_set(
            self._filter_excluded_entries(
                self.repository.load_watchlist(str(previous_meta.get("stem") or ""))
            )
        ) if previous_meta else set()
        entries = self._enrich_entries(self._filter_excluded_entries(self.repository.load_watchlist(stem)))
        self._attach_entry_latest_rating_snapshots(entries)
        self._attach_entry_technical_indicator_ratings(entries)
        has_previous_scan = previous_meta is not None
        if has_previous_scan:
            for entry in entries:
                ticker = normalize_ticker_symbol(str(entry.get("ticker") or ""))
                entry["is_new"] = bool(ticker) and ticker not in previous_tickers
        else:
            for entry in entries:
                entry["is_new"] = False
        new_ticker_count = sum(1 for entry in entries if bool(entry.get("is_new")))
        return _normalize_json_payload(
            {
            "stem": stem,
            "strategy_id": current_strategy_id,
            "has_previous_scan": has_previous_scan,
            "previous_stem": str(previous_meta.get("stem") or "") if previous_meta else "",
            "new_ticker_count": new_ticker_count,
            "entry_count": len(entries),
            "entries": entries,
            "is_deprecated": bool(metadata.get("is_deprecated")) if isinstance(metadata, dict) else False,
            "deprecation_reason": str(metadata.get("deprecation_reason") or "") if isinstance(metadata, dict) else "",
            }
        )

    def get_chart_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        requested_as_of_date = as_of_date
        cache_key = _build_chart_payload_cache_key(
            ticker=normalized_ticker,
            period=period,
            as_of_date=as_of_date,
            benchmark_ticker=self.benchmark_ticker,
            market_data_source=self.market_data_source,
            include_setup_markers=include_setup_markers,
        )
        cached_payload = _read_chart_payload_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        chart_request = _resolve_chart_request(period=period, as_of_date=as_of_date)
        frame, benchmark_frame, data_source = self._load_chart_frames(
            ticker=normalized_ticker,
            benchmark_ticker=self.benchmark_ticker,
            required_start_date=chart_request.visible_start_date,
            start_date=chart_request.fetch_start_date,
            end_date=chart_request.fetch_end_date,
            warmup_trading_days=chart_request.warmup_trading_days,
        )
        if frame is None or frame.empty:
            return _empty_chart_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        frame = frame.sort_index()
        resolved_as_of_timestamp = frame.index.max()
        resolved_as_of_date = pd.Timestamp(resolved_as_of_timestamp).date()
        visible_start_date = chart_request.visible_start_date
        visible_frame = frame.loc[(frame.index.date >= visible_start_date) & (frame.index.date <= resolved_as_of_date)].copy()
        if visible_frame.empty:
            return _empty_chart_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                resolved_as_of_date=resolved_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        frame["ma20"] = frame["Close"].rolling(20).mean()
        frame["ma50"] = frame["Close"].rolling(50).mean()
        frame["ma200"] = frame["Close"].rolling(200).mean()
        frame["ema8"] = frame["Close"].ewm(span=8, adjust=False).mean()
        frame["ema21"] = frame["Close"].ewm(span=21, adjust=False).mean()
        weekly_close = frame["Close"].resample("W-FRI").last().dropna()
        weekly_ema8 = weekly_close.ewm(span=8, adjust=False).mean()
        frame["weekly_ema8"] = weekly_ema8.reindex(frame.index, method="ffill")
        frame["ipo_vwap"] = _compute_ipo_vwap(frame)
        candles: list[dict[str, Any]] = []
        volume: list[dict[str, Any]] = []
        ma20: list[dict[str, Any]] = []
        ma50: list[dict[str, Any]] = []
        ma200: list[dict[str, Any]] = []
        ema8: list[dict[str, Any]] = []
        ema21: list[dict[str, Any]] = []
        weekly_ema8_points: list[dict[str, Any]] = []
        ipo_vwap: list[dict[str, Any]] = []

        visible_index_set = set(visible_frame.index)

        for index, row in frame.iterrows():
            if index not in visible_index_set:
                continue
            time_value = pd.Timestamp(index).date().isoformat()
            open_value = float(row["Open"])
            close_value = float(row["Close"])
            candles.append(
                {
                    "time": time_value,
                    "open": open_value,
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": close_value,
                }
            )
            volume.append(
                {
                    "time": time_value,
                    "value": int(row["Volume"]),
                    "color": "rgba(34, 197, 94, 0.72)" if close_value >= open_value else "rgba(239, 68, 68, 0.72)",
                }
            )
            if pd.notna(row["ma20"]):
                ma20.append({"time": time_value, "value": float(row["ma20"])})
            if pd.notna(row["ma50"]):
                ma50.append({"time": time_value, "value": float(row["ma50"])})
            if pd.notna(row["ma200"]):
                ma200.append({"time": time_value, "value": float(row["ma200"])})
            if pd.notna(row["ema8"]):
                ema8.append({"time": time_value, "value": float(row["ema8"])})
            if pd.notna(row["ema21"]):
                ema21.append({"time": time_value, "value": float(row["ema21"])})
            if pd.notna(row["weekly_ema8"]):
                weekly_ema8_points.append({"time": time_value, "value": float(row["weekly_ema8"])})
            if pd.notna(row["ipo_vwap"]):
                ipo_vwap.append({"time": time_value, "value": float(row["ipo_vwap"])})

        payload = {
            "ticker": normalized_ticker,
            "benchmark_ticker": self.benchmark_ticker,
            "period": period,
            "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "latest_available_date": resolved_as_of_date.isoformat(),
            "data_source": data_source,
            "candles": candles,
            "volume": volume,
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
            "ema8": ema8,
            "ema21": ema21,
            "weekly_ema8": weekly_ema8_points,
            "ipo_vwap": ipo_vwap,
            "market_extension": _empty_market_extension_overlay(),
            "rs_line": [],
            "daily_rs_rating": [],
            "weekly_rs_rating": [],
            "rs_markers": [],
            "setup_markers": [],
            "fearzone_panel": {"rows": [], "signals": []},
            "trend_template": None,
            "vcs": None,
            "sepa_dashboard": None,
        }
        if payload["candles"]:
            _write_chart_payload_cache(cache_key, payload)
        return payload

    def get_chart_gex_payload(self, ticker: str) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        cached_payload = _read_chart_gex_cache(normalized_ticker)
        if cached_payload is not None:
            return cached_payload

        try:
            report = build_gamma_exposure_report(symbol=normalized_ticker, timeout_seconds=12)
            payload = _build_chart_gex_payload(report=report)
        except Exception as exc:
            payload = {
                "ticker": normalized_ticker,
                "available": False,
                "error": str(exc),
                "plots": None,
            }

        _write_chart_gex_cache(normalized_ticker, payload)
        return payload

    def get_chart_overlays_payload(
        self,
        ticker: str,
        period: str = "18mo",
        *,
        as_of_date: dt.date | None = None,
        include_setup_markers: bool = False,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        requested_as_of_date = as_of_date
        cache_key = _build_chart_overlay_cache_key(
            ticker=normalized_ticker,
            period=period,
            as_of_date=as_of_date,
            benchmark_ticker=self.benchmark_ticker,
            market_data_source=self.market_data_source,
            include_setup_markers=include_setup_markers,
        )
        cached_payload = _read_chart_overlay_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        chart_request = _resolve_chart_request(period=period, as_of_date=as_of_date)
        frame, benchmark_frame, data_source = self._load_chart_frames(
            ticker=normalized_ticker,
            benchmark_ticker=self.benchmark_ticker,
            required_start_date=chart_request.visible_start_date,
            start_date=chart_request.fetch_start_date,
            end_date=chart_request.fetch_end_date,
            warmup_trading_days=chart_request.warmup_trading_days,
        )
        if frame is None or frame.empty:
            return _empty_chart_overlay_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        frame = frame.sort_index()
        resolved_as_of_timestamp = frame.index.max()
        resolved_as_of_date = pd.Timestamp(resolved_as_of_timestamp).date()
        visible_start_date = chart_request.visible_start_date
        visible_frame = frame.loc[(frame.index.date >= visible_start_date) & (frame.index.date <= resolved_as_of_date)].copy()
        if visible_frame.empty:
            return _empty_chart_overlay_payload(
                normalized_ticker,
                period=period,
                requested_as_of_date=requested_as_of_date,
                resolved_as_of_date=resolved_as_of_date,
                benchmark_ticker=self.benchmark_ticker,
                data_source=data_source,
            )

        visible_dates = {pd.Timestamp(index).date().isoformat() for index in visible_frame.index}
        market_extension = _compute_market_extension_overlay(frame, visible_dates=visible_dates)

        rs_line: pd.Series | None = None
        rs_new_high: pd.Series | None = None
        rs_new_high_before_price: pd.Series | None = None
        daily_rs_rating: pd.Series | None = None
        weekly_rs_rating: pd.Series | None = None
        fearzone_panel = _filter_fearzone_panel(
            _compute_fearzone_panel(frame),
            visible_dates=visible_dates,
        )
        trend_template_snapshot = evaluate_trend_template(frame)
        vcs_snapshot = latest_vcs_snapshot(frame)
        setup_markers: list[dict[str, Any]] = []
        if include_setup_markers:
            setup_markers.extend(_compute_mark_daily_extend_markers(frame, visible_dates=visible_dates))
            setup_markers.extend(
                _compute_ftd_sweep_markers(
                    frame=frame,
                    visible_dates=visible_dates,
                    ticker=normalized_ticker,
                    benchmark_ticker=self.benchmark_ticker,
                )
            )
            setup_markers.extend(compute_wyckoff_markers(frame, visible_dates=visible_dates))
        if benchmark_frame is not None and not benchmark_frame.empty:
            benchmark_frame = benchmark_frame.sort_index()
            benchmark_frame = benchmark_frame.loc[benchmark_frame.index <= pd.Timestamp(resolved_as_of_date)].copy()
            rs_line = _compute_rs_line(frame["Close"], benchmark_frame["Close"])
            daily_rs_rating = _compute_rs_rating_series(frame["Close"], benchmark_frame["Close"])
            if daily_rs_rating is not None and not daily_rs_rating.empty:
                weekly_rs_rating = daily_rs_rating.resample("W-FRI").last().dropna()
        sepa_dashboard = (
            build_sepa_dashboard_snapshot(
                frame,
                benchmark_frame if benchmark_frame is not None else pd.DataFrame(),
                benchmark_ticker=self.benchmark_ticker,
            )
            if benchmark_frame is not None and not benchmark_frame.empty
            else None
        )
        danger_signals = _compute_danger_signals_snapshot(
            frame=frame,
            benchmark_frame=benchmark_frame,
            market_extension=market_extension,
        )

        rs_points: list[dict[str, Any]] = []
        daily_rs_rating_points: list[dict[str, Any]] = []
        weekly_rs_rating_points: list[dict[str, Any]] = []
        rs_markers: list[dict[str, Any]] = []
        visible_index_set = set(visible_frame.index)
        if rs_line is not None and benchmark_frame is not None and not benchmark_frame.empty:
            rs_new_high, rs_new_high_before_price = _compute_rs_new_high_flags(
                rs_line=rs_line,
                price_reference=frame["High"].reindex(rs_line.index),
                lookback=250,
            )
        for index, row in frame.iterrows():
            if index not in visible_index_set:
                continue
            time_value = pd.Timestamp(index).date().isoformat()
            if rs_line is not None and index in rs_line.index and pd.notna(rs_line.loc[index]):
                rs_points.append({"time": time_value, "value": float(rs_line.loc[index])})
                if rs_new_high is not None and bool(rs_new_high.loc[index]):
                    rs_markers.append(
                        {
                            "time": time_value,
                            "kind": "daily_new_high_before_price" if rs_new_high_before_price is not None and bool(rs_new_high_before_price.loc[index]) else "daily_new_high",
                        }
                    )
            if daily_rs_rating is not None and index in daily_rs_rating.index and pd.notna(daily_rs_rating.loc[index]):
                daily_rs_rating_points.append({"time": time_value, "value": float(daily_rs_rating.loc[index])})

        if weekly_rs_rating is not None and not weekly_rs_rating.empty:
            for index, value in weekly_rs_rating.items():
                week_date = pd.Timestamp(index).date()
                if week_date < visible_start_date or week_date > resolved_as_of_date:
                    continue
                if pd.notna(value):
                    weekly_rs_rating_points.append({"time": week_date.isoformat(), "value": float(value)})

        payload = {
            "ticker": normalized_ticker,
            "benchmark_ticker": self.benchmark_ticker,
            "period": period,
            "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "latest_available_date": resolved_as_of_date.isoformat(),
            "data_source": data_source,
            "market_extension": market_extension,
            "rs_line": rs_points,
            "daily_rs_rating": daily_rs_rating_points,
            "weekly_rs_rating": weekly_rs_rating_points,
            "rs_markers": rs_markers,
            "setup_markers": setup_markers,
            "danger_signals": danger_signals,
            "fearzone_panel": fearzone_panel,
            "trend_template": trend_template_snapshot.to_dict() if trend_template_snapshot is not None else None,
            "vcs": vcs_snapshot.to_dict() if vcs_snapshot is not None else None,
            "sepa_dashboard": sepa_dashboard.to_dict() if sepa_dashboard is not None else None,
        }
        _write_chart_overlay_cache(cache_key, payload)
        return payload

    def get_chart_fundamentals_payload(self, ticker: str, *, earnings_limit: int = 4) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        ratings_repository = RatingsRepository(self.database_url) if self.database_url else None
        ratings_bundle = ratings_repository.load_latest_ticker_rating_bundle(normalized_ticker) if ratings_repository else None
        technical_indicator_ratings = ratings_repository.load_latest_technical_indicator_ratings_for_tickers([normalized_ticker]).get(normalized_ticker, {}) if ratings_repository else {}
        cached_entry = ratings_repository.load_latest_chart_fundamentals_cache_entry(normalized_ticker) if ratings_repository else None
        canslim_score_map = self._load_latest_stored_canslim_score_map([normalized_ticker])
        stored_canslim_score = canslim_score_map.get(normalized_ticker) or {}
        vcp_score_map = self._load_latest_stored_vcp_score_map([normalized_ticker])
        stored_vcp_score = vcp_score_map.get(normalized_ticker) or {}
        growth_acceleration_map = self._load_latest_stored_growth_acceleration_map([normalized_ticker])
        stored_growth_acceleration = growth_acceleration_map.get(normalized_ticker) or {}
        technical_snapshot = ratings_repository.load_latest_technical_rating_snapshots_for_tickers([normalized_ticker], allow_older_as_of_date=True).get(normalized_ticker, {}) if ratings_repository else {}

        canslim_snapshot = self._load_latest_stored_canslim_snapshot(normalized_ticker)
        if canslim_snapshot is None and ratings_repository and ratings_bundle:
            fundamentals_snapshot = ratings_bundle.get("fundamentals_snapshot")
            canslim_as_of_date = dt.date.today()
            if isinstance(fundamentals_snapshot, dict):
                as_of_text = str(fundamentals_snapshot.get("as_of_date") or "").strip()
                if as_of_text:
                    try:
                        canslim_as_of_date = dt.date.fromisoformat(as_of_text)
                    except ValueError:
                        canslim_as_of_date = dt.date.today()
            technical_rating_snapshot = ratings_repository.load_latest_technical_rating_snapshots_for_tickers(
                [normalized_ticker],
                as_of_date=canslim_as_of_date,
                allow_older_as_of_date=True,
            ).get(normalized_ticker)
            frame_map = load_many_ticker_windows(
                [normalized_ticker, self.benchmark_ticker],
                canslim_as_of_date,
                CANSLIM_HISTORY_DAYS,
                database_url=self.database_url,
            )
            insider_signal_map = load_finviz_insider_signal_map(
                [normalized_ticker],
                as_of_date=canslim_as_of_date,
                lookback_days=CANSLIM_INSIDER_LOOKBACK_DAYS,
                artifacts_dir=self.repository.artifacts_dir,
            )
            benchmark_frame = frame_map.get(self.benchmark_ticker.upper())
            if benchmark_frame is None:
                benchmark_frame = frame_map.get(self.benchmark_ticker)
            ticker_frame = frame_map.get(normalized_ticker)
            if benchmark_frame is not None and not benchmark_frame.empty:
                hit, _failure_reason = evaluate_canslim_ticker(
                    UniverseTicker(symbol=normalized_ticker),
                    current=fundamentals_snapshot if isinstance(fundamentals_snapshot, dict) else None,
                    technical=technical_rating_snapshot if isinstance(technical_rating_snapshot, dict) else None,
                    frame=ticker_frame,
                    benchmark_metrics=compute_canslim_frame_metrics(benchmark_frame),
                    as_of_date=canslim_as_of_date,
                    insider_signal=insider_signal_map.get(normalized_ticker),
                )
                canslim_snapshot = hit.to_dict() if hit is not None else None

        if _chart_fundamentals_cache_is_complete(cached_entry):
            return {
                "ticker": normalized_ticker,
                "earnings_eps_history": list(cached_entry.get("earnings_eps_history") or [])[: max(1, earnings_limit)],
                "holders_float_held_by_institutions_pct": cached_entry.get("holders_float_held_by_institutions_pct"),
                "revenue_yoy_pct": cached_entry.get("revenue_yoy_pct"),
                "earnings_yoy_pct": cached_entry.get("earnings_yoy_pct"),
                "implied_move": cached_entry.get("implied_move"),
                "fundamentals_snapshot": ratings_bundle.get("fundamentals_snapshot") if ratings_bundle else None,
                "rating_snapshot": ratings_bundle.get("rating_snapshot") if ratings_bundle else None,
                "fundamental_rank": ratings_bundle.get("fundamental_rank") if ratings_bundle else None,
                "rating_diagnostics": ratings_bundle.get("rating_diagnostics") if ratings_bundle else None,
                "technical_indicator_ratings": technical_indicator_ratings,
                "canslim_v2_score": stored_canslim_score.get("canslim_score"),
                "canslim_v2_max_score": stored_canslim_score.get("canslim_max_score"),
                "canslim_v2_rank": stored_canslim_score.get("canslim_rank"),
                "vcp_score": stored_vcp_score.get("vcp_score"),
                "vcp_rating": stored_vcp_score.get("vcp_rating"),
                "vcp_execution_state": stored_vcp_score.get("vcp_execution_state"),
                "vcp_pattern_type": stored_vcp_score.get("vcp_pattern_type"),
                "vcp_signal_date": stored_vcp_score.get("vcp_signal_date"),
                "canslim_snapshot": canslim_snapshot,
                "diagnostics": _chart_cache_diagnostics(cached_entry),
            }

        earnings_rows, holders_pct, revenue_yoy_pct, earnings_yoy_pct, browser_diagnostics = _load_yahoo_earnings_and_holders_playwright(
            normalized_ticker,
            earnings_limit=max(earnings_limit, 8),
        )
        implied_move, options_diagnostics = _load_yahoo_implied_move_playwright(normalized_ticker)
        merged_payload = _merge_chart_fundamentals_cache_fields(
            cached_entry,
            earnings_rows=earnings_rows,
            holders_pct=holders_pct,
            revenue_yoy_pct=revenue_yoy_pct,
            earnings_yoy_pct=earnings_yoy_pct,
            implied_move=implied_move,
        )
        if ratings_repository:
            ratings_repository.ensure_ticker_metadata_stub(normalized_ticker, source="chart-fundamentals-cache")
            ratings_repository.upsert_chart_fundamentals_cache_entry(
                ticker=normalized_ticker,
                as_of_date=dt.date.today(),
                earnings_eps_history=list(merged_payload["earnings_eps_history"]),
                holders_float_held_by_institutions_pct=merged_payload["holders_float_held_by_institutions_pct"],
                revenue_yoy_pct=merged_payload["revenue_yoy_pct"],
                earnings_yoy_pct=merged_payload["earnings_yoy_pct"],
                implied_move=merged_payload["implied_move"],
                source_summary={
                    "source": "yahoo-playwright",
                    "diagnostics": {
                        "earnings": browser_diagnostics["earnings"],
                        "holders": browser_diagnostics["holders"],
                        "statistics": browser_diagnostics["statistics"],
                        "options": options_diagnostics,
                    },
                    "cached_entry_used": bool(cached_entry),
                },
            )
        return {
            "ticker": normalized_ticker,
            "earnings_eps_history": list(merged_payload["earnings_eps_history"])[: max(1, earnings_limit)],
            "holders_float_held_by_institutions_pct": merged_payload["holders_float_held_by_institutions_pct"],
            "revenue_yoy_pct": merged_payload["revenue_yoy_pct"],
            "earnings_yoy_pct": merged_payload["earnings_yoy_pct"],
            "implied_move": merged_payload["implied_move"],
            "fundamentals_snapshot": ratings_bundle.get("fundamentals_snapshot") if ratings_bundle else None,
            "rating_snapshot": ratings_bundle.get("rating_snapshot") if ratings_bundle else None,
            "fundamental_rank": ratings_bundle.get("fundamental_rank") if ratings_bundle else None,
            "rating_diagnostics": ratings_bundle.get("rating_diagnostics") if ratings_bundle else None,
            "technical_snapshot": technical_snapshot,
            "technical_indicator_ratings": technical_indicator_ratings,
            "canslim_v2_score": stored_canslim_score.get("canslim_score"),
            "canslim_v2_max_score": stored_canslim_score.get("canslim_max_score"),
            "canslim_v2_rank": stored_canslim_score.get("canslim_rank"),
            "vcp_score": stored_vcp_score.get("vcp_score"),
            "vcp_rating": stored_vcp_score.get("vcp_rating"),
            "vcp_execution_state": stored_vcp_score.get("vcp_execution_state"),
            "vcp_pattern_type": stored_vcp_score.get("vcp_pattern_type"),
            "vcp_signal_date": stored_vcp_score.get("vcp_signal_date"),
            "growth_acceleration_score": stored_growth_acceleration.get("growth_acceleration_score"),
            "growth_acceleration_label": stored_growth_acceleration.get("growth_acceleration_label"),
            "growth_acceleration_pass_count": stored_growth_acceleration.get("growth_acceleration_pass_count"),
            "growth_acceleration_signal_date": stored_growth_acceleration.get("growth_acceleration_signal_date"),
            "canslim_snapshot": canslim_snapshot,
            "diagnostics": {
                "earnings": browser_diagnostics["earnings"],
                "holders": browser_diagnostics["holders"],
                "statistics": browser_diagnostics["statistics"],
                "options": options_diagnostics,
            },
        }

    def _load_latest_stored_canslim_snapshot(self, ticker: str) -> dict[str, Any] | None:
        normalized_ticker = str(ticker or "").strip().upper()
        if not normalized_ticker:
            return None
        for metadata in self.repository.list_recent_watchlists(limit=400, include_deprecated=False):
            if str(metadata.get("group_key") or "") != "canslim":
                continue
            payload = self._load_canslim_snapshot_from_watchlist_metadata(metadata, normalized_ticker)
            if payload is not None:
                return payload
        return None

    def _load_canslim_snapshot_from_watchlist_metadata(self, metadata: dict[str, Any], ticker: str) -> dict[str, Any] | None:
        stem = str(metadata.get("stem") or "").strip()
        if not stem:
            return None
        entries = self.repository.load_watchlist(stem)
        payload = self._normalize_canslim_snapshot(self._extract_stored_canslim_hit(entries, ticker))
        if payload is not None and self._is_complete_canslim_snapshot(payload):
            return payload

        path_text = str(metadata.get("path") or "").strip()
        if not path_text:
            return payload if self._is_complete_canslim_snapshot(payload or {}) else None
        path = Path(path_text)
        if path.name != "watchlist.json":
            return payload if self._is_complete_canslim_snapshot(payload or {}) else None
        raw_path = path.with_name("raw_results.json")
        if not raw_path.exists():
            return payload if self._is_complete_canslim_snapshot(payload or {}) else None
        try:
            raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except Exception:
            return payload if self._is_complete_canslim_snapshot(payload or {}) else None
        hits = raw_payload.get("hits") if isinstance(raw_payload, dict) else None
        if not isinstance(hits, list):
            return payload if self._is_complete_canslim_snapshot(payload or {}) else None
        raw_hit = self._normalize_canslim_snapshot(self._extract_stored_canslim_hit(hits, ticker))
        if raw_hit is not None:
            return raw_hit
        return payload if self._is_complete_canslim_snapshot(payload or {}) else None

    def _extract_stored_canslim_hit(self, rows: list[dict[str, Any]], ticker: str) -> dict[str, Any] | None:
        normalized_ticker = str(ticker or "").strip().upper()
        for item in rows:
            if not isinstance(item, dict):
                continue
            if str(item.get("ticker") or "").strip().upper() != normalized_ticker:
                continue
            return dict(item)
        return None

    def _normalize_canslim_snapshot(self, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        normalized = dict(payload)
        letter_scores = normalized.get("letter_scores")
        if isinstance(letter_scores, dict):
            raw_passes = normalized.get("letter_passes")
            normalized_passes: dict[str, bool] = {}
            if isinstance(raw_passes, dict):
                normalized_passes.update({str(key): bool(value) for key, value in raw_passes.items()})
            for key, value in letter_scores.items():
                if key in normalized_passes:
                    continue
                normalized_passes[str(key)] = bool(value)
            normalized["letter_passes"] = normalized_passes
        if not isinstance(normalized.get("leader_flags"), list):
            normalized["leader_flags"] = []
        return normalized

    def _is_complete_canslim_snapshot(self, payload: dict[str, Any]) -> bool:
        return (
            isinstance(payload.get("letter_scores"), dict)
            and isinstance(payload.get("letter_passes"), dict)
            and isinstance(payload.get("metrics"), dict)
            and isinstance(payload.get("reasons"), list)
            and isinstance(payload.get("leader_flags"), list)
        )

    def _load_latest_stored_canslim_score_map(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        return self.repository.load_latest_stored_canslim_score_map(tickers)

    def _load_latest_stored_vcp_score_map(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        return self.repository.load_latest_stored_vcp_score_map(tickers)

    def _load_latest_stored_growth_acceleration_map(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        return self.repository.load_latest_stored_growth_acceleration_map(tickers)

    def get_top_ratings_payload(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        rating_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        if not self.database_url:
            return {
                "as_of_date": None,
                "limit": limit,
                "rating_status": rating_status,
                "sector": sector,
                "rows": [],
                "status_counts": {},
                "database_configured": False,
            }
        payload = RatingsRepository(self.database_url).list_top_rating_snapshots(
            as_of_date=as_of_date,
            limit=limit,
            rating_status=rating_status,
            sector=sector,
        )
        resolved_as_of_raw = str(payload.get("as_of_date") or "").strip()
        resolved_as_of_date = dt.date.fromisoformat(resolved_as_of_raw) if resolved_as_of_raw else None
        self._attach_top_rows_canslim_scores(payload.get("rows", []))
        self._attach_top_rows_technical_indicator_ratings(payload.get("rows", []), as_of_date=resolved_as_of_date)
        self._attach_top_rows_latest_scanner_hit_counts(payload.get("rows", []))
        payload["limit"] = max(1, min(int(limit), 500))
        payload["rating_status"] = str(rating_status or "").strip().lower() or "ok"
        payload["sector"] = str(sector or "").strip()
        payload["database_configured"] = True
        return payload

    def get_top_technical_ratings_payload(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        technical_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        if not self.database_url:
            return {
                "as_of_date": None,
                "limit": limit,
                "technical_status": technical_status,
                "sector": sector,
                "rows": [],
                "status_counts": {},
                "database_configured": False,
            }
        payload = RatingsRepository(self.database_url).list_top_technical_rating_snapshots(
            as_of_date=as_of_date,
            limit=limit,
            technical_status=technical_status,
            sector=sector,
        )
        resolved_as_of_raw = str(payload.get("as_of_date") or "").strip()
        resolved_as_of_date = dt.date.fromisoformat(resolved_as_of_raw) if resolved_as_of_raw else None
        self._attach_top_rows_canslim_scores(payload.get("rows", []))
        self._attach_top_rows_technical_indicator_ratings(payload.get("rows", []), as_of_date=resolved_as_of_date)
        payload["limit"] = max(1, min(int(limit), 500))
        payload["technical_status"] = str(technical_status or "").strip().lower() or "ok"
        payload["sector"] = str(sector or "").strip()
        payload["database_configured"] = True
        return payload

    def get_top_technical_indicator_ratings_payload(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        technical_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        if not self.database_url:
            return {
                "as_of_date": None,
                "limit": limit,
                "technical_status": technical_status,
                "sector": sector,
                "rows": [],
                "status_counts": {},
                "database_configured": False,
            }
        payload = RatingsRepository(self.database_url).list_top_technical_indicator_rating_snapshots(
            as_of_date=as_of_date,
            limit=limit,
            technical_status=technical_status,
            sector=sector,
        )
        self._attach_top_rows_canslim_scores(payload.get("rows", []))
        payload["limit"] = max(1, min(int(limit), 500))
        payload["technical_status"] = str(technical_status or "").strip().lower() or "ok"
        payload["sector"] = str(sector or "").strip()
        payload["database_configured"] = True
        return payload

    def get_chart_insider_payload(
        self,
        ticker: str,
        *,
        lookback_days: int = 14,
        as_of_date: dt.date | None = None,
    ) -> dict[str, Any]:
        normalized_ticker = str(ticker or "").strip().upper()
        resolved_as_of_date = as_of_date or dt.date.today()
        normalized_lookback_days = max(1, int(lookback_days))
        window_start_date = resolved_as_of_date - dt.timedelta(days=normalized_lookback_days)
        resolved_as_of_iso = resolved_as_of_date.isoformat()
        cache_window = self.insider_repository.load_cache_window(
            ticker=normalized_ticker,
            as_of_date=resolved_as_of_iso,
            lookback_days=normalized_lookback_days,
        )
        cache_status = "hit" if cache_window else "miss"
        fetch_status = "skipped"
        notice: str | None = None

        if not self.insider_repository.is_cache_window_fresh(cache_window, ttl_hours=_INSIDER_CACHE_TTL_HOURS):
            cache_status = "stale" if cache_window else "miss"
            try:
                fetched = fetch_insider_trades_window(
                    tickers=[normalized_ticker],
                    as_of_date=resolved_as_of_date,
                    lookback_days=normalized_lookback_days,
                )
                cache_window = {
                    "ticker": normalized_ticker,
                    "requested_tickers": fetched.get("requested_tickers", [normalized_ticker]),
                    "as_of_date": resolved_as_of_iso,
                    "lookback_days": normalized_lookback_days,
                    "refreshed_at": str(fetched.get("generated_at") or ""),
                    "entries": fetched.get("entries", []),
                }
                self.insider_repository.save_cache_window(
                    ticker=normalized_ticker,
                    as_of_date=resolved_as_of_iso,
                    lookback_days=normalized_lookback_days,
                    refreshed_at=cache_window["refreshed_at"],
                    entries=list(cache_window.get("entries", [])) if isinstance(cache_window.get("entries"), list) else [],
                    requested_tickers=list(cache_window.get("requested_tickers", [normalized_ticker])),
                    source=str(fetched.get("source") or "sec_form4_submissions"),
                )
                fetch_status = "fetched"
            except Exception as exc:
                logger.warning("Insider fetch failed for %s: %s", normalized_ticker, exc)
                fetch_status = "failed"
                notice = f"Live insider refresh failed: {exc}"

        entries = _normalize_insider_entries(
            cache_window.get("entries") if isinstance(cache_window, dict) else [],
            ticker=normalized_ticker,
            window_start_date=window_start_date,
            resolved_as_of_date=resolved_as_of_date,
        )

        entries.sort(
            key=lambda item: (
                item.get("transaction_date") or "",
                item.get("filing_date") or "",
                item.get("gross_amount") or 0.0,
            ),
            reverse=True,
        )

        total_buy_amount = round(
            sum(float(item.get("gross_amount") or 0.0) for item in entries if item.get("type") == "BUY"),
            2,
        )
        total_sell_amount = round(
            sum(float(item.get("gross_amount") or 0.0) for item in entries if item.get("type") == "SELL"),
            2,
        )
        return {
            "ticker": normalized_ticker,
            "requested_as_of_date": as_of_date.isoformat() if as_of_date else None,
            "resolved_as_of_date": resolved_as_of_date.isoformat(),
            "lookback_days": normalized_lookback_days,
            "window_start_date": window_start_date.isoformat(),
            "window_end_date": resolved_as_of_date.isoformat(),
            "generated_at": _coerce_iso_datetime(cache_window.get("refreshed_at")) if isinstance(cache_window, dict) else None,
            "cache_status": cache_status,
            "fetch_status": fetch_status,
            "notice": notice,
            "entries": entries,
            "summary": {
                "total_count": len(entries),
                "buy_count": sum(1 for item in entries if item.get("type") == "BUY"),
                "sell_count": sum(1 for item in entries if item.get("type") == "SELL"),
                "total_buy_amount": total_buy_amount,
                "total_sell_amount": total_sell_amount,
                "net_amount": round(total_buy_amount - total_sell_amount, 2),
            },
        }

    def _load_chart_frames(
        self,
        *,
        ticker: str,
        benchmark_ticker: str,
        required_start_date: dt.date,
        start_date: dt.date,
        end_date: dt.date,
        warmup_trading_days: int,
    ) -> tuple[pd.DataFrame | None, pd.DataFrame | None, str]:
        if self.market_data_source == "database-first" and self.database_url:
            frames = load_many_ticker_windows_for_range(
                [ticker, benchmark_ticker],
                start_date=start_date,
                end_date=end_date,
                trading_days_needed=warmup_trading_days,
                database_url=self.database_url,
            )
            ticker_frame = _normalize_download_frame(frames.get(ticker))
            benchmark_frame = _normalize_download_frame(frames.get(benchmark_ticker))
            if _frame_covers_requested_window(ticker_frame, start_date=required_start_date, end_date=end_date):
                if benchmark_frame is None or benchmark_frame.empty:
                    benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
                    if benchmark_frame is not None and not benchmark_frame.empty:
                        return ticker_frame, benchmark_frame, "database+ticker/internet+benchmark"
                elif not _frame_covers_requested_window(benchmark_frame, start_date=required_start_date, end_date=end_date):
                    benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
                    if benchmark_frame is not None and not benchmark_frame.empty:
                        return ticker_frame, benchmark_frame, "database+ticker/internet+benchmark"
                return ticker_frame, benchmark_frame, "database"

        ticker_frame = _download_history_frame(ticker=ticker, start_date=start_date, end_date=end_date)
        benchmark_frame = _download_history_frame(ticker=benchmark_ticker, start_date=start_date, end_date=end_date)
        return ticker_frame, benchmark_frame, "internet"

    def _enrich_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        universe_index = self._get_universe_index()
        overrides = load_ticker_theme_overrides()
        theme_catalog = self._get_theme_catalog()
        latest_market_map = self._load_latest_market_snapshot_map(entries)
        enriched: list[dict[str, Any]] = []
        for raw_entry in entries:
            entry = dict(raw_entry)
            ticker = normalize_ticker_symbol(str(entry.get("ticker", "")))
            metadata = universe_index.get(ticker)
            sector = _coalesce_text(entry.get("sector"), metadata.sector if metadata else None)
            industry = _coalesce_text(entry.get("industry"), metadata.industry if metadata else None)
            exchange = _coalesce_text(entry.get("exchange"), metadata.exchange if metadata else None)
            theme_tags = _normalize_theme_tags(entry.get("theme_tags"))
            if not theme_tags and ticker:
                theme_tags = infer_theme_tags_for_ticker(
                    ticker=ticker,
                    sector=sector,
                    industry=industry,
                    catalog=theme_catalog,
                    overrides=overrides,
                )
            if ticker:
                entry["ticker"] = ticker
            if sector:
                entry["sector"] = sector
            if industry:
                entry["industry"] = industry
            if exchange:
                entry["exchange"] = exchange
            if theme_tags:
                entry["theme_tags"] = theme_tags
            latest_market = latest_market_map.get(ticker)
            if latest_market:
                entry["latest_trade_date"] = latest_market["trade_date"]
                entry["current_volume"] = latest_market["volume"]
                entry["current_close"] = latest_market["close"]
                entry["daily_change_pct"] = latest_market["change_pct"]
            enriched.append(entry)
        self._attach_entry_latest_rating_snapshots(enriched)
        return enriched

    def _prepare_scanner_top_hit_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        universe_index = self._get_universe_index()
        normalized: list[dict[str, Any]] = []
        for raw_entry in entries:
            entry = dict(raw_entry)
            ticker = normalize_ticker_symbol(str(entry.get("ticker", "")))
            metadata = universe_index.get(ticker)
            sector = _coalesce_text(entry.get("sector"), metadata.sector if metadata else None)
            industry = _coalesce_text(entry.get("industry"), metadata.industry if metadata else None)
            exchange = _coalesce_text(entry.get("exchange"), metadata.exchange if metadata else None)
            if ticker:
                entry["ticker"] = ticker
            if sector:
                entry["sector"] = sector
            if industry:
                entry["industry"] = industry
            if exchange:
                entry["exchange"] = exchange
            normalized.append(entry)
        return normalized

    def _load_latest_market_snapshot_map(self, entries: list[dict[str, Any]]) -> dict[str, dict[str, float | int | str | None]]:
        if not self.database_url:
            return {}
        tickers = [
            normalize_ticker_symbol(str(item.get("ticker") or ""))
            for item in entries
            if isinstance(item, dict) and str(item.get("ticker") or "").strip()
        ]
        normalized = [ticker for ticker in tickers if ticker]
        if not normalized:
            return {}
        try:
            frames = load_many_ticker_windows(
                normalized,
                dt.date.today(),
                2,
                database_url=self.database_url,
            )
        except Exception as exc:
            logger.warning("Watchlist latest market enrichment unavailable; continuing without DB day-volume/change data: %s", exc)
            return {}

        snapshots: dict[str, dict[str, float | int | str | None]] = {}
        for ticker, frame in frames.items():
            if frame is None or frame.empty:
                continue
            latest = frame.iloc[-1]
            latest_index = frame.index.max()
            latest_date = latest_index.date().isoformat() if hasattr(latest_index, "date") else str(latest_index)
            latest_close = _coerce_optional_float(latest.get("Close"))
            previous_close = None
            if len(frame.index) >= 2:
                previous_close = _coerce_optional_float(frame.iloc[-2].get("Close"))
            change_pct = None
            if latest_close is not None and previous_close is not None and previous_close > 0:
                change_pct = ((latest_close / previous_close) - 1.0) * 100.0
            snapshots[ticker] = {
                "trade_date": latest_date,
                "close": latest_close,
                "volume": int(round(_coerce_float(latest.get("Volume")))),
                "change_pct": change_pct,
            }
        return snapshots

    def _merge_scanner_top_hit_entry(self, bucket: dict[str, Any], entry: dict[str, Any]) -> None:
        company = _coalesce_text(bucket.get("company"), entry.get("company_name"), entry.get("company"))
        sector = _coalesce_text(bucket.get("sector"), entry.get("sector"))
        industry = _coalesce_text(bucket.get("industry"), entry.get("industry"))
        day_close = bucket.get("day_close")
        if day_close is None:
            day_close = _resolve_entry_display_price(entry)
        change_pct = bucket.get("change_pct")
        if change_pct is None:
            change_pct = _resolve_entry_change_pct(entry)
        perf_year_pct = bucket.get("perf_year_pct")
        if perf_year_pct is None:
            perf_year_pct = _coerce_optional_float(entry.get("perf_year_pct"))
        perf_ytd_pct = bucket.get("perf_ytd_pct")
        if perf_ytd_pct is None:
            perf_ytd_pct = _coerce_optional_float(entry.get("perf_ytd_pct"))
        rs_rating = bucket.get("rs_rating")
        if rs_rating is None:
            rs_rating = _coerce_optional_float(entry.get("rs_rating"))
        ta_rating = bucket.get("ta_rating")
        if ta_rating is None:
            ta_rating = _coerce_optional_float(entry.get("ta_rating"))
        fa_rating = bucket.get("fa_rating")
        if fa_rating is None:
            fa_rating = _coerce_optional_float(entry.get("fa_rating"))
        canslim_score = bucket.get("canslim_score")
        if canslim_score is None:
            canslim_score = _coerce_optional_int(entry.get("canslim_score"))
        canslim_max_score = bucket.get("canslim_max_score")
        if canslim_max_score is None:
            canslim_max_score = _coerce_optional_int(entry.get("canslim_max_score"))
        vcp_score = bucket.get("vcp_score")
        if vcp_score is None:
            vcp_score = _coerce_optional_float(entry.get("vcp_score"))
        vcp_rating = _coalesce_text(bucket.get("vcp_rating"), entry.get("vcp_rating"), entry.get("score_label"))
        industry_group_rs_rank = bucket.get("industry_group_rs_rank")
        if industry_group_rs_rank is None:
            industry_group_rs_rank = _coerce_optional_float(entry.get("industry_group_rs_rank"))
        growth_acceleration_score = bucket.get("growth_acceleration_score")
        if growth_acceleration_score is None:
            growth_acceleration_score = _coerce_optional_float(entry.get("growth_acceleration_score") or entry.get("acceleration_score"))
        growth_acceleration_label = _coalesce_text(bucket.get("growth_acceleration_label"), entry.get("growth_acceleration_label"), entry.get("acceleration_label"))
        technical_indicator_ratings = bucket.get("technical_indicator_ratings")
        if not isinstance(technical_indicator_ratings, dict) or not technical_indicator_ratings:
            raw_indicator_ratings = entry.get("technical_indicator_ratings")
            technical_indicator_ratings = raw_indicator_ratings if isinstance(raw_indicator_ratings, dict) else {}
        if company:
            bucket["company"] = company
        if sector:
            bucket["sector"] = sector
        if industry:
            bucket["industry"] = industry
        bucket["day_close"] = day_close
        bucket["change_pct"] = change_pct
        bucket["perf_year_pct"] = perf_year_pct
        bucket["perf_ytd_pct"] = perf_ytd_pct
        bucket["rs_rating"] = rs_rating
        bucket["ta_rating"] = ta_rating
        bucket["fa_rating"] = fa_rating
        bucket["canslim_score"] = canslim_score
        bucket["canslim_max_score"] = canslim_max_score
        bucket["vcp_score"] = vcp_score
        bucket["vcp_rating"] = vcp_rating
        bucket["industry_group_rs_rank"] = industry_group_rs_rank
        bucket["growth_acceleration_score"] = growth_acceleration_score
        bucket["growth_acceleration_label"] = growth_acceleration_label
        bucket["technical_indicator_ratings"] = technical_indicator_ratings

    def _select_scanner_top_hit_live_cards(self, board_payload: dict[str, Any]) -> list[dict[str, Any]]:
        cards = [dict(item) for item in board_payload.get("cards", []) if isinstance(item, dict)]
        card_config_by_id = {
            str(item.get("id") or ""): item
            for item in _SCANNER_BOARD_CONFIG
            if str(item.get("id") or "").strip()
        }
        candidate_cards = [
            item
            for item in cards
            if item.get("available")
            and str(item.get("stem") or "").strip()
            and str(card_config_by_id.get(str(item.get("id") or ""), {}).get("bias_group") or "bullish") != "bearish"
        ]
        candidate_cards_by_id = {
            str(item.get("id") or ""): item
            for item in candidate_cards
            if str(item.get("id") or "").strip()
        }
        live_cards = [
            item
            for item in candidate_cards
            if str(item.get("timeframe") or "").strip().lower() != "weekly"
        ]
        fallback_pairs = (
            ("daily_rs_new_high", "weekly_rs_new_high"),
            ("rs", "weekly_rs_before_price"),
        )
        live_card_ids = {str(item.get("id") or "") for item in live_cards}
        for primary_id, fallback_id in fallback_pairs:
            if primary_id in live_card_ids:
                continue
            fallback_card = candidate_cards_by_id.get(fallback_id)
            if fallback_card is None:
                continue
            live_cards.append(fallback_card)
            live_card_ids.add(fallback_id)
        return live_cards

    def _build_latest_scanner_hit_count_map(self, *, now: dt.datetime | None = None) -> dict[str, int]:
        board_payload = self.get_scanner_board(now=now)
        live_cards = self._select_scanner_top_hit_live_cards(board_payload)
        counts: dict[str, int] = {}
        for card in live_cards:
            stem = str(card.get("stem") or "").strip()
            if not stem:
                continue
            entries = self._prepare_scanner_top_hit_entries(
                self._filter_excluded_entries(self.repository.load_watchlist(stem))
            )
            seen_tickers: set[str] = set()
            for entry in entries:
                ticker = normalize_ticker_symbol(str(entry.get("ticker") or ""))
                if not ticker or ticker in seen_tickers:
                    continue
                seen_tickers.add(ticker)
                counts[ticker] = counts.get(ticker, 0) + 1
        return counts

    def _attach_latest_market_snapshots(self, rows_by_ticker: dict[str, dict[str, Any]], tickers: list[str]) -> None:
        if not self.database_url or not tickers:
            return
        try:
            frames = load_many_ticker_windows(
                tickers,
                dt.date.today(),
                2,
                database_url=self.database_url,
            )
        except Exception as exc:
            logger.warning("Scanner top hits latest market enrichment unavailable; continuing without DB day-volume/change data: %s", exc)
            return
        for ticker in tickers:
            row = rows_by_ticker.get(ticker)
            frame = frames.get(ticker)
            if row is None or frame is None or frame.empty:
                continue
            latest = frame.iloc[-1]
            latest_close = _coerce_optional_float(latest.get("Close"))
            previous_close = _coerce_optional_float(frame.iloc[-2].get("Close")) if len(frame.index) >= 2 else None
            change_pct = row.get("change_pct")
            if latest_close is not None and row.get("day_close") is None:
                row["day_close"] = latest_close
            if change_pct is None and latest_close is not None and previous_close is not None and previous_close > 0:
                row["change_pct"] = ((latest_close / previous_close) - 1.0) * 100.0

    def _attach_latest_rating_snapshots(self, rows_by_ticker: dict[str, dict[str, Any]], tickers: list[str]) -> None:
        if not self.database_url or not tickers:
            return
        repository = RatingsRepository(self.database_url)
        fundamental_map = repository.load_latest_rating_snapshots_for_tickers(tickers)
        technical_map = repository.load_latest_technical_rating_snapshots_for_tickers(tickers)
        technical_indicator_map = repository.load_latest_technical_indicator_ratings_for_tickers(tickers)
        canslim_map = self._load_latest_stored_canslim_score_map(tickers)
        vcp_map = self._load_latest_stored_vcp_score_map(tickers)
        growth_acceleration_map = self._load_latest_stored_growth_acceleration_map(tickers)
        for ticker in tickers:
            row = rows_by_ticker.get(ticker)
            if row is None:
                continue
            fundamental = fundamental_map.get(ticker) or {}
            technical = technical_map.get(ticker) or {}
            technical_indicator = technical_indicator_map.get(ticker) or {}
            canslim = canslim_map.get(ticker) or {}
            vcp = vcp_map.get(ticker) or {}
            growth_acceleration = growth_acceleration_map.get(ticker) or {}
            if not row.get("sector"):
                row["sector"] = _coalesce_text(row.get("sector"), fundamental.get("sector"), technical.get("sector"))
            row["perf_year_pct"] = _coerce_optional_float(fundamental.get("perf_year_pct"))
            row["perf_ytd_pct"] = _coerce_optional_float(fundamental.get("perf_ytd_pct"))
            row["fa_rating"] = _coerce_optional_float(fundamental.get("overall_rating"))
            row["fa_current_rank"] = _coerce_optional_int(fundamental.get("current_rank"))
            row["ta_rating"] = _coerce_optional_float(technical.get("overall_rating"))
            row["rs_rating"] = _coerce_optional_float(technical.get("leadership_score"))
            row["daily_rs_rating"] = _coerce_optional_float(technical.get("daily_rs_rating"))
            row["weekly_rs_rating"] = _coerce_optional_float(technical.get("weekly_rs_rating"))
            row["industry_group"] = _coalesce_text(technical.get("industry_group"))
            row["industry_group_rs_rank"] = _coerce_optional_float(technical.get("industry_group_rs_rank"))
            row["industry_group_member_count"] = _coerce_optional_int(technical.get("industry_group_member_count"))
            row["canslim_score"] = _coerce_optional_int(canslim.get("canslim_score"))
            row["canslim_max_score"] = _coerce_optional_int(canslim.get("canslim_max_score"))
            row["canslim_rank"] = _coerce_optional_int(canslim.get("canslim_rank"))
            row["vcp_score"] = _coerce_optional_float(vcp.get("vcp_score"))
            row["vcp_rating"] = _coalesce_text(vcp.get("vcp_rating"))
            row["growth_acceleration_score"] = _coerce_optional_float(growth_acceleration.get("growth_acceleration_score"))
            row["growth_acceleration_label"] = _coalesce_text(growth_acceleration.get("growth_acceleration_label"))
            row["growth_acceleration_pass_count"] = _coerce_optional_int(growth_acceleration.get("growth_acceleration_pass_count"))
            row["growth_acceleration_signal_date"] = _coalesce_text(growth_acceleration.get("growth_acceleration_signal_date"))
            row["technical_indicator_ratings"] = technical_indicator

    def _attach_top_rows_latest_scanner_hit_counts(
        self,
        rows: list[dict[str, Any]],
        *,
        now: dt.datetime | None = None,
    ) -> None:
        if not rows:
            return
        hit_count_map = self._build_latest_scanner_hit_count_map(now=now)
        for row in rows:
            ticker = normalize_ticker_symbol(str(row.get("ticker") or ""))
            row["latest_scanner_hit_count"] = int(hit_count_map.get(ticker, 0)) if ticker else 0

    def _attach_entry_technical_indicator_ratings(self, entries: list[dict[str, Any]]) -> None:
        if not self.database_url or not entries:
            return
        tickers = sorted(
            {
                normalize_ticker_symbol(str(entry.get("ticker") or ""))
                for entry in entries
                if normalize_ticker_symbol(str(entry.get("ticker") or ""))
            }
        )
        if not tickers:
            return
        technical_indicator_map = RatingsRepository(self.database_url).load_latest_technical_indicator_ratings_for_tickers(tickers)
        for entry in entries:
            ticker = normalize_ticker_symbol(str(entry.get("ticker") or ""))
            if not ticker:
                continue
            entry["technical_indicator_ratings"] = technical_indicator_map.get(ticker, {})

    def _attach_entry_latest_rating_snapshots(self, entries: list[dict[str, Any]]) -> None:
        if not self.database_url or not entries:
            return
        tickers = sorted(
            {
                normalize_ticker_symbol(str(entry.get("ticker") or ""))
                for entry in entries
                if normalize_ticker_symbol(str(entry.get("ticker") or ""))
            }
        )
        if not tickers:
            return
        repository = RatingsRepository(self.database_url)
        fundamental_map = repository.load_latest_rating_snapshots_for_tickers(tickers)
        technical_map = repository.load_latest_technical_rating_snapshots_for_tickers(tickers)
        canslim_map = self._load_latest_stored_canslim_score_map(tickers)
        vcp_map = self._load_latest_stored_vcp_score_map(tickers)
        growth_acceleration_map = self._load_latest_stored_growth_acceleration_map(tickers)
        for entry in entries:
            ticker = normalize_ticker_symbol(str(entry.get("ticker") or ""))
            if not ticker:
                continue
            fundamental = fundamental_map.get(ticker) or {}
            technical = technical_map.get(ticker) or {}
            canslim = canslim_map.get(ticker) or {}
            vcp = vcp_map.get(ticker) or {}
            growth_acceleration = growth_acceleration_map.get(ticker) or {}
            entry["perf_year_pct"] = _coerce_optional_float(fundamental.get("perf_year_pct"))
            entry["perf_ytd_pct"] = _coerce_optional_float(fundamental.get("perf_ytd_pct"))
            entry["fa_rating"] = _coerce_optional_float(fundamental.get("overall_rating"))
            entry["ta_rating"] = _coerce_optional_float(technical.get("overall_rating"))
            entry["rs_rating"] = _coerce_optional_float(technical.get("leadership_score"))
            entry["daily_rs_rating"] = _coerce_optional_float(technical.get("daily_rs_rating"))
            entry["weekly_rs_rating"] = _coerce_optional_float(technical.get("weekly_rs_rating"))
            entry["industry_group"] = _coalesce_text(technical.get("industry_group"))
            entry["industry_group_rs_rank"] = _coerce_optional_float(technical.get("industry_group_rs_rank"))
            entry["industry_group_member_count"] = _coerce_optional_int(technical.get("industry_group_member_count"))
            entry["canslim_score"] = _coerce_optional_int(canslim.get("canslim_score"))
            entry["canslim_max_score"] = _coerce_optional_int(canslim.get("canslim_max_score"))
            entry["canslim_rank"] = _coerce_optional_int(canslim.get("canslim_rank"))
            entry["vcp_score"] = _coerce_optional_float(vcp.get("vcp_score"))
            entry["vcp_rating"] = _coalesce_text(vcp.get("vcp_rating"))
            entry["growth_acceleration_score"] = _coerce_optional_float(growth_acceleration.get("growth_acceleration_score"))
            entry["growth_acceleration_label"] = _coalesce_text(growth_acceleration.get("growth_acceleration_label"))
            entry["growth_acceleration_pass_count"] = _coerce_optional_int(growth_acceleration.get("growth_acceleration_pass_count"))
            entry["growth_acceleration_signal_date"] = _coalesce_text(growth_acceleration.get("growth_acceleration_signal_date"))

    def _attach_top_rows_canslim_scores(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        tickers = sorted(
            {
                normalize_ticker_symbol(str(row.get("ticker") or ""))
                for row in rows
                if normalize_ticker_symbol(str(row.get("ticker") or ""))
            }
        )
        if not tickers:
            return
        canslim_map = self._load_latest_stored_canslim_score_map(tickers)
        for row in rows:
            ticker = normalize_ticker_symbol(str(row.get("ticker") or ""))
            if not ticker:
                continue
            canslim = canslim_map.get(ticker) or {}
            row["canslim_score"] = _coerce_optional_int(canslim.get("canslim_score"))
            row["canslim_max_score"] = _coerce_optional_int(canslim.get("canslim_max_score"))
            row["canslim_rank"] = _coerce_optional_int(canslim.get("canslim_rank"))

    def _attach_top_rows_technical_indicator_ratings(
        self,
        rows: list[dict[str, Any]],
        *,
        as_of_date: dt.date | None,
    ) -> None:
        if not self.database_url or not rows:
            return
        tickers = sorted(
            {
                normalize_ticker_symbol(str(row.get("ticker") or ""))
                for row in rows
                if normalize_ticker_symbol(str(row.get("ticker") or ""))
            }
        )
        if not tickers:
            return
        technical_indicator_map = RatingsRepository(self.database_url).load_latest_technical_indicator_ratings_for_tickers(
            tickers,
            as_of_date=as_of_date,
        )
        for row in rows:
            ticker = normalize_ticker_symbol(str(row.get("ticker") or ""))
            if not ticker:
                continue
            row["technical_indicator_ratings"] = technical_indicator_map.get(ticker, {})

    def _load_sector_momentum_map(self, rrg_service: Any | None) -> dict[str, dict[str, Any]]:
        if rrg_service is None:
            return {}
        cache_key = (
            self.benchmark_ticker,
            "sector",
            "3y",
            "weekly",
        )
        cached_payload = _read_sector_momentum_cache(cache_key)
        if cached_payload is not None:
            return cached_payload
        try:
            payload = rrg_service.get_universe_report(
                "sector",
                benchmark=self.benchmark_ticker,
                period="3y",
                trail_weeks=12,
                cadence="weekly",
            )
        except Exception as exc:
            logger.warning("Scanner top hits sector momentum unavailable; continuing without RRG enrichment: %s", exc)
            return {}
        result: dict[str, dict[str, Any]] = {}
        for item in payload.get("series", []):
            if not isinstance(item, dict):
                continue
            sector = _coalesce_text(item.get("label"))
            latest = item.get("latest") if isinstance(item.get("latest"), dict) else {}
            if not sector:
                continue
            result[sector] = {
                "sector": sector,
                "etf_ticker": str(item.get("ticker") or "").strip().upper(),
                "quadrant": str(item.get("quadrant") or "").strip(),
                "rs_ratio": _coerce_optional_float(latest.get("x")),
                "momentum": _coerce_optional_float(latest.get("y")),
                "as_of_date": str(latest.get("date") or "").strip() or None,
            }
        _write_sector_momentum_cache(cache_key, result)
        return result

    def _get_universe_index(self) -> dict[str, UniverseTicker]:
        if self._universe_index is not None:
            return self._universe_index
        try:
            universe = load_universe(load_app_config())
        except Exception as exc:
            logger.warning("Watchlist universe enrichment unavailable; continuing without universe metadata: %s", exc)
            universe = []
        self._universe_index = {
            normalize_ticker_symbol(item.symbol): item
            for item in universe
            if getattr(item, "symbol", "")
        }
        return self._universe_index

    def _get_theme_catalog(self) -> list[dict[str, object]]:
        if self._theme_catalog is not None:
            return self._theme_catalog
        try:
            self._theme_catalog = load_etf_catalog()
        except Exception as exc:
            logger.warning("Watchlist theme enrichment unavailable; continuing without ETF catalog: %s", exc)
            self._theme_catalog = []
        return self._theme_catalog

    def _get_excluded_tickers(self) -> set[str]:
        if self._excluded_tickers is not None:
            return self._excluded_tickers
        try:
            self._excluded_tickers = load_excluded_tickers(load_app_config())
        except Exception as exc:
            logger.warning("Watchlist exclusion filter unavailable; continuing without exclusions: %s", exc)
            self._excluded_tickers = set()
        return self._excluded_tickers

    def _filter_excluded_entries(self, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        excluded = self._get_excluded_tickers()
        if not excluded:
            return [item for item in entries if isinstance(item, dict)]
        filtered: list[dict[str, Any]] = []
        for item in entries:
            if not isinstance(item, dict):
                continue
            ticker = normalize_ticker_symbol(str(item.get("ticker") or ""))
            if not ticker or is_excluded_ticker(ticker, excluded):
                continue
            filtered.append(item)
        return filtered


def _empty_chart_payload(
    ticker: str,
    *,
    period: str = "18mo",
    requested_as_of_date: dt.date | None = None,
    resolved_as_of_date: dt.date | None = None,
    benchmark_ticker: str = "SPY",
    data_source: str = "internet",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "benchmark_ticker": benchmark_ticker,
        "period": period,
        "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
        "resolved_as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "latest_available_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "data_source": data_source,
        "candles": [],
        "volume": [],
        "ma20": [],
        "ma50": [],
        "ma200": [],
        "ema8": [],
        "ema21": [],
        "weekly_ema8": [],
        "ipo_vwap": [],
        "market_extension": _empty_market_extension_overlay(),
        "rs_line": [],
        "daily_rs_rating": [],
        "weekly_rs_rating": [],
        "rs_markers": [],
        "setup_markers": [],
        "danger_signals": {"as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None, "active_count": 0, "highest_severity": None, "signals": []},
        "fearzone_panel": {"rows": [], "signals": []},
        "trend_template": None,
        "vcs": None,
        "sepa_dashboard": None,
    }


def _empty_chart_overlay_payload(
    ticker: str,
    *,
    period: str = "18mo",
    requested_as_of_date: dt.date | None = None,
    resolved_as_of_date: dt.date | None = None,
    benchmark_ticker: str = "SPY",
    data_source: str = "internet",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "benchmark_ticker": benchmark_ticker,
        "period": period,
        "requested_as_of_date": requested_as_of_date.isoformat() if requested_as_of_date else None,
        "resolved_as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "latest_available_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None,
        "data_source": data_source,
        "market_extension": _empty_market_extension_overlay(),
        "rs_line": [],
        "daily_rs_rating": [],
        "weekly_rs_rating": [],
        "rs_markers": [],
        "setup_markers": [],
        "danger_signals": {"as_of_date": resolved_as_of_date.isoformat() if resolved_as_of_date else None, "active_count": 0, "highest_severity": None, "signals": []},
        "fearzone_panel": {"rows": [], "signals": []},
        "trend_template": None,
        "vcs": None,
        "sepa_dashboard": None,
    }


def _empty_market_extension_overlay() -> dict[str, Any]:
    return {
        "config": {
            "timeframe": "weekly",
            "ma_type": "sma",
            "length": 10,
            "warning_pct": 11.0,
            "extreme_pct": 15.0,
            "label": "10W SMA",
        },
        "line": [],
        "signals": [],
        "latest": None,
    }


def _normalize_download_frame(history: pd.DataFrame | None) -> pd.DataFrame | None:
    if history is None or history.empty:
        return None
    frame = history.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame = frame.rename(columns=str)
    for column in ("Open", "High", "Low", "Close", "Adj Close", "Volume"):
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    return frame if not frame.empty else None


def _build_chart_gex_payload(*, report: dict[str, Any]) -> dict[str, Any]:
    net_gex = _coerce_optional_float(report.get("net_gex"))
    gamma_flip = _coerce_optional_float(report.get("gamma_flip"))
    spot = _coerce_optional_float(report.get("underlying_price"))
    distance_to_flip_pct = None
    if spot not in (None, 0.0) and gamma_flip not in (None, 0.0):
        distance_to_flip_pct = round(((spot / gamma_flip) - 1.0) * 100.0, 2)
    return {
        "ticker": str(report.get("symbol") or "").strip().upper(),
        "available": True,
        "as_of": str(report.get("as_of") or ""),
        "spot": spot,
        "net_gex": net_gex,
        "gex_regime": "negative" if (net_gex or 0.0) < 0 else "positive",
        "gex_label": "Negative Gamma" if (net_gex or 0.0) < 0 else "Positive Gamma",
        "gamma_flip": gamma_flip,
        "distance_to_flip_pct": distance_to_flip_pct,
        "call_gex_total": _coerce_optional_float(report.get("call_gex_total")),
        "put_gex_total": _coerce_optional_float(report.get("put_gex_total")),
        "call_wall": _coerce_optional_float(report.get("call_wall")),
        "put_wall": _coerce_optional_float(report.get("put_wall")),
        "atm_pin_strike": _coerce_optional_float(report.get("atm_pin_strike")),
        "put_call_oi_ratio": _coerce_optional_float(report.get("put_call_oi_ratio")),
        "strike_count": _coerce_optional_int(report.get("strike_count")),
        "next_expiry": str(report.get("next_expiry") or ""),
        "next_monthly_expiry": str(report.get("next_monthly_expiry") or ""),
        "summary": str(report.get("summary") or ""),
        "methodology": str(report.get("methodology") or ""),
        "source_url": str(report.get("source_url") or ""),
        "plots": render_gamma_exposure_report_svgs(report),
    }


def _frame_covers_requested_window(
    frame: pd.DataFrame | None,
    *,
    start_date: dt.date,
    end_date: dt.date,
) -> bool:
    if frame is None or frame.empty:
        return False
    first_index = frame.index.min()
    first_date = first_index.date() if hasattr(first_index, "date") else first_index
    if first_date is None or first_date > start_date:
        return False
    return db_frame_has_recent_coverage(frame, end_date)


def _download_history_frame(*, ticker: str, start_date: dt.date, end_date: dt.date) -> pd.DataFrame | None:
    history = yf.download(
        tickers=ticker,
        start=start_date.isoformat(),
        end=(end_date + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    return _normalize_download_frame(history)


def _build_chart_payload_cache_key(
    *,
    ticker: str,
    period: str,
    as_of_date: dt.date | None,
    benchmark_ticker: str,
    market_data_source: str,
    include_setup_markers: bool,
) -> tuple[str, str, str, str, str, str]:
    return (
        str(ticker or "").strip().upper(),
        str(period or "18mo").strip().lower() or "18mo",
        as_of_date.isoformat() if as_of_date else "latest",
        str(benchmark_ticker or "SPY").strip().upper() or "SPY",
        str(market_data_source or "").strip().lower() or "internet",
        "setup-markers" if include_setup_markers else "base",
    )


def _read_chart_gex_cache(ticker: str) -> dict[str, Any] | None:
    now = time.time()
    with _chart_gex_cache_lock:
        cached_entry = _chart_gex_cache.get(ticker)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _chart_gex_cache.pop(ticker, None)
            return None
        return copy.deepcopy(payload)


def _write_chart_gex_cache(ticker: str, payload: dict[str, Any]) -> None:
    with _chart_gex_cache_lock:
        _chart_gex_cache[ticker] = (time.time() + _CHART_GEX_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _read_chart_payload_cache(key: tuple[str, str, str, str, str, str]) -> dict[str, Any] | None:
    now = time.time()
    with _chart_payload_cache_lock:
        cached_entry = _chart_payload_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _chart_payload_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_chart_payload_cache(key: tuple[str, str, str, str, str, str], payload: dict[str, Any]) -> None:
    with _chart_payload_cache_lock:
        _chart_payload_cache[key] = (time.time() + _CHART_PAYLOAD_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _read_scanner_top_hits_cache(key: tuple[str, str]) -> dict[str, Any] | None:
    now = time.time()
    with _scanner_top_hits_cache_lock:
        cached_entry = _scanner_top_hits_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _scanner_top_hits_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_scanner_top_hits_cache(key: tuple[str, str], payload: dict[str, Any]) -> None:
    with _scanner_top_hits_cache_lock:
        _scanner_top_hits_cache[key] = (time.time() + _SCANNER_TOP_HITS_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _read_sector_momentum_cache(key: tuple[str, str, str, str]) -> dict[str, dict[str, Any]] | None:
    now = time.time()
    with _sector_momentum_cache_lock:
        cached_entry = _sector_momentum_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _sector_momentum_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_sector_momentum_cache(key: tuple[str, str, str, str], payload: dict[str, dict[str, Any]]) -> None:
    with _sector_momentum_cache_lock:
        _sector_momentum_cache[key] = (time.time() + _SECTOR_MOMENTUM_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _clear_chart_payload_cache() -> None:
    with _chart_gex_cache_lock:
        _chart_gex_cache.clear()
    with _chart_payload_cache_lock:
        _chart_payload_cache.clear()
    with _chart_overlay_cache_lock:
        _chart_overlay_cache.clear()
    with _scanner_top_hits_cache_lock:
        _scanner_top_hits_cache.clear()
    with _sector_momentum_cache_lock:
        _sector_momentum_cache.clear()


def _build_chart_overlay_cache_key(
    *,
    ticker: str,
    period: str,
    as_of_date: dt.date | None,
    benchmark_ticker: str,
    market_data_source: str,
    include_setup_markers: bool,
) -> tuple[str, str, str, str, str, str]:
    return (
        str(ticker or "").strip().upper(),
        str(period or "18mo").strip().lower() or "18mo",
        as_of_date.isoformat() if as_of_date else "latest",
        str(benchmark_ticker or "SPY").strip().upper() or "SPY",
        str(market_data_source or "").strip().lower() or "internet",
        "setup-markers" if include_setup_markers else "base",
    )


def _read_chart_overlay_cache(key: tuple[str, str, str, str, str, str]) -> dict[str, Any] | None:
    now = time.time()
    with _chart_overlay_cache_lock:
        cached_entry = _chart_overlay_cache.get(key)
        if cached_entry is None:
            return None
        expires_at, payload = cached_entry
        if expires_at <= now:
            _chart_overlay_cache.pop(key, None)
            return None
        return copy.deepcopy(payload)


def _write_chart_overlay_cache(key: tuple[str, str, str, str, str, str], payload: dict[str, Any]) -> None:
    with _chart_overlay_cache_lock:
        _chart_overlay_cache[key] = (time.time() + _CHART_PAYLOAD_CACHE_TTL_SECONDS, copy.deepcopy(payload))


def _chart_fundamentals_cache_is_complete(entry: dict[str, Any] | None) -> bool:
    if not isinstance(entry, dict):
        return False
    earnings_rows = entry.get("earnings_eps_history")
    implied_move = entry.get("implied_move")
    return bool(
        isinstance(earnings_rows, list)
        and len(earnings_rows) > 0
        and entry.get("holders_float_held_by_institutions_pct") is not None
        and entry.get("revenue_yoy_pct") is not None
        and entry.get("earnings_yoy_pct") is not None
        and isinstance(implied_move, dict)
        and implied_move.get("percent_move") is not None
    )


def _merge_chart_fundamentals_cache_fields(
    cached_entry: dict[str, Any] | None,
    *,
    earnings_rows: list[dict[str, Any]],
    holders_pct: float | None,
    revenue_yoy_pct: float | None,
    earnings_yoy_pct: float | None,
    implied_move: dict[str, Any] | None,
) -> dict[str, Any]:
    cached_entry = cached_entry if isinstance(cached_entry, dict) else {}
    cached_earnings_rows = cached_entry.get("earnings_eps_history")
    cached_implied_move = cached_entry.get("implied_move")
    return {
        "earnings_eps_history": earnings_rows if earnings_rows else (cached_earnings_rows if isinstance(cached_earnings_rows, list) else []),
        "holders_float_held_by_institutions_pct": (
            holders_pct
            if holders_pct is not None
            else cached_entry.get("holders_float_held_by_institutions_pct")
        ),
        "revenue_yoy_pct": revenue_yoy_pct if revenue_yoy_pct is not None else cached_entry.get("revenue_yoy_pct"),
        "earnings_yoy_pct": earnings_yoy_pct if earnings_yoy_pct is not None else cached_entry.get("earnings_yoy_pct"),
        "implied_move": implied_move if implied_move is not None else (cached_implied_move if isinstance(cached_implied_move, dict) else None),
    }


def _chart_cache_diagnostics(entry: dict[str, Any]) -> dict[str, Any]:
    source_summary = entry.get("source_summary") if isinstance(entry.get("source_summary"), dict) else {}
    diagnostics = source_summary.get("diagnostics") if isinstance(source_summary.get("diagnostics"), dict) else {}
    if diagnostics:
        return diagnostics
    cache_attempt = {
        "cache": True,
        "as_of_date": entry.get("as_of_date"),
        "scraped_at": entry.get("scraped_at"),
        "updated_at": entry.get("updated_at"),
    }
    return {
        "earnings": {"status": "cache", "attempts": [cache_attempt]},
        "holders": {"status": "cache", "attempts": [cache_attempt]},
        "statistics": {"status": "cache", "attempts": [cache_attempt]},
        "options": {"status": "cache", "attempts": [cache_attempt]},
    }


def _load_yahoo_earnings_and_holders_playwright(
    ticker: str,
    *,
    earnings_limit: int = 4,
) -> tuple[list[dict[str, Any]], float | None, float | None, float | None, dict[str, dict[str, Any]]]:
    normalized_ticker = str(ticker or "").strip().upper()
    empty_result = (
        [],
        None,
        None,
        None,
        {
            "earnings": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
            "holders": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
            "statistics": {"status": "skipped", "reason": "missing_ticker", "attempts": []},
        },
    )
    if not normalized_ticker:
        return empty_result
    if not _YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT.exists():
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
            "holders": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
            "statistics": {
                "status": "error",
                "reason": "missing_probe_script",
                "attempts": [{"script": str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT)}],
            },
        }
        return [], None, None, None, diagnostics

    command = ["node", str(_YAHOO_ANALYSIS_HOLDERS_PROBE_SCRIPT), normalized_ticker]
    try:
        result = subprocess.run(
            command,
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        diagnostics = {
            "earnings": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
            "holders": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
            "statistics": {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]},
        }
        return [], None, None, None, diagnostics
    except Exception as exc:
        diagnostics = {
            "earnings": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
            "holders": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
            "statistics": {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]},
        }
        return [], None, None, None, diagnostics

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
            "holders": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
            "statistics": {
                "status": "error",
                "reason": "nonzero_exit",
                "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
            },
        }
        return [], None, None, None, diagnostics

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        diagnostics = {
            "earnings": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
            "holders": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
            "statistics": {
                "status": "error",
                "reason": "invalid_json",
                "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
            },
        }
        return [], None, None, None, diagnostics

    earnings_rows_raw = payload.get("earnings_eps_history")
    earnings_rows: list[dict[str, Any]] = []
    if isinstance(earnings_rows_raw, list):
        for row in earnings_rows_raw[: max(1, earnings_limit)]:
            if not isinstance(row, dict):
                continue
            date_value = str(row.get("date") or "").strip()
            if not date_value:
                continue
            earnings_rows.append(
                {
                    "date": date_value,
                    "eps_estimate": _coerce_optional_float(row.get("eps_estimate")),
                    "reported_eps": _coerce_optional_float(row.get("reported_eps")),
                    "surprise_pct": _coerce_optional_float(row.get("surprise_pct")),
                }
            )

    holders_pct = _coerce_optional_float(payload.get("holders_float_held_by_institutions_pct"))
    revenue_yoy_pct = _coerce_optional_float(payload.get("revenue_yoy_pct"))
    earnings_yoy_pct = _coerce_optional_float(payload.get("earnings_yoy_pct"))
    analysis_payload = payload.get("analysis_nz") if isinstance(payload.get("analysis_nz"), dict) else payload.get("analysis")
    holders_payload = payload.get("holders") if isinstance(payload.get("holders"), dict) else {}
    statistics_payload = payload.get("key_statistics_nz") if isinstance(payload.get("key_statistics_nz"), dict) else {}

    earnings_status = "ok" if earnings_rows else "empty"
    holders_status = "ok" if holders_pct is not None else "empty"
    statistics_status = "ok" if revenue_yoy_pct is not None or earnings_yoy_pct is not None else "empty"
    diagnostics = {
        "earnings": {
            "status": earnings_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": analysis_payload.get("final_url") if isinstance(analysis_payload, dict) else "",
                    "status_code": analysis_payload.get("status") if isinstance(analysis_payload, dict) else None,
                    "title": analysis_payload.get("title") if isinstance(analysis_payload, dict) else "",
                    "row_count": len(earnings_rows),
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
        "holders": {
            "status": holders_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": holders_payload.get("final_url") if isinstance(holders_payload, dict) else "",
                    "status_code": holders_payload.get("status") if isinstance(holders_payload, dict) else None,
                    "title": holders_payload.get("title") if isinstance(holders_payload, dict) else "",
                    "value": holders_pct,
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
        "statistics": {
            "status": statistics_status,
            "attempts": [
                {
                    "command": command,
                    "final_url": statistics_payload.get("final_url") if isinstance(statistics_payload, dict) else "",
                    "status_code": statistics_payload.get("status") if isinstance(statistics_payload, dict) else None,
                    "title": statistics_payload.get("title") if isinstance(statistics_payload, dict) else "",
                    "revenue_yoy_pct": revenue_yoy_pct,
                    "earnings_yoy_pct": earnings_yoy_pct,
                    "stderr": stderr[-400:] if stderr else "",
                }
            ],
        },
    }
    return earnings_rows, holders_pct, revenue_yoy_pct, earnings_yoy_pct, diagnostics


def _scrape_yahoo_earnings_eps_history(ticker: str, *, limit: int = 12) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker or limit <= 0:
        return [], {"status": "skipped", "reason": "missing_ticker_or_limit", "attempts": []}

    session = requests.Session()
    rows: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    page_size = min(limit, 12)
    page_offset = 0
    url = (
        "https://finance.yahoo.com/calendar/earnings"
        f"?symbol={normalized_ticker}&offset={page_offset}&size={page_size}"
    )
    try:
        response = session.get(url, headers=_YAHOO_BROWSER_HEADERS, timeout=_YAHOO_SCRAPE_TIMEOUT)
        blocked_reason = _detect_yahoo_block_reason(response)
        if blocked_reason is not None:
            attempts.append({"url": url, "offset": page_offset, "size": page_size, "status_code": response.status_code, "blocked": blocked_reason})
        else:
            response.raise_for_status()
            tables = pd.read_html(StringIO(response.text))
            if not tables:
                attempts.append({"url": url, "offset": page_offset, "size": page_size, "status_code": response.status_code, "table_count": 0})
            else:
                table = tables[0]
                column_names = [str(column) for column in table.columns]
                required_columns = {"EPS Estimate", "Reported EPS"}
                if "Surprise(%)" in column_names:
                    surprise_column = "Surprise(%)"
                elif "Surprise (%)" in column_names:
                    surprise_column = "Surprise (%)"
                else:
                    surprise_column = None
                has_date_column = "Earnings Date" in column_names
                schema_ok = has_date_column and required_columns.issubset(set(column_names)) and surprise_column is not None
                attempt_payload: dict[str, Any] = {
                    "url": url,
                    "offset": page_offset,
                    "size": page_size,
                    "status_code": response.status_code,
                    "table_count": len(tables),
                    "row_count": int(len(table)),
                    "columns": column_names,
                }
                if not schema_ok:
                    attempt_payload["schema_mismatch"] = True
                    attempt_payload["reason"] = "missing_expected_earnings_columns"
                attempts.append(attempt_payload)
                if schema_ok and not table.empty:
                    for _, raw_row in table.iterrows():
                        earnings_date = _parse_yahoo_earnings_date(raw_row.get("Earnings Date"))
                        if earnings_date is None:
                            continue
                        rows.append(
                            {
                                "date": earnings_date.isoformat(),
                                "eps_estimate": _coerce_optional_float(raw_row.get("EPS Estimate")),
                                "reported_eps": _coerce_optional_float(raw_row.get("Reported EPS")),
                                "surprise_pct": _coerce_optional_float(raw_row.get(surprise_column)),
                            }
                        )
                        if len(rows) >= limit:
                            break
    except Exception as exc:
        logger.warning("Unable to scrape Yahoo earnings EPS history for %s: %s", normalized_ticker, exc)
        attempts.append({"url": url, "offset": page_offset, "size": page_size, "error": str(exc)})

    deduped_rows: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    for row in rows:
        date_value = row["date"]
        if date_value in seen_dates:
            continue
        seen_dates.add(date_value)
        deduped_rows.append(row)
    if deduped_rows:
        status = "ok"
    elif any("schema_mismatch" in attempt for attempt in attempts):
        status = "schema_mismatch"
    elif any("error" in attempt for attempt in attempts):
        status = "error"
    else:
        status = "empty"
    return deduped_rows, {"status": status, "attempts": attempts}


def _scrape_yahoo_float_held_by_institutions_pct(ticker: str) -> tuple[float | None, dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return None, {"status": "skipped", "reason": "missing_ticker", "attempts": []}

    urls = [
        f"https://finance.yahoo.com/quote/{normalized_ticker}/holders",
        f"https://uk.finance.yahoo.com/quote/{normalized_ticker}/holders",
        f"https://au.finance.yahoo.com/quote/{normalized_ticker}/holders",
    ]
    patterns = [
        r"([0-9][0-9,]*\.?[0-9]*)%\s*\|\s*%\s+of\s+float\s+held\s+by\s+institutions",
        r"%\s+of\s+float\s+held\s+by\s+institutions[^0-9]{0,40}([0-9][0-9,]*\.?[0-9]*)%",
    ]

    session = requests.Session()
    attempts: list[dict[str, Any]] = []
    for url in urls:
        try:
            response = session.get(url, headers=_YAHOO_BROWSER_HEADERS, timeout=_YAHOO_SCRAPE_TIMEOUT)
            blocked_reason = _detect_yahoo_block_reason(response)
            if blocked_reason is not None:
                attempts.append({"url": url, "status_code": response.status_code, "blocked": blocked_reason})
                break
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Unable to scrape Yahoo holders page for %s at %s: %s", normalized_ticker, url, exc)
            attempts.append({"url": url, "error": str(exc)})
            continue

        normalized_text = _normalize_html_text(response.text)
        for pattern in patterns:
            match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
            if not match:
                continue
            value = _coerce_optional_float(match.group(1))
            attempts.append({"url": url, "status_code": response.status_code, "matched": True, "value": value})
            return value, {"status": "ok" if value is not None else "empty", "attempts": attempts}
        attempts.append({"url": url, "status_code": response.status_code, "matched": False})
    status = "error" if any("error" in attempt for attempt in attempts) else "empty"
    return None, {"status": status, "attempts": attempts}


def _load_yahoo_implied_move_playwright(ticker: str) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return None, {"status": "skipped", "reason": "missing_ticker", "attempts": []}
    if not _YAHOO_OPTIONS_PROBE_SCRIPT.exists():
        return None, {"status": "error", "reason": "missing_probe_script", "attempts": [{"script": str(_YAHOO_OPTIONS_PROBE_SCRIPT)}]}

    command = ["node", str(_YAHOO_OPTIONS_PROBE_SCRIPT), normalized_ticker]
    try:
        result = subprocess.run(
            command,
            cwd=str(_REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=_YAHOO_PLAYWRIGHT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return None, {"status": "error", "reason": "timeout", "attempts": [{"command": command, "error": str(exc)}]}
    except Exception as exc:
        return None, {"status": "error", "reason": "launch_failed", "attempts": [{"command": command, "error": str(exc)}]}

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    if result.returncode != 0:
        return None, {
            "status": "error",
            "reason": "nonzero_exit",
            "attempts": [{"command": command, "returncode": result.returncode, "stderr": stderr[-1000:], "stdout": stdout[-1000:]}],
        }

    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        return None, {
            "status": "error",
            "reason": "invalid_json",
            "attempts": [{"command": command, "error": str(exc), "stdout": stdout[-1000:], "stderr": stderr[-1000:]}],
        }

    implied_move_raw = payload.get("implied_move")
    implied_move: dict[str, Any] | None = None
    if isinstance(implied_move_raw, dict):
        implied_move = {
            "strike": _coerce_optional_float(implied_move_raw.get("strike")),
            "straddle_mid": _coerce_optional_float(implied_move_raw.get("straddle_mid")),
            "dollar_move": _coerce_optional_float(implied_move_raw.get("dollar_move")),
            "percent_move": _coerce_optional_float(implied_move_raw.get("percent_move")),
        }

    options_diagnostics = {
        "status": "ok" if implied_move is not None else "empty",
        "attempts": [
            {
                "command": command,
                "status_code": payload.get("status"),
                "final_url": payload.get("final_url"),
                "price": payload.get("price"),
                "closest_call": payload.get("closest_call"),
                "closest_put": payload.get("closest_put"),
                "stderr": stderr[-400:] if stderr else "",
            }
        ],
    }
    return implied_move, options_diagnostics


def _parse_yahoo_earnings_date(value: object) -> dt.date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    cleaned = text.replace("AM", "").replace("PM", "")
    for separator in (" EDT", " EST", " UTC", " GMT", " NZDT", " NZST"):
        if separator in cleaned:
            cleaned = cleaned.split(separator, 1)[0]
    primary = cleaned.split(",", 2)
    if len(primary) >= 2:
        candidate = ",".join(primary[:2]).strip()
    else:
        candidate = cleaned
    try:
        return dt.datetime.strptime(candidate, "%b %d, %Y").date()
    except ValueError:
        return None


def _detect_yahoo_block_reason(response: requests.Response) -> str | None:
    if response.status_code == 429:
        return "http_429"
    text = (response.text or "")[:512]
    for marker in _YAHOO_BLOCK_MARKERS:
        if marker.lower() in text.lower():
            return marker
    if len(response.text or "") < 128:
        return "short_block_page"
    return None


def _build_weekly_signal_badges(entry: dict[str, Any]) -> list[str]:
    setup_label = str(entry.get("setup_label") or "").strip().lower()
    summary = str(entry.get("summary") or "").strip().lower()
    master_note = str(entry.get("master_note") or "").strip().lower()
    badges: list[str] = []

    def add_badge(label: str) -> None:
        if label not in badges:
            badges.append(label)

    if "weekly rs new high" in setup_label or "weekly rs new high" in master_note:
        add_badge("Weekly RS")
    if "weekly rs nh before price: true" in summary or "weekly_rs_new_high_before_price" in summary:
        add_badge("Before Price")
    if "daily rs new high" in master_note or "daily rs nh before price: true" in summary:
        add_badge("Daily RS")
    if "strong rs window performance" in master_note:
        add_badge("Strong RS")
    if "sector etf  strong" in master_note or "sector etf strong" in master_note:
        add_badge("Sector Strong")
    signal_tags = entry.get("signal_tags")
    if isinstance(signal_tags, list):
        for tag in signal_tags:
            normalized_tag = str(tag or "").strip()
            if normalized_tag:
                add_badge(normalized_tag)

    distance_match = re.search(r"distance from year high:\s*([0-9.]+)%", summary)
    if distance_match:
        try:
            if float(distance_match.group(1)) <= 3.0:
                add_badge("Near 52W High")
        except ValueError:
            pass

    return badges


def _normalize_scanner_now(now: dt.datetime | None) -> dt.datetime:
    if now is None:
        return dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=dt.timezone.utc)
    return now


def _previous_weekday(value: dt.date) -> dt.date:
    current = value
    while current.weekday() >= 5:
        current -= dt.timedelta(days=1)
    return current


def _latest_completed_trading_day(now: dt.datetime) -> dt.date:
    local_now = now.astimezone(_NEW_YORK_TZ)
    local_date = local_now.date()
    weekday = local_date.weekday()
    if weekday >= 5:
        return _previous_weekday(local_date)
    if local_now.time() >= dt.time(hour=_SCANNER_BOARD_CUTOFF_HOUR, minute=_SCANNER_BOARD_CUTOFF_MINUTE):
        return local_date
    return _previous_weekday(local_date - dt.timedelta(days=1))


def _latest_visible_trading_day(now: dt.datetime) -> dt.date:
    local_now = now.astimezone(_NEW_YORK_TZ)
    local_date = local_now.date()
    if local_date.weekday() >= 5:
        return _previous_weekday(local_date)
    return local_date


def _select_scanner_board_watchlist(
    watchlists: list[dict[str, Any]],
    *,
    strategy_id: str,
    target_date: dt.date,
) -> dict[str, Any] | None:
    eligible: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for item in watchlists:
        if _strategy_id_for_watchlist_meta(item) != strategy_id:
            continue
        fallback.append(item)
        sort_date_raw = str(item.get("sort_date") or "").strip()
        if not sort_date_raw:
            continue
        try:
            sort_date = dt.date.fromisoformat(sort_date_raw)
        except ValueError:
            continue
        if sort_date <= target_date:
            eligible.append(item)

    candidates = eligible if eligible else fallback
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            str(item.get("sort_date") or ""),
            str(item.get("captured_at") or ""),
        ),
        reverse=True,
    )
    return candidates[0]


def _strategy_id_for_watchlist_meta(item: dict[str, Any]) -> str:
    explicit_strategy_id = _normalize_scanner_strategy_id(str(item.get("strategy_id") or "").strip())
    if explicit_strategy_id:
        return explicit_strategy_id
    stem = str(item.get("stem") or "").strip()
    if not stem:
        return ""
    if stem.startswith("weekly_rs_new_high_all_"):
        return "weekly_rs_new_high"
    if stem.startswith("weekly_rs_new_high_"):
        return "weekly_rs"
    if stem.startswith("daily_rs_new_high_"):
        return "daily_rs_new_high"
    if stem.startswith("fearzone_zeiierman_"):
        return "fearzone_zeiierman"
    return _normalize_scanner_strategy_id(_stem_strategy_id(stem))


def _find_previous_watchlist_meta(
    items: list[dict[str, Any]],
    *,
    stem: str,
    strategy_id: str,
) -> dict[str, Any] | None:
    if not stem or not strategy_id:
        return None
    same_strategy = [item for item in items if _strategy_id_for_watchlist_meta(item) == strategy_id]
    for index, item in enumerate(same_strategy):
        if str(item.get("stem") or "") != stem:
            continue
        if index + 1 < len(same_strategy):
            return same_strategy[index + 1]
        return None
    return same_strategy[0] if same_strategy else None


def _watchlist_ticker_set(entries: list[dict[str, Any]]) -> set[str]:
    return {
        normalize_ticker_symbol(str(item.get("ticker") or ""))
        for item in entries
        if isinstance(item, dict) and normalize_ticker_symbol(str(item.get("ticker") or ""))
    }


def _stem_strategy_id(stem: str) -> str:
    from ...artifact_paths import strategy_id_from_legacy_stem

    return strategy_id_from_legacy_stem(stem)


def _normalize_scanner_strategy_id(strategy_id: str) -> str:
    normalized = str(strategy_id or "").strip()
    if normalized == "sean_peg":
        return "sean_gap_up"
    return normalized


def _normalize_html_text(value: str) -> str:
    stripped = re.sub(r"<[^>]+>", " ", value or "")
    stripped = html.unescape(stripped)
    return re.sub(r"\s+", " ", stripped).strip()


def _normalize_json_payload(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _normalize_json_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_payload(item) for item in value]
    return value


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-" or text.lower() == "nan":
        return None
    if text.endswith("%"):
        text = text[:-1].strip()
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


class _ChartRequest:
    def __init__(
        self,
        *,
        visible_start_date: dt.date,
        fetch_start_date: dt.date,
        fetch_end_date: dt.date,
        warmup_trading_days: int,
    ) -> None:
        self.visible_start_date = visible_start_date
        self.fetch_start_date = fetch_start_date
        self.fetch_end_date = fetch_end_date
        self.warmup_trading_days = warmup_trading_days


def _resolve_chart_request(*, period: str, as_of_date: dt.date | None) -> _ChartRequest:
    resolved_period = str(period or "18mo").strip().lower() or "18mo"
    anchor_date = as_of_date or dt.date.today()
    visible_days = _period_to_calendar_days(resolved_period)
    visible_start_date = anchor_date - dt.timedelta(days=visible_days)
    warmup_days = 420
    fetch_start_date = visible_start_date - dt.timedelta(days=warmup_days * 2)
    return _ChartRequest(
        visible_start_date=visible_start_date,
        fetch_start_date=fetch_start_date,
        fetch_end_date=anchor_date,
        warmup_trading_days=warmup_days,
    )


def _period_to_calendar_days(period: str) -> int:
    lookup = {
        "6mo": 190,
        "12mo": 380,
        "18mo": 570,
        "24mo": 760,
        "3y": 1140,
        "5y": 1900,
    }
    return lookup.get(period, 570)


def _compute_ipo_vwap(frame: pd.DataFrame) -> pd.Series:
    source = frame.dropna(subset=["High", "Low", "Close", "Volume"]).copy()
    typical_price = (source["High"] + source["Low"] + source["Close"]) / 3.0
    cumulative_value = (typical_price * source["Volume"]).cumsum()
    cumulative_volume = source["Volume"].cumsum().replace(0, pd.NA)
    ipo_vwap = cumulative_value / cumulative_volume
    return ipo_vwap.reindex(frame.index)


def _compute_rs_line(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    aligned = pd.concat([stock, benchmark], axis=1, join="inner").dropna()
    aligned.columns = ["stock", "benchmark"]
    return aligned["stock"] / aligned["benchmark"]


def _compute_rs_rating_series(stock: pd.Series, benchmark: pd.Series) -> pd.Series:
    score_series = compute_weighted_rs_score(stock, benchmark)
    if score_series.empty:
        return pd.Series(dtype=float)
    rating_series = score_series.apply(approximate_rs_rating).dropna()
    return rating_series.astype(float)


def _compute_rs_new_high_flags(rs_line: pd.Series, price_reference: pd.Series, lookback: int) -> tuple[pd.Series, pd.Series]:
    aligned = pd.concat([rs_line, price_reference], axis=1, join="inner").dropna()
    aligned.columns = ["rs_line", "price_reference"]
    rolling_rs_high = aligned["rs_line"].rolling(window=lookback, min_periods=1).max()
    rolling_price_high = aligned["price_reference"].rolling(window=lookback, min_periods=1).max()
    tolerance = 1e-12
    new_high = aligned["rs_line"] >= (rolling_rs_high - tolerance)
    new_high_before_price = new_high & (aligned["price_reference"] < (rolling_price_high - tolerance))
    return new_high.reindex(rs_line.index, fill_value=False), new_high_before_price.reindex(rs_line.index, fill_value=False)


def _compute_ftd_sweep_markers(
    *,
    frame: pd.DataFrame,
    visible_dates: set[str],
    ticker: str,
    benchmark_ticker: str,
) -> list[dict[str, Any]]:
    normalized = _normalize_download_frame(frame)
    if normalized is None or normalized.empty or not visible_dates:
        return []
    config = load_app_config()
    ticker_meta = UniverseTicker(symbol=ticker.upper(), sector=None, industry=None, exchange=None)
    markers: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    minimum_bars = max(int(config.ftd_sweep_pivot_lookback_left) + 15, 40)
    for end_index in range(minimum_bars - 1, len(normalized)):
        prefix = normalized.iloc[: end_index + 1]
        current_date = prefix.index[-1].date().isoformat()
        if current_date not in visible_dates:
            continue
        hit = find_recent_ftd_sweep_hit(
            prefix,
            ticker=ticker_meta,
            benchmark_ticker=benchmark_ticker,
            config=config,
        )
        if hit is None or hit.sweep_breakout_date != current_date or current_date in seen_dates:
            continue
        seen_dates.add(current_date)
        markers.append(
            {
                "time": current_date,
                "kind": "ftd_sweep_breakout",
                "label": "FTD Sweep",
            }
        )
    return markers


def _compute_fearzone_panel(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"rows": [], "signals": []}

    config = AppConfig()
    source = frame[["Open", "High", "Low", "Close"]].mean(axis=1)
    high_period = int(config.fearzone_high_period)
    band_period = int(config.fearzone_band_period)

    highest_source = source.rolling(high_period).max()
    fz1_value = (highest_source - source) / highest_source.replace(0, np.nan)
    fz1_basis = fz1_value.rolling(band_period).mean()
    fz1_std = fz1_value.rolling(band_period).std(ddof=0)
    fz1_upper = fz1_basis + (fz1_std * float(config.fearzone_band_std_multiplier))
    in_fz1 = (fz1_value > fz1_upper).fillna(False)

    source_ma = source.rolling(high_period).mean()
    fz2_value = source - source_ma
    fz2_basis = fz2_value.rolling(band_period).mean()
    fz2_std = fz2_value.rolling(band_period).std(ddof=0)
    fz2_lower = fz2_basis - (fz2_std * float(config.fearzone_band_std_multiplier))
    in_fz2 = (fz2_value < fz2_lower).fillna(False)

    impulse_pct = ((frame["Close"] / frame["Close"].shift(int(config.fearzone_negative_impulse_lookback_days))) - 1.0) * 100.0
    negative_impulse = (impulse_pct <= (-abs(float(config.fearzone_negative_impulse_pct)))).fillna(False)

    bar_range = frame["High"] - frame["Low"]
    range_floor = frame["Low"] + (bar_range * float(config.fearzone_ricochet_zone_pct))
    in_ricochet_zone = (frame["Close"] <= range_floor).fillna(False)

    lowest_low = frame["Low"].rolling(int(config.fearzone_stochastic_k)).min()
    highest_high = frame["High"].rolling(int(config.fearzone_stochastic_k)).max()
    stoch_range = highest_high - lowest_low
    raw_k = pd.Series(
        np.where(stoch_range > 0, (frame["Close"] - lowest_low) * 100.0 / stoch_range, np.nan),
        index=frame.index,
    )
    fast_k = raw_k.rolling(int(config.fearzone_stochastic_d)).mean()
    slow_k = fast_k.rolling(int(config.fearzone_stochastic_d)).mean()
    magic_k1 = (slow_k < float(config.fearzone_magic_k1_threshold)).fillna(False)

    ma200 = frame["Close"].rolling(int(config.fearzone_ma_long_period)).mean()
    above_ma200 = (frame["Close"] > ma200).fillna(False)

    signals = (in_fz1 & in_fz2 & above_ma200 & (negative_impulse | in_ricochet_zone | magic_k1)).fillna(False)

    row_defs = [
        ("fz1", "FZ1", "#4ade80", in_fz1),
        ("fz2", "FZ2", "#4ade80", in_fz2),
        ("negative_impulse", "Down 10%", "#60a5fa", negative_impulse),
        ("ricochet_zone", "Ricochet", "#fde047", in_ricochet_zone),
        ("magic_k1", "Magic-K1", "#f8fafc", magic_k1),
        ("above_ma200", "Above MA200", "#4ade80", above_ma200),
    ]

    rows: list[dict[str, Any]] = []
    for key, label, active_color, series in row_defs:
        points: list[dict[str, Any]] = []
        for index, active in series.items():
            points.append(
                {
                    "time": pd.Timestamp(index).date().isoformat(),
                    "active": bool(active),
                }
            )
        rows.append(
            {
                "key": key,
                "label": label,
                "active_color": active_color,
                "inactive_color": "#71717a",
                "points": points,
            }
        )

    signal_points = [{"time": pd.Timestamp(index).date().isoformat()} for index, active in signals.items() if bool(active)]
    return {"rows": rows, "signals": signal_points}


def _filter_fearzone_panel(panel: dict[str, Any], *, visible_dates: set[str]) -> dict[str, Any]:
    if not visible_dates:
        return {"rows": [], "signals": []}
    return {
        "rows": [
            {
                **row,
                "points": [point for point in row.get("points", []) if point.get("time") in visible_dates],
            }
            for row in panel.get("rows", [])
        ],
        "signals": [signal for signal in panel.get("signals", []) if signal.get("time") in visible_dates],
    }


def _compute_market_extension_overlay(frame: pd.DataFrame, *, visible_dates: set[str]) -> dict[str, Any]:
    config = {
        "timeframe": "weekly",
        "ma_type": "sma",
        "length": 10,
        "warning_pct": 11.0,
        "extreme_pct": 15.0,
        "label": "10W SMA",
    }
    if frame.empty:
        return {"config": config, "line": [], "signals": [], "latest": None}

    weekly_frame = resample_to_weekly(frame[["Open", "High", "Low", "Close", "Volume"]])
    enriched = compute_extension_frame(
        weekly_frame,
        length=int(config["length"]),
        ma_type=str(config["ma_type"]),
        warning_pct=float(config["warning_pct"]),
        extreme_pct=float(config["extreme_pct"]),
    )
    if enriched.empty:
        return {"config": config, "line": [], "signals": [], "latest": None}

    market_extension_ma = enriched["moving_average"].reindex(frame.index, method="ffill")
    line = [
        {
            "time": pd.Timestamp(index).date().isoformat(),
            "value": float(value),
        }
        for index, value in market_extension_ma.items()
        if pd.notna(value) and pd.Timestamp(index).date().isoformat() in visible_dates
    ]

    extension_series = enriched["extension_pct"]
    signals: list[dict[str, Any]] = []
    for idx in range(1, len(enriched) - 1):
        current = extension_series.iloc[idx]
        if pd.isna(current):
            continue
        if current < extension_series.iloc[idx - 1] or current < extension_series.iloc[idx + 1]:
            continue
        state = str(enriched["threshold_state"].iloc[idx])
        if state == "normal":
            continue
        row = enriched.iloc[idx]
        moving_average = row.get("moving_average")
        signal_time = enriched.index[idx].date().isoformat()
        if signal_time not in visible_dates or pd.isna(moving_average):
            continue
        signals.append(
            {
                "time": signal_time,
                "state": state,
                "close": round(float(row["Close"]), 2),
                "moving_average": round(float(moving_average), 2),
                "distance": round(float(row["Close"] - moving_average), 2),
                "extension_pct": round(float(current), 2),
            }
        )

    latest_valid = enriched.dropna(subset=["moving_average", "extension_pct"]).tail(1)
    latest: dict[str, Any] | None = None
    if not latest_valid.empty:
        row = latest_valid.iloc[0]
        moving_average = float(row["moving_average"])
        latest = {
            "time": latest_valid.index[-1].date().isoformat(),
            "state": str(row["threshold_state"]),
            "close": round(float(row["Close"]), 2),
            "moving_average": round(moving_average, 2),
            "distance": round(float(row["Close"]) - moving_average, 2),
            "extension_pct": round(float(row["extension_pct"]), 2),
        }

    return {
        "config": config,
        "line": line,
        "signals": signals,
        "latest": latest,
    }


def _compute_danger_signals_snapshot(
    *,
    frame: pd.DataFrame,
    benchmark_frame: pd.DataFrame | None,
    market_extension: dict[str, Any] | None,
) -> dict[str, Any]:
    if frame.empty:
        return {"as_of_date": None, "active_count": 0, "highest_severity": None, "signals": []}

    bars = frame.copy().sort_index()
    for column in ("Open", "High", "Low", "Close", "Volume"):
        if column not in bars.columns:
            return {"as_of_date": None, "active_count": 0, "highest_severity": None, "signals": []}
        bars[column] = pd.to_numeric(bars[column], errors="coerce")

    bars = bars.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if bars.empty:
        return {"as_of_date": None, "active_count": 0, "highest_severity": None, "signals": []}

    bars["ema8"] = bars["Close"].ewm(span=8, adjust=False).mean()
    bars["ema21"] = bars["Close"].ewm(span=21, adjust=False).mean()
    bars["ma50"] = bars["Close"].rolling(50).mean()
    bars["ma200"] = bars["Close"].rolling(200).mean()

    lowest_low = bars["Low"].rolling(21).min()
    highest_high = bars["High"].rolling(21).max()
    stoch_range = highest_high - lowest_low
    raw_k = pd.Series(
        np.where(stoch_range > 0, (bars["Close"] - lowest_low) * 100.0 / stoch_range, np.nan),
        index=bars.index,
    )
    fast_k = raw_k.rolling(4).mean()
    slow_k = fast_k.rolling(4).mean()

    last = bars.iloc[-1]
    as_of_date = pd.Timestamp(bars.index[-1]).date().isoformat()
    signals: list[dict[str, Any]] = []

    def add_signal(
        *,
        active: bool,
        key: str,
        label: str,
        category: str,
        severity: str,
        summary: str,
        details: str,
        metrics: list[tuple[str, str]],
    ) -> None:
        if not active:
            return
        signals.append(
            {
                "key": key,
                "label": label,
                "category": category,
                "severity": severity,
                "summary": summary,
                "details": details,
                "metrics": [{"label": metric_label, "value": metric_value} for metric_label, metric_value in metrics if metric_value],
            }
        )

    day_range = float(last["High"] - last["Low"])
    dcr_pct = ((float(last["Close"]) - float(last["Low"])) / day_range) * 100.0 if day_range > 0 else None
    last_volume = float(last["Volume"])
    trailing_volume = bars["Volume"].tail(5)
    heavy_volume_5 = len(trailing_volume) >= 5 and bool(last_volume >= float(trailing_volume.max()))
    add_signal(
        active=dcr_pct is not None and dcr_pct <= 30.0,
        key="price_closes_near_low",
        label="Price Closes Near Low",
        category="early",
        severity="warning",
        summary="Close finished in bottom 30% of daily range.",
        details="Matches Trading Mindwheel daily closing-range warning.",
        metrics=[("DCR", f"{dcr_pct:.1f}%") if dcr_pct is not None else ("", "")],
    )
    add_signal(
        active=dcr_pct is not None and dcr_pct <= 30.0 and heavy_volume_5,
        key="price_closes_near_low_heavy_volume",
        label="Price Closes Near Low on Heavy Volume",
        category="early",
        severity="risk",
        summary="Weak close landed near low on heaviest volume of last 5 bars.",
        details="Pressure more dangerous when close near low happens with short-term volume expansion.",
        metrics=[
            ("DCR", f"{dcr_pct:.1f}%") if dcr_pct is not None else ("", ""),
            ("Volume", f"{int(last_volume):,}"),
        ],
    )

    ma_checks = [
        ("EMA 8", bars["ema8"].iloc[-1]),
        ("EMA 21", bars["ema21"].iloc[-1]),
        ("SMA 50", bars["ma50"].iloc[-1]),
        ("SMA 200", bars["ma200"].iloc[-1]),
    ]
    below_ma_labels = [label for label, value in ma_checks if pd.notna(value) and float(last["Close"]) < float(value)]
    add_signal(
        active=len(below_ma_labels) > 0,
        key="price_closes_below_moving_average",
        label="Price Closes Below Moving Average",
        category="early",
        severity="risk",
        summary="Close slipped below one or more key moving averages.",
        details="Current webapp version checks EMA 8, EMA 21, SMA 50, and SMA 200.",
        metrics=[("Below", ", ".join(below_ma_labels))],
    )

    swing_low = bars["Low"].shift(1).rolling(6).min().iloc[-1]
    add_signal(
        active=pd.notna(swing_low) and float(last["Close"]) < float(swing_low),
        key="price_closes_below_swing_low",
        label="Price Closes Below Swing Low",
        category="early",
        severity="risk",
        summary="Close broke below recent 6-bar swing low.",
        details="Uses previous-bar lookback so today must break prior support, not current-bar low.",
        metrics=[("Swing Low", f"${float(swing_low):.2f}") if pd.notna(swing_low) else ("", "")],
    )

    lower_lows = len(bars) >= 4 and bool(
        bars["Low"].iloc[-1] < bars["Low"].iloc[-2] < bars["Low"].iloc[-3] < bars["Low"].iloc[-4]
    )
    add_signal(
        active=lower_lows,
        key="three_consecutive_days_lower_lows",
        label="3 Consecutive Days of Lower Lows",
        category="early",
        severity="risk",
        summary="Three straight sessions printed lower lows.",
        details="Simple sequence check from latest four bars.",
        metrics=[],
    )

    close_below_prev_lows = len(bars) >= 4 and bool(
        float(last["Close"]) < float(bars["Low"].iloc[-2])
        and float(last["Close"]) < float(bars["Low"].iloc[-3])
        and float(last["Close"]) < float(bars["Low"].iloc[-4])
    )
    add_signal(
        active=close_below_prev_lows,
        key="close_lower_than_3_previous_lows",
        label="Close Lower than 3 Previous Lows",
        category="early",
        severity="risk",
        summary="Close finished under low of each prior three sessions.",
        details="Another sharp character-deterioration rule from script.",
        metrics=[],
    )

    stoch_bearish = len(fast_k.dropna()) > 0 and len(slow_k.dropna()) > 0 and bool(fast_k.iloc[-1] < slow_k.iloc[-1])
    add_signal(
        active=stoch_bearish,
        key="fast_stochastic_below_slow_stochastic",
        label="Fast Stochastic Below Slow Stochastic",
        category="mid",
        severity="warning",
        summary="Fast stochastic sits below slow stochastic.",
        details="Uses 21,4,4 full-stochastic approximation like attached script.",
        metrics=[
            ("Fast %K", f"{float(fast_k.iloc[-1]):.1f}") if pd.notna(fast_k.iloc[-1]) else ("", ""),
            ("Slow %D", f"{float(slow_k.iloc[-1]):.1f}") if pd.notna(slow_k.iloc[-1]) else ("", ""),
        ],
    )

    stoch_curving_down = len(fast_k) >= 3 and len(slow_k) >= 3 and all(
        pd.notna(value)
        for value in [fast_k.iloc[-1], fast_k.iloc[-2], fast_k.iloc[-3], slow_k.iloc[-1], slow_k.iloc[-2], slow_k.iloc[-3]]
    ) and bool(
        float(slow_k.iloc[-1]) < float(slow_k.iloc[-2]) < float(slow_k.iloc[-3])
        and float(fast_k.iloc[-1]) < float(fast_k.iloc[-2]) < float(fast_k.iloc[-3])
    )
    add_signal(
        active=stoch_curving_down,
        key="fast_slow_stochastic_curved_down",
        label="Fast & Slow Stochastic Curved Down",
        category="mid",
        severity="warning",
        summary="Both stochastic lines fell for two straight bars.",
        details="Momentum cooling even if price breakdown still looks early.",
        metrics=[],
    )

    rs_line: pd.Series | None = None
    if benchmark_frame is not None and not benchmark_frame.empty and "Close" in benchmark_frame.columns:
        benchmark_close = pd.to_numeric(benchmark_frame["Close"], errors="coerce").dropna()
        if not benchmark_close.empty:
            rs_line = _compute_rs_line(bars["Close"], benchmark_close)
    rs_curving_down = rs_line is not None and len(rs_line.dropna()) >= 3 and bool(
        float(rs_line.dropna().iloc[-1]) < float(rs_line.dropna().iloc[-2]) < float(rs_line.dropna().iloc[-3])
    )
    add_signal(
        active=rs_curving_down,
        key="rs_starts_curving_down",
        label="RS Starts Curving Down",
        category="mid",
        severity="warning",
        summary="Relative-strength line fell for two straight bars.",
        details="Shows leadership cooling even before a larger price failure.",
        metrics=[],
    )

    rs_underperforms_price = False
    if rs_line is not None and not rs_line.empty:
        rs_63_high = rs_line.rolling(63, min_periods=1).max().iloc[-1]
        price_63_high = bars["High"].rolling(63, min_periods=1).max().iloc[-1]
        rs_underperforms_price = bool(
            pd.notna(rs_63_high)
            and pd.notna(price_63_high)
            and float(last["High"]) >= float(price_63_high) - 1e-12
            and float(rs_line.iloc[-1]) < float(rs_63_high) - 1e-12
        )
    add_signal(
        active=rs_underperforms_price,
        key="rs_underperforms_price",
        label="RS Underperforms Price",
        category="mid",
        severity="risk",
        summary="Price pushed to a fresh short-term high without RS confirmation.",
        details="Current check compares today high and RS line versus trailing 63-bar highs.",
        metrics=[],
    )

    downward_ma_labels = []
    for label, series in [("EMA 8", bars["ema8"]), ("EMA 21", bars["ema21"]), ("SMA 50", bars["ma50"]), ("SMA 200", bars["ma200"])]:
        if len(series) < 3 or any(pd.isna(series.iloc[-offset]) for offset in (1, 2, 3)):
            continue
        if float(series.iloc[-1]) < float(series.iloc[-2]) < float(series.iloc[-3]):
            downward_ma_labels.append(label)
    add_signal(
        active=len(downward_ma_labels) > 0,
        key="moving_averages_begin_to_slope_downward",
        label="Moving Averages Begin to Slope Downward",
        category="late",
        severity="risk",
        summary="One or more key averages turned lower for two straight bars.",
        details="This is later-stage deterioration versus simple intraday weakness.",
        metrics=[("Averages", ", ".join(downward_ma_labels))],
    )

    prev_close = float(bars["Close"].iloc[-2]) if len(bars) >= 2 else None
    ma200_value = bars["ma200"].iloc[-1]
    gap_up_pct = ((float(last["Open"]) - prev_close) / prev_close) * 100.0 if prev_close and prev_close > 0 else None
    dist_200_pct = ((float(last["Close"]) / float(ma200_value)) - 1.0) * 100.0 if pd.notna(ma200_value) and float(ma200_value) > 0 else None
    add_signal(
        active=gap_up_pct is not None and gap_up_pct >= 5.0 and dist_200_pct is not None and dist_200_pct >= 125.0,
        key="exhaustion_gap",
        label="Exhaustion Gap",
        category="late",
        severity="high",
        summary="Gap-up bar is severely extended above 200-day average.",
        details="Mapped from attached script exhaustion-gap rule.",
        metrics=[
            ("Gap", f"{gap_up_pct:.1f}%") if gap_up_pct is not None else ("", ""),
            ("Vs 200SMA", f"{dist_200_pct:.1f}%") if dist_200_pct is not None else ("", ""),
        ],
    )

    latest_market_extension = market_extension.get("latest") if isinstance(market_extension, dict) else None
    if isinstance(latest_market_extension, dict):
        state = str(latest_market_extension.get("state") or "").strip()
        extension_pct = latest_market_extension.get("extension_pct")
        add_signal(
            active=state in {"warning", "extreme"},
            key="price_hits_overextension_zone",
            label="Price Hits Overextension Zone",
            category="late",
            severity="high" if state == "extreme" else "risk",
            summary="Price is stretched versus 10-week moving average.",
            details="Local webapp rule, added because extension often aligns with late-trade danger.",
            metrics=[("10W Ext", f"{float(extension_pct):.1f}%") if extension_pct is not None else ("", "")],
        )

    severity_rank = {"warning": 1, "risk": 2, "high": 3}
    category_rank = {"early": 1, "mid": 2, "late": 3}
    signals.sort(
        key=lambda item: (
            -severity_rank.get(str(item.get("severity")), 0),
            category_rank.get(str(item.get("category")), 99),
            str(item.get("label") or ""),
        )
    )
    highest_severity = None
    if signals:
        highest_severity = max(signals, key=lambda item: severity_rank.get(str(item.get("severity")), 0)).get("severity")
    return {
        "as_of_date": as_of_date,
        "active_count": len(signals),
        "highest_severity": highest_severity,
        "signals": signals,
    }


def _compute_mark_daily_extend_markers(frame: pd.DataFrame, *, visible_dates: set[str]) -> list[dict[str, Any]]:
    if frame.empty or not visible_dates:
        return []

    bars = frame.copy()
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    bars = bars.sort_index()
    for column in ("Open", "High", "Low", "Close"):
        if column not in bars.columns:
            return []
        bars[column] = pd.to_numeric(bars[column], errors="coerce")

    ema10 = bars["Close"].ewm(span=10, adjust=False).mean()
    prev_close = bars["Close"].shift(1)
    true_range = pd.concat(
        [
            bars["High"] - bars["Low"],
            (bars["High"] - prev_close).abs(),
            (bars["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr14 = true_range.rolling(window=14, min_periods=14).mean()
    extension_distance = bars["High"] - ema10
    triggered = extension_distance > (2.1 * atr14)

    markers: list[dict[str, Any]] = []
    for index in bars.index[triggered.fillna(False)]:
        time_value = pd.Timestamp(index).date().isoformat()
        if time_value not in visible_dates:
            continue
        ema_value = ema10.loc[index]
        atr_value = atr14.loc[index]
        distance_value = extension_distance.loc[index]
        if pd.isna(ema_value) or pd.isna(atr_value) or pd.isna(distance_value):
            continue
        markers.append(
            {
                "time": time_value,
                "kind": "mark_daily_extend",
                "label": "Mark Extend",
                "ema10": round(float(ema_value), 2),
                "atr14": round(float(atr_value), 2),
                "distance": round(float(distance_value), 2),
                "threshold": round(float(2.1 * atr_value), 2),
            }
        )
    return markers


def _coalesce_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _resolve_entry_display_price(entry: dict[str, Any]) -> float | None:
    for key in (
        "current_close",
        "current_price",
        "last_price",
        "signal_close",
        "close",
        "close_price",
        "entry_price",
        "trigger_price",
        "secondary_entry_price",
    ):
        value = _coerce_optional_float(entry.get(key))
        if value is not None:
            return value
    return None


def _resolve_entry_change_pct(entry: dict[str, Any]) -> float | None:
    for key in ("price_change_pct", "daily_change_pct", "change_pct", "pct_change"):
        value = _coerce_optional_float(entry.get(key))
        if value is not None:
            return value
    return None


def _normalize_insider_entries(
    raw_entries: object,
    *,
    ticker: str,
    window_start_date: dt.date,
    resolved_as_of_date: dt.date,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not isinstance(raw_entries, list):
        return entries
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            continue
        entry_ticker = str(raw_entry.get("ticker") or "").strip().upper()
        if entry_ticker != ticker:
            continue
        event_date = _coerce_insider_event_date(raw_entry)
        if event_date is None:
            continue
        if event_date < window_start_date or event_date > resolved_as_of_date:
            continue
        entries.append(
            {
                "ticker": entry_ticker,
                "filing_date": _coerce_iso_date(raw_entry.get("filing_date")),
                "transaction_date": _coerce_iso_date(raw_entry.get("transaction_date")),
                "owner_name": str(raw_entry.get("owner_name") or "").strip(),
                "position": str(raw_entry.get("position") or "").strip(),
                "type": str(raw_entry.get("type") or "").strip().upper(),
                "shares": int(round(_coerce_float(raw_entry.get("shares")))),
                "price": _coerce_optional_float(raw_entry.get("price")),
                "gross_amount": _coerce_optional_float(raw_entry.get("gross_amount")),
                "net_amount": _coerce_optional_float(raw_entry.get("net_amount")),
                "shares_owned_after": int(round(_coerce_float(raw_entry.get("shares_owned_after")))),
                "is_10b5_1": bool(raw_entry.get("is_10b5_1")),
                "source_url": str(raw_entry.get("source_url") or "").strip(),
            }
        )
    return entries


def _coerce_insider_event_date(payload: dict[str, Any]) -> dt.date | None:
    transaction_date = _coerce_iso_date(payload.get("transaction_date"))
    if transaction_date:
        return dt.date.fromisoformat(transaction_date)
    filing_date = _coerce_iso_date(payload.get("filing_date"))
    if filing_date:
        return dt.date.fromisoformat(filing_date)
    return None


def _coerce_iso_date(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _coerce_iso_datetime(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return None


def _coerce_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _coerce_optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_theme_tags(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    tags: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in tags:
            tags.append(text)
    return tags
