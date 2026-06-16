from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import threading
import uuid
from typing import Any

from src.artifact_paths import watchlist_stem_from_path
from src.config import load_app_config
from src.market_data_access import db_frame_has_recent_coverage, load_many_ticker_windows
from src.screener_catalog import build_screener_catalog
from src.universe_filters import build_filter_option_catalog
from src.universe import UniverseTicker, load_universe
from src.universe_filters import UniverseFilterCriteria, filter_universe_by_criteria
from src.webapp.services.screener_history_service import ScreenerHistoryService
from src.webapp.repositories.history_repository import HistoryRepository


@dataclass(frozen=True)
class RunAction:
    action_id: str
    label: str
    script_path: str
    supports_limit: bool = True
    extra_args: tuple[str, ...] = ()
    fields: tuple["RunField", ...] = ()
    visible_in_runs: bool = True
    bias_group: str = "other"
    bullish_subgroup: str = ""


@dataclass(frozen=True)
class RunField:
    field_id: str
    label: str
    field_type: str
    placeholder: str | None = None
    help_text: str | None = None
    options: tuple[tuple[str, str], ...] = ()


class RunService:
    REMOTE_WORKER_STALE_SECONDS = 90
    _remote_execution_action_ids = {
        "sync_finviz_fundamentals",
        "sync_chart_fundamentals_cache",
        "build_sector_rating_baselines",
        "build_ticker_ratings",
        "build_technical_ratings",
        "run_finviz_ratings_pipeline",
    }
    _progress_pattern = re.compile(r"\[(\d{1,6})/(\d{1,6})\]")
    _passed_pattern = re.compile(r"passed=(\d{1,6})")
    _stage_pattern = re.compile(r"^Stage (\d{1,2})/(\d{1,2}): (.+)$")
    _summary_path_pattern = re.compile(r"Wrote run summary to (.+)$")
    _watchlist_path_pattern = re.compile(r"Wrote watchlist to (.+)$")
    _filter_catalog_cache: dict[str, dict[str, list[str]]] = {}
    _limit_field = RunField(
        "limit",
        "Universe Limit",
        "number",
        placeholder="Optional",
        help_text="Leave blank to scan the full configured universe.",
    )
    _tickers_field = RunField(
        "tickers",
        "Tickers",
        "text",
        placeholder="AAPL NVDA CRWD",
        help_text="Optional space- or comma-separated ticker list.",
    )
    _date_label_field = RunField(
        "date_label",
        "Date Label",
        "date",
        help_text="Optional artifact label override.",
    )
    _as_of_date_field = RunField(
        "as_of_date",
        "As Of Date",
        "date",
        help_text="Optional historical replay date.",
    )
    _trade_date_field = RunField(
        "trade_date",
        "Trade Date",
        "date",
        help_text="Required trade date to delete and reload from Postgres daily_bars.",
    )
    _source_field = RunField(
        "source",
        "Source",
        "select",
        help_text="Choose whether PEG scans the full universe or the earnings watchlist.",
        options=(("universe", "Exchange Universe"), ("earnings-watchlist", "Earnings Watchlist")),
    )
    _reference_date_field = RunField(
        "reference_date",
        "Reference Date",
        "date",
        help_text="Optional date anchor for the earnings watchlist source.",
    )
    _market_data_source_field = RunField(
        "market_data_source",
        "Market Data Source",
        "select",
        help_text="Choose whether screeners pull directly from the internet or prefer Postgres daily_bars and fall back to the internet if needed.",
        options=(("internet", "Internet"), ("database-first", "Database First, Fallback to Internet")),
    )
    _start_date_field = RunField("start_date", "Start Date", "date")
    _end_date_field = RunField("end_date", "End Date", "date")
    _chunk_size_field = RunField("chunk_size", "Chunk Size", "number", placeholder="100")
    _max_retries_field = RunField("max_retries", "Max Retries", "number", placeholder="4")
    _retry_base_seconds_field = RunField("retry_base_seconds", "Retry Base Seconds", "number", placeholder="2")
    _chunk_sleep_seconds_field = RunField("chunk_sleep_seconds", "Chunk Sleep Seconds", "number", placeholder="1")
    _single_ticker_sleep_seconds_field = RunField(
        "single_ticker_sleep_seconds",
        "Single Ticker Sleep",
        "number",
        placeholder="0.5",
    )
    _batch_size_field = RunField("batch_size", "DB Batch Size", "number", placeholder="5000")
    _ensure_schema_field = RunField(
        "ensure_schema",
        "Ensure Schema",
        "boolean",
        help_text="Apply sql/postgres_app_schema.sql before the reload run.",
    )
    _strategy_ids_field = RunField(
        "strategy_ids",
        "Screeners",
        "multiselect",
        help_text="Choose one or more screener ids for warm or backtest runs.",
    )
    _overwrite_policy_field = RunField(
        "overwrite_policy",
        "Overwrite Policy",
        "select",
        options=(("skip-existing", "Skip Existing"), ("replace-date", "Replace Same Date"), ("latest-date", "Skip Newer Or Same")),
    )
    _candidate_threshold_field = RunField(
        "candidate_threshold",
        "Candidate Threshold",
        "number",
        placeholder="4",
        help_text="Minimum same-day signal count for overlap candidates.",
    )
    _max_parallel_field = RunField(
        "max_parallel",
        "Max Parallel",
        "number",
        placeholder="5",
        help_text="How many screener sub-jobs to run at once for warm batch.",
    )
    _resume_from_field = RunField(
        "resume_from",
        "Resume From",
        "text",
        placeholder="NVDA",
        help_text="Optional ticker to resume a fundamentals scrape batch from.",
    )
    _delay_min_seconds_field = RunField(
        "delay_min_seconds",
        "Delay Min Seconds",
        "number",
        placeholder="0.15",
        help_text="Minimum delay between Finviz ticker scrapes. API-first defaults are tuned faster than the old browser path.",
    )
    _delay_max_seconds_field = RunField(
        "delay_max_seconds",
        "Delay Max Seconds",
        "number",
        placeholder="0.4",
        help_text="Maximum delay between Finviz ticker scrapes. Raise this if Finviz starts throttling.",
    )
    _batch_size_before_rest_field = RunField(
        "batch_size_before_rest",
        "Batch Before Rest",
        "number",
        placeholder="500",
        help_text="How many tickers to scrape before a longer rest.",
    )
    _rest_seconds_field = RunField(
        "rest_seconds",
        "Rest Seconds",
        "number",
        placeholder="5",
        help_text="Longer sleep between Finviz scrape batches.",
    )
    _retry_failed_from_manifest_field = RunField(
        "retry_failed_from_manifest",
        "Retry Manifest",
        "boolean",
        help_text="Retry only failed or blocked tickers from the Finviz manifest for this as-of date.",
    )
    _circuit_breaker_consecutive_503_field = RunField(
        "circuit_breaker_consecutive_503",
        "503 Breaker",
        "number",
        placeholder="25",
        help_text="Stop early after this many consecutive HTTP 503 scrape failures. Set 0 to disable.",
    )
    _min_sector_peers_field = RunField(
        "min_sector_peers",
        "Min Sector Peers",
        "number",
        placeholder="20",
        help_text="Minimum filtered peer count required per sector metric baseline.",
    )
    _min_category_metrics_field = RunField(
        "min_category_metrics",
        "Min Category Metrics",
        "number",
        placeholder="1.0",
        help_text="Current ratings pipeline expects 1.0 for full category coverage.",
    )
    _entry_signal_threshold_field = RunField(
        "entry_signal_threshold",
        "Entry Threshold",
        "number",
        placeholder="4",
        help_text="Minimum same-day signal count for entries.",
    )
    _hold_periods_json_field = RunField(
        "hold_periods_json",
        "Hold Periods JSON",
        "text",
        placeholder="[5, 10]",
        help_text="Trading-day hold list as JSON array.",
    )
    _execution_mode_field = RunField(
        "execution_mode",
        "Execution Mode",
        "select",
        help_text="Run locally on this server, or queue for a remote worker.",
        options=(("local", "Local"), ("remote", "Remote Worker Queue")),
    )
    _target_worker_field = RunField(
        "target_worker",
        "Target Worker",
        "text",
        placeholder="worker-a",
        help_text="Optional worker name. Leave blank to let any worker claim the queued job.",
    )
    _fundamental_limit_field = RunField(
        "fundamental_limit",
        "Top Fundamental",
        "number",
        placeholder="200",
        help_text="How many top fundamental-rating tickers to include in the focused refresh set.",
    )
    _technical_limit_field = RunField(
        "technical_limit",
        "Top Technical",
        "number",
        placeholder="200",
        help_text="How many top technical-rating tickers to include in the focused refresh set.",
    )
    _upcoming_weeks_field = RunField(
        "upcoming_weeks",
        "Upcoming Weeks",
        "number",
        placeholder="2",
        help_text="How many upcoming earnings weeks to union into the refresh set.",
    )
    _earnings_limit_field = RunField(
        "earnings_limit",
        "Earnings Rows",
        "number",
        placeholder="8",
        help_text="How many earnings EPS history rows to persist per ticker.",
    )
    _filter_precedence_field = RunField(
        "filter_precedence",
        "Filter Precedence",
        "select",
        help_text="Choose which side wins when the same sector, industry, or theme appears in both include and exclude.",
        options=(("exclude", "Exclude First"), ("include", "Include First")),
    )
    _include_sectors_field = RunField("include_sectors", "Only Sectors", "multiselect")
    _exclude_sectors_field = RunField("exclude_sectors", "Exclude Sectors", "multiselect")
    _include_industries_field = RunField("include_industries", "Only Industries", "multiselect")
    _exclude_industries_field = RunField("exclude_industries", "Exclude Industries", "multiselect")
    _include_themes_field = RunField("include_themes", "Only Themes", "multiselect")
    _exclude_themes_field = RunField("exclude_themes", "Exclude Themes", "multiselect")
    _actions = {
        "screener_history_batch": RunAction(
            "screener_history_batch",
            "Batch Screener History Cache",
            "scripts/run_screener_history_batch.py",
            supports_limit=False,
            visible_in_runs=False,
        ),
        "signal_warm_batch": RunAction(
            "signal_warm_batch",
            "Warm Signals + Overlap",
            "scripts/run_signal_warm_batch.py",
            supports_limit=False,
            fields=(
                _strategy_ids_field,
                _start_date_field,
                _end_date_field,
                _market_data_source_field,
                _overwrite_policy_field,
                _candidate_threshold_field,
                _max_parallel_field,
            ),
        ),
        "overlap_backtest_v1": RunAction(
            "overlap_backtest_v1",
            "Run Overlap Backtest V1",
            "scripts/run_overlap_backtest_v1.py",
            supports_limit=False,
            fields=(
                _strategy_ids_field,
                _start_date_field,
                _end_date_field,
                _entry_signal_threshold_field,
                _hold_periods_json_field,
            ),
        ),
        "sync_postgres_market_data": RunAction(
            "sync_postgres_market_data",
            "Sync Postgres Market Data",
            "scripts/sync_postgres_market_data.py",
            supports_limit=False,
            fields=(
                _tickers_field,
            ),
            visible_in_runs=False,
        ),
        "reload_postgres_market_data_date": RunAction(
            "reload_postgres_market_data_date",
            "Reload Postgres Market Data Date",
            "scripts/reload_postgres_market_data_date.py",
            supports_limit=False,
            fields=(
                _trade_date_field,
                _chunk_size_field,
                _max_retries_field,
                _retry_base_seconds_field,
                _chunk_sleep_seconds_field,
                _single_ticker_sleep_seconds_field,
                _batch_size_field,
                _ensure_schema_field,
            ),
        ),
        "backfill_trendline_snapshots": RunAction(
            "backfill_trendline_snapshots",
            "Backfill Trendline Snapshots",
            "scripts/backfill_trendline_snapshots.py",
            supports_limit=False,
            fields=(
                _tickers_field,
                _start_date_field,
                _end_date_field,
            ),
        ),
        "sync_finviz_fundamentals": RunAction(
            "sync_finviz_fundamentals",
            "Sync Finviz Fundamentals",
            "scripts/sync_finviz_fundamentals.py",
            fields=(
                _limit_field,
                _tickers_field,
                _include_sectors_field,
                _execution_mode_field,
                _target_worker_field,
                _as_of_date_field,
                _resume_from_field,
                _delay_min_seconds_field,
                _delay_max_seconds_field,
                _batch_size_before_rest_field,
                _rest_seconds_field,
                _overwrite_policy_field,
                _retry_failed_from_manifest_field,
                _circuit_breaker_consecutive_503_field,
            ),
        ),
        "sync_chart_fundamentals_cache": RunAction(
            "sync_chart_fundamentals_cache",
            "Sync Chart Fundamentals Cache",
            "scripts/sync_chart_fundamentals_cache.py",
            supports_limit=False,
            fields=(
                _tickers_field,
                _execution_mode_field,
                _target_worker_field,
                _as_of_date_field,
                _fundamental_limit_field,
                _technical_limit_field,
                _upcoming_weeks_field,
                _earnings_limit_field,
                _overwrite_policy_field,
            ),
            bias_group="other",
        ),
        "build_sector_rating_baselines": RunAction(
            "build_sector_rating_baselines",
            "Build Sector Rating Baselines",
            "scripts/build_sector_rating_baselines.py",
            supports_limit=False,
            fields=(
                _as_of_date_field,
                _include_sectors_field,
                _execution_mode_field,
                _target_worker_field,
            ),
        ),
        "build_ticker_ratings": RunAction(
            "build_ticker_ratings",
            "Build Ticker Ratings",
            "scripts/build_ticker_ratings.py",
            supports_limit=False,
            fields=(
                _as_of_date_field,
                _include_sectors_field,
                _execution_mode_field,
                _target_worker_field,
                _min_sector_peers_field,
                _min_category_metrics_field,
            ),
        ),
        "build_technical_ratings": RunAction(
            "build_technical_ratings",
            "Build Technical Ratings",
            "scripts/build_technical_ratings.py",
            fields=(
                _limit_field,
                _tickers_field,
                _include_sectors_field,
                _execution_mode_field,
                _target_worker_field,
                _as_of_date_field,
            ),
        ),
        "run_finviz_ratings_pipeline": RunAction(
            "run_finviz_ratings_pipeline",
            "Run Finviz Ratings Pipeline",
            "scripts/run_finviz_ratings_pipeline.py",
            fields=(
                _limit_field,
                _tickers_field,
                _include_sectors_field,
                _execution_mode_field,
                _target_worker_field,
                _as_of_date_field,
                _resume_from_field,
                _delay_min_seconds_field,
                _delay_max_seconds_field,
                _batch_size_before_rest_field,
                _rest_seconds_field,
                _overwrite_policy_field,
                _retry_failed_from_manifest_field,
                _circuit_breaker_consecutive_503_field,
                _min_sector_peers_field,
                _min_category_metrics_field,
            ),
        ),
        "earnings_weekly_criteria": RunAction(
            "earnings_weekly_criteria",
            "Run Earnings Weekly Criteria",
            "scripts/run_earnings_weekly_criteria_screen.py",
            fields=(
                _limit_field,
                _date_label_field,
                _reference_date_field,
            ),
            bias_group="other",
        ),
        "rs": RunAction(
            "rs",
            "Run RS",
            "scripts/run_rs_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "vcp": RunAction(
            "vcp",
            "Run VCP",
            "scripts/run_vcp_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "cup_handle": RunAction(
            "cup_handle",
            "Run Cup Handle",
            "scripts/run_cup_handle_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "gap_fill": RunAction(
            "gap_fill",
            "Run Gap Fill",
            "scripts/run_gap_fill_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "inside_dryup": RunAction(
            "inside_dryup",
            "Run Inside Dry-Up",
            "scripts/run_inside_dryup_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "ftd_sweep": RunAction(
            "ftd_sweep",
            "Run FTD Sweep",
            "scripts/run_ftd_sweep_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "fearzone": RunAction(
            "fearzone",
            "Run Fearzone",
            "scripts/run_fearzone_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "fearzone_zeiierman": RunAction(
            "fearzone_zeiierman",
            "Run Fearzone Zeiierman",
            "scripts/run_fearzone_zeiierman_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "td9_bullish": RunAction(
            "td9_bullish",
            "Run Bullish TD9",
            "scripts/run_td9_bullish_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "td9_bearish": RunAction(
            "td9_bearish",
            "Run Bearish TD9",
            "scripts/run_td9_bearish_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bearish",
        ),
        "macd_golden_cross": RunAction(
            "macd_golden_cross",
            "Run MACD Golden Cross",
            "scripts/run_macd_golden_cross_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "macd_dead_cross": RunAction(
            "macd_dead_cross",
            "Run MACD Dead Cross",
            "scripts/run_macd_dead_cross_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bearish",
        ),
        "rsi_ma_bb_bullish": RunAction(
            "rsi_ma_bb_bullish",
            "Run RSI MA/BB Bullish",
            "scripts/run_rsi_ma_bb_bullish_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "rsi_ma_bb_bearish": RunAction(
            "rsi_ma_bb_bearish",
            "Run RSI MA/BB Bearish",
            "scripts/run_rsi_ma_bb_bearish_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bearish",
        ),
        "bb_squeeze": RunAction(
            "bb_squeeze",
            "Run BB Squeeze",
            "scripts/run_bb_squeeze_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "high_tight_flag": RunAction(
            "high_tight_flag",
            "Run High Tight Flag",
            "scripts/run_high_tight_flag_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "sepa_vcp": RunAction(
            "sepa_vcp",
            "Run SEPA VCP",
            "scripts/run_sepa_vcp_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "rti": RunAction(
            "rti",
            "Run RTI",
            "scripts/run_rti_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "sean_breakout": RunAction(
            "sean_breakout",
            "Run Sean Breakout",
            "scripts/run_sean_breakout_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "vcs_setup_stage": RunAction(
            "vcs_setup_stage",
            "Run VCS Setup Stage",
            "scripts/run_vcs_setup_stage_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "vcs_critical_tightness": RunAction(
            "vcs_critical_tightness",
            "Run VCS Critical Tightness",
            "scripts/run_vcs_critical_tightness_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "base_detection": RunAction(
            "base_detection",
            "Run Base Detection",
            "scripts/run_base_detection_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "cup_detection": RunAction(
            "cup_detection",
            "Run Cup Detection",
            "scripts/run_cup_detection_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "double_bottom_detection": RunAction(
            "double_bottom_detection",
            "Run Double Bottom Detection",
            "scripts/run_double_bottom_detection_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="bottoming",
        ),
        "weekly_tight_close": RunAction(
            "weekly_tight_close",
            "Run Weekly Tight Close",
            "scripts/run_weekly_tight_close_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "weekly_tight_close_breakout": RunAction(
            "weekly_tight_close_breakout",
            "Run Weekly Tight Close Breakout",
            "scripts/run_weekly_tight_close_breakout_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "three_weeks_tight": RunAction(
            "three_weeks_tight",
            "Run Three Weeks Tight",
            "scripts/run_three_weeks_tight_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "weekly_htf_pullback": RunAction(
            "weekly_htf_pullback",
            "Run Weekly HTF Pullback",
            "scripts/run_weekly_htf_pullback_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "htf_8w_runup": RunAction(
            "htf_8w_runup",
            "Run HTF 8W Runup",
            "scripts/run_htf_runup_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "weekly_rs": RunAction(
            "weekly_rs",
            "Run Weekly RS New High Before Price",
            "scripts/run_weekly_rs_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            visible_in_runs=False,
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "weekly_rs_before_price": RunAction(
            "weekly_rs_before_price",
            "Run Weekly RS New High Before Price",
            "scripts/run_weekly_rs_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "near_200ma": RunAction(
            "near_200ma",
            "Run Near 200MA",
            "scripts/run_near_200ma_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "lost_21ema": RunAction(
            "lost_21ema",
            "Run Lost 21EMA",
            "scripts/run_lost_21ema_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bearish",
        ),
        "trend_template": RunAction(
            "trend_template",
            "Run Trend Template",
            "scripts/run_trend_template_screen.py",
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "legacy_peg": RunAction(
            "legacy_peg",
            "Run Legacy PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "legacy"),
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _source_field,
                _reference_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
        "sean_peg": RunAction(
            "sean_peg",
            "Run Sean PEG",
            "scripts/run_peg_screen.py",
            extra_args=("--strategy-profile", "sean-peg"),
            fields=(
                _limit_field,
                _tickers_field,
                _date_label_field,
                _as_of_date_field,
                _source_field,
                _reference_date_field,
                _market_data_source_field,
                _filter_precedence_field,
                _include_sectors_field,
                _exclude_sectors_field,
                _include_industries_field,
                _exclude_industries_field,
                _include_themes_field,
                _exclude_themes_field,
            ),
            bias_group="bullish",
            bullish_subgroup="leaders",
        ),
    }
    _jobs_lock = threading.Lock()
    _jobs: list[dict[str, Any]] = []
    _jobs_by_id: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _is_template_token(value: str) -> bool:
        stripped = str(value or "").strip()
        return stripped.startswith("{{") and stripped.endswith("}}")

    def __init__(self, project_root: Path, *, database_url: str = "", artifacts_dir: Path | None = None) -> None:
        self.project_root = project_root
        self.database_url = database_url
        self.artifacts_dir = artifacts_dir or (project_root / "artifacts")
        self.history_repository = HistoryRepository(database_url=database_url, artifacts_dir=self.artifacts_dir)
        self.screener_history_service = ScreenerHistoryService(
            database_url=database_url,
            artifacts_dir=self.artifacts_dir,
            repository=self.history_repository,
        )

    def list_actions(self) -> list[dict[str, Any]]:
        filter_catalog = self._get_filter_catalog()
        return [
            {
                "id": action.action_id,
                "label": action.label,
                "bias_group": action.bias_group,
                "bullish_subgroup": action.bullish_subgroup,
                "command": " ".join([sys.executable, action.script_path, *action.extra_args]).strip(),
                "supports_limit": action.supports_limit,
                "fields": [
                    {
                        "id": field.field_id,
                        "label": field.label,
                        "type": field.field_type,
                        "placeholder": field.placeholder,
                        "help_text": field.help_text,
                        "options": self._field_options(field, filter_catalog),
                    }
                    for field in action.fields
                ],
            }
            for action in self._actions.values()
            if action.visible_in_runs
        ]

    def list_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._jobs_lock:
            jobs = [self._serialize_job(item) for item in self._jobs[:limit]]
        jobs.extend(self._list_local_persisted_jobs(limit=limit))
        jobs.extend(self._list_remote_jobs(limit=limit))
        jobs = self._dedupe_jobs(self._sort_jobs(jobs))[:limit]
        return self._attach_child_jobs(jobs)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._jobs_lock:
            job = self._jobs_by_id.get(job_id)
        if job is not None:
            jobs = [self._serialize_job(job)]
            enriched = self._attach_child_jobs(jobs)
            return enriched[0]
        remote_job = self._load_remote_job(job_id)
        if remote_job is not None:
            jobs = [remote_job]
            enriched = self._attach_child_jobs(jobs)
            return enriched[0]
        local_job = self._load_persisted_local_job(job_id)
        if local_job is None:
            raise ValueError(f"Unknown job: {job_id}")
        jobs = [local_job]
        enriched = self._attach_child_jobs(jobs)
        return enriched[0]

    def get_child_job(self, child_job_run_id: int) -> dict[str, Any]:
        row = self.history_repository.get_job_run(child_job_run_id)
        if row is None or row.get("parent_job_run_id") is None:
            raise ValueError(f"Unknown child job: {child_job_run_id}")
        return self._serialize_child_job_run(row)

    def precheck(self, action_id: str, *, options: dict[str, Any] | None = None) -> dict[str, Any]:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")
        normalized = self._normalize_options(action, options or {})
        market_data_source = str(normalized.get("market_data_source") or "internet").strip().lower()
        if market_data_source != "database-first":
            return {
                "applicable": False,
                "configured": self.history_repository.is_configured(),
                "action_id": action_id,
                "market_data_source": market_data_source or "internet",
                "message": "DB coverage precheck is only used for database-first runs.",
            }
        if not self.history_repository.is_configured():
            return {
                "applicable": False,
                "configured": False,
                "action_id": action_id,
                "market_data_source": market_data_source,
                "message": "Database URL is not configured for DB coverage precheck.",
            }

        config = load_app_config()
        catalog = build_screener_catalog(config)
        spec = catalog.get(action_id)
        if spec is None:
            return {
                "applicable": False,
                "configured": True,
                "action_id": action_id,
                "market_data_source": market_data_source,
                "message": "DB coverage precheck is not available for this screener yet.",
            }

        target_date = self._resolve_as_of_date(normalized)
        lookback_trading_days = int(spec.lookback_trading_days) + int(spec.warmup_trading_days)
        universe = self._resolve_precheck_universe(config=config, normalized=normalized)
        universe_symbols = [item.symbol.upper() for item in universe]
        benchmark_ticker = config.benchmark_ticker.upper()
        query_tickers = universe_symbols + ([benchmark_ticker] if "benchmark_bars" in spec.required_inputs else [])
        frames = load_many_ticker_windows(
            query_tickers,
            target_date,
            lookback_trading_days,
            database_url=self.database_url,
        )
        benchmark_ready = True
        benchmark_bar_count = None
        if "benchmark_bars" in spec.required_inputs:
            benchmark_frame = frames.get(benchmark_ticker)
            benchmark_bar_count = len(benchmark_frame) if benchmark_frame is not None else 0
            benchmark_ready = self._frame_is_db_ready(benchmark_frame, target_date, lookback_trading_days)

        db_ready_tickers = 0
        fallback_tickers: list[str] = []
        for symbol in universe_symbols:
            frame = frames.get(symbol)
            ticker_ready = self._frame_is_db_ready(frame, target_date, lookback_trading_days)
            if ticker_ready and benchmark_ready:
                db_ready_tickers += 1
            else:
                fallback_tickers.append(symbol)

        total_tickers = len(universe_symbols)
        return {
            "applicable": True,
            "configured": True,
            "action_id": action_id,
            "market_data_source": market_data_source,
            "as_of_date": target_date.isoformat(),
            "lookback_trading_days": lookback_trading_days,
            "total_tickers": total_tickers,
            "db_ready_tickers": db_ready_tickers,
            "fallback_tickers": len(fallback_tickers),
            "db_ready_pct": round((db_ready_tickers / total_tickers) * 100, 1) if total_tickers > 0 else 0.0,
            "sample_fallback_tickers": fallback_tickers[:12],
            "benchmark": {
                "ticker": benchmark_ticker,
                "required": "benchmark_bars" in spec.required_inputs,
                "db_ready": benchmark_ready,
                "bar_count": benchmark_bar_count,
            },
            "notes": [
                "Counts estimate whether DB coverage is good enough before fallback would be needed.",
                "Fallback-needed means at least one required DB input looks incomplete or too stale for this screener.",
            ],
        }

    def cancel(self, job_id: str) -> dict[str, Any]:
        remote_job = self._load_remote_job(job_id)
        if remote_job is not None:
            if str(remote_job.get("status") or "") not in {"queued", "running"}:
                raise ValueError(f"Job is not running: {job_id}")
            updated_row = self.history_repository.request_remote_job_cancel(remote_job.get("job_run_id"))
            if updated_row is None:
                raise ValueError(f"Unknown job: {job_id}")
            return self._serialize_remote_job_run(updated_row)
        with self._jobs_lock:
            job = self._jobs_by_id.get(job_id)
            if job is None:
                raise ValueError(f"Unknown job: {job_id}")
            process = job.get("_process")
            if job.get("status") != "running" or process is None:
                raise ValueError(f"Job is not running: {job_id}")
            job["cancel_requested"] = True
            self._append_log_line(job, f"Cancellation requested at {self._now_iso()}")
            self._terminate_process(process)
            return self._serialize_job(job)

    def launch(self, action_id: str, *, options: dict[str, Any] | None = None, trigger_source: str = "manual") -> str:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")

        normalized = self._normalize_options(action, options or {})
        execution_mode = str(normalized.get("execution_mode") or "local").strip().lower() or "local"
        if execution_mode not in {"local", "remote"}:
            raise ValueError("Execution mode must be local or remote.")
        if execution_mode == "remote" and action_id not in self._remote_execution_action_ids:
            raise ValueError("Remote worker execution is currently supported only for Finviz rating sync actions.")
        if execution_mode == "remote" and not self.has_healthy_remote_workers():
            execution_mode = "local"
            normalized["execution_mode"] = "local"
            normalized["_remote_fallback_reason"] = "No healthy remote workers detected at launch time."
        request_payload = {
            "action_id": action_id,
            "execution_mode": execution_mode,
            "target_worker": str(normalized.get("target_worker") or ""),
            "options": normalized,
        }
        initial_status = "queued" if execution_mode == "remote" else "running"
        job_run_id = self.history_repository.create_job_run(
            job_type=self._job_type_for_action(action_id),
            job_name=action.label,
            status=initial_status,
            trigger_source=trigger_source,
            request_payload=request_payload,
            parent_job_run_id=None,
        )
        if execution_mode == "remote" and job_run_id is None:
            raise ValueError("Remote worker queue requires a configured database connection.")
        if job_run_id is not None:
            normalized["job_run_id"] = job_run_id
        command = self.build_command(action_id, normalized, normalized=True)
        if execution_mode == "remote":
            self.history_repository.patch_job_run_result(
                job_run_id,
                result_payload_patch={
                    "job_id": self._remote_job_id(job_run_id),
                    "execution_mode": "remote",
                    "target_worker": str(normalized.get("target_worker") or ""),
                    "command": " ".join(command),
                    "progress_label": "Queued for remote worker",
                    "message": "Queued for remote worker claim.",
                },
                status="queued",
            )
            return self._remote_job_id(job_run_id)

        return self._start_local_job(
            action_id,
            action.label,
            command,
            normalized,
            job_run_id=job_run_id,
        )

    def has_healthy_remote_workers(self) -> bool:
        return self.history_repository.healthy_remote_worker_count(stale_after_seconds=self.REMOTE_WORKER_STALE_SECONDS) > 0

    def recover_remote_jobs(self, *, max_local_fallbacks: int = 1) -> dict[str, int]:
        recovered = self.history_repository.requeue_stale_remote_job_runs(
            stale_after_seconds=self.REMOTE_WORKER_STALE_SECONDS
        )
        fallback_started = 0
        if not self.has_healthy_remote_workers():
            while fallback_started < max(1, int(max_local_fallbacks)):
                row = self.history_repository.claim_remote_job_run_for_local_fallback()
                if row is None:
                    break
                self.resume_remote_job_locally(row)
                fallback_started += 1
        return {"requeued": len(recovered), "local_fallback_started": fallback_started}

    def resume_remote_job_locally(self, row: dict[str, Any]) -> str:
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        options = dict(request_payload.get("options") or {}) if isinstance(request_payload.get("options"), dict) else {}
        action_id = str(request_payload.get("action_id") or "")
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")
        normalized = self._normalize_options(action, options)
        normalized["execution_mode"] = "local"
        normalized["_remote_fallback_reason"] = str(
            result_payload.get("message") or "Recovered remote job is now running locally."
        )
        normalized["job_run_id"] = int(row["id"])
        command = self.build_command(action_id, normalized, normalized=True)
        return self._start_local_job(
            action_id,
            action.label,
            command,
            normalized,
            job_id=self._remote_job_id(int(row["id"])),
            job_run_id=int(row["id"]),
        )

    def _start_local_job(
        self,
        action_id: str,
        label: str,
        command: list[str],
        normalized: dict[str, Any],
        *,
        job_id: str | None = None,
        job_run_id: int | None = None,
    ) -> str:
        local_job_id = str(job_id or uuid.uuid4().hex[:12])
        started_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        log_file = self.artifacts_dir / "status" / "logs" / f"{local_job_id}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("", encoding="utf-8")
        initial_log = "Starting...\n"
        fallback_reason = str(normalized.get("_remote_fallback_reason") or "").strip()
        if fallback_reason:
            initial_log = f"{fallback_reason}\n{initial_log}"
        job = {
            "job_id": local_job_id,
            "action_id": action_id,
            "job_run_id": job_run_id,
            "label": label,
            "status": "running",
            "command": " ".join(command),
            "started_at": started_at,
            "finished_at": "",
            "return_code": None,
            "log_tail": initial_log,
            "progress_current": None,
            "progress_total": None,
            "progress_percent": None,
            "progress_label": "Starting…",
            "success_count": 0,
            "watchlist_file": "",
            "summary_file": "",
            "cancel_requested": False,
            "options": normalized,
            "execution_mode": "local",
            "log_file": str(log_file),
            "_started_monotonic": time.monotonic(),
        }

        with self._jobs_lock:
            self._jobs = [item for item in self._jobs if str(item.get("job_id") or "") != local_job_id]
            self._jobs.insert(0, job)
            self._jobs_by_id[local_job_id] = job
            del self._jobs[50:]
        if job_run_id is not None:
            self.history_repository.patch_job_run_result(
                job_run_id,
                result_payload_patch={
                    "job_id": local_job_id,
                    "execution_mode": "local",
                    "command": " ".join(command),
                    "progress_label": "Starting…",
                    "log_tail": initial_log.rstrip(),
                    "log_file": str(log_file),
                },
                status="running",
            )

        env = os.environ.copy()
        if normalized.get("market_data_source"):
            env["TICKER_SCREENER_MARKET_DATA_SOURCE"] = str(normalized["market_data_source"])
        if self.database_url:
            env["TICKER_SCREENER_DATABASE_URL"] = self.database_url
        thread = threading.Thread(target=self._run_job, args=(local_job_id, command, env), daemon=True)
        thread.start()
        return local_job_id

    def build_command(self, action_id: str, options: dict[str, Any] | None = None, *, normalized: bool = False) -> list[str]:
        action = self._actions.get(action_id)
        if action is None:
            raise ValueError(f"Unknown run action: {action_id}")

        normalized_options = dict(options or {}) if normalized else self._normalize_options(action, options or {})
        command = [sys.executable, action.script_path]
        command.extend(action.extra_args)
        if action.supports_limit and normalized_options.get("limit") is not None:
            command.extend(["--limit", str(normalized_options["limit"])])
        if normalized_options.get("tickers"):
            command.append("--tickers")
            command.extend(normalized_options["tickers"])
        if action_id == "reload_postgres_market_data_date":
            trade_date = str(normalized_options.get("trade_date") or "").strip()
            if not trade_date:
                raise ValueError("Trade Date is required.")
            command.append(trade_date)
        if normalized_options.get("date_label"):
            command.extend(["--date-label", str(normalized_options["date_label"])])
        if normalized_options.get("as_of_date"):
            command.extend(["--as-of-date", str(normalized_options["as_of_date"])])
        if normalized_options.get("source"):
            command.extend(["--source", str(normalized_options["source"])])
        if normalized_options.get("reference_date"):
            command.extend(["--reference-date", str(normalized_options["reference_date"])])
        if normalized_options.get("start_date"):
            command.extend(["--start-date", str(normalized_options["start_date"])])
        if normalized_options.get("end_date"):
            command.extend(["--end-date", str(normalized_options["end_date"])])
        if normalized_options.get("chunk_size") is not None:
            command.extend(["--chunk-size", str(normalized_options["chunk_size"])])
        if normalized_options.get("max_retries") is not None:
            command.extend(["--max-retries", str(normalized_options["max_retries"])])
        if normalized_options.get("retry_base_seconds") is not None:
            command.extend(["--retry-base-seconds", str(normalized_options["retry_base_seconds"])])
        if normalized_options.get("chunk_sleep_seconds") is not None:
            command.extend(["--chunk-sleep-seconds", str(normalized_options["chunk_sleep_seconds"])])
        if normalized_options.get("single_ticker_sleep_seconds") is not None:
            command.extend(["--single-ticker-sleep-seconds", str(normalized_options["single_ticker_sleep_seconds"])])
        if normalized_options.get("batch_size") is not None:
            command.extend(["--batch-size", str(normalized_options["batch_size"])])
        if normalized_options.get("resume_from"):
            command.extend(["--resume-from", str(normalized_options["resume_from"])])
        if normalized_options.get("retry_failed_from_manifest"):
            command.append("--retry-failed-from-manifest")
        if action_id == "sync_postgres_market_data" and normalized_options.get("include_excluded_tickers"):
            command.append("--include-excluded-tickers")
        if normalized_options.get("ensure_schema"):
            command.append("--ensure-schema")
        if normalized_options.get("strategy_ids_json"):
            command.extend(["--strategy-ids-json", str(normalized_options["strategy_ids_json"])])
        if normalized_options.get("overwrite_policy"):
            command.extend(["--overwrite-policy", str(normalized_options["overwrite_policy"])])
        if normalized_options.get("delay_min_seconds") is not None:
            command.extend(["--delay-min-seconds", str(normalized_options["delay_min_seconds"])])
        if normalized_options.get("delay_max_seconds") is not None:
            command.extend(["--delay-max-seconds", str(normalized_options["delay_max_seconds"])])
        if normalized_options.get("rest_seconds") is not None:
            command.extend(["--rest-seconds", str(normalized_options["rest_seconds"])])
        if normalized_options.get("fundamental_limit") is not None:
            command.extend(["--fundamental-limit", str(normalized_options["fundamental_limit"])])
        if normalized_options.get("technical_limit") is not None:
            command.extend(["--technical-limit", str(normalized_options["technical_limit"])])
        if normalized_options.get("upcoming_weeks") is not None:
            command.extend(["--upcoming-weeks", str(normalized_options["upcoming_weeks"])])
        if normalized_options.get("earnings_limit") is not None:
            command.extend(["--earnings-limit", str(normalized_options["earnings_limit"])])
        if normalized_options.get("scope_json"):
            command.extend(["--scope-json", str(normalized_options["scope_json"])])
        if normalized_options.get("candidate_threshold") is not None:
            command.extend(["--candidate-threshold", str(normalized_options["candidate_threshold"])])
        if normalized_options.get("max_parallel") is not None:
            command.extend(["--max-parallel", str(normalized_options["max_parallel"])])
        if normalized_options.get("batch_size_before_rest") is not None:
            command.extend(["--batch-size-before-rest", str(normalized_options["batch_size_before_rest"])])
        if normalized_options.get("circuit_breaker_consecutive_503") is not None:
            command.extend(["--circuit-breaker-consecutive-503", str(normalized_options["circuit_breaker_consecutive_503"])])
        if normalized_options.get("entry_signal_threshold") is not None:
            command.extend(["--entry-signal-threshold", str(normalized_options["entry_signal_threshold"])])
        if normalized_options.get("min_sector_peers") is not None:
            command.extend(["--min-sector-peers", str(normalized_options["min_sector_peers"])])
        if normalized_options.get("min_category_metrics") is not None:
            command.extend(["--min-category-metrics", str(normalized_options["min_category_metrics"])])
        if normalized_options.get("hold_periods_json"):
            command.extend(["--hold-periods-json", str(normalized_options["hold_periods_json"])])
        if normalized_options.get("entry_rule_json"):
            command.extend(["--entry-rule-json", str(normalized_options["entry_rule_json"])])
        if normalized_options.get("date_range_json"):
            command.extend(["--date-range-json", str(normalized_options["date_range_json"])])
        if normalized_options.get("exit_rules_json"):
            command.extend(["--exit-rules-json", str(normalized_options["exit_rules_json"])])
        if normalized_options.get("position_rules_json"):
            command.extend(["--position-rules-json", str(normalized_options["position_rules_json"])])
        if normalized_options.get("signal_cache_policy"):
            command.extend(["--signal-cache-policy", str(normalized_options["signal_cache_policy"])])
        if normalized_options.get("market_data_mode"):
            command.extend(["--market-data-mode", str(normalized_options["market_data_mode"])])
        if action_id in {"screener_history_batch", "signal_warm_batch"} and normalized_options.get("market_data_source"):
            command.extend(["--market-data-source", str(normalized_options["market_data_source"])])
        if action_id in {"screener_history_batch", "signal_warm_batch", "overlap_backtest_v1"} and normalized_options.get("job_run_id") is not None:
            command.extend(["--job-run-id", str(normalized_options["job_run_id"])])
        if normalized_options.get("filter_precedence"):
            command.extend(["--filter-precedence", str(normalized_options["filter_precedence"])])
        self._append_multi_args(command, "--include-sectors", normalized_options.get("include_sectors"))
        self._append_multi_args(command, "--exclude-sectors", normalized_options.get("exclude_sectors"))
        self._append_multi_args(command, "--include-industries", normalized_options.get("include_industries"))
        self._append_multi_args(command, "--exclude-industries", normalized_options.get("exclude_industries"))
        self._append_multi_args(command, "--include-themes", normalized_options.get("include_themes"))
        self._append_multi_args(command, "--exclude-themes", normalized_options.get("exclude_themes"))
        return command

    def _normalize_options(self, action: RunAction, options: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        if action.action_id in {"sync_finviz_fundamentals", "run_finviz_ratings_pipeline"}:
            normalized["overwrite_policy"] = "skip-existing"
        if action.supports_limit:
            raw_limit = options.get("limit")
            if raw_limit not in (None, ""):
                try:
                    limit = int(raw_limit)
                except (TypeError, ValueError) as exc:
                    raise ValueError("Limit must be an integer.") from exc
                if limit <= 0 or limit > 10000:
                    raise ValueError("Limit must be between 1 and 10000.")
                normalized["limit"] = limit

        raw_tickers = options.get("tickers")
        if isinstance(raw_tickers, str) and raw_tickers.strip():
            tickers = [item.strip().upper() for item in re.split(r"[\s,]+", raw_tickers.strip()) if item.strip()]
            if tickers:
                normalized["tickers"] = tickers
        elif isinstance(raw_tickers, list):
            tickers = [str(item).strip().upper() for item in raw_tickers if str(item).strip()]
            if tickers:
                normalized["tickers"] = tickers

        for key in (
            "date_label",
            "as_of_date",
            "trade_date",
            "reference_date",
            "source",
            "filter_precedence",
            "market_data_source",
            "start_date",
            "end_date",
            "overwrite_policy",
            "signal_cache_policy",
            "market_data_mode",
            "resume_from",
            "execution_mode",
            "target_worker",
        ):
            value = options.get(key)
            if isinstance(value, str) and value.strip():
                normalized[key] = value.strip()
        if "trade_date" in normalized:
            trade_date_value = str(normalized["trade_date"])
            if not self._is_template_token(trade_date_value):
                try:
                    dt.date.fromisoformat(trade_date_value)
                except ValueError as exc:
                    raise ValueError("Trade Date must be YYYY-MM-DD.") from exc
        if "as_of_date" in normalized:
            as_of_date_value = str(normalized["as_of_date"])
            if not self._is_template_token(as_of_date_value):
                try:
                    dt.date.fromisoformat(as_of_date_value)
                except ValueError as exc:
                    raise ValueError("As Of Date must be YYYY-MM-DD.") from exc

        for key in ("chunk_size", "max_retries", "batch_size", "candidate_threshold", "entry_signal_threshold", "max_parallel", "fundamental_limit", "technical_limit", "upcoming_weeks", "earnings_limit"):
            value = options.get(key)
            if value in (None, ""):
                continue
            try:
                normalized[key] = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{key.replace('_', ' ').title()} must be an integer.") from exc
        for key in ("delay_min_seconds", "delay_max_seconds", "rest_seconds", "min_category_metrics", "retry_base_seconds", "chunk_sleep_seconds", "single_ticker_sleep_seconds"):
            value = options.get(key)
            if value in (None, ""):
                continue
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{key.replace('_', ' ').title()} must be a number.") from exc
        for key in ("batch_size_before_rest", "min_sector_peers", "circuit_breaker_consecutive_503"):
            value = options.get(key)
            if value in (None, ""):
                continue
            try:
                normalized[key] = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{key.replace('_', ' ').title()} must be an integer.") from exc
        if "max_parallel" in normalized and (normalized["max_parallel"] < 1 or normalized["max_parallel"] > 20):
            raise ValueError("Max parallel must be between 1 and 20.")

        if "include_excluded_tickers" in options:
            normalized["include_excluded_tickers"] = bool(options.get("include_excluded_tickers"))
        if "retry_failed_from_manifest" in options:
            normalized["retry_failed_from_manifest"] = bool(options.get("retry_failed_from_manifest"))
        if "ensure_schema" in options:
            normalized["ensure_schema"] = bool(options.get("ensure_schema"))

        for key in (
            "include_sectors",
            "exclude_sectors",
            "include_industries",
            "exclude_industries",
            "include_themes",
            "exclude_themes",
        ):
            value = options.get(key)
            if isinstance(value, list):
                normalized_values = [str(item).strip() for item in value if str(item).strip()]
                if normalized_values:
                    normalized[key] = normalized_values

        strategy_ids: list[str] = []
        if isinstance(options.get("strategy_ids"), list):
            strategy_ids = [str(item).strip() for item in options["strategy_ids"] if str(item).strip()]
        elif isinstance(options.get("strategy_ids"), str) and options["strategy_ids"].strip():
            strategy_ids = [item.strip() for item in re.split(r"[\s,]+", str(options["strategy_ids"]).strip()) if item.strip()]
        if strategy_ids:
            normalized["strategy_ids"] = strategy_ids
            normalized["strategy_ids_json"] = json.dumps(strategy_ids)

        hold_periods_json = options.get("hold_periods_json")
        if isinstance(hold_periods_json, str) and hold_periods_json.strip():
            normalized["hold_periods_json"] = hold_periods_json.strip()

        for key, fallback in (
            ("scope", {}),
            ("entry_rule", {}),
            ("date_range", {}),
            ("position_rules", {}),
        ):
            value = options.get(key)
            if isinstance(value, dict):
                normalized[f"{key}_json"] = json.dumps(value)
        if isinstance(options.get("exit_rules"), list):
            normalized["exit_rules_json"] = json.dumps(options["exit_rules"])
        if "job_run_id" in options and options.get("job_run_id") not in (None, ""):
            normalized["job_run_id"] = int(options["job_run_id"])

        return normalized

    def _append_multi_args(self, command: list[str], flag: str, values: list[str] | None) -> None:
        if values:
            command.append(flag)
            command.extend(values)

    def _get_filter_catalog(self) -> dict[str, list[str]]:
        cache_key = str(self.project_root)
        cached = self._filter_catalog_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            catalog = build_filter_option_catalog(load_app_config())
        except Exception:
            catalog = {"sectors": [], "industries": [], "themes": []}
        self._filter_catalog_cache[cache_key] = catalog
        return catalog

    def _field_options(self, field: RunField, filter_catalog: dict[str, list[str]]) -> list[dict[str, str]]:
        if field.field_id == "strategy_ids":
            return [
                {"value": action.action_id, "label": action.label}
                for action in self._actions.values()
                if action.visible_in_runs and action.action_id not in {"signal_warm_batch", "overlap_backtest_v1"}
            ]
        if field.field_id.endswith("sectors"):
            return [{"value": value, "label": value} for value in filter_catalog.get("sectors", [])]
        if field.field_id.endswith("industries"):
            return [{"value": value, "label": value} for value in filter_catalog.get("industries", [])]
        if field.field_id.endswith("themes"):
            return [{"value": value, "label": value} for value in filter_catalog.get("themes", [])]
        return [{"value": value, "label": label} for value, label in field.options]

    def _run_job(self, job_id: str, command: list[str], env: dict[str, str]) -> None:
        process = subprocess.Popen(
            command,
            cwd=str(self.project_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            start_new_session=True,
        )

        log_lines: list[str] = []
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            job["_process"] = process
            log_path = Path(str(job.get("log_file") or ""))
        assert process.stdout is not None
        with log_path.open("a", encoding="utf-8") as log_handle:
            for line in process.stdout:
                log_handle.write(line)
                log_handle.flush()
                log_lines.append(line.rstrip())
                log_lines = log_lines[-80:]
                with self._jobs_lock:
                    job = self._jobs_by_id[job_id]
                    job["log_tail"] = "\n".join(log_lines)
                    self._update_progress(job, log_lines)
                    self._update_artifacts(job, line.rstrip())
                    snapshot = {
                        "job_id": str(job.get("job_id") or ""),
                        "execution_mode": "local",
                        "log_tail": str(job.get("log_tail") or ""),
                        "log_file": str(job.get("log_file") or ""),
                        "progress_current": job.get("progress_current"),
                        "progress_total": job.get("progress_total"),
                        "progress_percent": job.get("progress_percent"),
                        "progress_label": job.get("progress_label"),
                        "success_count": int(job.get("success_count") or 0),
                        "summary_file": str(job.get("summary_file") or ""),
                        "watchlist_file": str(job.get("watchlist_file") or ""),
                        "raw_results_file": str(job.get("raw_results_file") or ""),
                        "job_run_id": job.get("job_run_id"),
                    }
                self.history_repository.patch_job_run_result(
                    snapshot.get("job_run_id"),
                    result_payload_patch=snapshot,
                    artifact_path=str(snapshot.get("summary_file") or snapshot.get("watchlist_file") or "") or None,
                )

        return_code = process.wait()
        finished_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
        with self._jobs_lock:
            job = self._jobs_by_id[job_id]
            was_cancelled = bool(job.get("cancel_requested"))
            job["status"] = "cancelled" if was_cancelled else ("success" if return_code == 0 else "failed")
            job["return_code"] = return_code
            job["finished_at"] = finished_at
            job["log_tail"] = "\n".join(log_lines) if log_lines else job["log_tail"]
            job["_finished_monotonic"] = time.monotonic()
            job.pop("_process", None)
            self._load_summary_metadata(job)
            if was_cancelled:
                job["progress_label"] = "Cancelled"
            elif return_code == 0:
                job["progress_percent"] = 100
                job["progress_label"] = "Completed"
            elif job.get("progress_percent") is None:
                job["progress_label"] = "Failed"
        self._persist_completed_job(job_id)

    def _terminate_process(self, process: Any) -> None:
        pid = getattr(process, "pid", None)
        if isinstance(pid, int) and pid > 0:
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
                return
            except Exception:
                pass
        process.terminate()

    def _update_progress(self, job: dict[str, Any], log_lines: list[str]) -> None:
        progress = self._extract_progress(log_lines)
        if progress["success_count"] is not None:
            job["success_count"] = progress["success_count"]
        if progress["current"] is None or progress["total"] is None or progress["total"] <= 0:
            if progress["percent"] is not None or progress["label"] is not None:
                job["progress_percent"] = progress["percent"]
                job["progress_label"] = progress["label"]
            return
        job["progress_current"] = progress["current"]
        job["progress_total"] = progress["total"]
        job["progress_percent"] = progress["percent"]
        job["progress_label"] = progress["label"]

    def _extract_progress(self, log_lines: list[str]) -> dict[str, Any]:
        current = None
        total = None
        last_line = ""
        success_count = None
        stage_current = None
        stage_total = None
        stage_label = None
        for line in reversed(log_lines):
            if current is None:
                match = self._progress_pattern.search(line)
            else:
                match = None
            if match:
                current = int(match.group(1))
                total = int(match.group(2))
                last_line = line
            if stage_current is None:
                stage_match = self._stage_pattern.search(line.strip())
                if stage_match:
                    stage_current = int(stage_match.group(1))
                    stage_total = int(stage_match.group(2))
                    stage_label = stage_match.group(3).strip()
            if success_count is None:
                passed_match = self._passed_pattern.search(line)
                if passed_match:
                    success_count = int(passed_match.group(1))
            if current is not None and total is not None and success_count is not None and stage_current is not None:
                break
        percent = None
        label = None
        if current is not None and total is not None and total > 0:
            percent = max(0, min(100, round((current / total) * 100)))
            if stage_current is not None and stage_total is not None and stage_label:
                label = f"Stage {stage_current}/{stage_total} · {current}/{total} {stage_label.lower()}"
            else:
                detail = "screening" if "screening" in last_line.lower() else "processing"
                label = f"{current}/{total} {detail}"
        elif stage_current is not None and stage_total is not None and stage_total > 0 and stage_label:
            percent = max(0, min(99, round(((stage_current - 1) / stage_total) * 100)))
            label = f"Stage {stage_current}/{stage_total} · {stage_label}"
        return {
            "current": current,
            "total": total,
            "percent": percent,
            "label": label,
            "success_count": success_count,
        }

    def _update_artifacts(self, job: dict[str, Any], line: str) -> None:
        watchlist_match = self._watchlist_path_pattern.search(line)
        if watchlist_match:
            job["watchlist_file"] = watchlist_match.group(1).strip()

        summary_match = self._summary_path_pattern.search(line)
        if summary_match:
            job["summary_file"] = summary_match.group(1).strip()

    def _load_summary_metadata(self, job: dict[str, Any]) -> None:
        summary_file = str(job.get("summary_file") or "").strip()
        if not summary_file:
            return
        summary_path = Path(summary_file)
        if not summary_path.exists():
            return
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return
        passed_tickers = payload.get("passed_tickers")
        if isinstance(passed_tickers, int):
            job["success_count"] = passed_tickers
        watchlist_file = payload.get("watchlist_file")
        if isinstance(watchlist_file, str) and watchlist_file.strip():
            job["watchlist_file"] = watchlist_file.strip()
        raw_results_file = payload.get("raw_results_file")
        if isinstance(raw_results_file, str) and raw_results_file.strip():
            job["raw_results_file"] = raw_results_file.strip()
        backtest_run_id = payload.get("backtest_run_id")
        if isinstance(backtest_run_id, int):
            job["backtest_run_id"] = backtest_run_id

    def _serialize_job(self, job: dict[str, Any]) -> dict[str, Any]:
        duration_seconds = self._job_duration_seconds(job)
        watchlist_file = str(job.get("watchlist_file") or "")
        watchlist_stem = self._watchlist_stem_from_path(watchlist_file)
        options = dict(job.get("options") or {})
        return {
            "job_id": str(job.get("job_id") or ""),
            "action_id": str(job.get("action_id") or ""),
            "label": str(job.get("label") or ""),
            "status": str(job.get("status") or "failed"),
            "command": str(job.get("command") or ""),
            "started_at": str(job.get("started_at") or ""),
            "finished_at": str(job.get("finished_at") or ""),
            "return_code": job.get("return_code"),
            "log_tail": str(job.get("log_tail") or ""),
            "progress_current": job.get("progress_current"),
            "progress_total": job.get("progress_total"),
            "progress_percent": job.get("progress_percent"),
            "progress_label": job.get("progress_label"),
            "success_count": int(job.get("success_count") or 0),
            "log_file": str(job.get("log_file") or ""),
            "watchlist_file": watchlist_file,
            "watchlist_stem": watchlist_stem,
            "watchlist_url": f"/watchlists?stem={watchlist_stem}" if watchlist_stem else "",
            "summary_file": str(job.get("summary_file") or ""),
            "raw_results_file": str(job.get("raw_results_file") or ""),
            "scan_target": self._describe_job_scan_target(str(job.get("action_id") or ""), options),
            "job_run_id": job.get("job_run_id"),
            "screen_run_id": job.get("screen_run_id"),
            "backtest_run_id": job.get("backtest_run_id"),
            "cancel_requested": bool(job.get("cancel_requested")),
            "execution_mode": str(job.get("execution_mode") or "local"),
            "worker_name": str(job.get("worker_name") or ""),
            "target_worker": str(options.get("target_worker") or ""),
            "duration_seconds": duration_seconds,
            "child_jobs": [],
            "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
        }

    def _job_duration_seconds(self, job: dict[str, Any]) -> int:
        started = job.get("_started_monotonic")
        if not isinstance(started, (int, float)):
            return 0
        finished = job.get("_finished_monotonic")
        end = finished if isinstance(finished, (int, float)) else time.monotonic()
        return max(0, int(round(end - started)))

    def _append_log_line(self, job: dict[str, Any], line: str) -> None:
        log_tail = str(job.get("log_tail") or "")
        log_lines = log_tail.splitlines() if log_tail else []
        log_lines.append(line)
        log_lines = log_lines[-80:]
        job["log_tail"] = "\n".join(log_lines)

    def _list_remote_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.history_repository.list_remote_job_runs(limit=max(limit * 2, 20))
        return [self._serialize_remote_job_run(row) for row in rows]

    def _list_local_persisted_jobs(self, *, limit: int) -> list[dict[str, Any]]:
        rows = self.history_repository.list_local_job_runs(limit=max(limit * 2, 20))
        jobs: list[dict[str, Any]] = []
        for row in rows:
            serialized = self._serialize_persisted_local_job_run(row)
            if serialized is not None:
                jobs.append(serialized)
        return jobs

    def _load_persisted_local_job(self, job_id: str) -> dict[str, Any] | None:
        row = self.history_repository.get_job_run_by_result_job_id(job_id)
        if row is None:
            return None
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        if str(request_payload.get("execution_mode") or request_payload.get("options", {}).get("execution_mode") or "local") == "remote":
            return None
        return self._serialize_persisted_local_job_run(row)

    def _load_remote_job(self, job_id: str) -> dict[str, Any] | None:
        remote_job_run_id = self._remote_job_run_id(job_id)
        if remote_job_run_id is None:
            return None
        row = self.history_repository.get_job_run(remote_job_run_id)
        if row is None:
            return None
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        if str(request_payload.get("execution_mode") or request_payload.get("options", {}).get("execution_mode") or "local") != "remote":
            return None
        return self._serialize_remote_job_run(row)

    def _serialize_remote_job_run(self, row: dict[str, Any]) -> dict[str, Any]:
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
        action_id = str(request_payload.get("action_id") or "")
        started_at = self._stringify_timestamp(row.get("started_at"))
        finished_at = self._stringify_timestamp(row.get("finished_at"))
        command = str(result_payload.get("command") or "")
        if not command and action_id:
            try:
                command = " ".join(self.build_command(action_id, options, normalized=True))
            except Exception:
                command = ""
        job_run_id = int(row["id"])
        watchlist_file = str(result_payload.get("watchlist_file") or "")
        watchlist_stem = self._watchlist_stem_from_path(watchlist_file)
        return {
            "job_id": self._remote_job_id(job_run_id),
            "action_id": action_id,
            "label": str(row.get("job_name") or ""),
            "status": str(row.get("status") or "failed"),
            "command": command,
            "started_at": started_at,
            "finished_at": finished_at,
            "return_code": result_payload.get("return_code"),
            "log_tail": str(result_payload.get("log_tail") or ""),
            "progress_current": result_payload.get("progress_current"),
            "progress_total": result_payload.get("progress_total"),
            "progress_percent": result_payload.get("progress_percent"),
            "progress_label": result_payload.get("progress_label"),
            "success_count": int(result_payload.get("success_count") or 0),
            "log_file": str(result_payload.get("log_file") or ""),
            "watchlist_file": watchlist_file,
            "watchlist_stem": watchlist_stem,
            "watchlist_url": f"/watchlists?stem={watchlist_stem}" if watchlist_stem else "",
            "summary_file": str(result_payload.get("summary_file") or ""),
            "raw_results_file": str(result_payload.get("raw_results_file") or ""),
            "scan_target": self._describe_job_scan_target(action_id, options),
            "job_run_id": job_run_id,
            "screen_run_id": result_payload.get("screen_run_id"),
            "backtest_run_id": result_payload.get("backtest_run_id"),
            "cancel_requested": bool(result_payload.get("cancel_requested")),
            "execution_mode": "remote",
            "worker_name": str(result_payload.get("worker_name") or ""),
            "target_worker": str(options.get("target_worker") or ""),
            "duration_seconds": self._duration_seconds_from_iso(started_at, finished_at),
            "child_jobs": [],
            "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
        }

    def _serialize_persisted_local_job_run(self, row: dict[str, Any]) -> dict[str, Any] | None:
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        action_id = str(request_payload.get("action_id") or "")
        if not action_id:
            return None
        options = request_payload.get("options") if isinstance(request_payload.get("options"), dict) else {}
        started_at = self._stringify_timestamp(row.get("started_at"))
        finished_at = self._stringify_timestamp(row.get("finished_at"))
        command = str(result_payload.get("command") or "")
        if not command:
            try:
                command = " ".join(self.build_command(action_id, options, normalized=True))
            except Exception:
                command = ""
        watchlist_file = str(result_payload.get("watchlist_file") or "")
        watchlist_stem = self._watchlist_stem_from_path(watchlist_file)
        return {
            "job_id": str(result_payload.get("job_id") or ""),
            "action_id": action_id,
            "label": str(row.get("job_name") or ""),
            "status": str(row.get("status") or "failed"),
            "command": command,
            "started_at": started_at,
            "finished_at": finished_at,
            "return_code": result_payload.get("return_code"),
            "log_tail": str(result_payload.get("log_tail") or ""),
            "progress_current": result_payload.get("progress_current"),
            "progress_total": result_payload.get("progress_total"),
            "progress_percent": result_payload.get("progress_percent"),
            "progress_label": result_payload.get("progress_label"),
            "success_count": int(result_payload.get("success_count") or 0),
            "log_file": str(result_payload.get("log_file") or ""),
            "watchlist_file": watchlist_file,
            "watchlist_stem": watchlist_stem,
            "watchlist_url": f"/watchlists?stem={watchlist_stem}" if watchlist_stem else "",
            "summary_file": str(result_payload.get("summary_file") or ""),
            "raw_results_file": str(result_payload.get("raw_results_file") or ""),
            "scan_target": self._describe_job_scan_target(action_id, options),
            "job_run_id": int(row["id"]),
            "screen_run_id": result_payload.get("screen_run_id"),
            "backtest_run_id": result_payload.get("backtest_run_id"),
            "cancel_requested": bool(result_payload.get("cancel_requested")),
            "execution_mode": "local",
            "worker_name": "",
            "target_worker": "",
            "duration_seconds": self._duration_seconds_from_iso(started_at, finished_at),
            "child_jobs": [],
            "child_job_summary": {"total": 0, "running": 0, "success": 0, "failed": 0, "cancelled": 0},
        }

    def _sort_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def sort_key(job: dict[str, Any]) -> tuple[int, str, str]:
            status = str(job.get("status") or "")
            running_rank = 2 if status == "running" else (1 if status == "queued" else 0)
            started = str(job.get("started_at") or "")
            finished = str(job.get("finished_at") or "")
            return (running_rank, max(started, finished), str(job.get("job_id") or ""))

        return sorted(jobs, key=sort_key, reverse=True)

    def _dedupe_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for job in jobs:
            job_id = str(job.get("job_id") or "").strip()
            if not job_id or job_id in seen:
                continue
            seen.add(job_id)
            deduped.append(job)
        return deduped

    def _remote_job_id(self, job_run_id: int | None) -> str:
        return f"remote-{int(job_run_id)}" if job_run_id is not None else ""

    def _remote_job_run_id(self, job_id: str) -> int | None:
        text = str(job_id or "").strip()
        if not text.startswith("remote-"):
            return None
        try:
            return int(text.split("-", 1)[1])
        except (TypeError, ValueError):
            return None

    def _now_iso(self) -> str:
        return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    def _watchlist_stem_from_path(self, watchlist_file: str) -> str:
        if not watchlist_file:
            return ""
        return watchlist_stem_from_path(watchlist_file.strip())

    def _job_type_for_action(self, action_id: str) -> str:
        if action_id in {"screener_history_batch", "signal_warm_batch"}:
            return "screen_cache_batch"
        if action_id in {"overlap_backtest_v1"}:
            return "backtest_run"
        if action_id in {"sync_postgres_market_data", "reload_postgres_market_data_date", "sync_finviz_fundamentals", "sync_chart_fundamentals_cache", "build_sector_rating_baselines", "build_ticker_ratings", "build_technical_ratings", "run_finviz_ratings_pipeline"}:
            return "admin_sync"
        return "screen_run"

    def _persist_completed_job(self, job_id: str) -> None:
        with self._jobs_lock:
            job = dict(self._jobs_by_id.get(job_id) or {})
        if not job:
            return
        result_payload = {
            "job_id": job.get("job_id"),
            "status": job.get("status"),
            "return_code": job.get("return_code"),
            "summary_file": job.get("summary_file"),
            "watchlist_file": job.get("watchlist_file"),
            "raw_results_file": job.get("raw_results_file"),
            "success_count": job.get("success_count"),
        }
        artifact_path = str(job.get("summary_file") or job.get("watchlist_file") or "")
        self.history_repository.update_job_run(
            job.get("job_run_id"),
            status=str(job.get("status") or "failed"),
            result_payload=result_payload,
            artifact_path=artifact_path or None,
            finished_at=str(job.get("finished_at")) if job.get("finished_at") else None,
        )
        if str(job.get("status")) != "success":
            return
        action_id = str(job.get("action_id") or "")
        if action_id in {"screener_history_batch", "signal_warm_batch", "sync_postgres_market_data", "reload_postgres_market_data_date", "run_finviz_ratings_pipeline", "sync_finviz_fundamentals", "sync_chart_fundamentals_cache", "build_sector_rating_baselines", "build_ticker_ratings", "build_technical_ratings", "overlap_backtest_v1"}:
            return
        summary_file = str(job.get("summary_file") or "").strip()
        if not summary_file:
            return
        summary_path = Path(summary_file)
        if not summary_path.exists():
            return
        try:
            summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return
        raw_results_file = str(summary_payload.get("raw_results_file") or job.get("raw_results_file") or "").strip()
        if not raw_results_file:
            return
        raw_path = Path(raw_results_file)
        if not raw_path.exists():
            return
        try:
            raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except Exception:
            return
        screen_run_id = self.screener_history_service.persist_screen_run(
            strategy_id=action_id,
            options=dict(job.get("options") or {}),
            summary_payload=summary_payload,
            raw_payload=raw_payload,
            job_run_id=job.get("job_run_id"),
        )
        if screen_run_id is not None:
            with self._jobs_lock:
                live = self._jobs_by_id.get(job_id)
                if live is not None:
                    live["screen_run_id"] = screen_run_id

    def _attach_child_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        parent_job_run_ids = [
            int(job["job_run_id"])
            for job in jobs
            if job.get("action_id") in {"screener_history_batch", "signal_warm_batch"} and isinstance(job.get("job_run_id"), int)
        ]
        if not parent_job_run_ids:
            return jobs
        child_rows = self.history_repository.list_child_job_runs(parent_job_run_ids)
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in child_rows:
            parent_id = row.get("parent_job_run_id")
            if isinstance(parent_id, int):
                grouped.setdefault(parent_id, []).append(self._serialize_child_job_run(row))
        for job in jobs:
            parent_id = job.get("job_run_id")
            if not isinstance(parent_id, int):
                continue
            child_jobs = grouped.get(parent_id, [])
            job["child_jobs"] = child_jobs
            job["child_job_summary"] = self._summarize_child_jobs(child_jobs)
            if job.get("action_id") in {"screener_history_batch", "signal_warm_batch"}:
                job["scan_target"] = self._describe_batch_scan_target(job, child_jobs)
        return jobs

    def _serialize_child_job_run(self, row: dict[str, Any]) -> dict[str, Any]:
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        result_payload = row.get("result_payload") if isinstance(row.get("result_payload"), dict) else {}
        started_at = self._stringify_timestamp(row.get("started_at"))
        finished_at = self._stringify_timestamp(row.get("finished_at"))
        log_tail = str(result_payload.get("log_tail") or "")
        progress = self._extract_progress(log_tail.splitlines() if log_tail else [])
        return {
            "job_run_id": int(row["id"]),
            "parent_job_run_id": row.get("parent_job_run_id"),
            "job_type": str(row.get("job_type") or ""),
            "label": str(row.get("job_name") or ""),
            "status": str(row.get("status") or "failed"),
            "started_at": started_at,
            "finished_at": finished_at,
            "artifact_path": str(row.get("artifact_path") or ""),
            "command": str(request_payload.get("command") or ""),
            "strategy_id": str(result_payload.get("strategy_id") or request_payload.get("strategy_id") or ""),
            "run_date": str(result_payload.get("run_date") or request_payload.get("run_date") or ""),
            "screen_run_id": result_payload.get("screen_run_id"),
            "success_count": int(result_payload.get("success_count") or progress["success_count"] or 0),
            "summary_file": str(result_payload.get("summary_file") or ""),
            "watchlist_file": str(result_payload.get("watchlist_file") or ""),
            "raw_results_file": str(result_payload.get("raw_results_file") or ""),
            "log_tail": log_tail,
            "log_file": str(result_payload.get("log_file") or ""),
            "message": str(result_payload.get("message") or ""),
            "skipped": bool(result_payload.get("skipped")),
            "progress_current": progress["current"],
            "progress_total": progress["total"],
            "progress_percent": progress["percent"],
            "progress_label": progress["label"],
            "duration_seconds": self._duration_seconds_from_iso(started_at, finished_at),
        }

    def _summarize_child_jobs(self, child_jobs: list[dict[str, Any]]) -> dict[str, int]:
        summary = {"total": len(child_jobs), "running": 0, "success": 0, "failed": 0, "cancelled": 0}
        for job in child_jobs:
            status = str(job.get("status") or "")
            if status in summary:
                summary[status] += 1
        return summary

    def _describe_job_scan_target(self, action_id: str, options: dict[str, Any]) -> str:
        if action_id in {"screener_history_batch", "signal_warm_batch", "overlap_backtest_v1"}:
            start_date = str(options.get("start_date") or "").strip()
            end_date = str(options.get("end_date") or "").strip()
            if start_date and end_date:
                return f"{start_date} to {end_date}"
            return start_date or end_date
        as_of_date = str(options.get("as_of_date") or "").strip()
        if as_of_date:
            return as_of_date
        trade_date = str(options.get("trade_date") or "").strip()
        if trade_date:
            return trade_date
        date_label = str(options.get("date_label") or "").strip()
        if date_label:
            return date_label
        return ""

    def _describe_batch_scan_target(self, job: dict[str, Any], child_jobs: list[dict[str, Any]]) -> str:
        running_child = next((item for item in child_jobs if item.get("status") == "running" and str(item.get("run_date") or "").strip()), None)
        if running_child is not None:
            return str(running_child.get("run_date") or "")
        return self._describe_job_scan_target(str(job.get("action_id") or ""), dict(job.get("options") or {}))

    def _duration_seconds_from_iso(self, started_at: str, finished_at: str) -> int:
        if not started_at:
            return 0
        try:
            started = dt.datetime.fromisoformat(started_at)
        except ValueError:
            return 0
        end_raw = finished_at or self._now_iso()
        try:
            finished = dt.datetime.fromisoformat(end_raw)
        except ValueError:
            return 0
        return max(0, int(round((finished - started).total_seconds())))

    def _stringify_timestamp(self, value: Any) -> str:
        if isinstance(value, dt.datetime):
            return value.isoformat()
        return str(value or "")

    def _resolve_as_of_date(self, normalized: dict[str, Any]) -> dt.date:
        value = str(normalized.get("as_of_date") or "").strip()
        if value:
            if self._is_template_token(value):
                # Scheduler resolves template tokens before launch. For precheck/save-time flows,
                # fall back to current local date instead of raising on the unresolved token.
                return dt.date.today()
            return dt.date.fromisoformat(value)
        return dt.date.today()

    def _resolve_precheck_universe(self, *, config: Any, normalized: dict[str, Any]) -> list[UniverseTicker]:
        tickers = normalized.get("tickers")
        if isinstance(tickers, list) and tickers:
            return [UniverseTicker(symbol=str(item).strip().upper()) for item in tickers if str(item).strip()]
        universe = load_universe(config, limit=normalized.get("limit"))
        criteria = UniverseFilterCriteria(
            filter_precedence=str(normalized.get("filter_precedence") or "exclude"),
            include_sectors=tuple(str(item).strip().lower() for item in normalized.get("include_sectors") or [] if str(item).strip()),
            exclude_sectors=tuple(str(item).strip().lower() for item in normalized.get("exclude_sectors") or [] if str(item).strip()),
            include_industries=tuple(str(item).strip().lower() for item in normalized.get("include_industries") or [] if str(item).strip()),
            exclude_industries=tuple(str(item).strip().lower() for item in normalized.get("exclude_industries") or [] if str(item).strip()),
            include_themes=tuple(str(item).strip().lower() for item in normalized.get("include_themes") or [] if str(item).strip()),
            exclude_themes=tuple(str(item).strip().lower() for item in normalized.get("exclude_themes") or [] if str(item).strip()),
        )
        return filter_universe_by_criteria(universe, criteria)

    def _frame_is_db_ready(self, frame: Any, target_date: dt.date, minimum_rows: int) -> bool:
        if frame is None:
            return False
        try:
            row_count = len(frame)
        except TypeError:
            return False
        return row_count >= minimum_rows and db_frame_has_recent_coverage(frame, target_date)
