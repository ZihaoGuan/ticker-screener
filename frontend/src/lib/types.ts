export type StrategyCard = {
  id: string;
  label: string;
  description: string;
  lastRun?: string;
  hits?: number;
  accent?: "up" | "neutral";
};

export type JobStatus = "queued" | "running" | "success" | "failed" | "cancelled";

export type RoleName = "visitor" | "premium" | "admin";

export type CapabilityName =
  | "view_results"
  | "run_screeners"
  | "manage_exclusions"
  | "sync_history"
  | "manage_users";

export type UserSummary = {
  authenticated: boolean;
  user_id?: number | null;
  email?: string | null;
  role: RoleName;
  capabilities: CapabilityName[];
  is_active: boolean;
};

export type AuthMeResponse = {
  authenticated: boolean;
  user: UserSummary | null;
  role: RoleName;
  capabilities: CapabilityName[];
};

export type AccessRequestStatus = "pending" | "approved" | "denied";

export type AccessRequestSummary = {
  id: number;
  email: string;
  requested_role: RoleName;
  status: AccessRequestStatus;
  requested_at?: string | null;
  reviewed_at?: string | null;
  reviewed_by_user_id?: number | null;
  reviewed_by_email?: string | null;
  deny_reason?: string;
  invited_user_id?: number | null;
  invited_user_email?: string | null;
  created_at?: string | null;
};

export type AuditEventSummary = {
  id: number;
  event_at: string;
  actor_user_id?: number | null;
  actor_email?: string | null;
  actor_role?: string | null;
  request_ip?: string | null;
  request_user_agent?: string | null;
  action: string;
  resource_type: string;
  resource_id?: string | null;
  resource_label?: string | null;
  status: string;
  message: string;
  metadata_json: Record<string, unknown>;
};

export type AuditEventsResponse = {
  events: AuditEventSummary[];
  filters: {
    actorEmail: string;
    action: string;
    resourceType: string;
    from: string;
    to: string;
    limit: number;
    offset: number;
  };
  limit: number;
  offset: number;
  has_more: boolean;
};

export type ScreenerJob = {
  jobId: string;
  label: string;
  status: JobStatus;
  startedAt: string;
  finishedAt: string;
  returnCode: string | number | null;
  command?: string;
  logTail?: string;
};

export type WatchlistFile = {
  stem: string;
  name: string;
  path: string;
  group_key: string;
  group_label: string;
  captured_at: string;
  sort_date?: string | null;
};

export type WatchlistTicker = {
  ticker: string;
  company?: string;
  scoreLabel?: string;
  score?: number;
  lastPrice?: number;
  dailyChangePct?: number;
  summary?: string;
  industry?: string;
  sector?: string;
};

export type CandlePoint = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type OverlapEntry = {
  ticker: string;
  pipelines?: string[];
  pipeline_count: number;
  pipeline_labels: string[];
  sector?: string;
  theme_tags?: string[];
  atr14?: number | null;
  adr14_pct?: number | null;
  adr14_in_range?: boolean | null;
  atr_multiple_from_50ma?: number | null;
  trim_warning?: boolean;
};

export type OverlapPipelineStatus = {
  id: string;
  label: string;
  count: number;
  file_present: boolean;
  bias_group?: "bullish" | "bearish" | "other";
  bullish_subgroup?: "leaders" | "bottoming" | "";
};

export type DashboardResponse = {
  overview: {
    database_configured: boolean;
    artifacts_dir: string;
    latest_sync_at: string | null;
    screen_run_count: number | null;
  };
  market_health: {
    regime: {
      ticker: string;
      data_source: string;
      latest: {
        date: string;
        weekly_bar_date: string;
        daily_close: number;
        daily_ema21: number;
        weekly_close: number;
        weekly_ema21: number;
        weekly_uptrend: boolean;
        daily_downtrend: boolean;
        regime:
          | "healthy_chaos"
          | "perfect_convergence_bull"
          | "perfect_convergence_bear"
          | "bear_market_rally";
        regime_label: string;
        summary: string;
        explanation: string;
        daily_distance_pct: number | null;
        weekly_distance_pct: number | null;
      } | null;
    };
    rsi_divergence: {
      ticker: string;
      data_source: string;
      latest: {
        signal_date: string;
        signal_price: number;
        previous_signal_date: string;
        previous_signal_price: number;
        signal_rsi: number;
        previous_signal_rsi: number;
        bars_apart: number;
        bars_since_signal: number;
        active_bars: number;
        fresh_bars: number;
        reset_rsi_threshold: number;
        current_close: number;
        current_rsi: number;
        current_ema21: number;
        distance_from_signal_pct: number;
        state: "fresh_top_warning" | "active_top_warning" | "lifted" | "invalidated";
        label: string;
        lift_reason: "rsi_reset" | "below_ema21" | "expired" | null;
        explanation: string;
      } | null;
    };
    bearish_td9: {
      ticker: string;
      data_source: string;
      latest: {
        signal_date: string;
        direction: "bearish";
        setup_count: number;
        current_price: number;
        signal_close: number;
        comparison_close: number;
        reasons: string[];
        label: string;
        explanation: string;
        distance_from_compare_pct: number | null;
      } | null;
    };
    spy_extension: {
      ticker: string;
      label: string;
      timeframe: "weekly";
      ma_type: "sma" | "ema";
      length: number;
      warning_pct: number;
      extreme_pct: number;
      data_source: string;
      latest: {
        time: string;
        state: "normal" | "warning" | "extreme";
        close: number;
        moving_average: number;
        distance: number;
        extension_pct: number;
      } | null;
    };
  };
  recent_watchlists: WatchlistFile[];
  strategy_cards: StrategyCard[];
};

export type RunAction = {
  id: string;
  label: string;
  bias_group?: "bullish" | "bearish" | "other";
  bullish_subgroup?: "leaders" | "bottoming" | "";
  command: string;
  supports_limit: boolean;
  fields: Array<{
    id: string;
    label: string;
    type: "text" | "number" | "date" | "select" | "multiselect";
    placeholder?: string | null;
    help_text?: string | null;
    options: Array<{ value: string; label: string }>;
  }>;
};

export type RunPrecheckResponse = {
  applicable: boolean;
  configured: boolean;
  action_id: string;
  market_data_source: string;
  message?: string;
  as_of_date?: string;
  lookback_trading_days?: number;
  total_tickers?: number;
  db_ready_tickers?: number;
  fallback_tickers?: number;
  db_ready_pct?: number;
  sample_fallback_tickers?: string[];
  benchmark?: {
    ticker: string;
    required: boolean;
    db_ready: boolean;
    bar_count?: number | null;
  };
  notes?: string[];
};

export type JobsResponse = {
  actions: RunAction[];
  jobs: {
    job_id: string;
    action_id: string;
    job_run_id?: number | null;
    label: string;
    status: JobStatus;
    started_at: string;
    finished_at: string;
    return_code: number | null;
    command: string;
    log_tail: string;
    progress_current: number | null;
    progress_total: number | null;
    progress_percent: number | null;
    progress_label: string | null;
    success_count: number;
    watchlist_file: string;
    watchlist_stem: string;
    watchlist_url: string;
    summary_file: string;
    raw_results_file?: string;
    scan_target?: string;
    screen_run_id?: number | null;
    backtest_run_id?: number | null;
    cancel_requested: boolean;
    duration_seconds: number;
    child_job_summary: {
      total: number;
      running: number;
      success: number;
      failed: number;
      cancelled: number;
    };
    child_jobs: Array<{
      job_run_id: number;
      parent_job_run_id?: number | null;
      job_type: string;
      label: string;
      status: JobStatus;
      started_at: string;
      finished_at: string;
      artifact_path: string;
      command: string;
      strategy_id: string;
      run_date: string;
      screen_run_id?: number | null;
      success_count: number;
      summary_file: string;
      watchlist_file: string;
      raw_results_file: string;
      log_tail: string;
      log_file: string;
      message: string;
      skipped: boolean;
      progress_current: number | null;
      progress_total: number | null;
      progress_percent: number | null;
      progress_label: string | null;
      duration_seconds: number;
    }>;
  }[];
};

export type WatchlistsResponse = {
  watchlists: WatchlistFile[];
};

export type WatchlistDetailResponse = {
  stem: string;
  entry_count: number;
  entries: Record<string, unknown>[];
};

export type WeeklyWatchlistEntry = Record<string, unknown> & {
  ticker: string;
  setup_label?: string;
  summary?: string;
  master_note?: string;
  sector?: string;
  industry?: string;
  exchange?: string;
  theme_tags?: string[];
  signal_badges?: string[];
  rs_rank?: number;
  score?: number;
  trigger_price?: number;
};

export type WeeklyWatchlistResponse = {
  source_stem: string;
  source_name: string;
  captured_at: string;
  sort_date?: string | null;
  group_label: string;
  entry_count: number;
  entries: WeeklyWatchlistEntry[];
  available_files: WatchlistFile[];
};

export type ExclusionEntry = {
  ticker: string;
  reason: string;
  reasons: string[];
  sources: string[];
  source_kinds: string[];
  removable: boolean;
};

export type PartialTickerSummary = {
  ticker: string;
};

export type PartialTickerDetailResponse = {
  ticker: string;
  coverage_start: string;
  coverage_end: string;
  first_trade_date: string | null;
  last_trade_date: string | null;
  bar_count: number;
  missing_ranges: Array<{ start: string; end: string; days: number }>;
  missing_date_count: number;
  sample_missing_dates: string[];
};

export type WatchlistChartResponse = {
  ticker: string;
  benchmark_ticker: string;
  period?: string;
  requested_as_of_date?: string | null;
  resolved_as_of_date?: string | null;
  latest_available_date?: string | null;
  data_source?: string | null;
  candles: Array<{ time: string; open: number; high: number; low: number; close: number }>;
  volume: Array<{ time: string; value: number }>;
  ma20: Array<{ time: string; value: number }>;
  ma50: Array<{ time: string; value: number }>;
  ma200: Array<{ time: string; value: number }>;
  ema8: Array<{ time: string; value: number }>;
  ema21: Array<{ time: string; value: number }>;
  weekly_ema8: Array<{ time: string; value: number }>;
  ipo_vwap: Array<{ time: string; value: number }>;
  market_extension: {
    config: {
      timeframe: "weekly";
      ma_type: "sma" | "ema";
      length: number;
      warning_pct: number;
      extreme_pct: number;
      label: string;
    };
    line: Array<{ time: string; value: number }>;
    signals: Array<{
      time: string;
      state: "warning" | "extreme";
      close: number;
      moving_average: number;
      distance: number;
      extension_pct: number;
    }>;
    latest: {
      time: string;
      state: "normal" | "warning" | "extreme";
      close: number;
      moving_average: number;
      distance: number;
      extension_pct: number;
    } | null;
  };
  rs_line: Array<{ time: string; value: number }>;
  daily_rs_rating?: Array<{ time: string; value: number }>;
  weekly_rs_rating?: Array<{ time: string; value: number }>;
  rs_markers: Array<{ time: string; kind: "daily_new_high" | "daily_new_high_before_price" }>;
  setup_markers?: Array<{ time: string; kind: string; label?: string }>;
  fearzone_panel: {
    rows: Array<{
      key: string;
      label: string;
      active_color: string;
      inactive_color: string;
      points: Array<{ time: string; active: boolean }>;
    }>;
    signals: Array<{ time: string }>;
  };
  vcs?: {
    score: number;
    stage: "critical" | "setup" | "base";
    stage_label: string;
    color_zone: "green" | "blue" | "base";
    is_setup_stage: boolean;
    is_critical_tightness: boolean;
    tr_short: number;
    tr_long_avg: number;
    std_short: number;
    std_long_avg: number;
    vol_short_avg: number;
    vol_avg: number;
    trend_factor: number;
    efficiency: number;
    days_tight: number;
    is_higher_low: boolean;
  } | null;
};

export type ChartFundamentalsResponse = {
  ticker: string;
  earnings_eps_history: Array<{
    date: string;
    eps_estimate: number | null;
    reported_eps: number | null;
    surprise_pct: number | null;
  }>;
  holders_float_held_by_institutions_pct?: number | null;
  revenue_yoy_pct?: number | null;
  earnings_yoy_pct?: number | null;
  implied_move?: {
    strike: number | null;
    straddle_mid: number | null;
    dollar_move: number | null;
    percent_move: number | null;
  } | null;
  diagnostics: {
    earnings: {
      status: string;
      reason?: string;
      attempts: Array<Record<string, unknown>>;
    };
    holders: {
      status: string;
      reason?: string;
      attempts: Array<Record<string, unknown>>;
    };
    statistics: {
      status: string;
      reason?: string;
      attempts: Array<Record<string, unknown>>;
    };
    options: {
      status: string;
      reason?: string;
      attempts: Array<Record<string, unknown>>;
    };
  };
};

export type ChartInsiderResponse = {
  ticker: string;
  requested_as_of_date?: string | null;
  resolved_as_of_date?: string | null;
  lookback_days: number;
  window_start_date?: string | null;
  window_end_date?: string | null;
  generated_at?: string | null;
  cache_status?: "hit" | "miss" | "stale" | string;
  fetch_status?: "skipped" | "fetched" | "failed" | string;
  notice?: string | null;
  entries: Array<{
    ticker: string;
    filing_date?: string | null;
    transaction_date?: string | null;
    owner_name: string;
    position?: string | null;
    type: "BUY" | "SELL" | string;
    shares: number;
    price?: number | null;
    gross_amount?: number | null;
    net_amount?: number | null;
    shares_owned_after?: number | null;
    is_10b5_1: boolean;
    source_url?: string | null;
  }>;
  summary: {
    total_count: number;
    buy_count: number;
    sell_count: number;
    total_buy_amount: number;
    total_sell_amount: number;
    net_amount: number;
  };
};

export type ChartAnnotations = {
  setupLabel?: string;
  eventDate?: string | null;
  eventLabel?: string | null;
  triggerPrice?: number | null;
  triggerLabel?: string | null;
  entryPrice?: number | null;
  entryLabel?: string | null;
  secondaryEntryPrice?: number | null;
  secondaryEntryLabel?: string | null;
  secondaryEntryLow?: number | null;
  secondaryEntryHigh?: number | null;
  stopPrice?: number | null;
  stopLabel?: string | null;
};

export type AdHocScreenResult = {
  id: string;
  passed: boolean;
  error?: string | null;
  timing_ms: number;
  metrics: Record<string, unknown>;
  reasons: string[];
  hit: Record<string, unknown> | null;
};

export type AdHocScreenResponse = {
  ticker: string;
  as_of_date: string;
  screeners: AdHocScreenResult[];
  timing: {
    total_ms: number;
    market_data_source?: string;
    market_data_tickers_loaded?: string[];
    trading_days_requested?: number;
  };
  summary: {
    requested_screener_count: number;
    passed_screener_count: number;
    failed_screener_count: number;
  };
};

export type OverlapResponse = {
  date_label: string;
  available_dates?: string[];
  unique_ticker_count: number;
  overlap_two_plus_count: number;
  overlap_three_plus_count: number;
  overlap_two_plus: OverlapEntry[];
  pipeline_status: OverlapPipelineStatus[];
  pipeline_tickers: Record<string, string[]>;
  fearzone_tickers: string[];
};

export type ScreenerRunSummary = {
  id: number;
  strategy_id: string;
  run_date: string;
  config_hash: string;
  market_data_mode: string;
  source_kind: string;
  hit_count: number;
  failure_count: number;
  raw_artifact_path: string;
  watchlist_artifact_path: string;
  report_artifact_path: string;
  result_summary_json?: Record<string, unknown>;
  deleted_at?: string | null;
  deleted_reason?: string | null;
  created_at?: string;
};

export type SignalCacheCoverage = {
  strategy_id: string;
  run_count: number;
  run_with_hits_count: number;
  first_run_date: string | null;
  last_run_date: string | null;
};

export type SignalCacheCalendarStrategy = {
  run_id: number;
  strategy_id: string;
  hit_count: number;
  failure_count: number;
  market_data_mode: string;
  source_kind: string;
  deleted_at?: string | null;
  deleted_reason?: string | null;
  created_at?: string | null;
};

export type SignalCacheCalendarDay = {
  date: string;
  strategy_count: number;
  cached_strategy_count: number;
  hit_strategy_count: number;
  total_hits: number;
  status: "none" | "partial" | "cached_no_hits" | "cached_with_hits";
  strategies: SignalCacheCalendarStrategy[];
};

export type SignalCacheCalendarResponse = {
  from: string;
  to: string;
  strategy_ids: string[];
  include_deleted: boolean;
  days: SignalCacheCalendarDay[];
};

export type OverlapWarmCoverageDay = {
  date: string;
  expected_strategy_count: number;
  screened_strategy_count: number;
  screened_strategy_ids: string[];
  missing_strategy_ids: string[];
  screen_status: "none" | "partial" | "complete";
  overlap_ready: boolean;
  candidate_count: number;
  overlap_two_plus_count: number;
  overlap_three_plus_count: number;
  overlap_four_plus_count: number;
  overlap_run_id?: number | null;
  updated_at?: string | null;
};

export type OverlapWarmCoverageResponse = {
  configured: boolean;
  from: string;
  to: string;
  strategy_ids: string[];
  candidate_threshold: number;
  days: OverlapWarmCoverageDay[];
};

export type BacktestHoldSummary = {
  trade_count: number;
  avg_return_pct?: number | null;
  median_return_pct?: number | null;
  win_rate_pct?: number | null;
  avg_spy_return_pct?: number | null;
  avg_excess_return_pct?: number | null;
};

export type BacktestRunSummaryV1 = {
  id: number;
  strategy_id: string;
  strategy_set_key: string;
  strategy_ids_json: string[];
  start_date: string;
  end_date: string;
  parameters: Record<string, unknown>;
  summary: {
    trade_count?: number;
    holds?: Record<string, BacktestHoldSummary>;
  };
  job_run_id?: number | null;
  hold_periods_json?: number[];
  entry_signal_threshold?: number;
  artifact_path?: string | null;
  created_at: string;
};

export type BacktestTradeV1 = {
  id: number;
  signal_date: string;
  ticker: string;
  signal_count: number;
  contributing_strategies_json: string[];
  entry_date: string;
  entry_price: number;
  hold_results_json: Record<string, unknown>;
  metadata_json: Record<string, unknown>;
  created_at?: string | null;
};

export type BacktestRunDetailV1 = BacktestRunSummaryV1 & {
  trades: BacktestTradeV1[];
};

export type BacktestRunsResponseV1 = {
  configured: boolean;
  runs: BacktestRunSummaryV1[];
};

export type ScreenerRunHit = {
  id: number;
  strategy_id: string;
  signal_date: string;
  ticker: string;
  passed: boolean;
  rank?: number | null;
  metrics_json: Record<string, unknown>;
  reasons_json: unknown[];
  hit_payload_json: Record<string, unknown>;
  created_at?: string | null;
};

export type ScreenerRunDetail = {
  id: number;
  strategy_id: string;
  run_date: string;
  job_run_id?: number | null;
  config_json: Record<string, unknown>;
  config_hash: string;
  scope_json: Record<string, unknown>;
  scope_hash: string;
  market_data_mode: string;
  source_kind: string;
  hit_count: number;
  failure_count: number;
  result_summary_json: Record<string, unknown>;
  raw_artifact_path: string;
  watchlist_artifact_path: string;
  report_artifact_path: string;
  notes?: string | null;
  deleted_at?: string | null;
  deleted_reason?: string | null;
  created_at?: string | null;
  hits?: ScreenerRunHit[];
};

export type ScreenerRunsResponse = {
  configured: boolean;
  runs: ScreenerRunSummary[];
  coverage: SignalCacheCoverage[];
  available_strategies: Array<{ id: string; label: string }>;
};

export type ScheduledJobStatus = "running" | "success" | "failed" | "unknown";

export type ScheduledJobSummary = {
  job_id: string;
  job_label: string;
  status: ScheduledJobStatus | string;
  last_started_at: string | null;
  last_finished_at: string | null;
  exit_code: number | null;
  log_file: string;
  artifact_file: string;
  message: string;
  status_file: string;
};

export type ScheduledJobConfig = {
  job_id: string;
  job_label: string;
  action_id: string;
  cron_expr: string;
  cron_tz: string;
  enabled: boolean;
  options: Record<string, unknown>;
};

export type ScheduledJobConfigResponse = {
  jobs: ScheduledJobConfig[];
  available_actions: Array<{
    id: string;
    label: string;
    fields: Array<{
      id: string;
      label: string;
      type: "text" | "number" | "date" | "select" | "multiselect";
      placeholder?: string | null;
      help_text?: string | null;
      options: Array<{ value: string; label: string }>;
    }>;
  }>;
  common_timezones: string[];
  scheduler_command: string;
  max_parallel_jobs: number;
};

export type AdminResponse = {
  excluded_tickers: ExclusionEntry[];
  excluded_count: number;
  included_tickers?: Array<{ ticker: string; reason: string }>;
  included_count?: number;
  users?: Array<{
    id: number;
    email: string;
    role: RoleName;
    is_active: boolean;
    created_at?: string | null;
    updated_at?: string | null;
    last_login_at?: string | null;
  }>;
  access_requests?: AccessRequestSummary[];
  database_status: {
    database_configured: boolean;
    coverage_start: string;
    coverage_end: string;
    target_universe_count: number;
    db_ticker_count: number;
    covered_ticker_count: number;
    partial_ticker_count: number;
    missing_ticker_count: number;
    total_bar_rows: number;
    overall_first_trade_date: string | null;
    overall_last_trade_date: string | null;
    latest_metadata_update_at: string | null;
    stale_ticker_count: number;
    coverage_percent: number;
    sample_missing_tickers: PartialTickerSummary[];
    sample_partial_tickers: PartialTickerSummary[];
    notes: string[];
  };
};

export type RatingsAdminDiagnostic = {
  ticker: string;
  category: string;
  reason: string;
  fundamentals_as_of_date: string | null;
  rating_as_of_date: string | null;
  parse_status: string | null;
  rating_status: string | null;
  sector: string | null;
  industry: string | null;
  overall_rating: number | null;
  missing_metric_names: string[];
  insufficient_baseline_metrics: string[];
};

export type RatingsAdminStatusResponse = {
  database_configured: boolean;
  target_universe_count: number;
  latest_fundamentals_as_of_date: string | null;
  latest_fundamentals_updated_at: string | null;
  latest_baselines_as_of_date: string | null;
  latest_baselines_updated_at: string | null;
  latest_ratings_as_of_date: string | null;
  latest_ratings_updated_at: string | null;
  latest_fundamentals_snapshot_count: number;
  latest_rating_snapshot_count: number;
  latest_fundamentals_parse_status_counts: Record<string, number>;
  latest_rating_status_counts: Record<string, number>;
  tickers_with_any_fundamentals: number;
  tickers_with_latest_ok_rating: number;
  diagnostics_count: number;
  diagnostic_category_counts: Record<string, number>;
  diagnostics: RatingsAdminDiagnostic[];
  notes: string[];
};

export type AdminTickerListStatusResponse = {
  ticker: string;
  is_excluded: boolean;
  is_included: boolean;
  exclusion_entry: ExclusionEntry | null;
  inclusion_entry: { ticker: string; reason: string } | null;
};

export type PortfolioAdvice = {
  as_of_date?: string | null;
  latest_trade_date?: string | null;
  market_data_status: "pending" | "ready" | "stale" | "missing" | string;
  close_price?: number | null;
  signal_status: "hold" | "trim" | "raise_stop" | "review" | string;
  stop_loss_price?: number | null;
  tp1_price?: number | null;
  tp2_price?: number | null;
  tp1_sell_fraction?: number | null;
  tp2_sell_fraction?: number | null;
  average_up_price?: number | null;
  average_up_share_fraction?: number | null;
  blended_entry_after_average_up?: number | null;
  net_cost_after_tp1?: number | null;
  remaining_cost_basis_after_tp1?: number | null;
  explanation?: string | null;
  data_source?: string | null;
  signal_context?: Record<string, unknown>;
  refreshed_at?: string | null;
};

export type PortfolioTransaction = {
  id: number;
  position_id: number;
  trade_date?: string | null;
  side: "buy" | "sell" | string;
  shares: number;
  price?: number | null;
  fees?: number | null;
  notes?: string | null;
  created_at?: string | null;
};

export type PortfolioPosition = {
  id: number;
  portfolio_id: number;
  portfolio_name: string;
  ticker: string;
  shares: number;
  entry_price: number;
  opened_at: string;
  notes?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
  seed_shares?: number;
  seed_entry_price?: number;
  realized_pl?: number | null;
  is_closed?: boolean;
  market_value?: number | null;
  unrealized_pl?: number | null;
  unrealized_pl_pct?: number | null;
  transactions: PortfolioTransaction[];
  advice: PortfolioAdvice;
};

export type PortfolioSummary = {
  position_count: number;
  total_market_value: number;
  total_cost_basis: number;
  total_unrealized_pl: number;
  total_unrealized_pl_pct: number;
  stale_advice_count: number;
  missing_advice_count: number;
  last_refreshed_at?: string | null;
};

export type PortfolioContextResponse = {
  database_configured: boolean;
  summary: PortfolioSummary;
  positions: PortfolioPosition[];
  portfolios: Array<{
    id: number;
    name: string;
    created_by_user_id?: number | null;
    created_at?: string | null;
    updated_at?: string | null;
  }>;
  market_regime: {
    title: string;
    status: string;
    description: string;
  };
};

export type PortfolioImportResponse = {
  ok: boolean;
  portfolio_name: string;
  import_batch_id?: number | null;
  accepted_count: number;
  error_count: number;
  accepted: Array<{
    row: number;
    position: Partial<PortfolioPosition> & { ticker: string };
  }>;
  errors: Array<{
    row: number;
    message: string;
  }>;
};

export type PortfolioRefreshResponse = {
  ok: boolean;
  refreshed_count: number;
  positions: Array<{
    position_id: number;
    ticker: string;
  }>;
};

export type RrgUniverse = "sector" | "industry" | "theme";
export type RrgCadence = "weekly" | "daily-2m";

export type RrgPoint = {
  x: number;
  y: number;
  date: string;
};

export type RrgSeries = {
  ticker: string;
  label: string;
  points: RrgPoint[];
  latest: RrgPoint;
  quadrant: "Leading" | "Weakening" | "Lagging" | "Improving" | string;
  distance: number;
  fearzone: {
    active: boolean;
    signal_date: string | null;
    signal_age_bars: number | null;
    trigger_labels: string[];
    conditions: Array<{
      key: string;
      label: string;
      active: boolean;
    }>;
  };
};

export type RrgGroup = {
  id: string;
  title: string;
  series: RrgSeries[];
};

export type RrgResponse = {
  universe: RrgUniverse;
  benchmark: string;
  period: string;
  trail_weeks: number;
  cadence?: RrgCadence;
  generated_at: string;
  series: RrgSeries[];
  groups?: RrgGroup[];
  quadrants: {
    center_x: number;
    center_y: number;
    definitions: Array<{ name: string; x: string; y: string }>;
  };
  meta: {
    count: number;
    notes: string[];
    failed_tickers?: string[];
  };
  static_report_url?: string;
};

export type EarningsCalendarEntry = {
  ticker: string;
  date: string;
  session?: string | null;
  summary?: string | null;
  sector?: string | null;
  industry?: string | null;
  exchange?: string | null;
  implied_move_signal?: {
    threshold_pct: number;
    near_earnings: boolean;
    matched: boolean;
    percent_move?: number | null;
    status?: string | null;
  } | null;
  criteria?: {
    passed: boolean;
    criteria: Record<string, boolean>;
    matched_criteria: string[];
    not_matched_criteria: string[];
    pass_mode?: string | null;
    error?: string | null;
  } | null;
};

export type EarningsCalendarDay = {
  date: string;
  weekday: string;
  before_market: EarningsCalendarEntry[];
  after_market: EarningsCalendarEntry[];
  during_market: EarningsCalendarEntry[];
  unknown: EarningsCalendarEntry[];
};

export type EarningsCalendarResponse = {
  week_start: string;
  week_end: string;
  reference_date: string;
  week_offset: number;
  days: EarningsCalendarDay[];
  filters: {
    exclude_sectors: string[];
    exclude_industries: string[];
    only_criteria: boolean;
  };
  available_sectors: string[];
  available_industries: string[];
  criteria_filter: {
    enabled: boolean;
    available: boolean;
    strategy_id: string;
    run_id?: number | null;
    run_date?: string | null;
    matched_count: number;
  };
};
