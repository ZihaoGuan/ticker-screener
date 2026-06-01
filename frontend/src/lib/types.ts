export type StrategyCard = {
  id: string;
  label: string;
  description: string;
  lastRun?: string;
  hits?: number;
  accent?: "up" | "neutral";
};

export type JobStatus = "running" | "success" | "failed" | "cancelled";

export type RoleName = "visitor" | "premium" | "admin";

export type CapabilityName =
  | "view_results"
  | "run_screeners"
  | "run_backtests"
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
  pipeline_count: number;
  pipeline_labels: string[];
  sector?: string;
  theme_tags?: string[];
};

export type DashboardResponse = {
  overview: {
    database_configured: boolean;
    artifacts_dir: string;
    latest_sync_at: string | null;
    screen_run_count: number | null;
    backtest_run_count: number | null;
  };
  recent_watchlists: WatchlistFile[];
  strategy_cards: StrategyCard[];
};

export type RunAction = {
  id: string;
  label: string;
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
  rs_line: Array<{ time: string; value: number }>;
  rs_markers: Array<{ time: string; kind: "daily_new_high" | "daily_new_high_before_price" }>;
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

export type OverlapResponse = {
  date_label: string;
  unique_ticker_count: number;
  overlap_two_plus_count: number;
  overlap_three_plus_count: number;
  overlap_two_plus: OverlapEntry[];
  pipeline_status: Array<{ label: string; count: number; file_present: boolean }>;
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

export type BacktestRunSummary = {
  id: number;
  strategy_id: string;
  start_date: string;
  end_date: string;
  parameters: Record<string, unknown>;
  summary: {
    entry_count?: number;
    signal_count?: number;
    partial?: boolean;
    results_by_rule?: Record<string, { trade_count: number; avg_return_pct: number | null; median_return_pct: number | null; win_rate_pct: number | null }>;
  };
  html_report_path: string;
  json_report_path: string;
  job_run_id?: number | null;
  created_at?: string;
};

export type BacktestsResponse = {
  backtest_templates: Array<{
    label: string;
    description: string;
    entry_rule: { mode: "min_count_same_day"; screener_ids: string[]; min_count: number };
    exit_rules: Array<Record<string, unknown>>;
    signal_cache_policy: "reuse_then_fill" | "reuse_only";
    market_data_mode: "database_only";
  }>;
  backtest_runs: BacktestRunSummary[];
  signal_cache: SignalCacheCoverage[];
  available_strategies: Array<{ id: string; label: string }>;
  default_exit_rules: Array<Record<string, unknown>>;
};

export type AdminResponse = {
  excluded_tickers: ExclusionEntry[];
  excluded_count: number;
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
