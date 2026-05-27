export type StrategyCard = {
  id: string;
  label: string;
  description: string;
  lastRun?: string;
  hits?: number;
  accent?: "up" | "neutral";
};

export type JobStatus = "running" | "success" | "failed";

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

export type JobsResponse = {
  actions: { id: string; label: string; command: string; supports_limit: boolean }[];
  jobs: {
    job_id: string;
    label: string;
    status: JobStatus;
    started_at: string;
    finished_at: string;
    return_code: number | null;
    command: string;
    log_tail: string;
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

export type WatchlistChartResponse = {
  ticker: string;
  candles: Array<{ time: string; open: number; high: number; low: number; close: number }>;
  volume: Array<{ time: string; value: number }>;
  ma20: Array<{ time: string; value: number }>;
  ma50: Array<{ time: string; value: number }>;
  ma200: Array<{ time: string; value: number }>;
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

export type BacktestsResponse = {
  backtest_templates: Array<{ label: string; description: string; command: string }>;
};

export type AdminResponse = {
  excluded_tickers: string[];
  excluded_count: number;
};
