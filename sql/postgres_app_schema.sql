CREATE TABLE IF NOT EXISTS ticker_metadata (
  ticker TEXT PRIMARY KEY,
  exchange TEXT,
  sector TEXT,
  industry TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  currency TEXT NOT NULL DEFAULT 'USD',
  source TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_bars (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  trade_date DATE NOT NULL,
  open NUMERIC(24,6),
  high NUMERIC(24,6),
  low NUMERIC(24,6),
  close NUMERIC(24,6),
  adj_close NUMERIC(24,6),
  volume BIGINT,
  dividend NUMERIC(24,6) NOT NULL DEFAULT 0,
  split_factor NUMERIC(24,6) NOT NULL DEFAULT 1,
  source TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, trade_date)
);

ALTER TABLE daily_bars ALTER COLUMN open TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN high TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN low TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN close TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN adj_close TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN dividend TYPE NUMERIC(24,6);
ALTER TABLE daily_bars ALTER COLUMN split_factor TYPE NUMERIC(24,6);

CREATE INDEX IF NOT EXISTS idx_daily_bars_trade_date
  ON daily_bars(trade_date);

CREATE INDEX IF NOT EXISTS idx_daily_bars_ticker_trade_date_desc
  ON daily_bars(ticker, trade_date DESC);

CREATE TABLE IF NOT EXISTS earnings_events (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  earnings_date DATE NOT NULL,
  fiscal_quarter TEXT,
  eps_actual NUMERIC(18,6),
  eps_estimate NUMERIC(18,6),
  revenue_actual NUMERIC(18,2),
  revenue_estimate NUMERIC(18,2),
  surprise_pct NUMERIC(18,6),
  session_timing TEXT,
  source TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, earnings_date)
);

CREATE INDEX IF NOT EXISTS idx_earnings_events_earnings_date
  ON earnings_events(earnings_date);

CREATE TABLE IF NOT EXISTS job_runs (
  id BIGSERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  job_name TEXT NOT NULL,
  status TEXT NOT NULL,
  trigger_source TEXT NOT NULL DEFAULT 'manual',
  request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  artifact_path TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job_type_started_at
  ON job_runs(job_type, started_at DESC);

CREATE TABLE IF NOT EXISTS screen_runs (
  id BIGSERIAL PRIMARY KEY,
  strategy_id TEXT NOT NULL,
  run_date DATE NOT NULL,
  job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL,
  hit_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  raw_artifact_path TEXT,
  watchlist_artifact_path TEXT,
  report_artifact_path TEXT,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (strategy_id, run_date)
);

CREATE INDEX IF NOT EXISTS idx_screen_runs_strategy_run_date
  ON screen_runs(strategy_id, run_date DESC);

CREATE TABLE IF NOT EXISTS backtest_runs (
  id BIGSERIAL PRIMARY KEY,
  strategy_id TEXT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
  summary JSONB NOT NULL DEFAULT '{}'::jsonb,
  html_report_path TEXT,
  json_report_path TEXT,
  job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy_created_at
  ON backtest_runs(strategy_id, created_at DESC);

CREATE TABLE IF NOT EXISTS report_artifacts (
  id BIGSERIAL PRIMARY KEY,
  artifact_type TEXT NOT NULL,
  strategy_id TEXT,
  run_date DATE,
  path TEXT NOT NULL,
  public_url TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_report_artifacts_strategy_date
  ON report_artifacts(strategy_id, run_date DESC);
