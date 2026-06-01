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
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  config_hash TEXT NOT NULL DEFAULT '',
  scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  scope_hash TEXT NOT NULL DEFAULT '',
  market_data_mode TEXT NOT NULL DEFAULT 'internet',
  source_kind TEXT NOT NULL DEFAULT 'exchange-universe',
  hit_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  result_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_artifact_path TEXT,
  watchlist_artifact_path TEXT,
  report_artifact_path TEXT,
  notes TEXT,
  deleted_at TIMESTAMPTZ,
  deleted_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (strategy_id, run_date, config_hash, scope_hash)
);

CREATE TABLE IF NOT EXISTS screen_run_hits (
  id BIGSERIAL PRIMARY KEY,
  screen_run_id BIGINT NOT NULL REFERENCES screen_runs(id) ON DELETE CASCADE,
  strategy_id TEXT NOT NULL,
  signal_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  passed BOOLEAN NOT NULL DEFAULT FALSE,
  rank INTEGER,
  metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  reasons_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  hit_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_screen_run_hits_strategy_signal_passed
  ON screen_run_hits(strategy_id, signal_date DESC, passed);

CREATE INDEX IF NOT EXISTS idx_screen_run_hits_ticker_signal_date
  ON screen_run_hits(ticker, signal_date DESC);

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

CREATE TABLE IF NOT EXISTS app_users (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  role TEXT NOT NULL DEFAULT 'visitor',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_login_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_app_users_role_active
  ON app_users(role, is_active);

CREATE TABLE IF NOT EXISTS app_magic_links (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
  token_hash TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  request_ip TEXT,
  request_user_agent TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_magic_links_user_id
  ON app_magic_links(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_app_magic_links_token_hash
  ON app_magic_links(token_hash);

CREATE TABLE IF NOT EXISTS app_sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
  session_id TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  created_ip TEXT,
  created_user_agent TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_sessions_session_id
  ON app_sessions(session_id);

CREATE INDEX IF NOT EXISTS idx_app_sessions_user_id
  ON app_sessions(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS app_access_requests (
  id BIGSERIAL PRIMARY KEY,
  email TEXT NOT NULL,
  requested_role TEXT NOT NULL DEFAULT 'premium',
  status TEXT NOT NULL DEFAULT 'pending',
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  reviewed_at TIMESTAMPTZ,
  reviewed_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  deny_reason TEXT,
  invited_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_access_requests_email_requested_at
  ON app_access_requests(email, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_app_access_requests_status_requested_at
  ON app_access_requests(status, requested_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_app_access_requests_pending_email
  ON app_access_requests(email)
  WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS app_audit_events (
  id BIGSERIAL PRIMARY KEY,
  event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  actor_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  actor_email TEXT,
  actor_role TEXT,
  request_ip TEXT,
  request_user_agent TEXT,
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT,
  resource_label TEXT,
  status TEXT NOT NULL DEFAULT 'success',
  message TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_app_audit_events_event_at
  ON app_audit_events(event_at DESC);

CREATE INDEX IF NOT EXISTS idx_app_audit_events_action_event_at
  ON app_audit_events(action, event_at DESC);

CREATE INDEX IF NOT EXISTS idx_app_audit_events_actor_user_event_at
  ON app_audit_events(actor_user_id, event_at DESC);

CREATE INDEX IF NOT EXISTS idx_app_audit_events_resource_event_at
  ON app_audit_events(resource_type, resource_id, event_at DESC);

ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS config_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS config_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS scope_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS scope_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS market_data_mode TEXT NOT NULL DEFAULT 'internet';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'exchange-universe';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS result_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS deleted_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_screen_runs_strategy_run_date
  ON screen_runs(strategy_id, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_screen_runs_not_deleted
  ON screen_runs(strategy_id, deleted_at, run_date DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_screen_runs_unique_scope
  ON screen_runs(strategy_id, run_date, config_hash, scope_hash);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_name = 'screen_runs'
      AND constraint_name = 'screen_runs_strategy_id_run_date_key'
  ) THEN
    ALTER TABLE screen_runs DROP CONSTRAINT screen_runs_strategy_id_run_date_key;
  END IF;
EXCEPTION
  WHEN undefined_table THEN
    NULL;
END
$$;
