CREATE TABLE IF NOT EXISTS ticker_metadata (
  ticker TEXT PRIMARY KEY,
  exchange TEXT,
  sector TEXT,
  industry TEXT,
  ipo_date DATE,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  currency TEXT NOT NULL DEFAULT 'USD',
  source TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE ticker_metadata ADD COLUMN IF NOT EXISTS ipo_date DATE;

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

CREATE TABLE IF NOT EXISTS ticker_trendline_snapshots (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  trade_date DATE NOT NULL,
  close NUMERIC(24,6),
  daily_ema9 NUMERIC(24,6),
  daily_ema21 NUMERIC(24,6),
  daily_sma50 NUMERIC(24,6),
  daily_sma200 NUMERIC(24,6),
  weekly_ema8 NUMERIC(24,6),
  weekly_sma200 NUMERIC(24,6),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_trendline_snapshots_trade_date
  ON ticker_trendline_snapshots(trade_date);

CREATE INDEX IF NOT EXISTS idx_ticker_trendline_snapshots_ticker_trade_date_desc
  ON ticker_trendline_snapshots(ticker, trade_date DESC);

CREATE TABLE IF NOT EXISTS ticker_fundamentals_snapshots (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  as_of_date DATE NOT NULL,
  sector TEXT,
  industry TEXT,
  market_cap NUMERIC(24,6),
  enterprise_value NUMERIC(24,6),
  forward_pe NUMERIC(18,6),
  peg_ratio_5y NUMERIC(18,6),
  price_to_sales NUMERIC(18,6),
  price_to_book NUMERIC(18,6),
  price_to_fcf NUMERIC(18,6),
  profit_margin_pct NUMERIC(18,6),
  operating_margin_pct NUMERIC(18,6),
  gross_margin_pct NUMERIC(18,6),
  roa_pct NUMERIC(18,6),
  roe_pct NUMERIC(18,6),
  roic_pct NUMERIC(18,6),
  institutional_ownership_pct NUMERIC(18,6),
  institutional_transactions_pct NUMERIC(18,6),
  insider_ownership_pct NUMERIC(18,6),
  insider_transactions_pct NUMERIC(18,6),
  shares_float NUMERIC(24,6),
  shares_outstanding NUMERIC(24,6),
  current_ratio NUMERIC(18,6),
  quick_ratio NUMERIC(18,6),
  debt_to_equity NUMERIC(18,6),
  lt_debt_to_equity NUMERIC(18,6),
  eps_next_q NUMERIC(18,6),
  eps_this_y_pct NUMERIC(18,6),
  eps_next_y_pct NUMERIC(18,6),
  eps_next_5y_pct NUMERIC(18,6),
  sales_qq_pct NUMERIC(18,6),
  sales_yoy_ttm_pct NUMERIC(18,6),
  eps_qq_pct NUMERIC(18,6),
  eps_yoy_ttm_pct NUMERIC(18,6),
  eps_surprise_pct NUMERIC(18,6),
  sales_surprise_pct NUMERIC(18,6),
  analyst_recommendation NUMERIC(18,6),
  target_price NUMERIC(18,6),
  perf_month_pct NUMERIC(18,6),
  perf_quarter_pct NUMERIC(18,6),
  perf_half_pct NUMERIC(18,6),
  perf_year_pct NUMERIC(18,6),
  perf_ytd_pct NUMERIC(18,6),
  volatility_week_pct NUMERIC(18,6),
  volatility_month_pct NUMERIC(18,6),
  source TEXT NOT NULL DEFAULT 'finviz',
  source_url TEXT NOT NULL,
  parse_status TEXT NOT NULL,
  parse_error TEXT,
  scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_fundamentals_snapshots_date_ticker
  ON ticker_fundamentals_snapshots(as_of_date, ticker);

CREATE INDEX IF NOT EXISTS idx_ticker_fundamentals_snapshots_date_sector
  ON ticker_fundamentals_snapshots(as_of_date, sector);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS institutional_ownership_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS institutional_transactions_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS insider_ownership_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS insider_transactions_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS shares_float NUMERIC(24,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS shares_outstanding NUMERIC(24,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS target_price NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS analyst_recommendation NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS sales_surprise_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS eps_surprise_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS eps_yoy_ttm_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS sales_yoy_ttm_pct NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS eps_next_q NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS lt_debt_to_equity NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS debt_to_equity NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS quick_ratio NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS current_ratio NUMERIC(18,6);

ALTER TABLE ticker_fundamentals_snapshots
  ADD COLUMN IF NOT EXISTS roic_pct NUMERIC(18,6);

CREATE TABLE IF NOT EXISTS ticker_chart_fundamentals_cache (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  as_of_date DATE NOT NULL,
  earnings_eps_history_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  holders_float_held_by_institutions_pct NUMERIC(18,6),
  revenue_yoy_pct NUMERIC(18,6),
  earnings_yoy_pct NUMERIC(18,6),
  implied_move_json JSONB,
  source_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_chart_fundamentals_cache_date_ticker
  ON ticker_chart_fundamentals_cache(as_of_date, ticker);

CREATE INDEX IF NOT EXISTS idx_ticker_chart_fundamentals_cache_updated_at
  ON ticker_chart_fundamentals_cache(updated_at DESC);

CREATE TABLE IF NOT EXISTS sector_metric_baselines (
  as_of_date DATE NOT NULL,
  sector TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  sample_size INTEGER NOT NULL,
  filtered_sample_size INTEGER NOT NULL,
  median_value NUMERIC(18,6),
  pct10_value NUMERIC(18,6),
  pct90_value NUMERIC(18,6),
  std_value NUMERIC(18,6),
  std_step_value NUMERIC(18,6),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (as_of_date, sector, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_sector_metric_baselines_date_sector
  ON sector_metric_baselines(as_of_date, sector);

CREATE TABLE IF NOT EXISTS ticker_rating_snapshots (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  as_of_date DATE NOT NULL,
  sector TEXT,
  valuation_score NUMERIC(18,6),
  profitability_score NUMERIC(18,6),
  growth_score NUMERIC(18,6),
  performance_score NUMERIC(18,6),
  overall_rating NUMERIC(18,6),
  valuation_grade TEXT,
  profitability_grade TEXT,
  growth_grade TEXT,
  performance_grade TEXT,
  rating_status TEXT NOT NULL,
  rating_status_reason TEXT,
  missing_metric_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  insufficient_baseline_metrics JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_rating_snapshots_date_status
  ON ticker_rating_snapshots(as_of_date, rating_status);

CREATE INDEX IF NOT EXISTS idx_ticker_rating_snapshots_date_overall
  ON ticker_rating_snapshots(as_of_date, overall_rating DESC);

CREATE TABLE IF NOT EXISTS ticker_technical_rating_snapshots (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  as_of_date DATE NOT NULL,
  trend_regime_score NUMERIC(18,6),
  dma_speed_score NUMERIC(18,6),
  divergence_health_score NUMERIC(18,6),
  daily_rs_rating NUMERIC(18,6),
  weekly_rs_rating NUMERIC(18,6),
  leadership_score NUMERIC(18,6),
  structure_volume_score NUMERIC(18,6),
  industry_group TEXT,
  industry_group_rs_rank NUMERIC(18,6),
  industry_group_member_count INTEGER,
  overall_rating NUMERIC(18,6),
  rating_band TEXT,
  technical_status TEXT NOT NULL,
  technical_status_reason TEXT,
  flags JSONB NOT NULL DEFAULT '[]'::jsonb,
  missing_metric_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_ticker_technical_rating_snapshots_date_status
  ON ticker_technical_rating_snapshots(as_of_date, technical_status);

CREATE INDEX IF NOT EXISTS idx_ticker_technical_rating_snapshots_date_overall
  ON ticker_technical_rating_snapshots(as_of_date, overall_rating DESC);

ALTER TABLE ticker_technical_rating_snapshots
  ADD COLUMN IF NOT EXISTS daily_rs_rating NUMERIC(18,6),
  ADD COLUMN IF NOT EXISTS weekly_rs_rating NUMERIC(18,6),
  ADD COLUMN IF NOT EXISTS industry_group TEXT,
  ADD COLUMN IF NOT EXISTS industry_group_rs_rank NUMERIC(18,6),
  ADD COLUMN IF NOT EXISTS industry_group_member_count INTEGER;

CREATE TABLE IF NOT EXISTS ticker_technical_indicator_rating_snapshots (
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  as_of_date DATE NOT NULL,
  timeframe TEXT NOT NULL,
  moving_average_score NUMERIC(18,6),
  oscillator_score NUMERIC(18,6),
  overall_score NUMERIC(18,6),
  rating_label TEXT,
  technical_status TEXT NOT NULL,
  technical_status_reason TEXT,
  missing_metric_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ticker, as_of_date, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_ticker_technical_indicator_rating_snapshots_date_tf_status
  ON ticker_technical_indicator_rating_snapshots(as_of_date, timeframe, technical_status);

CREATE INDEX IF NOT EXISTS idx_ticker_technical_indicator_rating_snapshots_date_tf_overall
  ON ticker_technical_indicator_rating_snapshots(as_of_date, timeframe, overall_score DESC);

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
  parent_job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL,
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

ALTER TABLE job_runs ADD COLUMN IF NOT EXISTS parent_job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_job_runs_job_type_started_at
  ON job_runs(job_type, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_runs_parent_started_at
  ON job_runs(parent_job_run_id, started_at DESC);

CREATE TABLE IF NOT EXISTS remote_workers (
  worker_name TEXT PRIMARY KEY,
  current_job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'idle',
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_remote_workers_last_heartbeat
  ON remote_workers(last_heartbeat_at DESC);

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

CREATE TABLE IF NOT EXISTS overlap_runs (
  id BIGSERIAL PRIMARY KEY,
  run_date DATE NOT NULL,
  strategy_set_key TEXT NOT NULL,
  strategy_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  market_data_mode TEXT NOT NULL DEFAULT 'database-first',
  candidate_threshold INTEGER NOT NULL DEFAULT 4,
  source_job_run_id BIGINT REFERENCES job_runs(id) ON DELETE SET NULL,
  artifact_path TEXT,
  summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_date, strategy_set_key, candidate_threshold)
);

CREATE INDEX IF NOT EXISTS idx_overlap_runs_strategy_set_date
  ON overlap_runs(strategy_set_key, run_date DESC);

CREATE TABLE IF NOT EXISTS overlap_run_members (
  id BIGSERIAL PRIMARY KEY,
  overlap_run_id BIGINT NOT NULL REFERENCES overlap_runs(id) ON DELETE CASCADE,
  run_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  signal_count INTEGER NOT NULL,
  contributing_strategies_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (overlap_run_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_overlap_run_members_date_count
  ON overlap_run_members(run_date DESC, signal_count DESC, ticker ASC);

ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS strategy_set_key TEXT NOT NULL DEFAULT '';
ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS strategy_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS entry_signal_threshold INTEGER NOT NULL DEFAULT 4;
ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS hold_periods_json JSONB NOT NULL DEFAULT '[5, 10]'::jsonb;
ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS artifact_path TEXT;

CREATE TABLE IF NOT EXISTS backtest_run_trades (
  id BIGSERIAL PRIMARY KEY,
  backtest_run_id BIGINT NOT NULL REFERENCES backtest_runs(id) ON DELETE CASCADE,
  signal_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  signal_count INTEGER NOT NULL,
  contributing_strategies_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  entry_date DATE NOT NULL,
  entry_price NUMERIC(24,6) NOT NULL,
  hold_results_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_run_trades_run_signal_date
  ON backtest_run_trades(backtest_run_id, signal_date DESC, ticker ASC);

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

CREATE TABLE IF NOT EXISTS app_user_identities (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  provider_subject TEXT NOT NULL,
  provider_email TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (provider, provider_subject)
);

CREATE INDEX IF NOT EXISTS idx_app_user_identities_user_id
  ON app_user_identities(user_id, provider);

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

CREATE TABLE IF NOT EXISTS portfolios (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS portfolio_positions (
  id BIGSERIAL PRIMARY KEY,
  portfolio_id BIGINT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  shares NUMERIC(24,6) NOT NULL,
  entry_price NUMERIC(24,6) NOT NULL,
  opened_at DATE NOT NULL,
  notes TEXT,
  created_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  updated_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_positions_portfolio_ticker
  ON portfolio_positions(portfolio_id, ticker);

CREATE INDEX IF NOT EXISTS idx_portfolio_positions_ticker_opened_at
  ON portfolio_positions(ticker, opened_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_position_transactions (
  id BIGSERIAL PRIMARY KEY,
  position_id BIGINT NOT NULL REFERENCES portfolio_positions(id) ON DELETE CASCADE,
  trade_date DATE NOT NULL,
  side TEXT NOT NULL,
  shares NUMERIC(24,6) NOT NULL,
  price NUMERIC(24,6) NOT NULL,
  fees NUMERIC(24,6) NOT NULL DEFAULT 0,
  notes TEXT,
  created_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_position_transactions_position_date
  ON portfolio_position_transactions(position_id, trade_date ASC, id ASC);

CREATE TABLE IF NOT EXISTS portfolio_import_batches (
  id BIGSERIAL PRIMARY KEY,
  portfolio_id BIGINT REFERENCES portfolios(id) ON DELETE CASCADE,
  source_name TEXT NOT NULL DEFAULT '',
  imported_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  row_count INTEGER NOT NULL DEFAULT 0,
  accepted_count INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  raw_csv_text TEXT NOT NULL DEFAULT '',
  summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_portfolio_import_batches_portfolio_created_at
  ON portfolio_import_batches(portfolio_id, created_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_advice_snapshots (
  position_id BIGINT PRIMARY KEY REFERENCES portfolio_positions(id) ON DELETE CASCADE,
  as_of_date DATE,
  latest_trade_date DATE,
  market_data_status TEXT NOT NULL DEFAULT 'pending',
  close_price NUMERIC(24,6),
  signal_status TEXT NOT NULL DEFAULT 'review',
  stop_loss_price NUMERIC(24,6),
  tp1_price NUMERIC(24,6),
  tp2_price NUMERIC(24,6),
  tp1_sell_fraction NUMERIC(12,6),
  tp2_sell_fraction NUMERIC(12,6),
  average_up_price NUMERIC(24,6),
  average_up_share_fraction NUMERIC(12,6),
  blended_entry_after_average_up NUMERIC(24,6),
  net_cost_after_tp1 NUMERIC(24,6),
  remaining_cost_basis_after_tp1 NUMERIC(24,6),
  explanation TEXT,
  data_source TEXT NOT NULL DEFAULT '',
  signal_context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS my_picks (
  id BIGSERIAL PRIMARY KEY,
  ticker TEXT NOT NULL REFERENCES ticker_metadata(ticker),
  notes TEXT,
  checklist_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by_user_id BIGINT REFERENCES app_users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_my_picks_created_at
  ON my_picks(created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_my_picks_ticker_created_at
  ON my_picks(ticker, created_at DESC);

CREATE TABLE IF NOT EXISTS daily_position_decisions (
  id BIGSERIAL PRIMARY KEY,
  as_of_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  action TEXT NOT NULL,
  action_score NUMERIC(12,6) NOT NULL DEFAULT 0,
  regime_state TEXT,
  trend_state TEXT,
  extension_state TEXT,
  support_reference TEXT,
  atr_dist_21 NUMERIC(24,6),
  atr_dist_10w NUMERIC(24,6),
  atr_pct NUMERIC(24,6),
  daily_atr_ratio NUMERIC(24,6),
  close_price NUMERIC(24,6),
  ema21 NUMERIC(24,6),
  sma50 NUMERIC(24,6),
  sma10w NUMERIC(24,6),
  danger_signal_count INTEGER NOT NULL DEFAULT 0,
  reason_summary TEXT,
  evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (as_of_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_daily_position_decisions_date_action
  ON daily_position_decisions(as_of_date DESC, action, action_score DESC);

CREATE INDEX IF NOT EXISTS idx_daily_position_decisions_ticker_date
  ON daily_position_decisions(ticker, as_of_date DESC);

ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS config_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS config_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS scope_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS scope_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS market_data_mode TEXT NOT NULL DEFAULT 'internet';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'exchange-universe';
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS result_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE screen_runs ADD COLUMN IF NOT EXISTS deleted_reason TEXT;
ALTER TABLE portfolio_advice_snapshots ADD COLUMN IF NOT EXISTS average_up_price NUMERIC(24,6);
ALTER TABLE portfolio_advice_snapshots ADD COLUMN IF NOT EXISTS average_up_share_fraction NUMERIC(12,6);
ALTER TABLE portfolio_advice_snapshots ADD COLUMN IF NOT EXISTS blended_entry_after_average_up NUMERIC(24,6);
ALTER TABLE my_picks ADD COLUMN IF NOT EXISTS checklist_json JSONB NOT NULL DEFAULT '{}'::jsonb;

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
