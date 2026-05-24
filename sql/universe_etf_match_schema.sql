CREATE TABLE IF NOT EXISTS etf_catalog (
  etf_ticker TEXT PRIMARY KEY,
  etf_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS etf_match_rules (
  etf_ticker TEXT NOT NULL,
  rule_type TEXT NOT NULL CHECK (rule_type IN ('sector', 'theme')),
  rule_value TEXT NOT NULL,
  PRIMARY KEY (etf_ticker, rule_type, rule_value),
  FOREIGN KEY (etf_ticker) REFERENCES etf_catalog(etf_ticker)
);

CREATE TABLE IF NOT EXISTS ticker_metadata (
  ticker TEXT PRIMARY KEY,
  sector TEXT,
  industry TEXT,
  exchange TEXT,
  source TEXT NOT NULL DEFAULT 'nasdaq-universe',
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticker_themes (
  ticker TEXT NOT NULL,
  theme TEXT NOT NULL,
  PRIMARY KEY (ticker, theme),
  FOREIGN KEY (ticker) REFERENCES ticker_metadata(ticker)
);

CREATE TABLE IF NOT EXISTS ticker_etf_matches (
  ticker TEXT NOT NULL,
  etf_ticker TEXT NOT NULL,
  etf_name TEXT NOT NULL,
  match_reason TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (ticker, etf_ticker),
  FOREIGN KEY (ticker) REFERENCES ticker_metadata(ticker),
  FOREIGN KEY (etf_ticker) REFERENCES etf_catalog(etf_ticker)
);

CREATE INDEX IF NOT EXISTS idx_ticker_metadata_sector ON ticker_metadata(sector);
CREATE INDEX IF NOT EXISTS idx_ticker_metadata_industry ON ticker_metadata(industry);
CREATE INDEX IF NOT EXISTS idx_ticker_themes_theme ON ticker_themes(theme);
CREATE INDEX IF NOT EXISTS idx_ticker_etf_matches_etf_ticker ON ticker_etf_matches(etf_ticker);
