# Oracle NoSQL market data plan

This repo can treat Oracle NoSQL as the durable home for normalized daily bars while keeping strategy scans and backtests in Python.

## Why this shape

Oracle NoSQL is a good fit here for:

- one row per ticker per trading day
- incremental refreshes for the latest bar
- fetching a full history slice for one ticker during backtests

It is less ergonomic for broad cross-sectional research queries such as "all tickers above MA200 on a given day", so the intended split is:

- Oracle NoSQL stores canonical market data
- Python reads from NoSQL and computes indicators locally
- optional local DuckDB or Parquet caches can accelerate research jobs later

## Suggested tables

### `ticker_metadata`

Use one row per ticker:

```sql
CREATE TABLE IF NOT EXISTS ticker_metadata (
  ticker STRING,
  exchange STRING,
  sector STRING,
  industry STRING,
  is_active BOOLEAN,
  source STRING,
  updated_at STRING,
  PRIMARY KEY(ticker)
);
```

### `daily_bars`

Use one row per ticker per date:

```sql
CREATE TABLE IF NOT EXISTS daily_bars (
  ticker STRING,
  trade_date STRING,
  open NUMBER,
  high NUMBER,
  low NUMBER,
  close NUMBER,
  adj_close NUMBER,
  volume LONG,
  dividend NUMBER,
  split_factor NUMBER,
  exchange STRING,
  sector STRING,
  source STRING,
  updated_at STRING,
  PRIMARY KEY(SHARD(ticker), trade_date)
);
```

The shard key choice follows the dominant access path for this repo: fetch one ticker across a date range. Oracle's table docs describe shard keys as the fields used to distribute rows and co-locate rows that share the same shard values. Their SQL docs also show `PRIMARY KEY(SHARD(...), ...)` as the table-definition form for this pattern. See Oracle's docs for the relevant syntax and key behavior: [Basic SQL Statements](https://docs.oracle.com/en/database/other-databases/nosql-database/25.3/sqlfornosql/basic-sql-statements.html) and [Choice of Keys in NoSQL Database](https://docs.oracle.com/en/database/other-databases/nosql-database/24.3/nsdev/choice-keys-nosql-database.html).

## Python SDK notes

Oracle's current guidance is to install the Python SDK with `pip3 install borneo`, and to add `oci` for Oracle Cloud auth flows. See [About Oracle NoSQL Database SDK drivers](https://docs.oracle.com/en/database/other-databases/nosql-database/24.3/nsdev/oracle-nosql-database-sdk-drivers.html).

This repo keeps that dependency separate in:

- [requirements-oracle-nosql.txt](/Users/Zihao.Guan/Personal/ticker-screener/requirements-oracle-nosql.txt)

## Sync script

The first-pass loader lives at:

- [scripts/sync_oracle_nosql_market_data.py](/Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py)

Default behavior:

- load the repo's configured ticker universe
- download daily history from `yfinance`
- export:
  - `ticker_metadata.jsonl`
  - `daily_bars.jsonl`
  - `oracle_nosql_schema.sql`
  - `manifest.json`

Example export run:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01
```

Incremental refresh for the last 10 days through a target end date:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --incremental-days 10 \
  --end-date 2026-05-01
```

Small smoke run:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --tickers AAPL MSFT NVDA \
  --start-date 2024-01-01 \
  --end-date 2024-03-31
```

Direct apply to existing Oracle NoSQL tables:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01 \
  --apply \
  --endpoint https://<your-endpoint> \
  --compartment <your-compartment-ocid>
```

First seed run that creates tables and then writes rows:

```bash
python3 /Users/Zihao.Guan/Personal/ticker-screener/scripts/sync_oracle_nosql_market_data.py \
  --start-date 2020-01-01 \
  --end-date 2026-05-01 \
  --apply \
  --create-tables \
  --endpoint https://<your-endpoint> \
  --compartment <your-compartment-ocid>
```

That direct apply path intentionally stays simple for now:

- one `PutRequest` per row
- no precomputed indicator tables
- no secondary indexes

For a first seed, exporting JSONL plus applying in smaller slices is the safer workflow.

## Recommended operating pattern

For this repo, the lowest-friction rhythm is:

1. first seed a full date range into `daily_bars`
2. after that, run daily with `--incremental-days 7` or similar
3. keep indicators and strategy outputs outside Oracle NoSQL for now

That keeps the database focused on canonical price history instead of turning it into a derived-signal warehouse too early.
