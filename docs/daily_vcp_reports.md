# Daily VCP reports

The web app exposes a protected report archive at `/daily-reports`. Each report also has a stable dated route:

```text
/daily-reports/2026-09-18/vcp-analyst
```

Premium and admin users can read the archive through:

```text
GET /api/daily-reports
GET /api/daily-reports/2026-09-18/vcp-analyst
```

An admin session or the server-side ingest token can create or replace a report for a date:

```text
POST /api/daily-reports
Content-Type: application/json
X-Daily-Report-Token: <TICKER_SCREENER_DAILY_REPORT_INGEST_TOKEN>
```

The pair of `report_date` and `agent_id` is the idempotency key. Retrying one agent atomically replaces only that agent's report; reports from other agents on the same date remain intact.

## Payload

```json
{
  "report_date": "2026-09-18",
  "agent_id": "vcp-analyst",
  "agent_name": "VCP Analyst",
  "model": "configured-openai-model",
  "target_trading_date": "2026-09-17",
  "generated_at": "2026-09-18T01:10:00Z",
  "title": "Daily VCP Report",
  "market_context": "SPY and QQQ remain constructive, but breadth is selective.",
  "candidate_count": 18,
  "analyzed_count": 18,
  "source_url": "https://app.gapseeker.win/api/scanner-board/top-hits?...",
  "candidates": [
    {
      "ticker": "NVDA",
      "group": "B",
      "score": 88,
      "data_confidence": "high",
      "last_price": 184.25,
      "pivot": 190.25,
      "entry_range": "190.26-193.00",
      "entry_trigger": "Break above 190.25 with expanding volume.",
      "stop": 181.90,
      "risk_pct": 4.4,
      "earnings_date": "2026-11-18",
      "main_reason": "Three progressively tighter contractions with volume dry-up near the pivot.",
      "supporting_evidence": ["Price is above rising 50-, 150-, and 200-day averages."],
      "counterargument": "The final contraction remains wider than ideal.",
      "pre_entry_invalidation": "A close below 181.90 before breakout.",
      "post_entry_failure": "Exit if the breakout fails and closes below 181.90.",
      "next_event": "Wait for a qualifying breakout above 190.25."
    }
  ],
  "watch_plan": [
    "NVDA: alert at 190.25 and require confirming volume before entry."
  ]
}
```

`agent_id` is required and may contain lowercase letters, numbers, underscores, and hyphens. Valid groups are `A`, `B`, `C`, `D`, `E`, and `U`. Scores, when present, must be between 0 and 100. Candidate tickers must be unique within a report.

Reports are stored under `artifacts/daily_reports/<report_date>/<agent_id>.json`, which is already mounted into the production web container.

## Daily OHLCV input

The report agent can retrieve one ticker's database-backed daily bars with an explicit inclusive date range. This public read endpoint does not require a session or ingest token:

```text
GET /api/market-data/NVDA/ohlcv?startDate=2025-09-18&endDate=2026-09-18
```

The response returns ascending raw daily OHLCV bars, optional adjusted closes, and the first and last dates actually available. The caller must compare `last_available_date` with the report's target trading date before claiming that it evaluated current price action. An empty range returns `bar_count: 0` and `bars: []`. Requests are limited to five calendar years.
