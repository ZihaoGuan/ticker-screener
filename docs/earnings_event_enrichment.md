# Earnings event context

The earnings enrichment CLI can optionally match a signal to a confirmed, reported event. The default CLI behavior remains the existing latest-earnings annotation.

```sh
python scripts/enrich_with_earnings.py --input-file signals.json --match-signal-date
python scripts/enrich_with_earnings.py --input-file signals.json --signal-date 2026-10-23 --as-of-date 2026-10-24 --provider yfinance
```

With `--match-signal-date`, each watchlist row or raw hit needs `ticker` and `signal_date` (`YYYY-MM-DD`). `--signal-date` supplies one date for all rows and overrides row values. `--as-of-date` sets the evidence cutoff; future signal dates are left unconfirmed. Context is attached to each row independently, so repeated tickers can have different signal dates. Output `earnings_context.status` distinguishes `earnings_confirmed`, `earnings_nearby_timing_unknown`, `earnings_nearby_unconfirmed`, `catalyst_unknown`, and `lookup_failed`. `provider`, `source`, `fetched_at`, `evidence_mode`, and `cache_status` describe provenance and lookup state.

Only reported actual EPS evidence is eligible for a match; zero and negative EPS values are valid. BMO events map to that XNYS session and AMC events to the next XNYS session, including exchange holidays. Provider timestamps are converted to `America/New_York`; naive timestamps have unknown timing, and market-hour events do not imply a morning gap. If `exchange_calendars` is unavailable, session matching fails conservatively.

This is retrospective enrichment: provider data fetched now does not prove the information was available on the historical signal date. Event queries are limited to the shortlisted symbols and seven-day windows around their signal dates. The optional `earnings_events` PostgreSQL cache stores positive reported-event evidence only; `cache_status` reports whether cache access was disabled, available, or failed. Cache absence does not mean no event. Scheduled dates and estimates are not confirmed events. Earnings context is informational and does not filter tickers or alter a trading strategy.

Set `TICKER_SCREENER_DATABASE_URL` to enable caching against the existing application schema. A complete cached reaction-session match avoids a provider request; incomplete timing can be enriched on a later lookup. Source names are retained when cached evidence is supplemented by another provider. Provider history is bounded (Yahoo requests up to 100 earnings-date rows), so an unknown result is not proof that a historical report never occurred.

This change does not add the proposed gap screener or connect TradingView. The default provider remains configured by `earnings_enrichment_provider`; `auto` uses AInvest, then FMP, then Yahoo according to available API keys.
