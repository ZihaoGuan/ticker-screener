# RS Expanded · 8 + Rotation v1

Independent paper strategy `martin_rs_expanded_rotation_v1`, API book key
`rs_expanded_rotation`. Existing books and saved history are not changed or copied.
The existing Martin daily job processes this book last, starting flat at its first
valid session. No broker orders or automatic historical replay are added.

## Inherited behavior

Uses RS Expanded classification, conditional RS80–89 eligibility, daily structural
v1 stops, ADR cautions, earnings/Avoid checks and next-session limit entries.
Management remains 10% at 3R and EMA9 close-based exits. Stop and market-open exits
include the existing 10bps adverse slippage; there are no simulated commissions.

## Allocation and risk

Eight slots include runners and normal reserved pending entries. An exit-linked
replacement shares the outgoing position's slot until its opening exit fills.
Fixed sizing base is $100,000; each new order is capped at $500 risk and $20,000
initial allocation. Sizing also respects cash and aggregate committed risk $2,500.

Cash = initial capital + realized PnL (including open-position trims) − remaining
entry cost − normal pending limit notional. No leverage or reinvestment of unrealized
profits. Committed stop risk = remaining shares × max(entry − current hard stop, 0),
plus normal pending order risk. Current mark-to-stop exposure is reported separately;
planned stop risk is not a guaranteed drawdown limit. Projected exit proceeds never
fund an actual fill until the exit is recorded.

## Rotation after the close

Only a capacity-blocked candidate can trigger rotation. First replace a weaker
unfilled reservation if cancelling it makes sufficient capacity. Both SPY and QQQ
must close above EMA21, with EMA21 higher than five exchange sessions earlier.
At most one pair is scheduled per session; after a rotation exit, five full exchange
sessions must complete before another after-close rotation decision is allowed,
whether or not its replacement fills. The next pair executes no earlier than the
following session's open.

Outgoing position must have five full sessions after entry, current gain <0.5R,
MFE <1R, five-session return at least 2 percentage points below SPY, no prior trim,
and no scheduled trend exit. R is frozen initial per-share risk. MFE is persisted
daily including entry-day highs; using full entry-day highs can conservatively
protect a position even if its high occurred before the simulated fill.

Newcomer must pass inherited gates, have same-session confirmation, RS90–99,
close > EMA9 > EMA21, rising EMA21, five-session return at least 2 points above the
outgoing position, ADV20 >= $20M, known earnings more than seven calendar days away,
and close no higher than its frozen maximum-entry limit. RS80–89 names may enter
normally with spare capacity but cannot displace a holding.

Outgoing tie-break: weakest SPY-relative return, lowest current R, longest holding
period, ticker. Incoming/pending priority: highest RS, highest five-session return,
highest liquidity, ticker. These numerical thresholds are an experimental adaptation,
not authenticated mechanical rules from Martin Luk or another trader.

## Linked execution and safety

Persisted `rotation_scheduled` contains stable decision ID, both tickers, metrics,
decision date and `rotation_v1` policy. Candidate and outgoing position store the
same intent. At the next open: gap stops and previously scheduled EMA exits first,
then any remaining rotation exit, then replacement sizing using actual available
cash and risk. Protective exits retain their true reason and can fund the pair.
Incoming positions retain `rotation_entry_intent` as provenance, not an active exit.

A replacement opening above its limit expires immediately; no chasing or automatic
repurchase. Orders otherwise retain one-session limit and stop handling. All pending
entries are sized before any entry-day intraday stop releases cash; entry-day 3R
trims are suppressed. An executed rotation exit starts cooldown even when the
replacement expires. Missing required exposed-ticker execution history rolls back
the whole session. Missing decision evidence blocks rotation, not valid normal exits.

Per-book transaction advisory lock plus one completed session per date prevents
duplicates even if the five-minute snapshot refresh changes its fingerprint.
Non-session/incomplete sessions, mismatched snapshots, and missed intermediate book
sessions are rejected. Missed sessions require explicit recovery; this change does
not extend the existing historical replay CLI to the new strategy.

## Observability and verification

Book payload exposes rotation limits, RS cohorts, saved account marks and SPY/QQQ
benchmarks. Run summary includes `rotation`, `available_cash`, `available_risk`,
`committed_stop_risk`, `mark_to_stop_exposure`. Events include `rotation_scheduled`,
`rotation_exit`, `rotation_entry_expired`, and `pending_replaced`; ordinary stop and
trend exit event types are retained. Compare matching sessions, costs, turnover and
drawdown. Since capacity and risk controls also differ, do not attribute all
performance differences to rotation.

Focused checks:

```sh
../venv/bin/python -m unittest tests.test_martin_rotation tests.test_martin_rs_expanded tests.test_martin_rs_replay tests.test_martin_forward_test_service tests.test_martin_watch_reasons tests.test_snapshot_trade_plan_policies -q
```
