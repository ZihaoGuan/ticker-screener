import assert from "node:assert/strict";
import { estimateMartinGapStress } from "../src/lib/martinGapStress.ts";

const winner = estimateMartinGapStress({ entry_price: 100, remaining_shares: 10, unrealized_pnl: 100 }, 120_000);
assert.equal(winner.status, "available");
assert.equal(winner.mark_price, 110, "mark derives from per-position unrealized P&L");
assert.equal(winner.scenarios[0].loss_dollars, 110, "gap loss uses current marked exposure, not entry exposure");
assert.equal(winner.scenarios[0].equity_pct, 110 / 120_000 * 100);
assert.equal(winner.equity_basis, "current_book");

const unavailable = estimateMartinGapStress({ entry_price: 100, remaining_shares: 10 }, null);
assert.equal(unavailable.status, "unavailable", "entry price alone is not presented as a current mark");
assert.equal(unavailable.reason, "current_mark_unavailable");
assert.equal(unavailable.equity, 100_000);
assert.equal(unavailable.equity_basis, "base_100k");

const invalid = estimateMartinGapStress({ entry_price: 100, remaining_shares: 0, unrealized_pnl: 10 }, null);
assert.equal(invalid.status, "unavailable", "zero shares do not produce a fabricated mark");
console.log("Martin gap stress self-check passed.");
