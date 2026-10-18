import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";

const source = await readFile(new URL("../src/lib/tradeManagement.ts", import.meta.url), "utf8");
const javascript = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const { calculateTradeManagement } = await import(`data:text/javascript;base64,${Buffer.from(javascript).toString("base64")}`);

const martin = calculateTradeManagement(103, 95, "martin");
assert.equal(martin.targets[0].price, 127);
assert.equal(martin.targets[0].rMultiple, 3);
assert.equal(martin.targets[0].sellPercent, "10–15%");
assert.equal(martin.runnerPercent, "remainder");

const balanced = calculateTradeManagement(103, 95, "balanced_daily");
assert.deepEqual(balanced.targets.map(({ price, rMultiple, sellPercent }) => ({ price, rMultiple, sellPercent })), [
  { price: 127, rMultiple: 3, sellPercent: 33 },
  { price: 151, rMultiple: 6, sellPercent: 33 },
]);
assert.equal(balanced.runnerPercent, 34);
assert.equal(balanced.runnerTrail, "daily EMA9");
for (const [trigger, stop] of [[null, 95], [NaN, 95], [Infinity, 95], [0, -1], [95, 95], [94, 95]]) {
  assert.equal(calculateTradeManagement(trigger, stop, "martin"), null, `Reject invalid levels ${trigger}/${stop}`);
}
console.log("Trade management self-check passed.");
