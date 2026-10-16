import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";

const source = await readFile(new URL("../src/lib/positionSizing.ts", import.meta.url), "utf8");
const javascript = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const moduleUrl = `data:text/javascript;base64,${Buffer.from(javascript).toString("base64")}`;
const { calculatePositionSize } = await import(moduleUrl);
const base = { accountValue: 100_000, riskPercent: 0.5, direction: "long", entryPrice: 50, stopPrice: 47.5, maxAllocationPercent: 20, slippagePerShare: 0, sizeMultiplier: 1, wholeShares: true };

const standard = calculatePositionSize(base);
assert.equal(standard.positionValue, 10_000);
assert.equal(standard.shares, 200);
assert.equal(standard.plannedLoss, 500);
const capped = calculatePositionSize({ ...base, stopPrice: 49.5 });
assert.equal(capped.positionValue, 20_000);
assert.equal(capped.shares, 400);
assert.equal(capped.plannedLoss, 200);
assert.equal(capped.allocationCapHit, true);
assert.throws(() => calculatePositionSize({ ...base, stopPrice: 51 }), /Stop must be/);
assert.throws(() => calculatePositionSize({ ...base, riskPercent: 101 }), /cannot exceed 100%/);
console.log("Position sizing self-check passed.");
