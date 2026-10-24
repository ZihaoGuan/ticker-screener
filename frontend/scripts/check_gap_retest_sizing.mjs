import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";

const source = await readFile(new URL("../src/lib/tradeReadinessPolicy.ts", import.meta.url), "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
const { getGapRetestSizingParams, resolveSizingOrigin } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);
const context = {
  status: "available",
  decision_date: "2026-10-23",
  strategy_id: "martin_daily_gap_retest_v1",
  config_version: "gap_retest_v1",
  plan: { reference_entry: 100, initial_stop: 97, max_entry: 100, reference_risk_pct: 3, adr14_pct: 5, stop_to_adr: 0.6 },
};
const params = getGapRetestSizingParams("ABCD", context);
assert.ok(params);
assert.equal(params.get("entry"), "100");
assert.equal(params.get("stop"), "97");
assert.equal(params.get("maxEntry"), "100");
assert.equal(params.get("signalDate"), "2026-10-23");
assert.equal(params.get("policyVersion"), "gap_daily_reclaim_next_open_v1");
assert.equal(params.get("entryPolicyVersion"), "gap_daily_reclaim_next_open_v1");
assert.equal(params.get("stopPolicyVersion"), "gap_reclaim_candle_low_v1");
assert.equal(resolveSizingOrigin(params).verifiedPolicy, true);

for (const changes of [
  { status: "unavailable" },
  { decision_date: null },
  { plan: { ...context.plan, initial_stop: 101 } },
  { plan: { ...context.plan, max_entry: null } },
]) assert.equal(getGapRetestSizingParams("ABCD", { ...context, ...changes }), null);

const staleOrigin = resolveSizingOrigin(new URLSearchParams("ticker=ABCD&source=gap-retest&policyVersion=current_v1&entry=100&stop=97&maxEntry=100"));
assert.equal(staleOrigin.verifiedPolicy, false);
assert.equal(staleOrigin.entry, null);
assert.equal(staleOrigin.stop, null);
const wrongStopPolicy = new URLSearchParams(params);
wrongStopPolicy.set("stopPolicyVersion", "martin_daily_structural_v1");
assert.equal(resolveSizingOrigin(wrongStopPolicy).verifiedPolicy, false);
assert.equal(resolveSizingOrigin(new URLSearchParams("source=top-hits&policyVersion=martin_daily_structural_v1&entry=100&stop=97")).verifiedPolicy, true);

process.stdout.write("Gap Retest sizing contract checks passed.\n");
