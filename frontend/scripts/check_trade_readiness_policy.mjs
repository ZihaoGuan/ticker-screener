import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import ts from "typescript";

const source = await readFile(new URL("../src/lib/tradeReadinessPolicy.ts", import.meta.url), "utf8");
const javascript = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const moduleUrl = `data:text/javascript;base64,${Buffer.from(javascript).toString("base64")}`;
const { resolveTradeReadiness, getReadinessSizingParams, resolveSizingOrigin } = await import(moduleUrl);
const managementSource = await readFile(new URL("../src/lib/tradeManagement.ts", import.meta.url), "utf8");
const managementJavascript = ts.transpileModule(managementSource, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2020 } }).outputText;
const { calculateTradeManagement } = await import(`data:text/javascript;base64,${Buffer.from(managementJavascript).toString("base64")}`);
const date = "2026-10-22";
const basePlan = { source_strategy_id: "ma_pullback_retest", state: "active", support_label: "EMA21", support_price: 50, trigger_label: "Reclaim", trigger_price: 51, stop_label: "Reclaim low", stop_price: 49, reference_risk_pct: 3.92, execution_timeframe: "daily", entry_policy: "close_confirmed", max_risk_pct: 5, max_entry_price: 51.58, risk_valid: true, risk_status: "within_limit", signal_date: date, profile_id: "ema21" };
const quality = { score: 5, state: "constructive", reasons: [] };
const currentPlan = { ...basePlan, trigger_price: 51, stop_price: 47, max_entry_price: 49.47, reference_risk_pct: 7.84, risk_valid: false, risk_status: "over_limit" };
const structuralPlan = { ...basePlan, trigger_price: 52, stop_price: 50, max_entry_price: 52.63, reference_risk_pct: 3.85, profile_id: "ema21" };
const current = { version: "current_v1", as_of_date: date, availability: "available", unavailable_reason: null, trade_plan: currentPlan, trade_readiness: "reclaim_trigger", pullback_quality: quality, adr14_pct: 4.5, stop_to_adr: 1.74, adr_suitability: "wide_stop", adr_reason: "Current policy reason", evidence: {} };
const structuralEvidence = { version: "martin_daily_structural_v1", source_strategy_id: structuralPlan.source_strategy_id, signal_date: date, stop_basis: "daily_reclaim_low", stop: 50, invalidation_low: 50, invalidation_date: date, reference_entry: 52, buffer: 0, profile_id: "ema21", provisional: false };
const structural = { ...current, version: "martin_daily_structural_v1", trade_plan: structuralPlan, trade_readiness: "defined_risk", pullback_quality: quality, adr14_pct: 4.5, stop_to_adr: 0.86, adr_suitability: "suitable", adr_reason: "Structural policy reason", evidence: structuralEvidence };
const row = { ticker: "TEST", day_close: 50, trade_plan: basePlan, trade_readiness: "defined_risk", pullback_quality: quality, adr14_pct: 4.5, stop_to_adr: 0.87, adr_suitability: "wide_stop", trade_plans: { current_v1: current, martin_daily_structural_v1: structural } };

const legacyRow = { ...row, trade_plans: undefined };
assert.equal(resolveTradeReadiness(legacyRow, "current_v1", date).readiness, "defined_risk");
assert.equal(resolveTradeReadiness(legacyRow, "current_v1", date).plan, basePlan);
assert.equal(resolveTradeReadiness(row, "martin_daily_structural_v1", date).readiness, "defined_risk");
const currentResolved = resolveTradeReadiness(row, "current_v1", date);
const structuralResolved = resolveTradeReadiness(row, "martin_daily_structural_v1", date);
assert.equal(currentResolved.plan.stop_price, 47);
assert.equal(structuralResolved.plan.stop_price, 50);
assert.equal(structuralResolved.plan.max_entry_price, 52.63);
assert.notEqual(currentResolved.adrReason, structuralResolved.adrReason);
assert.equal(getReadinessSizingParams("TEST", resolveTradeReadiness({ ...row, trade_plans: { martin_daily_structural_v1: { ...structural, availability: "unavailable", unavailable_reason: "structural_evidence_missing", trade_plan: null, trade_readiness: "leader" } } }, "martin_daily_structural_v1", date), "martin").has("entry"), false);
assert.equal(resolveTradeReadiness({ ...row, trade_readiness: "avoid" }, "martin_daily_structural_v1", date).isAvoided, true);
assert.equal(resolveTradeReadiness({ ...row, strike_zone: { state: "avoid" } }, "martin_daily_structural_v1", date).isAvoided, true);
assert.equal(resolveTradeReadiness({ ...row, pullback_quality: { ...quality, state: "failed" } }, "martin_daily_structural_v1", date).isAvoided, true);

for (const altered of [
  { ...row, trade_plans: {} },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, as_of_date: "2026-10-21" } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, evidence: { ...structuralEvidence, stop: 48 } } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, evidence: { ...structuralEvidence, stop_basis: "atr" } } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, trade_plan: { ...basePlan, trigger_price: 48 } } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, trade_plan: { ...structuralPlan, reference_risk_pct: "invalid" } } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, trade_plan: { ...structuralPlan, reference_risk_pct: 1 } } } },
  { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, trade_plan: { ...structuralPlan, max_entry_price: 1000 } } } },
]) {
  const resolved = resolveTradeReadiness(altered, "martin_daily_structural_v1", date);
  assert.equal(resolved.readiness, "needs-data");
  assert.equal(resolved.plan, null);
  assert.equal(resolved.adr14Pct, null);
}
const unsupported = { ...row, trade_plans: { martin_daily_structural_v1: { ...structural, availability: "unavailable", unavailable_reason: "no_supported_plan", trade_plan: null, trade_readiness: "leader" } } };
assert.equal(resolveTradeReadiness(unsupported, "martin_daily_structural_v1", date).readiness, "leader");
assert.equal(resolveTradeReadiness({ ...row, trade_plans: { martin_daily_structural_v1: { ...unsupported.trade_plans.martin_daily_structural_v1, version: "future" } } }, "martin_daily_structural_v1", date).readiness, "needs-data");
assert.equal(resolveTradeReadiness({ ...row, trade_plans: { martin_daily_structural_v1: { ...structural, trade_readiness: "unknown" } } }, "martin_daily_structural_v1", date).readiness, "needs-data");
const provisional = { ...structural, trade_readiness: "pullback_zone", trade_plan: { ...structuralPlan, state: "ready" }, evidence: { ...structuralEvidence, provisional: true } };
assert.equal(resolveTradeReadiness({ ...row, trade_plans: { martin_daily_structural_v1: provisional } }, "martin_daily_structural_v1", date).readiness, "pullback_zone");

const invalidLegacyPlan = { ...basePlan, trigger_price: null, stop_price: null, risk_status: "invalid_levels" };
assert.equal(resolveTradeReadiness({ ...legacyRow, trade_plan: invalidLegacyPlan }, "current_v1", date).plan, invalidLegacyPlan);
assert.equal(resolveTradeReadiness({ ...row, trade_plans: { current_v1: { ...current, trade_plan: invalidLegacyPlan } } }, "current_v1", date).plan, invalidLegacyPlan);
assert.equal(resolveTradeReadiness({ ...legacyRow, trade_readiness: undefined }, "current_v1", date).readiness, "needs-data");
const before = structuredClone(row);
resolveTradeReadiness(row, "martin_daily_structural_v1", date);
assert.deepEqual(row, before);

const resolved = resolveTradeReadiness(row, "martin_daily_structural_v1", date);
const sizing = getReadinessSizingParams("TEST", resolved, "balanced_daily");
assert.equal(sizing.get("policyVersion"), "martin_daily_structural_v1");
assert.equal(sizing.get("management"), "balanced_daily");
assert.equal(sizing.get("entry"), "52");
assert.equal(sizing.get("stop"), "50");
assert.equal(sizing.get("snapshotDate"), date);
assert.equal(resolveSizingOrigin(new URLSearchParams(sizing)).verifiedPolicy, true);
const avoidedParams = getReadinessSizingParams("TEST", resolveTradeReadiness({ ...row, trade_readiness: "avoid" }, "martin_daily_structural_v1", date), "martin");
assert.equal(avoidedParams.has("entry"), false);
const managementMartin = calculateTradeManagement(52, 50, "martin");
const managementBalanced = calculateTradeManagement(52, 50, "balanced_daily");
assert.equal(managementMartin.targets[0].price, managementBalanced.targets[0].price);
assert.notEqual(managementMartin.preset, managementBalanced.preset);
assert.notEqual(managementMartin.targets.length, managementBalanced.targets.length);
assert.equal(resolveSizingOrigin(new URLSearchParams("source=top-hits&policyVersion=future&entry=10&stop=9")).policyLabel, "Unrecognized / unverified");
assert.equal(resolveSizingOrigin(new URLSearchParams("source=top-hits&policyVersion=future&entry=10&stop=9")).entry, null);
assert.equal(resolveSizingOrigin(new URLSearchParams("source=top-hits&entry=10&stop=9")).policyVersion, "current_v1");
assert.equal(resolveSizingOrigin(new URLSearchParams("entry=10&stop=9")).entry, "10");
console.log("Trade readiness policy self-check passed.");
