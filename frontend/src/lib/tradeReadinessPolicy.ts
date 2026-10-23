import type { PullbackQuality, ScannerTopHitRow, ScannerTradePlan, SnapshotTradePlanPolicy } from "./types";

export type StopPolicy = "current_v1" | "martin_daily_structural_v1";
export type ResolvedTradeReadiness = {
  plan: ScannerTradePlan | null;
  readiness: string;
  pullbackQuality: Partial<PullbackQuality> | null;
  adr14Pct: number | null;
  stopToAdr: number | null;
  adrSuitability: string | null;
  adrReason: string | null;
  evidence: Record<string, unknown>;
  policyVersion: StopPolicy;
  snapshotDate: string;
  unavailableReason: string | null;
  isAvoided: boolean;
};

const finitePositive = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value) && value > 0;
const finiteOrNull = (value: unknown) => value === null || value === undefined || (typeof value === "number" && Number.isFinite(value));
const objectPlan = (value: unknown): value is ScannerTradePlan => Boolean(value && typeof value === "object" && !Array.isArray(value));
const validStructuralPlan = (value: unknown, targetDate: string): value is ScannerTradePlan => {
  if (!value || typeof value !== "object") return false;
  const plan = value as Partial<ScannerTradePlan>;
  return typeof plan.source_strategy_id === "string" && Boolean(plan.source_strategy_id)
    && (plan.state === "ready" || plan.state === "active")
    && finitePositive(plan.trigger_price) && finitePositive(plan.stop_price)
    && plan.trigger_price > plan.stop_price && finitePositive(plan.max_entry_price) && plan.max_entry_price > plan.stop_price
    && finitePositive(plan.reference_risk_pct) && finiteOrNull(plan.support_price) && plan.max_risk_pct === 5
    && plan.signal_date === targetDate && typeof plan.risk_valid === "boolean"
    && ["invalid_levels", "within_limit", "over_limit"].includes(String(plan.risk_status));
};
const qualityFailed = (quality: unknown) => Boolean(quality && typeof quality === "object" && (quality as { state?: unknown }).state === "failed");

function validSnapshotPolicy(value: unknown, version: StopPolicy, targetDate: string): boolean {
  if (!value || typeof value !== "object") return false;
  const policy = value as Partial<SnapshotTradePlanPolicy>;
  if (policy.version !== version || policy.as_of_date !== targetDate || !targetDate || typeof policy.evidence !== "object" || !policy.evidence || Array.isArray(policy.evidence)) return false;
  if (version === "current_v1") return typeof policy.trade_readiness === "string" && (policy.availability === "available" ? objectPlan(policy.trade_plan) : policy.availability === "unavailable");
  if (policy.availability === "unavailable") return policy.unavailable_reason === "no_supported_plan" && policy.trade_readiness === "leader";
  const evidence = policy.evidence as Record<string, unknown>;
  if (policy.availability !== "available" || !validStructuralPlan(policy.trade_plan, targetDate)
    || !["avoid", "defined_risk", "reclaim_trigger", "pullback_zone"].includes(String(policy.trade_readiness))
    || !finiteOrNull(policy.adr14_pct) || !finiteOrNull(policy.stop_to_adr)
    || (policy.pullback_quality != null && (typeof policy.pullback_quality !== "object" || Array.isArray(policy.pullback_quality)))) return false;
  const plan = policy.trade_plan;
  const source = plan.source_strategy_id;
  const expectedBasis = source === "ma_pullback_retest" ? "daily_reclaim_low"
    : ["ema21_pullback_buy", "sma200_pullback_buy"].includes(source) ? "saved_support_test_low" : "";
  const risk = (plan.trigger_price! - plan.stop_price!) / plan.trigger_price! * 100;
  const riskValid = risk <= 5;
  return Boolean(expectedBasis) && evidence.version === version && evidence.source_strategy_id === source
    && evidence.signal_date === targetDate && evidence.stop_basis === expectedBasis
    && evidence.stop === plan.stop_price && evidence.invalidation_low === plan.stop_price
    && evidence.reference_entry === plan.trigger_price && evidence.buffer === 0
    && evidence.provisional === (plan.state !== "active")
    && plan.risk_valid === riskValid && plan.risk_status === (riskValid ? "within_limit" : "over_limit")
    && Math.abs(plan.reference_risk_pct! - risk) <= .01 && Math.abs(plan.max_entry_price! - plan.stop_price! / .95) <= .01
    && (source !== "ma_pullback_retest" || (evidence.invalidation_date === targetDate && evidence.profile_id === (plan as ScannerTradePlan & { profile_id?: string }).profile_id && Boolean(evidence.profile_id)))
    && (policy.trade_readiness === "defined_risk" ? plan.state === "active" && riskValid
      : policy.trade_readiness === "reclaim_trigger" ? plan.state === "active" && !riskValid
        : policy.trade_readiness === "pullback_zone" ? plan.state === "ready" : policy.trade_readiness === "avoid");
}

export function resolveTradeReadiness(row: ScannerTopHitRow, policy: StopPolicy, targetDate: string): ResolvedTradeReadiness {
  const saved = row.trade_plans?.[policy];
  const savedData = saved as SnapshotTradePlanPolicy | undefined;
  const validSaved = validSnapshotPolicy(savedData, policy, targetDate);
  const legacyAvailable = policy === "current_v1" && !saved;
  const currentSavedPlan = validSaved && policy === "current_v1" && objectPlan(savedData?.trade_plan) ? savedData.trade_plan : null;
  const plan = validSaved ? policy === "current_v1" ? currentSavedPlan : savedData?.trade_plan ?? null : legacyAvailable && objectPlan(row.trade_plan) ? row.trade_plan : null;
  const readiness = validSaved ? savedData?.trade_readiness ?? "needs-data"
    : legacyAvailable ? row.trade_readiness || "needs-data" : "needs-data";
  const pullbackQuality = validSaved ? (savedData?.pullback_quality as Partial<PullbackQuality> | null | undefined) ?? null
    : legacyAvailable ? row.pullback_quality ?? null : null;
  const isAvoided = readiness === "avoid" || row.trade_readiness === "avoid" || row.strike_zone?.state === "avoid" || qualityFailed(pullbackQuality) || qualityFailed(row.pullback_quality);
  const metricsFromSaved = validSaved;
  return {
    plan,
    readiness: isAvoided ? "avoid" : readiness,
    pullbackQuality,
    adr14Pct: metricsFromSaved ? savedData?.adr14_pct ?? null : legacyAvailable ? row.adr14_pct ?? null : null,
    stopToAdr: metricsFromSaved ? savedData?.stop_to_adr ?? null : legacyAvailable ? row.stop_to_adr ?? null : null,
    adrSuitability: metricsFromSaved ? savedData?.adr_suitability ?? null : legacyAvailable ? row.adr_suitability ?? null : null,
    adrReason: metricsFromSaved ? savedData?.adr_reason ?? null : legacyAvailable ? row.adr_reason ?? null : null,
    evidence: metricsFromSaved ? savedData?.evidence ?? {} : {},
    policyVersion: policy,
    snapshotDate: targetDate,
    unavailableReason: validSaved ? savedData?.unavailable_reason ?? null : legacyAvailable ? null : savedData?.unavailable_reason || (savedData ? "Policy data is stale or malformed for this snapshot." : "Saved policy data is unavailable."),
    isAvoided,
  };
}

export function getReadinessSizingParams(ticker: string, resolved: ResolvedTradeReadiness, management: string) {
  const params = new URLSearchParams({ ticker, management, source: "top-hits", policyVersion: resolved.policyVersion });
  if (resolved.snapshotDate) params.set("snapshotDate", resolved.snapshotDate);
  if (resolved.isAvoided || !resolved.plan) return params;
  if (finitePositive(resolved.plan.trigger_price)) params.set("entry", String(resolved.plan.trigger_price));
  if (finitePositive(resolved.plan.stop_price)) params.set("stop", String(resolved.plan.stop_price));
  if (finitePositive(resolved.plan.max_entry_price)) params.set("maxEntry", String(resolved.plan.max_entry_price));
  if (resolved.plan.signal_date) params.set("signalDate", resolved.plan.signal_date);
  return params;
}

export function resolveSizingOrigin(params: URLSearchParams) {
  const requested = params.get("policyVersion");
  const policyVersion: StopPolicy | null = requested === null ? "current_v1"
    : requested === "current_v1" || requested === "martin_daily_structural_v1" ? requested : null;
  const source = params.get("source") === "top-hits";
  const verifiedPolicy = source && policyVersion !== null;
  return {
    source,
    verifiedPolicy,
    policyVersion,
    policyLabel: policyVersion === "current_v1" ? "Current v1" : policyVersion === "martin_daily_structural_v1" ? "Martin-inspired daily v1" : "Unrecognized / unverified",
    entry: source ? verifiedPolicy ? params.get("entry") : null : params.get("entry"),
    stop: source ? verifiedPolicy ? params.get("stop") : null : params.get("stop"),
    maxEntry: source ? verifiedPolicy ? params.get("maxEntry") : null : params.get("maxEntry"),
    signalDate: source ? verifiedPolicy ? params.get("signalDate") : null : params.get("signalDate"),
    snapshotDate: source ? verifiedPolicy ? params.get("snapshotDate") : null : params.get("snapshotDate"),
  };
}
