export type TradeManagementPreset = "martin" | "balanced_daily";

export type TradeManagementPlan = {
  preset: TradeManagementPreset;
  riskPerShare: number;
  targets: Array<{ label: "T1" | "T2"; rMultiple: 3 | 6; price: number; sellPercent: 33 | "10–15%"; shares?: number | { min: number; max: number } }>;
  runnerPercent: 34 | "remainder";
  runnerShares?: number | { min: number; max: number };
  trimUnavailable?: boolean;
  runnerTrail: "daily EMA9";
};

export function calculateTradeManagement(trigger: unknown, stop: unknown, preset: TradeManagementPreset, totalShares?: number, direction: "long" | "short" = "long"): TradeManagementPlan | null {
  if (typeof trigger !== "number" || !Number.isFinite(trigger) || trigger <= 0
    || typeof stop !== "number" || !Number.isFinite(stop) || stop <= 0
    || (direction === "long" ? trigger <= stop : trigger >= stop)) return null;
  const riskPerShare = Math.abs(trigger - stop);
  const hasWholeShares = Number.isInteger(totalShares) && (totalShares ?? 0) > 0;
  const target = (rMultiple: 3 | 6) => Number((trigger + (direction === "long" ? 1 : -1) * riskPerShare * rMultiple).toFixed(2));
  if (preset === "balanced_daily") {
    const first = hasWholeShares ? Math.floor(totalShares! * 0.33) : undefined;
    const second = hasWholeShares ? Math.floor(totalShares! * 0.33) : undefined;
    return { preset, riskPerShare, targets: [{ label: "T1", rMultiple: 3, price: target(3), sellPercent: 33, ...(first === undefined ? {} : { shares: first }) }, { label: "T2", rMultiple: 6, price: target(6), sellPercent: 33, ...(second === undefined ? {} : { shares: second }) }], runnerPercent: 34, ...(hasWholeShares ? { runnerShares: totalShares! - first! - second! } : {}), runnerTrail: "daily EMA9" };
  }
  const minTrim = hasWholeShares ? Math.ceil(totalShares! * 0.1) : undefined;
  const maxTrim = hasWholeShares ? Math.floor(totalShares! * 0.15) : undefined;
  const trimUnavailable = hasWholeShares && minTrim! > maxTrim!;
  return { preset: "martin", riskPerShare, targets: [{ label: "T1", rMultiple: 3, price: target(3), sellPercent: "10–15%", ...(hasWholeShares ? { shares: trimUnavailable ? { min: 0, max: 0 } : { min: minTrim!, max: maxTrim! } } : {}) }], runnerPercent: "remainder", ...(hasWholeShares ? { runnerShares: trimUnavailable ? totalShares : { min: totalShares! - maxTrim!, max: totalShares! - minTrim! }, trimUnavailable } : {}), runnerTrail: "daily EMA9" };
}
