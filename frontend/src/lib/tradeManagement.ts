export type TradeManagementPreset = "martin" | "balanced_daily";

export type TradeManagementPlan = {
  preset: TradeManagementPreset;
  riskPerShare: number;
  targets: Array<{ label: "T1" | "T2"; rMultiple: 3 | 6; price: number; sellPercent: 33 | "10–15%" }>;
  runnerPercent: 34 | "remainder";
  runnerTrail: "daily EMA9";
};

export function calculateTradeManagement(trigger: unknown, stop: unknown, preset: TradeManagementPreset): TradeManagementPlan | null {
  if (typeof trigger !== "number" || !Number.isFinite(trigger) || trigger <= 0
    || typeof stop !== "number" || !Number.isFinite(stop) || stop <= 0 || trigger <= stop) return null;
  const riskPerShare = trigger - stop;
  const target = (rMultiple: 3 | 6) => Number((trigger + riskPerShare * rMultiple).toFixed(2));
  if (preset === "balanced_daily") {
    return { preset, riskPerShare, targets: [{ label: "T1", rMultiple: 3, price: target(3), sellPercent: 33 }, { label: "T2", rMultiple: 6, price: target(6), sellPercent: 33 }], runnerPercent: 34, runnerTrail: "daily EMA9" };
  }
  return { preset: "martin", riskPerShare, targets: [{ label: "T1", rMultiple: 3, price: target(3), sellPercent: "10–15%" }], runnerPercent: "remainder", runnerTrail: "daily EMA9" };
}
