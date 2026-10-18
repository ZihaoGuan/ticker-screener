export type PositionSizingInput = {
  accountValue: number;
  riskPercent: number;
  direction: "long" | "short";
  entryPrice: number;
  stopPrice: number;
  maxAllocationPercent: number;
  slippagePerShare: number;
  sizeMultiplier: number;
  wholeShares: boolean;
};

export type PositionSizingResult = {
  stopDistance: number;
  stopDistancePercent: number;
  riskBudget: number;
  theoreticalShares: number;
  theoreticalPositionValue: number;
  shares: number;
  positionValue: number;
  plannedLoss: number;
  cashRiskPerShare: number;
  oneRTarget: number;
  allocationPercent: number;
  allocationCapHit: boolean;
  unusedRisk: number;
  target2R: number;
  target3R: number;
};

export function calculatePositionSize(input: PositionSizingInput): PositionSizingResult {
  const { accountValue, riskPercent, direction, entryPrice, stopPrice, maxAllocationPercent, slippagePerShare, sizeMultiplier, wholeShares } = input;
  const positive = [accountValue, riskPercent, entryPrice, stopPrice, maxAllocationPercent];
  if (!positive.every((value) => Number.isFinite(value) && value > 0) || !Number.isFinite(slippagePerShare) || slippagePerShare < 0) {
    throw new Error("Enter finite positive values; slippage may be zero.");
  }
  if (riskPercent > 100 || maxAllocationPercent > 100) throw new Error("Risk and allocation percentages cannot exceed 100%.");
  if (![0.5, 2 / 3, 1].includes(sizeMultiplier)) throw new Error("Choose half, two-thirds, or full size.");
  const stopDistance = direction === "long" ? entryPrice - stopPrice : stopPrice - entryPrice;
  if (direction !== "long" && direction !== "short") throw new Error("Choose long or short.");
  if (!Number.isFinite(stopDistance) || stopDistance <= 0) throw new Error("Stop must be below entry for long trades and above entry for short trades.");

  const riskBudget = accountValue * (riskPercent / 100) * sizeMultiplier;
  const riskPerShare = stopDistance + slippagePerShare;
  const theoreticalShares = riskBudget / riskPerShare;
  const allocationLimit = accountValue * (maxAllocationPercent / 100);
  const allocationShares = allocationLimit / entryPrice;
  const allowedShares = Math.min(theoreticalShares, allocationShares);
  const shares = wholeShares ? Math.floor(allowedShares) : allowedShares;
  const positionValue = shares * entryPrice;
  const plannedLoss = shares * riskPerShare;
  const target = (multiple: number) => entryPrice + (direction === "long" ? 1 : -1) * stopDistance * multiple;

  return {
    stopDistance,
    stopDistancePercent: (stopDistance / entryPrice) * 100,
    riskBudget,
    theoreticalShares,
    theoreticalPositionValue: theoreticalShares * entryPrice,
    shares,
    positionValue,
    plannedLoss,
    cashRiskPerShare: riskPerShare,
    oneRTarget: target(1),
    allocationPercent: (positionValue / accountValue) * 100,
    allocationCapHit: theoreticalShares > allocationShares,
    unusedRisk: Math.max(0, riskBudget - plannedLoss),
    target2R: target(2),
    target3R: target(3),
  };
}
