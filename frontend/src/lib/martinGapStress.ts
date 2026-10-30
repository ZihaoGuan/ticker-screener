type PositionLike = Record<string, unknown>;

export type GapStressEstimate = {
  status: "available" | "unavailable";
  reason?: string;
  mark_price?: number;
  mark_basis?: "latest_mark" | "unrealized_pnl";
  equity: number;
  equity_basis: "current_book" | "base_100k";
  scenarios?: Array<{ gap_pct: number; loss_dollars: number; equity_pct: number }>;
};

function finitePositive(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null;
}

export function estimateMartinGapStress(position: PositionLike, currentEquity: unknown): GapStressEstimate {
  const equity = finitePositive(currentEquity);
  const equityBasis = equity == null ? "base_100k" : "current_book";
  const denominator = equity ?? 100_000;
  const entry = finitePositive(position.entry_price);
  const shares = finitePositive(position.remaining_shares);
  const directMark = finitePositive(position.latest_mark_price ?? position.mark_price ?? position.marked_price);
  const unrealized = position.unrealized_pnl;
  const derivedMark = entry != null && shares != null && typeof unrealized === "number" && Number.isFinite(unrealized)
    ? finitePositive(entry + unrealized / shares)
    : null;
  const mark = directMark ?? derivedMark;
  if (mark == null || shares == null) {
    return { status: "unavailable", reason: mark == null ? "current_mark_unavailable" : "remaining_shares_unavailable",
      equity: denominator, equity_basis: equityBasis };
  }
  return {
    status: "available", mark_price: mark, mark_basis: directMark != null ? "latest_mark" : "unrealized_pnl",
    equity: denominator, equity_basis: equityBasis,
    scenarios: [10, 20, 30].map((gapPct) => {
      const loss = mark * shares * gapPct / 100;
      return { gap_pct: gapPct, loss_dollars: loss, equity_pct: loss / denominator * 100 };
    }),
  };
}
