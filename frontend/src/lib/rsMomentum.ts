export type RsMomentumKind = "accelerating" | "leadership" | "emerging" | "fading" | "neutral" | "unavailable";

export type RsMomentumSignal = {
  kind: RsMomentumKind;
  label: string;
  rank: number;
  toneClass: string;
  title: string;
};

export function resolveRsMomentumSignal(rs3m: number | null | undefined, rs6m: number | null | undefined, rs12m: number | null | undefined): RsMomentumSignal {
  if (!isFiniteNumber(rs3m) || !isFiniteNumber(rs6m) || !isFiniteNumber(rs12m)) {
    return {
      kind: "unavailable",
      label: "--",
      rank: 0,
      toneClass: "is-neutral",
      title: "Needs 3M, 6M, and 12M RS ratings.",
    };
  }

  if (rs3m > rs6m && rs6m > rs12m) {
    return {
      kind: "accelerating",
      label: "Accelerating",
      rank: 5,
      toneClass: "is-strong",
      title: `3M RS ${formatRs(rs3m)} > 6M RS ${formatRs(rs6m)} > 12M RS ${formatRs(rs12m)}.`,
    };
  }

  if (rs3m < rs6m || rs3m < rs12m) {
    return {
      kind: "fading",
      label: "Fading",
      rank: 1,
      toneClass: "is-negative",
      title: `3M RS ${formatRs(rs3m)} is below ${rs3m < rs6m ? `6M RS ${formatRs(rs6m)}` : `12M RS ${formatRs(rs12m)}`}.`,
    };
  }

  if (rs3m >= 80 && rs12m >= 80) {
    return {
      kind: "leadership",
      label: "Leadership",
      rank: 4,
      toneClass: "is-strong",
      title: `3M RS ${formatRs(rs3m)} and 12M RS ${formatRs(rs12m)} are both 80 or higher.`,
    };
  }

  if (rs3m >= 70 && rs12m < 70 && rs3m > rs12m) {
    return {
      kind: "emerging",
      label: "Emerging",
      rank: 3,
      toneClass: "is-warm",
      title: `3M RS ${formatRs(rs3m)} is above 70 while 12M RS ${formatRs(rs12m)} is still below 70.`,
    };
  }

  return {
    kind: "neutral",
    label: "Neutral",
    rank: 2,
    toneClass: "is-neutral",
    title: `3M RS ${formatRs(rs3m)}, 6M RS ${formatRs(rs6m)}, 12M RS ${formatRs(rs12m)}.`,
  };
}

function isFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function formatRs(value: number) {
  return value.toFixed(1);
}
