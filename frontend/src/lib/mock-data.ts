import type { CandlePoint, OverlapEntry, ScreenerJob, StrategyCard, WatchlistFile, WatchlistTicker } from "./types";

export const strategyCards: StrategyCard[] = [
  { id: "rs", label: "RS (Relative Strength)", description: "Daily RS leaders.", lastRun: "2026-05-26 09:30", hits: 14, accent: "up" },
  { id: "vcp", label: "VCP (Volatility Contraction)", description: "Volatility contraction setups.", lastRun: "2026-05-26 10:15", hits: 8, accent: "up" },
  { id: "cup_handle", label: "Cup & Handle", description: "Breakout candidates.", lastRun: "2026-05-26 16:00", hits: 3, accent: "neutral" },
  { id: "overlap", label: "Overlap", description: "Cross-strategy overlap.", lastRun: "2026-05-26 11:00", hits: 22, accent: "up" },
];

export const screenerJobs: ScreenerJob[] = [
  { jobId: "RUN-8821", label: "Relative Strength (RS)", status: "running", startedAt: "2026-05-26 14:30:05", finishedAt: "--", returnCode: null },
  { jobId: "RUN-8819", label: "VCP Scanner", status: "success", startedAt: "2026-05-26 12:15:10", finishedAt: "2026-05-26 12:18:42", returnCode: 0 },
  { jobId: "RUN-8815", label: "Cup & Handle", status: "failed", startedAt: "2026-05-26 10:00:00", finishedAt: "2026-05-26 10:00:15", returnCode: 127 },
];

export const consoleTail = [
  "[INFO] Initializing RS Screener Engine...",
  "[INFO] Loading ticker universe...",
  "[INFO] Starting primary scan pass 1/3...",
  "[INFO] Processed 482/2500 tickers (19.2%).",
  "[SYSTEM] Listening for stdout stream...",
].join("\n");

export const watchlistFiles: WatchlistFile[] = [
  {
    stem: "rs_20260526",
    name: "rs_20260526.json",
    path: "/tmp/rs_20260526.json",
    group_key: "rs",
    group_label: "RS",
    captured_at: "2026-05-26T09:30:00Z",
  },
  {
    stem: "vcp_20260525",
    name: "vcp_20260525.json",
    path: "/tmp/vcp_20260525.json",
    group_key: "vcp",
    group_label: "VCP",
    captured_at: "2026-05-25T09:30:00Z",
  },
  {
    stem: "growth_leaders_20260524",
    name: "growth_leaders_20260524.json",
    path: "/tmp/growth_leaders_20260524.json",
    group_key: "other",
    group_label: "Other",
    captured_at: "2026-05-24T09:30:00Z",
  },
];

export const watchlistTickers: WatchlistTicker[] = [
  {
    ticker: "NVDA",
    company: "NVIDIA Corporation",
    scoreLabel: "RS Score",
    score: 98,
    lastPrice: 435.17,
    dailyChangePct: 2.45,
    summary: "Leader near highs with strong RS and constructive volume support.",
    industry: "Semiconductors",
  },
  {
    ticker: "MSFT",
    company: "Microsoft Corp.",
    scoreLabel: "RS Score",
    score: 92,
    lastPrice: 329.67,
    dailyChangePct: 0.88,
    summary: "Tight action above moving averages; secondary entry still valid.",
    industry: "Software",
  },
  {
    ticker: "TSLA",
    company: "Tesla, Inc.",
    scoreLabel: "RS Score",
    score: 85,
    lastPrice: 212.08,
    dailyChangePct: -1.24,
    summary: "Volatility elevated, but reclaim attempts are visible around support.",
    industry: "Automotive",
  },
  {
    ticker: "META",
    company: "Meta Platforms",
    scoreLabel: "RS Score",
    score: 94,
    lastPrice: 312.55,
    dailyChangePct: 1.15,
    summary: "Still acting as a liquid leader with orderly pullbacks.",
    industry: "Internet",
  },
];

const priceSeed = [
  [420, 426, 418, 424, 31000000],
  [424, 431, 422, 430, 35000000],
  [430, 434, 427, 429, 29000000],
  [429, 438, 428, 436, 42000000],
  [436, 439, 432, 435, 39000000],
  [435, 437, 430, 433, 28000000],
  [433, 441, 432, 439, 36000000],
  [439, 444, 437, 442, 41000000],
  [442, 445, 438, 440, 33000000],
  [440, 446, 439, 444, 37000000],
  [444, 448, 442, 447, 43000000],
  [447, 451, 445, 450, 46000000],
  [450, 454, 447, 449, 41000000],
  [449, 456, 448, 455, 47000000],
  [455, 458, 451, 452, 39000000],
  [452, 459, 450, 457, 44000000],
  [457, 460, 454, 458, 43000000],
  [458, 461, 455, 459, 35000000],
  [459, 463, 456, 462, 38000000],
  [462, 466, 460, 465, 48000000],
];

export const chartData: CandlePoint[] = priceSeed.map((item, index) => ({
  time: `2026-05-${String(index + 1).padStart(2, "0")}`,
  open: item[0],
  high: item[1],
  low: item[2],
  close: item[3],
  volume: item[4],
}));

export const overlapEntries: OverlapEntry[] = [
  { ticker: "NVDA", pipeline_count: 4, pipeline_labels: ["RS", "VCP", "HTF 8W Runup", "Overlap"], sector: "Semiconductors" },
  { ticker: "CRWV", pipeline_count: 3, pipeline_labels: ["Sean PEG", "RS", "Gap Fill"], sector: "Software" },
  { ticker: "META", pipeline_count: 2, pipeline_labels: ["RS", "Weekly HTF Pullback"], sector: "Internet" },
];
