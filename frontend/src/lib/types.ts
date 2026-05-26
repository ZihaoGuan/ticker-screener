export type StrategyCard = {
  id: string;
  label: string;
  lastRun: string;
  hits: number;
  accent?: "up" | "neutral";
};

export type JobStatus = "running" | "success" | "failed";

export type ScreenerJob = {
  jobId: string;
  label: string;
  status: JobStatus;
  startedAt: string;
  finishedAt: string;
  returnCode: string;
};

export type WatchlistFile = {
  stem: string;
  label: string;
  dateLabel: string;
  sizeLabel: string;
};

export type WatchlistTicker = {
  ticker: string;
  company: string;
  scoreLabel: string;
  score: number;
  lastPrice: number;
  dailyChangePct: number;
  summary: string;
  industry: string;
};

export type CandlePoint = {
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type OverlapEntry = {
  ticker: string;
  overlapCount: number;
  pipelines: string[];
  sector: string;
};
