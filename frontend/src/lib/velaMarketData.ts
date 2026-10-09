import type { BarRange, DataProvider, OHLCV, ProviderInfo, SymbolDescriptor } from "@luxalgo/vela";
import { fetchJson } from "./api";

// Vela provider prefixes accept letters, numbers, underscores, and dots only.
const PROVIDER_NAME = "tickerscreener";
const MAX_LOOKBACK_DAYS = 5 * 366;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const TICKER_PATTERN = /^[A-Z0-9][A-Z0-9.-]{0,14}$/;

type OhlcvRangeResponse = {
  bars: Array<{
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
  }>;
};

type SupportedTimeframe = "1D" | "1W" | "1M";

function normalizeTicker(ticker: string): string {
  const normalized = ticker.trim().toUpperCase();
  if (!TICKER_PATTERN.test(normalized)) {
    throw new Error(`Unsupported ticker for the Vela data provider: ${ticker}`);
  }
  return normalized;
}

function normalizeTimeframe(timeframe: string): SupportedTimeframe {
  const normalized = timeframe.trim().toUpperCase();
  if (normalized === "D" || normalized === "1D") {
    return "1D";
  }
  if (normalized === "W" || normalized === "1W") {
    return "1W";
  }
  if (normalized === "M" || normalized === "1M") {
    return "1M";
  }
  throw new Error(`The Vela preview currently supports daily, weekly, and monthly data (received ${timeframe}).`);
}

function utcDate(timestamp: number): string {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function resolveRange(range: BarRange, latestTime?: number): { startDate: string; endDate: string } {
  const now = Math.min(Date.now(), latestTime ?? Date.now());
  const end = Math.min(range.to ?? now, now);
  const requestedStart = range.from ?? end - MAX_LOOKBACK_DAYS * ONE_DAY_MS;
  const start = Math.min(Math.max(requestedStart, end - MAX_LOOKBACK_DAYS * ONE_DAY_MS), end);
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    throw new Error("Invalid Vela data range.");
  }
  return { startDate: utcDate(start), endDate: utcDate(end) };
}

function validateBars(bars: OhlcvRangeResponse["bars"]): OHLCV[] {
  let previousTime = Number.NEGATIVE_INFINITY;
  return bars.map((bar) => {
    const time = Date.parse(`${bar.date}T00:00:00.000Z`);
    const values = [time, bar.open, bar.high, bar.low, bar.close, bar.volume];
    if (!values.every(Number.isFinite) || time <= previousTime || bar.high < Math.max(bar.open, bar.close) || bar.low > Math.min(bar.open, bar.close) || bar.volume < 0) {
      throw new Error(`Invalid OHLCV data returned for ${bar.date}.`);
    }
    previousTime = time;
    return { time, open: bar.open, high: bar.high, low: bar.low, close: bar.close, volume: bar.volume };
  });
}

function bucketStart(time: number, timeframe: Exclude<SupportedTimeframe, "1D">): number {
  const date = new Date(time);
  if (timeframe === "1M") {
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
  }
  const weekday = date.getUTCDay();
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - ((weekday + 6) % 7));
}

function aggregateBars(bars: OHLCV[], timeframe: SupportedTimeframe): OHLCV[] {
  if (timeframe === "1D") {
    return bars;
  }
  const grouped = new Map<number, OHLCV>();
  for (const bar of bars) {
    const time = bucketStart(bar.time, timeframe);
    const existing = grouped.get(time);
    if (existing) {
      existing.high = Math.max(existing.high, bar.high);
      existing.low = Math.min(existing.low, bar.low);
      existing.close = bar.close;
      existing.volume = (existing.volume ?? 0) + (bar.volume ?? 0);
    } else {
      grouped.set(time, { ...bar, time });
    }
  }
  return [...grouped.values()];
}

/**
 * Same-origin data provider for Vela and PineTS secondary series.
 *
 * The backend owns daily US-equity OHLCV. Weekly and monthly requests are
 * deterministically aggregated here so Pine `request.security()` has the same
 * database-backed source as the displayed chart.
 */
export class TickerScreenerVelaProvider implements DataProvider {
  private readonly inFlight = new Map<string, Promise<OHLCV[]>>();
  private readonly indexedSymbols: SymbolDescriptor[];

  constructor(private readonly latestTime?: number, indexedTickers: readonly string[] = []) {
    this.indexedSymbols = [...new Set(indexedTickers.map(normalizeTicker))].map((ticker) => ({
      ticker,
      description: ticker,
      type: "stock",
    }));
  }

  info(): ProviderInfo {
    return {
      name: PROVIDER_NAME,
      displayName: "Ticker Screener",
      supportedTimeframes: ["1D", "1W", "1M"],
      capabilities: { enumerate: true, stream: false, symbolInfo: false },
    };
  }

  listSymbols(): Promise<SymbolDescriptor[]> {
    return Promise.resolve(this.indexedSymbols);
  }

  getBars(ticker: string, timeframe: string, range: BarRange): Promise<OHLCV[]> {
    const symbol = normalizeTicker(ticker);
    const resolvedTimeframe = normalizeTimeframe(timeframe);
    const { startDate, endDate } = resolveRange(range, this.latestTime);
    const key = `${symbol}:${resolvedTimeframe}:${startDate}:${endDate}`;
    const existing = this.inFlight.get(key);
    if (existing) {
      return existing;
    }

    const request = fetchJson<OhlcvRangeResponse>(`/api/market-data/${encodeURIComponent(symbol)}/ohlcv?startDate=${startDate}&endDate=${endDate}`)
      .then((payload) => {
        const bars = aggregateBars(validateBars(payload.bars), resolvedTimeframe);
        return range.limit && range.limit > 0 ? bars.slice(-range.limit) : bars;
      })
      .finally(() => this.inFlight.delete(key));
    this.inFlight.set(key, request);
    return request;
  }
}

export const TICKER_SCREENER_VELA_PROVIDER = PROVIDER_NAME;

export function toVelaTicker(ticker: string): string {
  return normalizeTicker(ticker);
}

/**
 * Primary chart symbols use an explicit provider prefix. Pine secondary-series
 * requests are bare symbols and resolve through this chart's small eager index.
 */
export function toVelaProviderSymbol(ticker: string): string {
  return `${PROVIDER_NAME}:${toVelaTicker(ticker)}`;
}
