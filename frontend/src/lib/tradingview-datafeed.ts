type HistoryBar = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type SymbolInfo = {
  name: string;
  ticker: string;
  description: string;
  type: string;
  session: string;
  timezone: string;
  exchange: string;
  listed_exchange: string;
  format: string;
  minmov: number;
  pricescale: number;
  has_intraday: boolean;
  has_weekly_and_monthly: boolean;
  supported_resolutions: string[];
  volume_precision: number;
  data_status: string;
};

type ChartingConfig = {
  supported_resolutions: string[];
  supports_marks: boolean;
  supports_timescale_marks: boolean;
  supports_time: boolean;
};

type ChartingSearchResult = {
  symbol: string;
  full_name: string;
  description: string;
  exchange: string;
  ticker: string;
  type: string;
};

type HistoryResponse =
  | {
      s: "ok";
      t: number[];
      o: number[];
      h: number[];
      l: number[];
      c: number[];
      v: number[];
    }
  | {
      s: "no_data";
      nextTime?: number;
    };

export function createTradingViewDatafeed() {
  const configurationPromise = fetch("/api/charting/config").then(async (response) => {
    if (!response.ok) {
      throw new Error(`Charting config request failed: ${response.status}`);
    }
    return (await response.json()) as ChartingConfig;
  });

  return {
    onReady: (callback: (config: ChartingConfig) => void) => {
      void configurationPromise.then((config) => {
        window.setTimeout(() => callback(config), 0);
      });
    },
    searchSymbols: async (
      userInput: string,
      _exchange: string,
      _symbolType: string,
      onResultReadyCallback: (items: ChartingSearchResult[]) => void,
    ) => {
      const response = await fetch(`/api/charting/search?query=${encodeURIComponent(userInput)}`);
      const results = (await response.json()) as ChartingSearchResult[];
      onResultReadyCallback(results);
    },
    resolveSymbol: async (
      symbolName: string,
      onSymbolResolvedCallback: (symbolInfo: SymbolInfo) => void,
      onResolveErrorCallback: (reason: string) => void,
    ) => {
      try {
        const response = await fetch(`/api/charting/symbols?symbol=${encodeURIComponent(symbolName)}`);
        if (!response.ok) {
          throw new Error(`Resolve symbol failed: ${response.status}`);
        }
        const symbolInfo = (await response.json()) as SymbolInfo;
        onSymbolResolvedCallback(symbolInfo);
      } catch (error) {
        onResolveErrorCallback(error instanceof Error ? error.message : "resolve failed");
      }
    },
    getBars: async (
      symbolInfo: SymbolInfo,
      resolution: string,
      periodParams: { from: number; to: number; firstDataRequest: boolean },
      onHistoryCallback: (bars: HistoryBar[], meta: { noData?: boolean }) => void,
      onErrorCallback: (reason: string) => void,
    ) => {
      try {
        const params = new URLSearchParams({
          symbol: symbolInfo.ticker ?? symbolInfo.name,
          resolution,
          from: String(periodParams.from),
          to: String(periodParams.to),
        });
        const response = await fetch(`/api/charting/history?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`History request failed: ${response.status}`);
        }
        const payload = (await response.json()) as HistoryResponse;
        if (payload.s !== "ok") {
          onHistoryCallback([], { noData: true });
          return;
        }
        const bars = payload.t.map((time, index) => ({
          time: time * 1000,
          open: payload.o[index],
          high: payload.h[index],
          low: payload.l[index],
          close: payload.c[index],
          volume: payload.v[index],
        }));
        onHistoryCallback(bars, { noData: bars.length === 0 });
      } catch (error) {
        onErrorCallback(error instanceof Error ? error.message : "history failed");
      }
    },
    subscribeBars: (
      _symbolInfo: SymbolInfo,
      _resolution: string,
      _onRealtimeCallback: (bar: HistoryBar) => void,
      _subscriberUID: string,
      _onResetCacheNeededCallback: () => void,
    ) => {
      // Daily screener UI is read-mostly for now; no realtime stream yet.
    },
    unsubscribeBars: (_subscriberUID: string) => {
      // No-op until we add websocket/streaming data.
    },
  };
}
