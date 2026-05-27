import { useEffect, useMemo, useRef, useState } from "react";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";
import { createTradingViewDatafeed } from "../lib/tradingview-datafeed";
import { PriceChart } from "./PriceChart";

declare global {
  interface Window {
    TradingView?: {
      widget: new (options: Record<string, unknown>) => TradingViewWidget;
    };
  }
}

type TradingViewWidget = {
  onChartReady(callback: () => void): void;
  activeChart(): TradingViewChartApi;
  remove(): void;
};

type TradingViewChartApi = {
  createStudy(
    name: string,
    forceOverlay?: boolean,
    lock?: boolean,
    inputs?: Record<string, unknown>,
    overrides?: Record<string, unknown>,
  ): Promise<unknown>;
  createShape(
    point: Record<string, unknown>,
    options: Record<string, unknown>,
  ): Promise<unknown>;
  createMultipointShape(
    points: Array<Record<string, unknown>>,
    options: Record<string, unknown>,
  ): Promise<unknown>;
};

type AdvancedPriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays?: WatchlistChartResponse;
  annotations?: ChartAnnotations;
};

type GapZone = {
  startIndex: number;
  endIndex: number;
  remainingLowerPrice: number;
  remainingUpperPrice: number;
  direction: "up" | "down";
};

const ADVANCED_LIBRARY_PATH = "/charting_library/";

export function AdvancedPriceChart({ ticker, candles, overlays, annotations }: AdvancedPriceChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const widgetRef = useRef<TradingViewWidget | null>(null);
  const [libraryReady, setLibraryReady] = useState(false);
  const [libraryUnavailable, setLibraryUnavailable] = useState(false);
  const gapZones = useMemo(
    () => detectGapZones(candles).filter((zone) => zone.remainingUpperPrice > zone.remainingLowerPrice + 1e-6).slice(-4),
    [candles],
  );

  useEffect(() => {
    let cancelled = false;
    if (window.TradingView?.widget) {
      setLibraryReady(true);
      return;
    }
    const script = document.createElement("script");
    script.src = `${ADVANCED_LIBRARY_PATH}charting_library.js`;
    script.async = true;
    script.onload = () => {
      if (!cancelled && window.TradingView?.widget) {
        setLibraryReady(true);
      }
    };
    script.onerror = () => {
      if (!cancelled) {
        setLibraryUnavailable(true);
      }
    };
    document.body.appendChild(script);
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!libraryReady || !rootRef.current || !window.TradingView?.widget) {
      return;
    }

    rootRef.current.innerHTML = "";

    const widget = new window.TradingView.widget({
      symbol: ticker,
      interval: "1D",
      container: rootRef.current,
      library_path: ADVANCED_LIBRARY_PATH,
      locale: "en",
      autosize: true,
      theme: "dark",
      datafeed: createTradingViewDatafeed(),
      enabled_features: ["study_templates"],
      disabled_features: [
        "use_localstorage_for_settings",
        "header_symbol_search",
        "header_compare",
        "header_saveload",
        "header_screenshot",
      ],
    });

    widgetRef.current = widget;

    widget.onChartReady(() => {
      void decorateChart(widget.activeChart(), candles, gapZones, annotations);
    });

    return () => {
      widget.remove();
      widgetRef.current = null;
    };
  }, [annotations, candles, gapZones, libraryReady, ticker]);

  if (libraryUnavailable) {
    return <PriceChart ticker={ticker} candles={candles} overlays={overlays} annotations={annotations} />;
  }

  return (
    <div className="advanced-chart-shell">
      <div ref={rootRef} className="advanced-chart-root" />
      {!libraryReady ? (
        <div className="advanced-chart-note">
          Advanced Charts library not found yet. Drop the official files into <code>frontend/public/charting_library/</code> and this panel will switch over automatically.
        </div>
      ) : null}
    </div>
  );
}

async function decorateChart(
  chart: TradingViewChartApi,
  candles: CandlePoint[],
  gapZones: GapZone[],
  annotations?: ChartAnnotations,
) {
  const latest = candles[candles.length - 1];
  const latestTime = latest ? toUnix(latest.time) : Math.floor(Date.now() / 1000);

  await chart.createStudy("Moving Average Exponential", true, false, { length: 8 }, { "plot.color": "#38bdf8", "plot.linewidth": 2 });
  await chart.createStudy("Moving Average Exponential", true, false, { length: 21 }, { "plot.color": "#f59e0b", "plot.linewidth": 2 });
  await chart.createStudy("Moving Average", true, false, { length: 50 }, { "plot.color": "#a78bfa", "plot.linewidth": 1 });
  await chart.createStudy("Moving Average", true, false, { length: 200 }, { "plot.color": "#f97316", "plot.linewidth": 1 });
  await chart.createStudy("VWAP", true, false, {}, { "plot.color": "#f472b6", "plot.linewidth": 2 });

  if (annotations?.eventDate) {
    await chart.createShape(
      { time: toUnix(annotations.eventDate) },
      {
        shape: "vertical_line",
        lock: true,
        text: annotations.eventLabel ?? "Event",
      },
    );
  }

  await createHorizontalLabel(chart, latestTime, annotations?.triggerPrice, annotations?.triggerLabel ?? "Trigger", "#facc15");
  await createHorizontalLabel(chart, latestTime, annotations?.entryPrice, annotations?.entryLabel ?? "Entry", "#4ade80");
  await createHorizontalLabel(chart, latestTime, annotations?.secondaryEntryPrice, annotations?.secondaryEntryLabel ?? "Secondary", "#94a3b8");
  await createHorizontalLabel(chart, latestTime, annotations?.stopPrice, annotations?.stopLabel ?? "Stop", "#fb7185");

  if (annotations?.secondaryEntryLow != null && annotations?.secondaryEntryHigh != null) {
    await chart.createMultipointShape(
      [
        { time: latestTime - 20 * 24 * 60 * 60, price: annotations.secondaryEntryLow },
        { time: latestTime, price: annotations.secondaryEntryHigh },
      ],
      {
        shape: "rectangle",
        lock: true,
        overrides: {
          color: "#94a3b8",
          linewidth: 1,
          transparency: 85,
          backgroundColor: "#94a3b8",
        },
      },
    );
  }

  for (const zone of gapZones) {
    const start = candles[zone.startIndex];
    const end = candles[zone.endIndex] ?? candles[candles.length - 1];
    if (!start || !end) {
      continue;
    }
    await chart.createMultipointShape(
      [
        { time: toUnix(start.time), price: zone.remainingLowerPrice },
        { time: toUnix(end.time), price: zone.remainingUpperPrice },
      ],
      {
        shape: "rectangle",
        lock: true,
        overrides: {
          color: zone.direction === "up" ? "#22c55e" : "#ef4444",
          linewidth: 1,
          transparency: 86,
          backgroundColor: zone.direction === "up" ? "#22c55e" : "#ef4444",
        },
      },
    );
  }
}

async function createHorizontalLabel(
  chart: TradingViewChartApi,
  time: number,
  price: number | null | undefined,
  text: string,
  color: string,
) {
  if (price == null) {
    return;
  }
  await chart.createShape(
    { time, price },
    {
      shape: "horizontal_line",
      lock: true,
      text,
      overrides: {
        linecolor: color,
        textcolor: color,
        linewidth: 2,
      },
    },
  );
}

function toUnix(value: string) {
  return Math.floor(new Date(`${value}T00:00:00Z`).getTime() / 1000);
}

function detectGapZones(candles: CandlePoint[]): GapZone[] {
  const zones: GapZone[] = [];
  if (candles.length < 2) {
    return zones;
  }

  for (let index = 1; index < candles.length; index += 1) {
    const previous = candles[index - 1];
    const current = candles[index];
    let direction: "up" | "down" | null = null;
    let lower = 0;
    let upper = 0;

    if (current.low > previous.high) {
      direction = "up";
      lower = previous.high;
      upper = current.low;
    } else if (current.high < previous.low) {
      direction = "down";
      lower = current.high;
      upper = previous.low;
    }

    if (!direction) {
      continue;
    }

    let endIndex = candles.length - 1;
    let remainingLowerPrice = lower;
    let remainingUpperPrice = upper;
    for (let futureIndex = index + 1; futureIndex < candles.length; futureIndex += 1) {
      const future = candles[futureIndex];
      if (direction === "up") {
        remainingUpperPrice = Math.min(remainingUpperPrice, Math.max(future.low, lower));
        if (future.low <= lower) {
          endIndex = futureIndex;
          remainingUpperPrice = lower;
          break;
        }
      } else {
        remainingLowerPrice = Math.max(remainingLowerPrice, Math.min(future.high, upper));
        if (future.high >= upper) {
          endIndex = futureIndex;
          remainingLowerPrice = upper;
          break;
        }
      }
      endIndex = futureIndex;
    }

    zones.push({
      startIndex: index - 1,
      endIndex,
      remainingLowerPrice,
      remainingUpperPrice,
      direction,
    });
  }
  return zones;
}
