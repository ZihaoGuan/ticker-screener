import { Vela } from "@luxalgo/vela";
import { PineWorkerEngine } from "@luxalgo/vela-pinets";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import scannerOverlaysSource from "../../../scripts/pine/ticker_screener_scanner_overlays.pine?raw";
import { TickerScreenerVelaProvider, TICKER_SCREENER_VELA_PROVIDER, toVelaProviderSymbol } from "../lib/velaMarketData";
import type { CandlePoint, WatchlistChartResponse } from "../lib/types";
import { buildRelativeStrengthSource } from "./VelaPriceChart";

type VelaScannerChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays: Pick<WatchlistChartResponse, "benchmark_ticker" | "rs_line" | "daily_rs_rating">;
  dailyRs: number | null | undefined;
  height: number;
  timeframe: "daily" | "weekly";
  range: "3m" | "6m" | "1y";
  chartType: "candles" | "bars" | "line";
  showVolume: boolean;
  showEma8: boolean;
  showEma21: boolean;
  showEma60: boolean;
  showEma200: boolean;
  showSma50: boolean;
  fallback: ReactNode;
};

const SCANNER_OVERLAYS_ID = "ticker-screener-scanner-overlays";
const RELATIVE_STRENGTH_ID = "ticker-screener-relative-strength";

function replacePlaceholder(source: string, placeholder: string, value: boolean): string {
  const occurrences = source.split(placeholder).length - 1;
  if (occurrences !== 1) throw new Error(`Scanner Pine script is missing ${placeholder}.`);
  return source.replace(placeholder, String(value));
}

function buildScannerOverlaysSource(settings: Pick<VelaScannerChartProps, "showEma8" | "showEma21" | "showEma60" | "showEma200" | "showSma50">): string {
  const source = replacePlaceholder(
    replacePlaceholder(
      replacePlaceholder(
        replacePlaceholder(scannerOverlaysSource, "__TICKER_SCREENER_SHOW_EMA8__", settings.showEma8),
        "__TICKER_SCREENER_SHOW_EMA21__",
        settings.showEma21,
      ),
      "__TICKER_SCREENER_SHOW_EMA60__",
      settings.showEma60,
    ),
    "__TICKER_SCREENER_SHOW_EMA200__",
    settings.showEma200,
  );
  return replacePlaceholder(source, "__TICKER_SCREENER_SHOW_SMA50__", settings.showSma50);
}

export function VelaScannerChart({
  ticker,
  candles,
  overlays,
  dailyRs,
  height,
  timeframe,
  range,
  chartType,
  showVolume,
  showEma8,
  showEma21,
  showEma60,
  showEma200,
  showSma50,
  fallback,
}: VelaScannerChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const benchmarkTicker = overlays.benchmark_ticker || "SPY";
  const latestRsLine = overlays.rs_line[overlays.rs_line.length - 1]?.value ?? null;
  const latestRating = overlays.daily_rs_rating?.[overlays.daily_rs_rating.length - 1]?.value ?? dailyRs ?? null;
  const latestClose = candles[candles.length - 1]?.close ?? null;
  const benchmarkAnchor = latestRsLine != null && latestRsLine !== 0 && latestClose != null ? latestClose / latestRsLine : null;
  const latestCandleTime = useMemo(() => {
    const time = candles[candles.length - 1]?.time;
    const parsed = time ? Date.parse(`${time}T00:00:00.000Z`) : Number.NaN;
    return Number.isFinite(parsed) ? parsed : undefined;
  }, [candles]);
  const relativeStrengthSource = useMemo(
    () => buildRelativeStrengthSource(benchmarkTicker, benchmarkAnchor, latestRating),
    [benchmarkAnchor, benchmarkTicker, latestRating],
  );
  const configuredOverlaysSource = useMemo(
    () => buildScannerOverlaysSource({ showEma8, showEma21, showEma60, showEma200, showSma50 }),
    [showEma8, showEma21, showEma60, showEma200, showSma50],
  );

  useEffect(() => {
    if (!rootRef.current || unavailable) return;
    let disposed = false;
    let chart: Vela | null = null;
    try {
      chart = new Vela(rootRef.current, {
        symbol: toVelaProviderSymbol(ticker),
        timeframe: timeframe === "weekly" ? "1W" : "1D",
        visibleRange: range === "3m" ? "3M" : range === "6m" ? "6M" : "1Y",
        priceStyle: chartType,
        height,
        live: false,
        drawings: false,
        volume: showVolume,
        animations: { intro: false },
      });
      chart.data.registerProvider(
        TICKER_SCREENER_VELA_PROVIDER,
        new TickerScreenerVelaProvider(latestCandleTime, [ticker, benchmarkTicker]),
      );
      chart.registerEngine("pine", new PineWorkerEngine());
      void Promise.all([chart.ready(), chart.data.ready()]).then(async () => {
        for (const [id, source] of [[SCANNER_OVERLAYS_ID, configuredOverlaysSource], [RELATIVE_STRENGTH_ID, relativeStrengthSource]] as const) {
          const outcome = await chart!.runIndicator(source, { id, title: id === RELATIVE_STRENGTH_ID ? `RS Rating vs ${benchmarkTicker}` : "Scanner overlays" });
          if (disposed) return;
          if (!outcome.ok || !outcome.handle) {
            setUnavailable(true);
            return;
          }
          outcome.handle.on("error", () => {
            if (!disposed) setUnavailable(true);
          });
        }
      }).catch(() => {
        if (!disposed) setUnavailable(true);
      });
    } catch {
      setUnavailable(true);
    }
    return () => {
      disposed = true;
      chart?.destroy();
    };
  }, [benchmarkTicker, chartType, configuredOverlaysSource, height, latestCandleTime, range, relativeStrengthSource, showVolume, ticker, timeframe, unavailable]);

  if (unavailable) return <>{fallback}</>;
  return <div ref={rootRef} className="vela-scanner-chart" style={{ height }} aria-label={`${ticker} Vela chart`} />;
}
