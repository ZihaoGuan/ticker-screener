import { Vela } from "@luxalgo/vela";
import { PineWorkerEngine } from "@luxalgo/vela-pinets";
import { useEffect, useMemo, useRef, useState } from "react";
import marketOverextendedSource from "../../../scripts/pine/market_overextended_indicator.pine?raw";
import priceOverlaysSource from "../../../scripts/pine/ticker_screener_price_overlays.pine?raw";
import relativeStrengthSource from "../../../scripts/pine/ticker_screener_relative_strength.pine?raw";
import { TickerScreenerVelaProvider, TICKER_SCREENER_VELA_PROVIDER } from "../lib/velaMarketData";
import type { PriceChartProps } from "./PriceChart";

type VelaPriceChartProps = Pick<PriceChartProps, "ticker" | "candles" | "overlays"> & {
  onUnavailable: (message: string) => void;
};

type PineStatus = "starting" | "ready" | "error" | "disabled";
type IndicatorKey = "extension" | "priceOverlays" | "relativeStrength";

const MARKET_OVEREXTENDED_ID = "ticker-screener-market-overextended";
const PRICE_OVERLAYS_ID = "ticker-screener-price-overlays";
const RELATIVE_STRENGTH_ID = "ticker-screener-relative-strength";

export function VelaPriceChart({ ticker, candles, overlays, onUnavailable }: VelaPriceChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<Vela | null>(null);
  const [isPineEnabled, setIsPineEnabled] = useState(true);
  const [arePriceOverlaysEnabled, setArePriceOverlaysEnabled] = useState(true);
  const [isRelativeStrengthEnabled, setIsRelativeStrengthEnabled] = useState(true);
  const [indicatorStatuses, setIndicatorStatuses] = useState<Record<IndicatorKey, PineStatus>>({
    extension: "starting",
    priceOverlays: "starting",
    relativeStrength: "starting",
  });
  const [indicatorErrors, setIndicatorErrors] = useState<Partial<Record<IndicatorKey, string>>>({});
  const benchmarkTicker = overlays?.benchmark_ticker ?? "SPY";
  const latestCandleTime = useMemo(() => {
    const lastCandle = candles[candles.length - 1];
    const time = lastCandle ? Date.parse(`${lastCandle.time}T00:00:00.000Z`) : Number.NaN;
    return Number.isFinite(time) ? time : undefined;
  }, [candles]);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }

    let disposed = false;
    let chart: Vela | null = null;
    try {
      chart = new Vela(rootRef.current, {
        symbol: `${TICKER_SCREENER_VELA_PROVIDER}:${ticker}`,
        timeframe: "1D",
        theme: "dark",
        height: 520,
        live: false,
        drawings: false,
        animations: { intro: false },
      });
      chartRef.current = chart;
      chart.data.registerProvider(TICKER_SCREENER_VELA_PROVIDER, new TickerScreenerVelaProvider(latestCandleTime));
      chart.registerEngine("pine", new PineWorkerEngine());

      const enabledIndicators: Array<{ key: IndicatorKey; id: string; source: string; title: string; inputs?: Record<string, string> }> = [
        ...(isPineEnabled ? [{ key: "extension" as const, id: MARKET_OVEREXTENDED_ID, source: marketOverextendedSource, title: "Market Overextended Monitor" }] : []),
        ...(arePriceOverlaysEnabled ? [{ key: "priceOverlays" as const, id: PRICE_OVERLAYS_ID, source: priceOverlaysSource, title: "Ticker Screener Price Overlays" }] : []),
        ...(isRelativeStrengthEnabled ? [{ key: "relativeStrength" as const, id: RELATIVE_STRENGTH_ID, source: relativeStrengthSource, title: `Relative Strength vs ${benchmarkTicker}`, inputs: { Benchmark: benchmarkTicker } }] : []),
      ];
      const disabledStatuses: Partial<Record<IndicatorKey, PineStatus>> = {};
      if (!isPineEnabled) disabledStatuses.extension = "disabled";
      if (!arePriceOverlaysEnabled) disabledStatuses.priceOverlays = "disabled";
      if (!isRelativeStrengthEnabled) disabledStatuses.relativeStrength = "disabled";
      setIndicatorStatuses({ extension: "starting", priceOverlays: "starting", relativeStrength: "starting", ...disabledStatuses });
      setIndicatorErrors({});

      void chart.ready().then(async () => {
        for (const indicator of enabledIndicators) {
          const outcome = await chart!.runIndicator(indicator.source, {
            id: indicator.id,
            title: indicator.title,
            inputs: indicator.inputs,
          });
          if (disposed) {
            return;
          }
          if (!outcome.ok || !outcome.handle) {
            setIndicatorStatuses((current) => ({ ...current, [indicator.key]: "error" }));
            setIndicatorErrors((current) => ({ ...current, [indicator.key]: outcome.error?.message || "The Pine indicator could not run." }));
            continue;
          }
          setIndicatorStatuses((current) => ({ ...current, [indicator.key]: "ready" }));
          outcome.handle.on("error", ({ error }) => {
            if (!disposed) {
              setIndicatorStatuses((current) => ({ ...current, [indicator.key]: "error" }));
              setIndicatorErrors((current) => ({ ...current, [indicator.key]: error.message || "The Pine indicator failed while running." }));
            }
          });
        }
      }).catch((error: unknown) => {
        if (!disposed) {
          onUnavailable(error instanceof Error ? error.message : "Vela could not load ticker-screener market data.");
        }
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Vela could not initialize.";
      onUnavailable(message);
    }

    return () => {
      disposed = true;
      chart?.destroy();
      if (chartRef.current === chart) {
        chartRef.current = null;
      }
    };
  }, [arePriceOverlaysEnabled, benchmarkTicker, isPineEnabled, isRelativeStrengthEnabled, latestCandleTime, onUnavailable, ticker]);

  return (
    <div className="vela-chart-stack">
      <div className="vela-chart-toolbar">
        <span className="chart-pill chart-pill-setup">Vela preview</span>
        <label className="chart-toggle">
          <input
            type="checkbox"
            checked={isPineEnabled}
            onChange={(event) => setIsPineEnabled(event.target.checked)}
          />
          <span>Market Overextended Pine</span>
        </label>
        <label className="chart-toggle">
          <input type="checkbox" checked={arePriceOverlaysEnabled} onChange={(event) => setArePriceOverlaysEnabled(event.target.checked)} />
          <span>EMA 8/21 · SMA 50/200</span>
        </label>
        <label className="chart-toggle">
          <input type="checkbox" checked={isRelativeStrengthEnabled} onChange={(event) => setIsRelativeStrengthEnabled(event.target.checked)} />
          <span>RS vs {benchmarkTicker}</span>
        </label>
      </div>
      <div className="vela-pine-statuses" aria-live="polite">
        {(["extension", "priceOverlays", "relativeStrength"] as IndicatorKey[]).map((key) => (
          <span key={key} className={`vela-pine-status is-${indicatorStatuses[key]}`}>
            {key === "extension" ? "Extension" : key === "priceOverlays" ? "Price overlays" : "RS"}: {indicatorStatuses[key] === "starting" ? "loading" : indicatorStatuses[key]}
          </span>
        ))}
      </div>
      {Object.entries(indicatorErrors).map(([key, message]) => <p className="vela-pine-error" key={key}>{message}</p>)}
      <div ref={rootRef} className="vela-chart-root" aria-label={`${ticker} Vela chart`} />
      <p className="panel-copy vela-preview-note">Vela uses ticker-screener daily OHLCV for candles and Pine secondary-series requests. This preview now includes EMA 8/21, SMA 50/200, and RS versus {benchmarkTicker}; advanced annotations remain in Current.</p>
    </div>
  );
}
