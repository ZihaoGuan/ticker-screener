import { Vela } from "@luxalgo/vela";
import { PineWorkerEngine } from "@luxalgo/vela-pinets";
import { useEffect, useMemo, useRef, useState } from "react";
import marketOverextendedSource from "../../../scripts/pine/market_overextended_indicator.pine?raw";
import { toVelaBars } from "../lib/velaBars";
import type { PriceChartProps } from "./PriceChart";

type VelaPriceChartProps = Pick<PriceChartProps, "ticker" | "candles"> & {
  onUnavailable: (message: string) => void;
};

type PineStatus = "starting" | "ready" | "error" | "disabled";

const MARKET_OVEREXTENDED_ID = "ticker-screener-market-overextended";

export function VelaPriceChart({ ticker, candles, onUnavailable }: VelaPriceChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<Vela | null>(null);
  const [isPineEnabled, setIsPineEnabled] = useState(true);
  const [pineStatus, setPineStatus] = useState<PineStatus>("starting");
  const [pineError, setPineError] = useState("");
  const bars = useMemo(() => toVelaBars(candles), [candles]);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }

    let disposed = false;
    let chart: Vela | null = null;
    try {
      chart = new Vela(rootRef.current, {
        data: bars,
        timeframe: "1D",
        theme: "dark",
        height: 520,
        live: false,
        symbol: ticker,
        drawings: false,
        animations: { intro: false },
      });
      chartRef.current = chart;
      chart.registerEngine("pine", new PineWorkerEngine());

      if (!isPineEnabled) {
        setPineStatus("disabled");
        return () => {
          chart?.destroy();
          chartRef.current = null;
        };
      }

      setPineStatus("starting");
      setPineError("");
      const result = chart.runIndicator(marketOverextendedSource, {
        id: MARKET_OVEREXTENDED_ID,
        title: "Market Overextended Monitor",
      });
      void result.then((outcome) => {
        if (disposed) {
          return;
        }
        if (!outcome.ok || !outcome.handle) {
          setPineStatus("error");
          setPineError(outcome.error?.message || "The Pine indicator could not run.");
          return;
        }
        const handle = outcome.handle;
        setPineStatus("ready");
        handle.on("ready", () => {
          if (!disposed) {
            setPineStatus("ready");
          }
        });
        handle.on("error", ({ error }) => {
          if (!disposed) {
            setPineStatus("error");
            setPineError(error.message || "The Pine indicator failed while running.");
          }
        });
      }).catch((error: unknown) => {
        if (!disposed) {
          setPineStatus("error");
          setPineError(error instanceof Error ? error.message : "The Pine indicator could not run.");
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
  }, [bars, isPineEnabled, onUnavailable, ticker]);

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
        <span className={`vela-pine-status is-${pineStatus}`} aria-live="polite">
          {pineStatus === "starting" ? "Loading Pine…" : pineStatus === "ready" ? "Pine ready" : pineStatus === "disabled" ? "Pine disabled" : "Pine error"}
        </span>
      </div>
      {pineError ? <p className="vela-pine-error">{pineError}</p> : null}
      <div ref={rootRef} className="vela-chart-root" aria-label={`${ticker} Vela chart`} />
      <p className="panel-copy vela-preview-note">Preview currently includes daily candles, volume, and the bundled Market Overextended Pine indicator. Existing overlays remain available in Current.</p>
    </div>
  );
}
