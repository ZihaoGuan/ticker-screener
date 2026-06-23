import { ColorType, createChart } from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { CandlePoint } from "../lib/types";

type ScannerMiniChartProps = {
  ticker: string;
  candles: CandlePoint[];
  ema9?: Array<{ time: string; value: number }>;
  ema21?: Array<{ time: string; value: number }>;
};

export function ScannerMiniChart({ ticker, candles, ema9 = [], ema21 = [] }: ScannerMiniChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }
    const chart = createChart(rootRef.current, {
      autoSize: true,
      height: 320,
      layout: {
        background: { type: ColorType.Solid, color: "#1c1c1e" },
        textColor: "#8e8e93",
      },
      grid: {
        vertLines: { color: "rgba(56, 56, 58, 0.45)" },
        horzLines: { color: "rgba(56, 56, 58, 0.45)" },
      },
      crosshair: {
        vertLine: { color: "rgba(255, 189, 127, 0.32)", width: 1 },
        horzLine: { color: "rgba(255, 189, 127, 0.24)", width: 1 },
      },
      rightPriceScale: {
        borderColor: "rgba(56, 56, 58, 0.9)",
        scaleMargins: {
          top: 0.1,
          bottom: 0.32,
        },
      },
      timeScale: {
        borderColor: "rgba(56, 56, 58, 0.9)",
        timeVisible: false,
        secondsVisible: false,
      },
      localization: {
        locale: "en-US",
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#30d158",
      downColor: "#ff453a",
      wickUpColor: "#30d158",
      wickDownColor: "#ff453a",
      borderVisible: false,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const volumeSeries = chart.addHistogramSeries({
      priceScaleId: "",
      priceFormat: { type: "volume" },
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const ema9Series = chart.addLineSeries({
      color: "#2dd4bf",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const ema21Series = chart.addLineSeries({
      color: "#f59e0b",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale("").applyOptions({
      scaleMargins: {
        top: 0.76,
        bottom: 0,
      },
      borderVisible: false,
    });

    candleSeries.setData(
      candles.map((item) => ({
        time: item.time,
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
      })),
    );
    volumeSeries.setData(
      candles.map((item) => ({
        time: item.time,
        value: item.volume,
        color: item.close >= item.open ? "rgba(48, 209, 88, 0.34)" : "rgba(255, 69, 58, 0.34)",
      })),
    );
    ema9Series.setData(ema9);
    ema21Series.setData(ema21);
    chart.timeScale().fitContent();

    const resizeObserver = new ResizeObserver(() => {
      const width = rootRef.current?.clientWidth ?? 0;
      if (width > 0) {
        chart.applyOptions({ width });
      }
    });
    resizeObserver.observe(rootRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [candles, ema9, ema21, ticker]);

  return <div ref={rootRef} className="scanner-mini-chart" aria-label={`${ticker} candlestick chart`} />;
}
