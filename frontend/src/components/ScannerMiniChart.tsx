import { ColorType, createChart } from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { CandlePoint } from "../lib/types";

type ScannerMiniChartProps = {
  ticker: string;
  candles: CandlePoint[];
  chartType?: "candles" | "bars" | "line";
  height?: number;
  showVolume?: boolean;
  ema8?: Array<{ time: string; value: number }>;
  ema9?: Array<{ time: string; value: number }>;
  ema21?: Array<{ time: string; value: number }>;
  ema60?: Array<{ time: string; value: number }>;
  sma50?: Array<{ time: string; value: number }>;
};

export function ScannerMiniChart({
  ticker,
  candles,
  chartType = "candles",
  height = 320,
  showVolume = true,
  ema8 = [],
  ema9 = [],
  ema21 = [],
  ema60 = [],
  sma50 = [],
}: ScannerMiniChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }
    const chart = createChart(rootRef.current, {
      autoSize: true,
      height,
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
          bottom: showVolume ? 0.32 : 0.08,
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

    if (chartType === "line") {
      const priceSeries = chart.addLineSeries({
        color: "#d4d4d8",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      priceSeries.setData(candles.map((item) => ({ time: item.time, value: item.close })));
    } else if (chartType === "bars") {
      const priceSeries = chart.addBarSeries({
        upColor: "#30d158",
        downColor: "#ff453a",
        priceLineVisible: false,
        lastValueVisible: false,
      });
      priceSeries.setData(candles.map((item) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close })));
    } else {
      const priceSeries = chart.addCandlestickSeries({
        upColor: "#30d158",
        downColor: "#ff453a",
        wickUpColor: "#30d158",
        wickDownColor: "#ff453a",
        borderVisible: false,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      priceSeries.setData(candles.map((item) => ({ time: item.time, open: item.open, high: item.high, low: item.low, close: item.close })));
    }
    const volumeSeries = showVolume ? chart.addHistogramSeries({
      priceScaleId: "",
      priceFormat: { type: "volume" },
      priceLineVisible: false,
      lastValueVisible: false,
    }) : null;
    const ema8Series = chart.addLineSeries({
      color: "#22d3ee",
      lineWidth: 2,
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
    const ema60Series = chart.addLineSeries({
      color: "#a78bfa",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const sma50Series = chart.addLineSeries({
      color: "#fb7185",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale("").applyOptions({
      scaleMargins: {
        top: showVolume ? 0.76 : 1,
        bottom: 0,
      },
      borderVisible: false,
    });

    volumeSeries?.setData(candles.map((item) => ({
        time: item.time,
        value: item.volume,
        color: item.close >= item.open ? "rgba(48, 209, 88, 0.34)" : "rgba(255, 69, 58, 0.34)",
      })));
    ema8Series.setData(ema8);
    ema9Series.setData(ema9);
    ema21Series.setData(ema21);
    ema60Series.setData(ema60);
    sma50Series.setData(sma50);
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
  }, [candles, chartType, ema8, ema9, ema21, ema60, height, showVolume, sma50, ticker]);

  return <div ref={rootRef} className="scanner-mini-chart" aria-label={`${ticker} candlestick chart`} />;
}
