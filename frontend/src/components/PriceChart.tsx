import { ColorType, createChart, HistogramSeries, LineSeries, CandlestickSeries } from "lightweight-charts";
import { useEffect, useMemo, useRef } from "react";
import type { CandlePoint } from "../lib/types";

type PriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
};

export function PriceChart({ ticker, candles }: PriceChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  const ma20 = useMemo(() => buildMovingAverage(candles, 20), [candles]);
  const ma50 = useMemo(() => buildMovingAverage(candles, 50), [candles]);
  const ma200 = useMemo(() => buildMovingAverage(candles, 200), [candles]);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }

    const chart = createChart(rootRef.current, {
      height: 520,
      layout: {
        background: { type: ColorType.Solid, color: "#18181b" },
        textColor: "#f4f4f5",
      },
      grid: {
        vertLines: { color: "#27272a" },
        horzLines: { color: "#27272a" },
      },
      rightPriceScale: {
        borderColor: "#3f3f46",
      },
      timeScale: {
        borderColor: "#3f3f46",
      },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981",
      downColor: "#f43f5e",
      wickUpColor: "#10b981",
      wickDownColor: "#f43f5e",
      borderVisible: false,
    });

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });

    chart.priceScale("").applyOptions({
      scaleMargins: {
        top: 0.76,
        bottom: 0,
      },
    });

    const ma20Series = chart.addSeries(LineSeries, { color: "#38bdf8", lineWidth: 2 });
    const ma50Series = chart.addSeries(LineSeries, { color: "#fb923c", lineWidth: 2 });
    const ma200Series = chart.addSeries(LineSeries, { color: "#a78bfa", lineWidth: 2 });

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
        color: item.close >= item.open ? "rgba(16, 185, 129, 0.5)" : "rgba(244, 63, 94, 0.5)",
      })),
    );

    ma20Series.setData(ma20);
    ma50Series.setData(ma50);
    ma200Series.setData(ma200);
    chart.timeScale().fitContent();

    const resizeObserver = new ResizeObserver(() => {
      if (rootRef.current) {
        chart.applyOptions({ width: rootRef.current.clientWidth });
      }
    });

    resizeObserver.observe(rootRef.current);

    return () => {
      resizeObserver.disconnect();
      chart.remove();
    };
  }, [candles, ma20, ma50, ma200, ticker]);

  return <div ref={rootRef} className="chart-card" />;
}

function buildMovingAverage(candles: CandlePoint[], window: number) {
  const points: { time: string; value: number }[] = [];
  for (let index = window - 1; index < candles.length; index += 1) {
    const slice = candles.slice(index - window + 1, index + 1);
    const avg = slice.reduce((sum, item) => sum + item.close, 0) / window;
    points.push({ time: candles[index].time, value: Number(avg.toFixed(2)) });
  }
  return points;
}
