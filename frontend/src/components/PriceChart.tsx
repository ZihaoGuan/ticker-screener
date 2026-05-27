import { ColorType, createChart } from "lightweight-charts";
import { useEffect, useMemo, useRef } from "react";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";

type PriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays?: Pick<WatchlistChartResponse, "ma20" | "ma50" | "ma200">;
  annotations?: ChartAnnotations;
};

type GapZone = {
  startIndex: number;
  endIndex: number;
  remainingLowerPrice: number;
  remainingUpperPrice: number;
  direction: "up" | "down";
  filled: boolean;
};

export function PriceChart({ ticker, candles, overlays, annotations }: PriceChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);

  const ma20 = useMemo(() => overlays?.ma20 ?? buildMovingAverage(candles, 20), [candles, overlays?.ma20]);
  const ma50 = useMemo(() => overlays?.ma50 ?? buildMovingAverage(candles, 50), [candles, overlays?.ma50]);
  const ma200 = useMemo(() => overlays?.ma200 ?? buildMovingAverage(candles, 200), [candles, overlays?.ma200]);
  const visibleGapZones = useMemo(() => detectGapZones(candles).filter((zone) => zone.remainingUpperPrice > zone.remainingLowerPrice + 1e-6).slice(-4), [candles]);

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

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#10b981",
      downColor: "#f43f5e",
      wickUpColor: "#10b981",
      wickDownColor: "#f43f5e",
      borderVisible: false,
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });

    chart.priceScale("").applyOptions({
      scaleMargins: {
        top: 0.76,
        bottom: 0,
      },
    });

    const ma20Series = chart.addLineSeries({ color: "#38bdf8", lineWidth: 2 });
    const ma50Series = chart.addLineSeries({ color: "#fb923c", lineWidth: 2 });
    const ma200Series = chart.addLineSeries({ color: "#a78bfa", lineWidth: 2 });

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

    const gapSeries = visibleGapZones.flatMap((zone, index) => {
      const startTime = candles[zone.startIndex]?.time;
      const endTime = candles[zone.endIndex]?.time ?? candles[candles.length - 1]?.time;
      if (!startTime || !endTime) {
        return [];
      }
      const lineColor = zone.direction === "up" ? "rgba(110, 231, 183, 0.75)" : "rgba(252, 165, 165, 0.75)";
      const upperSeries = chart.addLineSeries({ color: lineColor, lineWidth: index === visibleGapZones.length - 1 ? 2 : 1 });
      upperSeries.setData([
        { time: startTime, value: zone.remainingUpperPrice },
        { time: endTime, value: zone.remainingUpperPrice },
      ]);
      const lowerSeries = chart.addLineSeries({ color: lineColor, lineWidth: index === visibleGapZones.length - 1 ? 2 : 1 });
      lowerSeries.setData([
        { time: startTime, value: zone.remainingLowerPrice },
        { time: endTime, value: zone.remainingLowerPrice },
      ]);
      return [upperSeries, lowerSeries];
    });

    const priceLineHandles = [
      annotations?.triggerPrice != null
        ? candleSeries.createPriceLine({
            price: annotations.triggerPrice,
            color: "#facc15",
            lineWidth: 2,
            title: annotations.triggerLabel ?? "Trigger",
          })
        : null,
      annotations?.entryPrice != null
        ? candleSeries.createPriceLine({
            price: annotations.entryPrice,
            color: "#4ade80",
            lineWidth: 2,
            title: annotations.entryLabel ?? "Entry",
          })
        : null,
      annotations?.secondaryEntryPrice != null
        ? candleSeries.createPriceLine({
            price: annotations.secondaryEntryPrice,
            color: "#94a3b8",
            lineWidth: 1,
            title: annotations.secondaryEntryLabel ?? "Secondary",
          })
        : null,
      annotations?.stopPrice != null
        ? candleSeries.createPriceLine({
            price: annotations.stopPrice,
            color: "#fb7185",
            lineWidth: 2,
            title: annotations.stopLabel ?? "Stop",
          })
        : null,
    ].filter(Boolean);

    const markers = [];
    if (annotations?.eventDate && annotations.eventDate.length > 0) {
      markers.push({
        time: annotations.eventDate,
        position: "aboveBar" as const,
        color: "#fbbf24",
        shape: "circle" as const,
        text: annotations.eventLabel ?? "Event",
      });
    }
    if (visibleGapZones.length > 0) {
      const latestGap = visibleGapZones[visibleGapZones.length - 1];
      const latestTime = candles[latestGap.startIndex]?.time;
      if (latestTime) {
        markers.push({
          time: latestTime,
          position: "belowBar" as const,
          color: latestGap.direction === "up" ? "#86efac" : "#fca5a5",
          shape: "square" as const,
          text: latestGap.direction === "up" ? "Gap up" : "Gap down",
        });
      }
    }
    if (markers.length > 0) {
      candleSeries.setMarkers(markers);
    }

    chart.timeScale().fitContent();

    const resizeObserver = new ResizeObserver(() => {
      if (rootRef.current) {
        chart.applyOptions({ width: rootRef.current.clientWidth });
      }
    });

    resizeObserver.observe(rootRef.current);

    return () => {
      resizeObserver.disconnect();
      void gapSeries;
      void priceLineHandles;
      chart.remove();
    };
  }, [annotations, candles, ma20, ma50, ma200, ticker, visibleGapZones]);

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

function detectGapZones(candles: CandlePoint[]): GapZone[] {
  const zones: GapZone[] = [];
  if (candles.length < 2) {
    return zones;
  }

  for (let index = 1; index < candles.length; index += 1) {
    const previous = candles[index - 1];
    const current = candles[index];

    let direction: "up" | "down" | null = null;
    let originalLowerPrice = 0;
    let originalUpperPrice = 0;

    if (current.low > previous.high) {
      direction = "up";
      originalLowerPrice = previous.high;
      originalUpperPrice = current.low;
    } else if (current.high < previous.low) {
      direction = "down";
      originalLowerPrice = current.high;
      originalUpperPrice = previous.low;
    }

    if (!direction) {
      continue;
    }

    let endIndex = candles.length - 1;
    let filled = false;
    let remainingLowerPrice = originalLowerPrice;
    let remainingUpperPrice = originalUpperPrice;

    for (let futureIndex = index + 1; futureIndex < candles.length; futureIndex += 1) {
      const future = candles[futureIndex];
      if (direction === "up") {
        remainingUpperPrice = Math.min(remainingUpperPrice, Math.max(future.low, originalLowerPrice));
        if (future.low <= originalLowerPrice) {
          endIndex = futureIndex;
          filled = true;
          remainingUpperPrice = originalLowerPrice;
          break;
        }
      } else {
        remainingLowerPrice = Math.max(remainingLowerPrice, Math.min(future.high, originalUpperPrice));
        if (future.high >= originalUpperPrice) {
          endIndex = futureIndex;
          filled = true;
          remainingLowerPrice = originalUpperPrice;
          break;
        }
      }
    }

    zones.push({
      startIndex: index,
      endIndex,
      remainingLowerPrice,
      remainingUpperPrice,
      direction,
      filled,
    });
  }

  return zones;
}
