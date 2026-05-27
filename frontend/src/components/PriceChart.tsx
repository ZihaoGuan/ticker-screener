import { ColorType, createChart } from "lightweight-charts";
import { useEffect, useMemo, useRef } from "react";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";

type PriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays?: Pick<
    WatchlistChartResponse,
    "ma20" | "ma50" | "ma200" | "ema8" | "ema21" | "weekly_ema8" | "ipo_vwap" | "rs_line" | "rs_markers" | "benchmark_ticker"
  >;
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
  const priceRootRef = useRef<HTMLDivElement | null>(null);
  const rsRootRef = useRef<HTMLDivElement | null>(null);

  const ma20 = useMemo(() => overlays?.ma20 ?? buildMovingAverage(candles, 20), [candles, overlays?.ma20]);
  const ma50 = useMemo(() => overlays?.ma50 ?? buildMovingAverage(candles, 50), [candles, overlays?.ma50]);
  const ma200 = useMemo(() => overlays?.ma200 ?? buildMovingAverage(candles, 200), [candles, overlays?.ma200]);
  const ema8 = useMemo(() => overlays?.ema8 ?? buildExponentialMovingAverage(candles, 8), [candles, overlays?.ema8]);
  const ema21 = useMemo(() => overlays?.ema21 ?? buildExponentialMovingAverage(candles, 21), [candles, overlays?.ema21]);
  const weeklyEma8 = useMemo(() => overlays?.weekly_ema8 ?? [], [overlays?.weekly_ema8]);
  const ipoVwap = useMemo(() => overlays?.ipo_vwap ?? [], [overlays?.ipo_vwap]);
  const rsLine = useMemo(() => overlays?.rs_line ?? [], [overlays?.rs_line]);
  const rsMarkers = useMemo(() => overlays?.rs_markers ?? [], [overlays?.rs_markers]);
  const benchmarkTicker = overlays?.benchmark_ticker ?? "SPY";
  const visibleGapZones = useMemo(
    () => detectGapZones(candles).filter((zone) => zone.remainingUpperPrice > zone.remainingLowerPrice + 1e-6).slice(-4),
    [candles],
  );

  useEffect(() => {
    if (!priceRootRef.current || !rsRootRef.current) {
      return;
    }

    const priceChart = createChart(priceRootRef.current, {
      height: 458,
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
      crosshair: {
        mode: 0,
      },
    });

    const rsChart = createChart(rsRootRef.current, {
      height: 152,
      layout: {
        background: { type: ColorType.Solid, color: "#111114" },
        textColor: "#d4d4d8",
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
        visible: true,
      },
      crosshair: {
        mode: 0,
      },
    });

    priceChart.timeScale().applyOptions({ visible: false });

    const candleSeries = priceChart.addCandlestickSeries({
      upColor: "#10b981",
      downColor: "#f43f5e",
      wickUpColor: "#10b981",
      wickDownColor: "#f43f5e",
      borderVisible: false,
    });
    const volumeSeries = priceChart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    priceChart.priceScale("").applyOptions({
      scaleMargins: {
        top: 0.78,
        bottom: 0,
      },
    });

    const ema8Series = priceChart.addLineSeries({ color: "#38bdf8", lineWidth: 2 });
    const ema21Series = priceChart.addLineSeries({ color: "#f59e0b", lineWidth: 2 });
    const weeklyEma8Series = priceChart.addLineSeries({ color: "#4ade80", lineWidth: 2 });
    const ipoVwapSeries = priceChart.addLineSeries({ color: "#f472b6", lineWidth: 2 });
    const ma20Series = priceChart.addLineSeries({ color: "rgba(56, 189, 248, 0.35)", lineWidth: 1 });
    const ma50Series = priceChart.addLineSeries({ color: "rgba(251, 146, 60, 0.45)", lineWidth: 1 });
    const ma200Series = priceChart.addLineSeries({ color: "rgba(167, 139, 250, 0.52)", lineWidth: 1 });
    const rsSeries = rsChart.addLineSeries({ color: "#60a5fa", lineWidth: 2 });

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
    ema8Series.setData(ema8);
    ema21Series.setData(ema21);
    weeklyEma8Series.setData(weeklyEma8);
    ipoVwapSeries.setData(ipoVwap);
    ma20Series.setData(ma20);
    ma50Series.setData(ma50);
    ma200Series.setData(ma200);
    rsSeries.setData(rsLine);

    const gapSeries = visibleGapZones.flatMap((zone, index) => {
      const startTime = candles[zone.startIndex]?.time;
      const endTime = candles[zone.endIndex]?.time ?? candles[candles.length - 1]?.time;
      if (!startTime || !endTime) {
        return [];
      }
      const lineColor = zone.direction === "up" ? "rgba(110, 231, 183, 0.78)" : "rgba(252, 165, 165, 0.78)";
      const upperSeries = priceChart.addLineSeries({ color: lineColor, lineWidth: index === visibleGapZones.length - 1 ? 2 : 1 });
      upperSeries.setData([
        { time: startTime, value: zone.remainingUpperPrice },
        { time: endTime, value: zone.remainingUpperPrice },
      ]);
      const lowerSeries = priceChart.addLineSeries({ color: lineColor, lineWidth: index === visibleGapZones.length - 1 ? 2 : 1 });
      lowerSeries.setData([
        { time: startTime, value: zone.remainingLowerPrice },
        { time: endTime, value: zone.remainingLowerPrice },
      ]);
      return [upperSeries, lowerSeries];
    });

    const priceLines = [
      buildPriceLine(candleSeries, annotations?.triggerPrice, "#facc15", annotations?.triggerLabel ?? "Trigger", 2),
      buildPriceLine(candleSeries, annotations?.entryPrice, "#4ade80", annotations?.entryLabel ?? "Entry", 2),
      buildPriceLine(candleSeries, annotations?.secondaryEntryPrice, "#94a3b8", annotations?.secondaryEntryLabel ?? "Secondary", 1),
      buildPriceLine(candleSeries, annotations?.secondaryEntryLow, "rgba(148, 163, 184, 0.75)", "Secondary low", 1),
      buildPriceLine(candleSeries, annotations?.secondaryEntryHigh, "rgba(148, 163, 184, 0.75)", "Secondary high", 1),
      buildPriceLine(candleSeries, annotations?.stopPrice, "#fb7185", annotations?.stopLabel ?? "Stop", 2),
    ].filter(Boolean);

    const priceMarkers = [];
    if (annotations?.eventDate) {
      priceMarkers.push({
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
        priceMarkers.push({
          time: latestTime,
          position: "belowBar" as const,
          color: latestGap.direction === "up" ? "#86efac" : "#fca5a5",
          shape: "square" as const,
          text: latestGap.direction === "up" ? "Gap up" : "Gap down",
        });
      }
    }
    for (const marker of rsMarkers) {
      priceMarkers.push({
        time: marker.time,
        position: "belowBar" as const,
        color: marker.kind === "daily_new_high_before_price" ? "#bfdbfe" : "#60a5fa",
        shape: marker.kind === "daily_new_high_before_price" ? "circle" as const : "square" as const,
        text: marker.kind === "daily_new_high_before_price" ? "RS NH before price" : "RS NH",
      });
    }
    if (priceMarkers.length > 0) {
      candleSeries.setMarkers(priceMarkers);
    }

    if (rsMarkers.length > 0) {
      rsSeries.setMarkers(
        rsMarkers.map((marker) => ({
          time: marker.time,
          position: "aboveBar" as const,
          color: marker.kind === "daily_new_high_before_price" ? "#bfdbfe" : "#60a5fa",
          shape: marker.kind === "daily_new_high_before_price" ? "circle" as const : "square" as const,
          text: marker.kind === "daily_new_high_before_price" ? "Before price" : "New high",
        })),
      );
    }

    priceChart.timeScale().fitContent();
    rsChart.timeScale().fitContent();

    let syncingPriceToRs = false;
    let syncingRsToPrice = false;
    priceChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!range || syncingRsToPrice) {
        return;
      }
      syncingPriceToRs = true;
      rsChart.timeScale().setVisibleRange(range);
      syncingPriceToRs = false;
    });
    rsChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!range || syncingPriceToRs) {
        return;
      }
      syncingRsToPrice = true;
      priceChart.timeScale().setVisibleRange(range);
      syncingRsToPrice = false;
    });

    const resizeObserver = new ResizeObserver(() => {
      const width = priceRootRef.current?.clientWidth ?? 0;
      if (width > 0) {
        priceChart.applyOptions({ width });
        rsChart.applyOptions({ width });
      }
    });
    resizeObserver.observe(priceRootRef.current);

    return () => {
      resizeObserver.disconnect();
      void gapSeries;
      void priceLines;
      priceChart.remove();
      rsChart.remove();
    };
  }, [
    annotations,
    benchmarkTicker,
    candles,
    ema8,
    ema21,
    ipoVwap,
    ma20,
    ma50,
    ma200,
    rsLine,
    rsMarkers,
    ticker,
    visibleGapZones,
    weeklyEma8,
  ]);

  return (
    <div className="chart-stack">
      <div ref={priceRootRef} className="chart-card chart-card-price" />
      <div className="chart-rs-header">RS line vs {benchmarkTicker}</div>
      <div ref={rsRootRef} className="chart-card chart-card-rs" />
    </div>
  );
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

function buildExponentialMovingAverage(candles: CandlePoint[], span: number) {
  const points: { time: string; value: number }[] = [];
  if (candles.length === 0) {
    return points;
  }
  const alpha = 2 / (span + 1);
  let ema = candles[0].close;
  points.push({ time: candles[0].time, value: Number(ema.toFixed(2)) });
  for (let index = 1; index < candles.length; index += 1) {
    ema = candles[index].close * alpha + ema * (1 - alpha);
    points.push({ time: candles[index].time, value: Number(ema.toFixed(2)) });
  }
  return points;
}

function buildPriceLine(
  series: { createPriceLine: (options: { price: number; color: string; lineWidth: number; title: string }) => unknown },
  price: number | null | undefined,
  color: string,
  title: string,
  lineWidth: number,
) {
  if (price == null) {
    return null;
  }
  return series.createPriceLine({
    price,
    color,
    lineWidth,
    title,
  });
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
      endIndex = futureIndex;
    }

    zones.push({
      startIndex: index - 1,
      endIndex,
      remainingLowerPrice,
      remainingUpperPrice,
      direction,
      filled,
    });
  }

  return zones;
}
