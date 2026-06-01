import { ColorType, LineStyle, createChart, type IChartApi } from "lightweight-charts";
import { useEffect, useMemo, useRef } from "react";
import { createGapZonePrimitive } from "./GapZonePrimitive";
import { createHighTightFlagPrimitive } from "./HighTightFlagPrimitive";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";

export type ChartVisibility = {
  ema8: boolean;
  ema21: boolean;
  weeklyEma8: boolean;
  ipoVwap: boolean;
  maStack: boolean;
  gapZones: boolean;
  htfBox: boolean;
  rsLine: boolean;
  rsSignals: boolean;
};

type PriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays?: Pick<
    WatchlistChartResponse,
    "ma20" | "ma50" | "ma200" | "ema8" | "ema21" | "weekly_ema8" | "ipo_vwap" | "rs_line" | "rs_markers" | "benchmark_ticker" | "fearzone_panel"
  >;
  annotations?: ChartAnnotations;
  visibility?: ChartVisibility;
  forceFearzonePanel?: boolean;
};

type GapZone = {
  startIndex: number;
  endIndex: number;
  remainingLowerPrice: number;
  remainingUpperPrice: number;
  direction: "up" | "down";
  filled: boolean;
};

type HorizontalAnnotation = {
  color: string;
  lineWidth: 1 | 2 | 3 | 4;
  lineStyle?: LineStyle;
  label: string;
  price: number;
};

export function PriceChart({ ticker, candles, overlays, annotations, visibility, forceFearzonePanel = false }: PriceChartProps) {
  const priceRootRef = useRef<HTMLDivElement | null>(null);
  const rsRootRef = useRef<HTMLDivElement | null>(null);
  const priceChartApiRef = useRef<IChartApi | null>(null);
  const rsChartApiRef = useRef<IChartApi | null>(null);
  const options = visibility ?? {
    ema8: true,
    ema21: true,
    weeklyEma8: true,
    ipoVwap: true,
    maStack: true,
    gapZones: true,
    htfBox: true,
    rsLine: true,
    rsSignals: true,
  };

  const ma20 = useMemo(() => overlays?.ma20 ?? buildMovingAverage(candles, 20), [candles, overlays?.ma20]);
  const ma50 = useMemo(() => overlays?.ma50 ?? buildMovingAverage(candles, 50), [candles, overlays?.ma50]);
  const ma200 = useMemo(() => overlays?.ma200 ?? buildMovingAverage(candles, 200), [candles, overlays?.ma200]);
  const ema8 = useMemo(() => overlays?.ema8 ?? buildExponentialMovingAverage(candles, 8), [candles, overlays?.ema8]);
  const ema21 = useMemo(() => overlays?.ema21 ?? buildExponentialMovingAverage(candles, 21), [candles, overlays?.ema21]);
  const weeklyEma8 = useMemo(() => overlays?.weekly_ema8 ?? [], [overlays?.weekly_ema8]);
  const ipoVwap = useMemo(() => overlays?.ipo_vwap ?? [], [overlays?.ipo_vwap]);
  const rsLine = useMemo(() => overlays?.rs_line ?? [], [overlays?.rs_line]);
  const rsMarkers = useMemo(() => overlays?.rs_markers ?? [], [overlays?.rs_markers]);
  const fearzonePanel = useMemo(() => overlays?.fearzone_panel ?? { rows: [], signals: [] }, [overlays?.fearzone_panel]);
  const benchmarkTicker = overlays?.benchmark_ticker ?? "SPY";
  const showRsPane = options.rsLine && rsLine.length > 0;
  const showFearzonePanel = useMemo(() => {
    if (forceFearzonePanel) {
      return fearzonePanel.rows.length > 0;
    }
    const setupLabel = String(annotations?.setupLabel ?? "").toLowerCase();
    return setupLabel.includes("fearzone") && fearzonePanel.rows.length > 0;
  }, [annotations?.setupLabel, fearzonePanel.rows.length, forceFearzonePanel]);
  const visibleGapZones = useMemo(
    () => detectGapZones(candles).filter((zone) => zone.remainingUpperPrice > zone.remainingLowerPrice + 1e-6).slice(-4),
    [candles],
  );
  const highTightFlagBox = useMemo(() => detectHighTightFlagBox(candles, annotations), [candles, annotations]);
  const annotationLines = useMemo(() => buildHorizontalAnnotations(annotations), [annotations]);

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
    priceChartApiRef.current = priceChart;
    rsChartApiRef.current = rsChart;

    priceChart.timeScale().applyOptions({ visible: !showRsPane });

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

    const ema8Series = priceChart.addLineSeries({ color: "#38bdf8", lineWidth: 2, priceLineVisible: false });
    const ema21Series = priceChart.addLineSeries({ color: "#f59e0b", lineWidth: 2, priceLineVisible: false });
    const weeklyEma8Series = priceChart.addLineSeries({ color: "#4ade80", lineWidth: 2, priceLineVisible: false });
    const ipoVwapSeries = priceChart.addLineSeries({ color: "#f472b6", lineWidth: 2, priceLineVisible: false });
    const ma20Series = priceChart.addLineSeries({ color: "rgba(56, 189, 248, 0.35)", lineWidth: 1, priceLineVisible: false });
    const ma50Series = priceChart.addLineSeries({ color: "rgba(251, 146, 60, 0.45)", lineWidth: 1, priceLineVisible: false });
    const ma200Series = priceChart.addLineSeries({ color: "rgba(167, 139, 250, 0.52)", lineWidth: 1, priceLineVisible: false });
    const rsSeries = rsChart.addLineSeries({ color: "#60a5fa", lineWidth: 2, priceLineVisible: false });
    const annotationSeries = annotationLines.map((line) =>
      priceChart.addLineSeries({
        color: line.color,
        lineWidth: line.lineWidth,
        lineStyle: line.lineStyle,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      }),
    );

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
    ema8Series.setData(options.ema8 ? ema8 : []);
    ema21Series.setData(options.ema21 ? ema21 : []);
    weeklyEma8Series.setData(options.weeklyEma8 ? weeklyEma8 : []);
    ipoVwapSeries.setData(options.ipoVwap ? ipoVwap : []);
    ma20Series.setData(options.maStack ? ma20 : []);
    ma50Series.setData(options.maStack ? ma50 : []);
    ma200Series.setData(options.maStack ? ma200 : []);
    rsSeries.setData(showRsPane ? rsLine : []);
    if (candles.length > 0) {
      const startTime = candles[0].time;
      const endTime = candles[candles.length - 1].time;
      annotationSeries.forEach((series, index) => {
        const line = annotationLines[index];
        series.setData([
          { time: startTime, value: line.price },
          { time: endTime, value: line.price },
        ]);
      });
    }

    if (options.gapZones && visibleGapZones.length > 0) {
      const gapPrimitive = createGapZonePrimitive(
        visibleGapZones
          .map((zone) => {
            const startTime = candles[zone.startIndex]?.time;
            const endTime = candles[Math.min(zone.endIndex, candles.length - 1)]?.time;
            if (!startTime || !endTime) {
              return null;
            }
            return {
              startTime,
              endTime,
              lowerPrice: zone.remainingLowerPrice,
              upperPrice: zone.remainingUpperPrice,
              direction: zone.direction,
            };
          })
          .filter((zone): zone is NonNullable<typeof zone> => zone !== null),
      );
      (candleSeries as any).attachPrimitive?.(gapPrimitive);
    }

    if (options.htfBox && highTightFlagBox) {
      const htfPrimitive = createHighTightFlagPrimitive(highTightFlagBox);
      (candleSeries as any).attachPrimitive?.(htfPrimitive);
    }

    const priceMarkers = [];
    if (annotations?.eventDate) {
      priceMarkers.push({
        time: annotations.eventDate,
        position: "aboveBar" as const,
        color: "#fbbf24",
        shape: "circle" as const,
      });
    }
    if (options.rsSignals) {
      for (const marker of rsMarkers) {
        priceMarkers.push({
          time: marker.time,
          position: "belowBar" as const,
          color: marker.kind === "daily_new_high_before_price" ? "#bfdbfe" : "#60a5fa",
          shape: marker.kind === "daily_new_high_before_price" ? "circle" as const : "square" as const,
        });
      }
    }
    if (priceMarkers.length > 0) {
      candleSeries.setMarkers(priceMarkers);
    }

    annotationSeries.forEach((series, index) => {
      const line = annotationLines[index];
      series.createPriceLine({
        price: line.price,
        color: line.color,
        lineWidth: line.lineWidth,
        lineStyle: line.lineStyle,
        axisLabelVisible: true,
        title: line.label,
      });
    });

    if (options.rsSignals && rsMarkers.length > 0) {
      rsSeries.setMarkers(
        rsMarkers.map((marker) => ({
          time: marker.time,
          position: "aboveBar" as const,
          color: marker.kind === "daily_new_high_before_price" ? "#bfdbfe" : "#60a5fa",
          shape: marker.kind === "daily_new_high_before_price" ? "circle" as const : "square" as const,
        })),
      );
    }

    priceChart.timeScale().fitContent();
    if (showRsPane) {
      rsChart.timeScale().fitContent();
    }
    rsChart.timeScale().applyOptions({ visible: showRsPane });
    rsChart.applyOptions({ handleScroll: showRsPane, handleScale: showRsPane });
    if (!showRsPane && rsRootRef.current) {
      rsRootRef.current.style.display = "none";
    } else if (rsRootRef.current) {
      rsRootRef.current.style.display = "";
    }

    let syncingPriceToRs = false;
    let syncingRsToPrice = false;
    priceChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!showRsPane || !range || syncingRsToPrice) {
        return;
      }
      syncingPriceToRs = true;
      rsChart.timeScale().setVisibleRange(range);
      syncingPriceToRs = false;
    });
    rsChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!showRsPane || !range || syncingPriceToRs) {
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
      priceChartApiRef.current = null;
      rsChartApiRef.current = null;
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
    options.ema8,
    options.ema21,
    options.weeklyEma8,
    options.ipoVwap,
    options.maStack,
    options.gapZones,
    options.htfBox,
    options.rsSignals,
    showRsPane,
    visibleGapZones,
    highTightFlagBox,
    annotationLines,
    weeklyEma8,
  ]);

  const handleZoom = (direction: "in" | "out" | "reset") => {
    const priceChart = priceChartApiRef.current;
    if (!priceChart || candles.length === 0) {
      return;
    }
    if (direction === "reset") {
      priceChart.timeScale().fitContent();
      if (showRsPane && rsChartApiRef.current) {
        rsChartApiRef.current.timeScale().fitContent();
      }
      return;
    }
    const logicalRange = priceChart.timeScale().getVisibleLogicalRange();
    if (!logicalRange) {
      priceChart.timeScale().fitContent();
      return;
    }
    const currentBars = logicalRange.to - logicalRange.from;
    const center = (logicalRange.from + logicalRange.to) / 2;
    const nextBars = direction === "in" ? Math.max(20, currentBars * 0.75) : Math.min(candles.length + 20, currentBars * 1.35);
    priceChart.timeScale().setVisibleLogicalRange({
      from: center - nextBars / 2,
      to: center + nextBars / 2,
    });
  };

  return (
    <div className="chart-stack">
      <div className="chart-zoom-row">
        <button className="chart-zoom-button" type="button" onClick={() => handleZoom("out")}>
          -
        </button>
        <button className="chart-zoom-button" type="button" onClick={() => handleZoom("reset")}>
          Reset
        </button>
        <button className="chart-zoom-button" type="button" onClick={() => handleZoom("in")}>
          +
        </button>
      </div>
      <div ref={priceRootRef} className="chart-card chart-card-price" />
      {showRsPane ? <div className="chart-rs-header">RS line vs {benchmarkTicker}</div> : null}
      <div ref={rsRootRef} className="chart-card chart-card-rs" />
      {showFearzonePanel ? <FearzonePanel panel={fearzonePanel} /> : null}
    </div>
  );
}

function FearzonePanel({
  panel,
}: {
  panel: WatchlistChartResponse["fearzone_panel"];
}) {
  const width = 1080;
  const labelWidth = 96;
  const rowHeight = 24;
  const topPadding = 20;
  const bottomPadding = 26;
  const rows = panel.rows;
  const pointCount = rows[0]?.points.length ?? 0;
  const innerWidth = Math.max(1, width - labelWidth - 12);
  const step = pointCount > 0 ? innerWidth / pointCount : innerWidth;
  const height = topPadding + rows.length * rowHeight + bottomPadding;
  const signalTimes = new Set(panel.signals.map((item) => item.time));
  const dateMarkers = buildFearzoneDateMarkers(rows[0]?.points ?? [], labelWidth, step);

  return (
    <div
      className="chart-card"
      style={{
        background: "#111114",
        padding: "10px 12px 12px",
      }}
    >
      <div style={{ color: "#d4d4d8", fontSize: 12, marginBottom: 8 }}>Fearzone Panel</div>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height: 178, display: "block" }} preserveAspectRatio="none">
        <rect x="0" y="0" width={width} height={height} fill="#111114" rx="10" />
        {rows.map((row, rowIndex) => {
          const y = topPadding + rowIndex * rowHeight;
          return (
            <g key={row.key}>
              <text x={labelWidth - 8} y={y + 15} fill="#d4d4d8" fontSize="12" textAnchor="end">
                {row.label}
              </text>
              {row.points.map((point, pointIndex) => {
                const x = labelWidth + pointIndex * step;
                return (
                  <rect
                    key={`${row.key}-${point.time}`}
                    x={x}
                    y={y}
                    width={Math.max(1, step - 0.6)}
                    height={18}
                    fill={point.active ? row.active_color : row.inactive_color}
                    opacity={point.active ? 0.92 : 0.68}
                    rx={1.8}
                  />
                );
              })}
            </g>
          );
        })}
        {rows[0]?.points.map((point, pointIndex) => {
          if (!signalTimes.has(point.time)) {
            return null;
          }
          const x = labelWidth + pointIndex * step + Math.max(0.5, step / 2);
          return <line key={`signal-${point.time}`} x1={x} y1={8} x2={x} y2={height - 6} stroke="#fb7185" strokeWidth="1.5" strokeDasharray="3 4" opacity="0.9" />;
        })}
        {dateMarkers.map((marker) => (
          <g key={`date-${marker.time}`}>
            <line
              x1={marker.x}
              y1={topPadding + rows.length * rowHeight}
              x2={marker.x}
              y2={topPadding + rows.length * rowHeight + 5}
              stroke="#a1a1aa"
              strokeWidth="1"
            />
            <text x={marker.x} y={height - 6} fill="#a1a1aa" fontSize="11" textAnchor="middle">
              {marker.label}
            </text>
          </g>
        ))}
      </svg>
    </div>
  );
}

function buildFearzoneDateMarkers(points: Array<{ time: string; active: boolean }>, labelWidth: number, step: number) {
  if (points.length === 0) {
    return [];
  }
  const targetCount = Math.min(6, points.length);
  const markers: Array<{ time: string; label: string; x: number }> = [];
  for (let markerIndex = 0; markerIndex < targetCount; markerIndex += 1) {
    const pointIndex = targetCount === 1 ? points.length - 1 : Math.round((markerIndex * (points.length - 1)) / (targetCount - 1));
    const point = points[pointIndex];
    if (!point) {
      continue;
    }
    markers.push({
      time: point.time,
      label: formatFearzoneDate(point.time),
      x: labelWidth + pointIndex * step + Math.max(0.5, step / 2),
    });
  }
  return markers.filter((marker, index, source) => source.findIndex((item) => item.time === marker.time) === index);
}

function formatFearzoneDate(value: string) {
  const [year, month, day] = value.split("-");
  if (!year || !month || !day) {
    return value;
  }
  return `${year.slice(2)}-${month}-${day}`;
}

function detectHighTightFlagBox(candles: CandlePoint[], annotations?: ChartAnnotations) {
  if (candles.length < 5) {
    return null;
  }

  const setupLabel = String(annotations?.setupLabel ?? "").toLowerCase();
  const looksLikeHighTightFlag =
    setupLabel.includes("htf") ||
    setupLabel.includes("high tight flag") ||
    setupLabel.includes("tight flag");
  if (!looksLikeHighTightFlag) {
    return null;
  }

  const anchorIndex =
    annotations?.eventDate != null
      ? candles.findIndex((candle) => candle.time >= annotations.eventDate!)
      : Math.max(0, candles.length - 15);
  const startIndex = anchorIndex >= 0 ? anchorIndex : Math.max(0, candles.length - 15);
  const window = candles.slice(startIndex);
  if (window.length < 3) {
    return null;
  }

  const upperFromWindow = Math.max(...window.map((candle) => candle.high));
  const lowerFromWindow = Math.min(...window.map((candle) => candle.low));
  const upperPrice = Math.max(upperFromWindow, annotations?.triggerPrice ?? Number.NEGATIVE_INFINITY);
  const lowerPrice = lowerFromWindow;
  if (!Number.isFinite(upperPrice) || !Number.isFinite(lowerPrice) || upperPrice <= lowerPrice) {
    return null;
  }

  const pullbackPct = ((upperPrice - lowerPrice) / upperPrice) * 100;
  if (pullbackPct > 35) {
    return null;
  }

  return {
    startTime: window[0].time,
    endTime: window[window.length - 1].time,
    lowerPrice: Number(lowerPrice.toFixed(4)),
    upperPrice: Number(upperPrice.toFixed(4)),
  };
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

function buildHorizontalAnnotations(annotations?: ChartAnnotations): HorizontalAnnotation[] {
  if (!annotations) {
    return [];
  }
  const lines: HorizontalAnnotation[] = [];
  const addLine = (
    price: number | null | undefined,
    label: string,
    color: string,
    lineWidth: 1 | 2 | 3 | 4,
    lineStyle?: LineStyle,
  ) => {
    if (price == null || !Number.isFinite(price)) {
      return;
    }
    lines.push({ price, label, color, lineWidth, lineStyle });
  };

  addLine(annotations.triggerPrice, annotations.triggerLabel ?? "Trigger", "#eab308", 2, LineStyle.Dashed);
  addLine(annotations.entryPrice, annotations.entryLabel ?? "Entry", "#22c55e", 2, LineStyle.Solid);
  if (
    annotations.secondaryEntryLow != null &&
    annotations.secondaryEntryHigh != null &&
    Number.isFinite(annotations.secondaryEntryLow) &&
    Number.isFinite(annotations.secondaryEntryHigh)
  ) {
    addLine(annotations.secondaryEntryLow, `${annotations.secondaryEntryLabel ?? "Secondary"} low`, "#94a3b8", 1, LineStyle.LargeDashed);
    addLine(annotations.secondaryEntryHigh, `${annotations.secondaryEntryLabel ?? "Secondary"} high`, "#94a3b8", 1, LineStyle.LargeDashed);
  } else {
    addLine(annotations.secondaryEntryPrice, annotations.secondaryEntryLabel ?? "Secondary", "#94a3b8", 1, LineStyle.LargeDashed);
  }
  addLine(annotations.stopPrice, annotations.stopLabel ?? "Stop", "#ef4444", 2, LineStyle.Dotted);
  return lines;
}
