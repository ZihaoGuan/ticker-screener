import { ColorType, LineStyle, createChart, type IChartApi } from "lightweight-charts";
import { useEffect, useMemo, useRef, useState } from "react";
import { createGapZonePrimitive } from "./GapZonePrimitive";
import { createHighTightFlagPrimitive } from "./HighTightFlagPrimitive";
import type { CandlePoint, ChartAnnotations, WatchlistChartResponse } from "../lib/types";

const EMPTY_ANNOTATIONS: ChartAnnotations[] = [];
const EMPTY_MARKERS: Array<{ time: string; label?: string; color: string; shape: "circle" | "square"; position: "aboveBar" | "belowBar" }> = [];

export type ChartVisibility = {
  ema8: boolean;
  ema21: boolean;
  sma50: boolean;
  sma200: boolean;
  weeklyEma8: boolean;
  ipoVwap: boolean;
  marketExtension: boolean;
  fibOverlay: boolean;
  gapZones: boolean;
  htfBox: boolean;
  rsLine: boolean;
  rsSignals: boolean;
  sellSignals: boolean;
  wyckoffSignals: boolean;
  wyckoffHoldSignals: boolean;
  flexSr: boolean;
};

type PriceChartProps = {
  ticker: string;
  candles: CandlePoint[];
  overlays?: Pick<
    WatchlistChartResponse,
    "ma20" | "ma50" | "ma200" | "ema8" | "ema21" | "weekly_ema8" | "ipo_vwap" | "market_extension" | "rs_line" | "rs_markers" | "setup_markers" | "benchmark_ticker" | "fearzone_panel"
  >;
  annotations?: ChartAnnotations;
  extraAnnotations?: ChartAnnotations[];
  extraMarkers?: Array<{ time: string; label?: string; color: string; shape: "circle" | "square"; position: "aboveBar" | "belowBar" }>;
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

type SrAnchor = {
  index: number;
  time: string;
  value: number;
  kind: "high" | "low";
  score: number;
};

type FlexibleSrCurve = {
  anchors: SrAnchor[];
  backfit: Array<{ time: string; value: number }>;
  projection: Array<{ time: string; value: number }>;
};

type FlexibleSrOverlay = {
  resistance: FlexibleSrCurve | null;
  support: FlexibleSrCurve | null;
};

type FibLevel = {
  label: string;
  value: number;
  color: string;
  lineWidth: 1 | 2 | 3 | 4;
  lineStyle: LineStyle;
};

type FibLevelTemplate = {
  ratio: number;
  label: string;
  color: string;
  lineWidth: 1 | 2 | 3 | 4;
  lineStyle: LineStyle;
};

type StructuralFibCandidate = {
  trend: "bullish" | "bearish";
  anchorStart: SrAnchor;
  anchorEnd: SrAnchor;
  bosAnchor: SrAnchor;
  atr: number;
};

type StructuralFibOverlay = {
  trend: "bullish" | "bearish";
  anchorStart: SrAnchor;
  anchorEnd: SrAnchor;
  levels: FibLevel[];
  impulseLine: Array<{ time: string; value: number }>;
  markers: Array<{ time: string; label?: string; color: string; shape: "circle" | "square"; position: "aboveBar" | "belowBar" }>;
};

export function PriceChart({ ticker, candles, overlays, annotations, extraAnnotations, extraMarkers, visibility, forceFearzonePanel = false }: PriceChartProps) {
  const priceRootRef = useRef<HTMLDivElement | null>(null);
  const rsRootRef = useRef<HTMLDivElement | null>(null);
  const fibRootRef = useRef<HTMLDivElement | null>(null);
  const priceChartApiRef = useRef<IChartApi | null>(null);
  const rsChartApiRef = useRef<IChartApi | null>(null);
  const fibChartApiRef = useRef<IChartApi | null>(null);
  const [visibleIndexRange, setVisibleIndexRange] = useState<{ from: number; to: number } | null>(null);
  const [hoverGuide, setHoverGuide] = useState<{ time: string; xRatio: number } | null>(null);
  const resolvedExtraAnnotations = extraAnnotations ?? EMPTY_ANNOTATIONS;
  const resolvedExtraMarkers = extraMarkers ?? EMPTY_MARKERS;
  const options = visibility ?? {
    ema8: true,
    ema21: true,
    sma50: true,
    sma200: true,
    weeklyEma8: true,
    ipoVwap: true,
    marketExtension: true,
    fibOverlay: false,
    gapZones: true,
    htfBox: true,
    rsLine: true,
    rsSignals: true,
    sellSignals: true,
    wyckoffSignals: true,
    wyckoffHoldSignals: true,
    flexSr: false,
  };

  const ma50 = useMemo(() => (overlays?.ma50?.length ? overlays.ma50 : buildMovingAverage(candles, 50)), [candles, overlays?.ma50]);
  const ma200 = useMemo(() => (overlays?.ma200?.length ? overlays.ma200 : buildMovingAverage(candles, 200)), [candles, overlays?.ma200]);
  const ema8 = useMemo(() => overlays?.ema8 ?? buildExponentialMovingAverage(candles, 8), [candles, overlays?.ema8]);
  const ema21 = useMemo(() => overlays?.ema21 ?? buildExponentialMovingAverage(candles, 21), [candles, overlays?.ema21]);
  const weeklyEma8 = useMemo(() => overlays?.weekly_ema8 ?? [], [overlays?.weekly_ema8]);
  const ipoVwap = useMemo(() => overlays?.ipo_vwap ?? [], [overlays?.ipo_vwap]);
  const marketExtension = useMemo(
    () =>
      overlays?.market_extension ?? {
        config: { timeframe: "weekly" as const, ma_type: "sma" as const, length: 10, warning_pct: 11, extreme_pct: 15, label: "10W SMA" },
        line: [],
        signals: [],
        latest: null,
      },
    [overlays?.market_extension],
  );
  const rsLine = useMemo(() => overlays?.rs_line ?? [], [overlays?.rs_line]);
  const rsMarkers = useMemo(() => overlays?.rs_markers ?? [], [overlays?.rs_markers]);
  const fearzonePanel = useMemo(() => overlays?.fearzone_panel ?? { rows: [], signals: [] }, [overlays?.fearzone_panel]);
  const benchmarkTicker = overlays?.benchmark_ticker ?? "SPY";
  const fibOverlay = useMemo(() => (options.fibOverlay ? buildStructuralFibOverlay(candles) : null), [candles, options.fibOverlay]);
  const showRsPane = options.rsLine && rsLine.length > 0;
  const showFibPane = options.fibOverlay && fibOverlay !== null;
  const showFearzonePanel = useMemo(() => forceFearzonePanel || fearzonePanel.rows.length > 0, [fearzonePanel.rows.length, forceFearzonePanel]);
  const visibleGapZones = useMemo(
    () => detectGapZones(candles).filter((zone) => zone.remainingUpperPrice > zone.remainingLowerPrice + 1e-6).slice(-4),
    [candles],
  );
  const highTightFlagBox = useMemo(() => detectHighTightFlagBox(candles, annotations, resolvedExtraAnnotations), [candles, annotations, resolvedExtraAnnotations]);
  const annotationLines = useMemo(() => buildHorizontalAnnotations(annotations, resolvedExtraAnnotations), [annotations, resolvedExtraAnnotations]);
  const flexibleSrOverlay = useMemo(() => (options.flexSr ? buildFlexibleSrOverlay(candles) : null), [candles, options.flexSr]);
  const updateHoverGuideFromSurface = (param: { point: { x: number; y: number } | undefined; time: unknown }, width: number) => {
    const point = param.point;
    const normalizedTime = normalizeCrosshairTime(param.time);
    if (
      !point ||
      width <= 0 ||
      !normalizedTime ||
      point.x < 0 ||
      point.x > width ||
      point.y < 0
    ) {
      setHoverGuide(null);
      return;
    }
    const xRatio = Math.max(0, Math.min(1, point.x / width));
    setHoverGuide((current) => {
      if (current && current.time === normalizedTime && Math.abs(current.xRatio - xRatio) < 0.0005) {
        return current;
      }
      return { time: normalizedTime, xRatio };
    });
  };

  useEffect(() => {
    if (!priceRootRef.current || !rsRootRef.current || !fibRootRef.current) {
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
    const fibChart = createChart(fibRootRef.current, {
      height: 228,
      layout: {
        background: { type: ColorType.Solid, color: "#101015" },
        textColor: "#e4e4e7",
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
    fibChartApiRef.current = fibChart;

    priceChart.timeScale().applyOptions({ visible: !showRsPane && !showFibPane });

    const candleSeries = priceChart.addCandlestickSeries({
      upColor: "#10b981",
      downColor: "#f43f5e",
      wickUpColor: "#10b981",
      wickDownColor: "#f43f5e",
      borderVisible: false,
    });
    const fibCandleSeries = fibChart.addCandlestickSeries({
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
    const marketExtensionSeries = priceChart.addLineSeries({ color: "rgba(96, 165, 250, 0.95)", lineWidth: 2, lineStyle: LineStyle.Dashed, priceLineVisible: false });
    const ma50Series = priceChart.addLineSeries({ color: "rgba(251, 146, 60, 0.45)", lineWidth: 1, priceLineVisible: false });
    const ma200Series = priceChart.addLineSeries({ color: "rgba(167, 139, 250, 0.52)", lineWidth: 1, priceLineVisible: false });
    const fibImpulseSeries = fibChart.addLineSeries({
      color: "rgba(248, 250, 252, 0.75)",
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
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
    const flexResistanceSeries = priceChart.addLineSeries({
      color: "#f87171",
      lineWidth: 3,
      lineStyle: LineStyle.Solid,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    const flexResistanceProjectionSeries = priceChart.addLineSeries({
      color: "rgba(248, 113, 113, 0.92)",
      lineWidth: 2,
      lineStyle: LineStyle.Dotted,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    const flexSupportSeries = priceChart.addLineSeries({
      color: "#4ade80",
      lineWidth: 3,
      lineStyle: LineStyle.Solid,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    const flexSupportProjectionSeries = priceChart.addLineSeries({
      color: "rgba(74, 222, 128, 0.92)",
      lineWidth: 2,
      lineStyle: LineStyle.Dotted,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
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
    fibCandleSeries.setData(
      showFibPane
        ? candles.map((item) => ({
            time: item.time,
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
          }))
        : [],
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
    marketExtensionSeries.setData(options.marketExtension ? marketExtension.line : []);
    ma50Series.setData(options.sma50 ? ma50 : []);
    ma200Series.setData(options.sma200 ? ma200 : []);
    fibImpulseSeries.setData(showFibPane ? fibOverlay?.impulseLine ?? [] : []);
    rsSeries.setData(showRsPane ? rsLine : []);
    flexResistanceSeries.setData(options.flexSr ? flexibleSrOverlay?.resistance?.backfit ?? [] : []);
    flexResistanceProjectionSeries.setData(options.flexSr ? flexibleSrOverlay?.resistance?.projection ?? [] : []);
    flexSupportSeries.setData(options.flexSr ? flexibleSrOverlay?.support?.backfit ?? [] : []);
    flexSupportProjectionSeries.setData(options.flexSr ? flexibleSrOverlay?.support?.projection ?? [] : []);
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
    const fibPaneMarkers: Array<{ time: string; position: "aboveBar" | "belowBar"; color: string; shape: "circle" | "square" }> = [];
    const eventAnnotations = [annotations, ...resolvedExtraAnnotations].filter((item): item is ChartAnnotations => Boolean(item?.eventDate));
    for (const eventAnnotation of eventAnnotations) {
      if (!eventAnnotation.eventDate) {
        continue;
      }
      priceMarkers.push({
        time: eventAnnotation.eventDate,
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
    for (const marker of resolvedExtraMarkers) {
      priceMarkers.push({
        time: marker.time,
        position: marker.position,
        color: marker.color,
        shape: marker.shape,
      });
    }
    if (options.marketExtension) {
      for (const marker of marketExtension.signals) {
        priceMarkers.push({
          time: marker.time,
          position: "aboveBar" as const,
          color: marker.state === "extreme" ? "#ef4444" : "#f59e0b",
          shape: marker.state === "extreme" ? "square" as const : "circle" as const,
        });
      }
    }
    if (showFibPane) {
      for (const marker of fibOverlay?.markers ?? []) {
        fibPaneMarkers.push({
          time: marker.time,
          position: marker.position,
          color: marker.color,
          shape: marker.shape,
        });
      }
    }
    if (options.flexSr) {
      for (const anchor of flexibleSrOverlay?.resistance?.anchors ?? []) {
        priceMarkers.push({
          time: anchor.time,
          position: "aboveBar" as const,
          color: "#f87171",
          shape: "circle" as const,
        });
      }
      for (const anchor of flexibleSrOverlay?.support?.anchors ?? []) {
        priceMarkers.push({
          time: anchor.time,
          position: "belowBar" as const,
          color: "#4ade80",
          shape: "circle" as const,
        });
      }
    }
    if (priceMarkers.length > 0) {
      candleSeries.setMarkers(priceMarkers);
    }
    if (showFibPane && fibPaneMarkers.length > 0) {
      fibCandleSeries.setMarkers(fibPaneMarkers);
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
    if (showFibPane) {
      for (const level of fibOverlay?.levels ?? []) {
        fibImpulseSeries.createPriceLine({
          price: level.value,
          color: level.color,
          lineWidth: level.lineWidth,
          lineStyle: level.lineStyle,
          axisLabelVisible: true,
          title: level.label,
        });
      }
    }

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
    if (showFibPane) {
      fibChart.timeScale().fitContent();
    }
    rsChart.timeScale().applyOptions({ visible: showRsPane });
    fibChart.timeScale().applyOptions({ visible: showFibPane });
    rsChart.applyOptions({ handleScroll: showRsPane, handleScale: showRsPane });
    fibChart.applyOptions({ handleScroll: showFibPane, handleScale: showFibPane });
    if (!showRsPane && rsRootRef.current) {
      rsRootRef.current.style.display = "none";
    } else if (rsRootRef.current) {
      rsRootRef.current.style.display = "";
    }
    if (!showFibPane && fibRootRef.current) {
      fibRootRef.current.style.display = "none";
    } else if (fibRootRef.current) {
      fibRootRef.current.style.display = "";
    }

    let syncingPriceToRs = false;
    let syncingRsToPrice = false;
    let syncingPriceToFib = false;
    let syncingFibToPrice = false;
    const syncVisibleIndexRange = (from: number, to: number) => {
      if (candles.length === 0) {
        return;
      }
      const nextFrom = Math.max(0, Math.floor(from));
      const nextTo = Math.min(candles.length - 1, Math.ceil(to));
      setVisibleIndexRange((current) => {
        if (current && current.from === nextFrom && current.to === nextTo) {
          return current;
        }
        return { from: nextFrom, to: nextTo };
      });
    };
    const priceLogicalRange = priceChart.timeScale().getVisibleLogicalRange();
    if (priceLogicalRange) {
      syncVisibleIndexRange(priceLogicalRange.from, priceLogicalRange.to);
    } else if (candles.length > 0) {
      syncVisibleIndexRange(0, candles.length - 1);
    }
    priceChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (showRsPane && range && !syncingRsToPrice) {
        syncingPriceToRs = true;
        rsChart.timeScale().setVisibleRange(range);
        syncingPriceToRs = false;
      }
      if (showFibPane && range && !syncingFibToPrice) {
        syncingPriceToFib = true;
        fibChart.timeScale().setVisibleRange(range);
        syncingPriceToFib = false;
      }
    });
    priceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
      if (!range) {
        return;
      }
      syncVisibleIndexRange(range.from, range.to);
    });
    priceChart.subscribeCrosshairMove((param) => {
      const width = priceRootRef.current?.clientWidth ?? 0;
      updateHoverGuideFromSurface({ point: param.point, time: param.time }, width);
    });
    rsChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!showRsPane || !range || syncingPriceToRs) {
        return;
      }
      syncingRsToPrice = true;
      priceChart.timeScale().setVisibleRange(range);
      syncingRsToPrice = false;
    });
    fibChart.timeScale().subscribeVisibleTimeRangeChange((range) => {
      if (!showFibPane || !range || syncingPriceToFib) {
        return;
      }
      syncingFibToPrice = true;
      priceChart.timeScale().setVisibleRange(range);
      syncingFibToPrice = false;
    });
    rsChart.subscribeCrosshairMove((param) => {
      if (!showRsPane) {
        return;
      }
      const width = rsRootRef.current?.clientWidth ?? 0;
      updateHoverGuideFromSurface({ point: param.point, time: param.time }, width);
    });
    fibChart.subscribeCrosshairMove((param) => {
      if (!showFibPane) {
        return;
      }
      const width = fibRootRef.current?.clientWidth ?? 0;
      updateHoverGuideFromSurface({ point: param.point, time: param.time }, width);
    });

    const resizeObserver = new ResizeObserver(() => {
      const width = priceRootRef.current?.clientWidth ?? 0;
      if (width > 0) {
        priceChart.applyOptions({ width });
        rsChart.applyOptions({ width });
        fibChart.applyOptions({ width });
      }
    });
    resizeObserver.observe(priceRootRef.current);

    return () => {
      resizeObserver.disconnect();
      priceChartApiRef.current = null;
      rsChartApiRef.current = null;
      fibChartApiRef.current = null;
      priceChart.remove();
      rsChart.remove();
      fibChart.remove();
    };
  }, [
    annotations,
    benchmarkTicker,
    candles,
    ema8,
    ema21,
    ipoVwap,
    marketExtension,
    ma50,
    ma200,
    fibOverlay,
    rsLine,
    rsMarkers,
    ticker,
    options.ema8,
    options.ema21,
    options.sma50,
    options.sma200,
    options.weeklyEma8,
    options.ipoVwap,
    options.marketExtension,
    options.fibOverlay,
    options.gapZones,
    options.htfBox,
    options.rsSignals,
    showRsPane,
    visibleGapZones,
    highTightFlagBox,
    annotationLines,
    resolvedExtraAnnotations,
    resolvedExtraMarkers,
    weeklyEma8,
    options.flexSr,
    flexibleSrOverlay,
    showFibPane,
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
      if (showFibPane && fibChartApiRef.current) {
        fibChartApiRef.current.timeScale().fitContent();
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
      <div className="chart-pane">
        <div ref={priceRootRef} className="chart-card chart-card-price" />
        {hoverGuide ? <div className="chart-hover-guide" style={{ left: `${hoverGuide.xRatio * 100}%` }} /> : null}
      </div>
      {showFibPane ? <div className="chart-rs-header">Fib structure pane</div> : null}
      <div className="chart-pane">
        <div ref={fibRootRef} className="chart-card chart-card-rs" />
        {showFibPane && hoverGuide ? <div className="chart-hover-guide" style={{ left: `${hoverGuide.xRatio * 100}%` }} /> : null}
      </div>
      {showRsPane ? <div className="chart-rs-header">RS line vs {benchmarkTicker}</div> : null}
      <div className="chart-pane">
        <div ref={rsRootRef} className="chart-card chart-card-rs" />
        {showRsPane && hoverGuide ? <div className="chart-hover-guide" style={{ left: `${hoverGuide.xRatio * 100}%` }} /> : null}
      </div>
      {showFearzonePanel ? (
        <FearzonePanel
          panel={fearzonePanel}
          visibleIndexRange={visibleIndexRange}
          hoveredTime={hoverGuide?.time ?? null}
          onHoverTime={(time, xRatio) => {
            if (!time || xRatio == null) {
              setHoverGuide((current) => (current == null ? current : null));
              return;
            }
            setHoverGuide((current) => {
              if (current && current.time === time && Math.abs(current.xRatio - xRatio) < 0.0005) {
                return current;
              }
              return { time, xRatio };
            });
          }}
        />
      ) : null}
    </div>
  );
}

function FearzonePanel({
  panel,
  visibleIndexRange,
  hoveredTime,
  onHoverTime,
}: {
  panel: WatchlistChartResponse["fearzone_panel"];
  visibleIndexRange: { from: number; to: number } | null;
  hoveredTime: string | null;
  onHoverTime: (time: string | null, xRatio: number | null) => void;
}) {
  const width = 1080;
  const labelWidth = 96;
  const rowHeight = 24;
  const topPadding = 20;
  const bottomPadding = 26;
  const rows = useMemo(() => {
    const range = visibleIndexRange;
    if (!range) {
      return panel.rows;
    }
    return panel.rows.map((row) => ({
      ...row,
      points: row.points.slice(range.from, range.to + 1),
    }));
  }, [panel.rows, visibleIndexRange]);
  const pointCount = rows[0]?.points.length ?? 0;
  const innerWidth = Math.max(1, width - labelWidth - 12);
  const step = pointCount > 0 ? innerWidth / pointCount : innerWidth;
  const height = topPadding + rows.length * rowHeight + bottomPadding;
  const signalTimes = new Set(panel.signals.map((item) => item.time));
  const dateMarkers = buildFearzoneDateMarkers(rows[0]?.points ?? [], labelWidth, step);
  const hoverPointIndex = hoveredTime ? rows[0]?.points.findIndex((point) => point.time === hoveredTime) ?? -1 : -1;
  const hoverX = hoverPointIndex >= 0 ? labelWidth + hoverPointIndex * step + Math.max(0.5, step / 2) : null;

  return (
    <div
      className="chart-card"
      style={{
        background: "#111114",
        padding: "10px 12px 12px",
      }}
    >
      <div style={{ color: "#d4d4d8", fontSize: 12, marginBottom: 8 }}>Fearzone Panel</div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        style={{ width: "100%", height: 178, display: "block" }}
        preserveAspectRatio="none"
        onMouseMove={(event) => {
          if (pointCount === 0) {
            onHoverTime(null, null);
            return;
          }
          const bounds = event.currentTarget.getBoundingClientRect();
          if (bounds.width <= 0) {
            onHoverTime(null, null);
            return;
          }
          const rawX = ((event.clientX - bounds.left) / bounds.width) * width;
          const translatedX = rawX - labelWidth;
          if (translatedX < 0) {
            onHoverTime(null, null);
            return;
          }
          const pointIndex = Math.max(0, Math.min(pointCount - 1, Math.floor(translatedX / Math.max(step, 1e-6))));
          const point = rows[0]?.points[pointIndex];
          if (!point) {
            onHoverTime(null, null);
            return;
          }
          const xRatio = Math.max(0, Math.min(1, (labelWidth + pointIndex * step + Math.max(0.5, step / 2)) / width));
          onHoverTime(point.time, xRatio);
        }}
        onMouseLeave={() => onHoverTime(null, null)}
      >
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
        {hoverX != null ? (
          <line
            x1={hoverX}
            y1={8}
            x2={hoverX}
            y2={height - 6}
            stroke="#60a5fa"
            strokeWidth="1"
            strokeDasharray="4 4"
            opacity="0.95"
          />
        ) : null}
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

function normalizeCrosshairTime(value: unknown): string | null {
  if (typeof value === "string" && value) {
    return value;
  }
  if (value && typeof value === "object") {
    const candidate = value as { year?: number; month?: number; day?: number };
    if (
      typeof candidate.year === "number" &&
      typeof candidate.month === "number" &&
      typeof candidate.day === "number"
    ) {
      return `${String(candidate.year).padStart(4, "0")}-${String(candidate.month).padStart(2, "0")}-${String(candidate.day).padStart(2, "0")}`;
    }
  }
  return null;
}

function detectHighTightFlagBox(candles: CandlePoint[], annotations?: ChartAnnotations, extraAnnotations: ChartAnnotations[] = []) {
  if (candles.length < 5) {
    return null;
  }

  const htfAnnotation =
    [annotations, ...extraAnnotations].find((item) => {
      const setupLabel = String(item?.setupLabel ?? "").toLowerCase();
      return setupLabel.includes("htf") || setupLabel.includes("high tight flag") || setupLabel.includes("tight flag");
    }) ?? null;
  if (!htfAnnotation) {
    return null;
  }
  const htfEventDate = htfAnnotation.eventDate ?? null;

  const anchorIndex =
    htfEventDate
      ? candles.findIndex((candle) => candle.time >= htfEventDate)
      : Math.max(0, candles.length - 15);
  const startIndex = anchorIndex >= 0 ? anchorIndex : Math.max(0, candles.length - 15);
  const window = candles.slice(startIndex);
  if (window.length < 3) {
    return null;
  }

  const upperFromWindow = Math.max(...window.map((candle) => candle.high));
  const lowerFromWindow = Math.min(...window.map((candle) => candle.low));
  const upperPrice = Math.max(upperFromWindow, htfAnnotation.triggerPrice ?? Number.NEGATIVE_INFINITY);
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

function buildHorizontalAnnotations(annotations?: ChartAnnotations, extraAnnotations: ChartAnnotations[] = []): HorizontalAnnotation[] {
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

  for (const item of [annotations, ...extraAnnotations]) {
    if (!item) {
      continue;
    }
    addLine(item.triggerPrice, item.triggerLabel ?? "Trigger", "#eab308", 2, LineStyle.Dashed);
    addLine(item.entryPrice, item.entryLabel ?? "Entry", "#22c55e", 2, LineStyle.Solid);
    if (
      item.secondaryEntryLow != null &&
      item.secondaryEntryHigh != null &&
      Number.isFinite(item.secondaryEntryLow) &&
      Number.isFinite(item.secondaryEntryHigh)
    ) {
      addLine(item.secondaryEntryLow, `${item.secondaryEntryLabel ?? "Secondary"} low`, "#94a3b8", 1, LineStyle.LargeDashed);
      addLine(item.secondaryEntryHigh, `${item.secondaryEntryLabel ?? "Secondary"} high`, "#94a3b8", 1, LineStyle.LargeDashed);
    } else {
      addLine(item.secondaryEntryPrice, item.secondaryEntryLabel ?? "Secondary", "#94a3b8", 1, LineStyle.LargeDashed);
    }
    addLine(item.stopPrice, item.stopLabel ?? "Stop", "#ef4444", 2, LineStyle.Dotted);
  }
  return lines;
}

function buildFlexibleSrOverlay(candles: CandlePoint[]): FlexibleSrOverlay {
  if (candles.length < 30) {
    return { resistance: null, support: null };
  }

  const resistanceAnchors = selectFlexibleSrAnchors(candles, "high");
  const supportAnchors = selectFlexibleSrAnchors(candles, "low");

  return {
    resistance: buildFlexibleSrCurve(candles, resistanceAnchors),
    support: buildFlexibleSrCurve(candles, supportAnchors),
  };
}

function buildStructuralFibOverlay(candles: CandlePoint[]): StructuralFibOverlay | null {
  if (candles.length < 60) {
    return null;
  }

  const atrValues = buildAtr14(candles);
  const pivotHighs = detectPivotAnchors(candles, "high");
  const pivotLows = detectPivotAnchors(candles, "low");
  if (pivotHighs.length < 2 || pivotLows.length < 2) {
    return null;
  }

  const bullish = findBullishFibCandidate(candles, pivotHighs, pivotLows, atrValues);
  const bearish = findBearishFibCandidate(candles, pivotHighs, pivotLows, atrValues);
  const candidate = pickBestFibCandidate(bullish, bearish, candles.length);
  if (!candidate) {
    return null;
  }

  const start = candidate.anchorStart;
  const end = candidate.anchorEnd;
  const zero = end.value;
  const one = start.value;
  const range = one - zero;
  if (!Number.isFinite(range) || Math.abs(range) < 0.0001) {
    return null;
  }

  const levelTemplates: FibLevelTemplate[] = [
    { ratio: 0.0, label: "Fib 0.000", color: "rgba(226, 232, 240, 0.72)", lineWidth: 1, lineStyle: LineStyle.Solid },
    { ratio: 0.382, label: "Fib 0.382", color: "rgba(96, 165, 250, 0.86)", lineWidth: 1, lineStyle: LineStyle.Dotted },
    { ratio: 0.5, label: "Fib 0.500", color: "rgba(250, 204, 21, 0.9)", lineWidth: 2, lineStyle: LineStyle.Dashed },
    { ratio: 0.618, label: "Fib 0.618", color: "rgba(245, 158, 11, 0.95)", lineWidth: 2, lineStyle: LineStyle.Solid },
    { ratio: 0.786, label: "Fib 0.786", color: "rgba(217, 119, 6, 0.92)", lineWidth: 2, lineStyle: LineStyle.Solid },
    { ratio: 1.0, label: "Fib 1.000", color: "rgba(226, 232, 240, 0.72)", lineWidth: 1, lineStyle: LineStyle.Solid },
  ];
  const levels: FibLevel[] = levelTemplates.map((level) => ({
    label: level.label,
    color: level.color,
    lineWidth: level.lineWidth,
    lineStyle: level.lineStyle,
    value: Number((zero + (range * level.ratio)).toFixed(4)),
  }));

  return {
    trend: candidate.trend,
    anchorStart: start,
    anchorEnd: end,
    levels,
    impulseLine: [
      { time: start.time, value: start.value },
      { time: end.time, value: end.value },
    ],
    markers: [
      {
        time: start.time,
        label: candidate.trend === "bullish" ? "Fib low" : "Fib high",
        color: candidate.trend === "bullish" ? "#22c55e" : "#f87171",
        shape: "circle",
        position: candidate.trend === "bullish" ? "belowBar" : "aboveBar",
      },
      {
        time: end.time,
        label: candidate.trend === "bullish" ? "Fib high" : "Fib low",
        color: candidate.trend === "bullish" ? "#38bdf8" : "#f59e0b",
        shape: "square",
        position: candidate.trend === "bullish" ? "aboveBar" : "belowBar",
      },
      {
        time: candidate.bosAnchor.time,
        label: "BOS",
        color: "#fde047",
        shape: "circle",
        position: candidate.trend === "bullish" ? "aboveBar" : "belowBar",
      },
    ],
  };
}

function pickBestFibCandidate(
  bullish: StructuralFibCandidate | null,
  bearish: StructuralFibCandidate | null,
  candleCount: number,
): StructuralFibCandidate | null {
  const maxAgeBars = Math.min(150, Math.max(45, Math.floor(candleCount * 0.55)));
  const candidates = [bullish, bearish]
    .filter((candidate): candidate is StructuralFibCandidate => candidate !== null)
    .filter((candidate) => candleCount - 1 - candidate.anchorEnd.index <= maxAgeBars);
  if (candidates.length === 0) {
    return null;
  }
  candidates.sort((left, right) => {
    if (left.anchorEnd.index !== right.anchorEnd.index) {
      return right.anchorEnd.index - left.anchorEnd.index;
    }
    const leftMagnitude = Math.abs(left.anchorEnd.value - left.anchorStart.value) / Math.max(left.atr, 0.0001);
    const rightMagnitude = Math.abs(right.anchorEnd.value - right.anchorStart.value) / Math.max(right.atr, 0.0001);
    return rightMagnitude - leftMagnitude;
  });
  return candidates[0] ?? null;
}

function findBullishFibCandidate(
  candles: CandlePoint[],
  pivotHighs: SrAnchor[],
  pivotLows: SrAnchor[],
  atrValues: Array<number | null>,
): StructuralFibCandidate | null {
  for (let highIndex = pivotHighs.length - 1; highIndex >= 0; highIndex -= 1) {
    const impulseHigh = pivotHighs[highIndex];
    const priorLow = findLatestPivotBefore(pivotLows, impulseHigh.index);
    if (!priorLow) {
      continue;
    }
    const bosAnchor = findLatestPivotBefore(pivotHighs, priorLow.index);
    if (!bosAnchor) {
      continue;
    }
    const atr = atrValues[impulseHigh.index] ?? null;
    if (atr == null || atr <= 0) {
      continue;
    }
    if (impulseHigh.value <= bosAnchor.value) {
      continue;
    }
    if (impulseHigh.value - priorLow.value < atr * 1.5) {
      continue;
    }
    const invalidation = priorLow.value - (atr * 0.25);
    let invalidated = false;
    for (let index = impulseHigh.index + 1; index < candles.length; index += 1) {
      if (candles[index].close < invalidation) {
        invalidated = true;
        break;
      }
    }
    if (invalidated) {
      continue;
    }
    return {
      trend: "bullish",
      anchorStart: priorLow,
      anchorEnd: impulseHigh,
      bosAnchor,
      atr,
    };
  }
  return null;
}

function findBearishFibCandidate(
  candles: CandlePoint[],
  pivotHighs: SrAnchor[],
  pivotLows: SrAnchor[],
  atrValues: Array<number | null>,
): StructuralFibCandidate | null {
  for (let lowIndex = pivotLows.length - 1; lowIndex >= 0; lowIndex -= 1) {
    const impulseLow = pivotLows[lowIndex];
    const priorHigh = findLatestPivotBefore(pivotHighs, impulseLow.index);
    if (!priorHigh) {
      continue;
    }
    const bosAnchor = findLatestPivotBefore(pivotLows, priorHigh.index);
    if (!bosAnchor) {
      continue;
    }
    const atr = atrValues[impulseLow.index] ?? null;
    if (atr == null || atr <= 0) {
      continue;
    }
    if (impulseLow.value >= bosAnchor.value) {
      continue;
    }
    if (priorHigh.value - impulseLow.value < atr * 1.5) {
      continue;
    }
    const invalidation = priorHigh.value + (atr * 0.25);
    let invalidated = false;
    for (let index = impulseLow.index + 1; index < candles.length; index += 1) {
      if (candles[index].close > invalidation) {
        invalidated = true;
        break;
      }
    }
    if (invalidated) {
      continue;
    }
    return {
      trend: "bearish",
      anchorStart: priorHigh,
      anchorEnd: impulseLow,
      bosAnchor,
      atr,
    };
  }
  return null;
}

function findLatestPivotBefore(pivots: SrAnchor[], index: number): SrAnchor | null {
  for (let cursor = pivots.length - 1; cursor >= 0; cursor -= 1) {
    if (pivots[cursor].index < index) {
      return pivots[cursor];
    }
  }
  return null;
}

function buildAtr14(candles: CandlePoint[]): Array<number | null> {
  const period = 14;
  const atr: Array<number | null> = new Array(candles.length).fill(null);
  let rolling = 0;

  for (let index = 0; index < candles.length; index += 1) {
    const current = candles[index];
    const previousClose = index > 0 ? candles[index - 1].close : current.close;
    const trueRange = Math.max(
      current.high - current.low,
      Math.abs(current.high - previousClose),
      Math.abs(current.low - previousClose),
    );
    if (index < period) {
      rolling += trueRange;
      if (index === period - 1) {
        atr[index] = rolling / period;
      }
      continue;
    }
    const previousAtr = atr[index - 1] ?? (rolling / period);
    atr[index] = ((previousAtr * (period - 1)) + trueRange) / period;
  }

  return atr;
}

function selectFlexibleSrAnchors(candles: CandlePoint[], kind: "high" | "low"): SrAnchor[] {
  const pivots = detectPivotAnchors(candles, kind);
  if (pivots.length < 3) {
    return [];
  }

  const selected: SrAnchor[] = [];
  const minGap = Math.max(8, Math.floor(candles.length / 18));
  let cursor = pivots.length - 1;

  while (cursor >= 0 && selected.length < 3) {
    const base = pivots[cursor];
    if (!base) {
      cursor -= 1;
      continue;
    }
    let best = base;
    const windowStartIndex = Math.max(0, base.index - Math.max(24, minGap * 3));
    while (cursor >= 0 && pivots[cursor].index >= windowStartIndex) {
      const candidate = pivots[cursor];
      if (candidate.score > best.score) {
        best = candidate;
      }
      cursor -= 1;
    }
    if (selected.length > 0 && best.index >= selected[0].index - minGap) {
      continue;
    }
    selected.unshift(best);
  }

  return selected.length === 3 ? selected : [];
}

function detectPivotAnchors(candles: CandlePoint[], kind: "high" | "low"): SrAnchor[] {
  const pivots: SrAnchor[] = [];
  const span = Math.min(8, Math.max(3, Math.floor(candles.length / 55)));
  const recentWeight = 0.35;

  for (let index = span; index < candles.length - span; index += 1) {
    const current = candles[index];
    const currentValue = kind === "high" ? current.high : current.low;
    let isPivot = true;

    for (let offset = 1; offset <= span; offset += 1) {
      const left = candles[index - offset];
      const right = candles[index + offset];
      const leftValue = kind === "high" ? left.high : left.low;
      const rightValue = kind === "high" ? right.high : right.low;
      if (kind === "high") {
        if (currentValue <= leftValue || currentValue < rightValue) {
          isPivot = false;
          break;
        }
      } else if (currentValue >= leftValue || currentValue > rightValue) {
        isPivot = false;
        break;
      }
    }

    if (!isPivot) {
      continue;
    }

    const leftBase = candles[index - span].close;
    const rightBase = candles[index + span].close;
    const swingMagnitude =
      kind === "high"
        ? Math.max(0, currentValue - Math.max(leftBase, rightBase))
        : Math.max(0, Math.min(leftBase, rightBase) - currentValue);
    const candleRange = Math.max(0.0001, current.high - current.low);
    const recencyScore = index / Math.max(1, candles.length - 1);
    const score = swingMagnitude + candleRange * 0.2 + recencyScore * recentWeight * current.close;

    pivots.push({
      index,
      time: current.time,
      value: Number(currentValue.toFixed(4)),
      kind,
      score,
    });
  }

  return pivots;
}

function buildFlexibleSrCurve(candles: CandlePoint[], anchors: SrAnchor[]): FlexibleSrCurve | null {
  if (anchors.length !== 3) {
    return null;
  }
  const [a, b, c] = anchors;
  if (a.index === b.index || a.index === c.index || b.index === c.index) {
    return null;
  }

  const coefficients = solveQuadraticThroughPoints(
    { x: a.index, y: a.value },
    { x: b.index, y: b.value },
    { x: c.index, y: c.value },
  );
  if (!coefficients) {
    return null;
  }

  const backfit: Array<{ time: string; value: number }> = [];
  for (let index = a.index; index <= c.index; index += 1) {
    const candle = candles[index];
    if (!candle) {
      continue;
    }
    backfit.push({
      time: candle.time,
      value: Number(evaluateQuadratic(coefficients, index).toFixed(4)),
    });
  }

  const projection: Array<{ time: string; value: number }> = [];
  const projectionBars = Math.min(40, Math.max(12, Math.floor(candles.length * 0.08)));
  for (let offset = 0; offset <= projectionBars; offset += 1) {
    const index = c.index + offset;
    const time =
      offset === 0
        ? candles[c.index]?.time
        : addBusinessDays(candles[c.index]?.time ?? anchors[2].time, offset);
    if (!time) {
      continue;
    }
    projection.push({
      time,
      value: Number(evaluateQuadratic(coefficients, index).toFixed(4)),
    });
  }

  return { anchors, backfit, projection };
}

function solveQuadraticThroughPoints(
  first: { x: number; y: number },
  second: { x: number; y: number },
  third: { x: number; y: number },
) {
  const denominator = (first.x - second.x) * (first.x - third.x) * (second.x - third.x);
  if (Math.abs(denominator) < 1e-9) {
    return null;
  }
  const qa =
    (third.x * (second.y - first.y) +
      second.x * (first.y - third.y) +
      first.x * (third.y - second.y)) /
    denominator;
  const qb =
    (third.x * third.x * (first.y - second.y) +
      second.x * second.x * (third.y - first.y) +
      first.x * first.x * (second.y - third.y)) /
    denominator;
  const qc =
    (second.x * third.x * (second.x - third.x) * first.y +
      third.x * first.x * (third.x - first.x) * second.y +
      first.x * second.x * (first.x - second.x) * third.y) /
    denominator;
  if (![qa, qb, qc].every((value) => Number.isFinite(value))) {
    return null;
  }
  return { qa, qb, qc };
}

function evaluateQuadratic(coefficients: { qa: number; qb: number; qc: number }, x: number) {
  return coefficients.qa * x * x + coefficients.qb * x + coefficients.qc;
}

function addBusinessDays(baseDate: string, offset: number) {
  const parsed = new Date(`${baseDate}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  let remaining = offset;
  while (remaining > 0) {
    parsed.setUTCDate(parsed.getUTCDate() + 1);
    const day = parsed.getUTCDay();
    if (day === 0 || day === 6) {
      continue;
    }
    remaining -= 1;
  }
  return parsed.toISOString().slice(0, 10);
}
