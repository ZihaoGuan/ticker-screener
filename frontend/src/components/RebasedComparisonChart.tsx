import { ColorType, createChart } from "lightweight-charts";
import { useEffect, useMemo, useRef } from "react";
import type { CandlePoint } from "../lib/types";

type RebasedComparisonChartProps = {
  sectorTicker: string;
  benchmarkTicker: string;
  sectorCandles: CandlePoint[];
  benchmarkCandles: CandlePoint[];
};

type ComparisonPoint = {
  time: string;
  sector: number;
  benchmark: number;
  relative: number;
};

export function RebasedComparisonChart({ sectorTicker, benchmarkTicker, sectorCandles, benchmarkCandles }: RebasedComparisonChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const comparison = useMemo(() => buildComparisonPoints(sectorCandles, benchmarkCandles), [sectorCandles, benchmarkCandles]);
  const latest = comparison[comparison.length - 1] ?? null;

  useEffect(() => {
    if (!rootRef.current || comparison.length === 0) {
      return;
    }
    const chart = createChart(rootRef.current, {
      autoSize: true,
      height: 340,
      layout: {
        background: { type: ColorType.Solid, color: "#1c1c1e" },
        textColor: "#8e8e93",
      },
      grid: {
        vertLines: { color: "rgba(56, 56, 58, 0.42)" },
        horzLines: { color: "rgba(56, 56, 58, 0.42)" },
      },
      crosshair: {
        vertLine: { color: "rgba(255, 189, 127, 0.32)", width: 1 },
        horzLine: { color: "rgba(255, 189, 127, 0.24)", width: 1 },
      },
      rightPriceScale: {
        borderColor: "rgba(56, 56, 58, 0.9)",
      },
      timeScale: {
        borderColor: "rgba(56, 56, 58, 0.9)",
        timeVisible: false,
        secondsVisible: false,
      },
      localization: {
        locale: "en-US",
        priceFormatter: (price: number) => price.toFixed(1),
      },
    });

    const sectorSeries = chart.addLineSeries({
      color: "#f59e0b",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const benchmarkSeries = chart.addLineSeries({
      color: "#a1a1aa",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    const relativeSeries = chart.addLineSeries({
      color: "#22d3ee",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    sectorSeries.setData(comparison.map((item) => ({ time: item.time, value: item.sector })));
    benchmarkSeries.setData(comparison.map((item) => ({ time: item.time, value: item.benchmark })));
    relativeSeries.setData(comparison.map((item) => ({ time: item.time, value: item.relative })));
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
  }, [benchmarkTicker, comparison, sectorTicker]);

  if (comparison.length === 0) {
    return <p className="panel-copy">No overlapping comparison data.</p>;
  }

  return (
    <div className="rebased-comparison-chart-wrap">
      <div className="rebased-comparison-legend">
        <span className="is-sector">{sectorTicker} {formatIndexedMove(latest?.sector)}</span>
        <span className="is-benchmark">{benchmarkTicker} {formatIndexedMove(latest?.benchmark)}</span>
        <span className="is-relative">RS {formatIndexedMove(latest?.relative)}</span>
      </div>
      <div ref={rootRef} className="rebased-comparison-chart" aria-label={`${sectorTicker} versus ${benchmarkTicker} rebased comparison chart`} />
    </div>
  );
}

function buildComparisonPoints(sectorCandles: CandlePoint[], benchmarkCandles: CandlePoint[]): ComparisonPoint[] {
  const benchmarkByTime = new Map(benchmarkCandles.map((item) => [item.time, item.close]));
  const paired = sectorCandles
    .map((item) => ({ time: item.time, sectorClose: item.close, benchmarkClose: benchmarkByTime.get(item.time) }))
    .filter((item): item is { time: string; sectorClose: number; benchmarkClose: number } => {
      const benchmarkClose = item.benchmarkClose;
      return Number.isFinite(item.sectorClose) && typeof benchmarkClose === "number" && Number.isFinite(benchmarkClose) && item.sectorClose > 0 && benchmarkClose > 0;
    });
  const first = paired[0];
  if (!first) {
    return [];
  }
  const sectorBase = first.sectorClose;
  const benchmarkBase = first.benchmarkClose;
  const relativeBase = sectorBase / benchmarkBase;
  return paired.map((item) => {
    const relative = item.sectorClose / item.benchmarkClose;
    return {
      time: item.time,
      sector: (item.sectorClose / sectorBase) * 100,
      benchmark: (item.benchmarkClose / benchmarkBase) * 100,
      relative: (relative / relativeBase) * 100,
    };
  });
}

function formatIndexedMove(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  const move = value - 100;
  const prefix = move > 0 ? "+" : "";
  return `${prefix}${move.toFixed(1)}%`;
}
