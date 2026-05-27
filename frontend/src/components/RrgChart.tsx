import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts/core";
import type { EChartsType } from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";
import { GraphicComponent, GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import type { GraphicComponentOption } from "echarts/components";
import type { RrgSeries } from "../lib/types";

echarts.use([LineChart, ScatterChart, GraphicComponent, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

type RrgChartProps = {
  benchmark: string;
  series: RrgSeries[];
  compact?: boolean;
  showLegend?: boolean;
};

type ChartBounds = {
  xMin: number;
  xMax: number;
  yMin: number;
  yMax: number;
};

const QUADRANT_COLORS = {
  Leading: "rgba(16, 185, 129, 0.10)",
  Weakening: "rgba(245, 158, 11, 0.10)",
  Lagging: "rgba(244, 63, 94, 0.10)",
  Improving: "rgba(59, 130, 246, 0.10)",
};

const PALETTE = ["#34d399", "#38bdf8", "#f59e0b", "#f472b6", "#a78bfa", "#fb7185", "#22c55e", "#eab308", "#60a5fa", "#14b8a6", "#f97316", "#c084fc"];

export function RrgChart({ benchmark, series, compact = false, showLegend = true }: RrgChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<EChartsType | null>(null);

  const bounds = useMemo(() => computeBounds(series), [series]);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }
    const chart = echarts.init(rootRef.current, undefined, { renderer: "canvas" });
    chartRef.current = chart;

    const resizeObserver = new ResizeObserver(() => {
      chart.resize();
      applyQuadrantGraphics(chart, bounds);
    });
    resizeObserver.observe(rootRef.current);

    const handleFinished = () => applyQuadrantGraphics(chart, bounds);
    chart.on("finished", handleFinished);

    return () => {
      chart.off("finished", handleFinished);
      resizeObserver.disconnect();
      chart.dispose();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) {
      return;
    }
    chart.setOption(buildOption(series, benchmark, bounds, compact, showLegend), true);
    applyQuadrantGraphics(chart, bounds);
  }, [benchmark, bounds, compact, series, showLegend]);

  return <div ref={rootRef} className={`rrg-chart-root${compact ? " is-compact" : ""}`} />;
}

function buildOption(series: RrgSeries[], benchmark: string, bounds: ChartBounds, compact: boolean, showLegend: boolean) {
  const chartSeries = series.flatMap((entry, index) => {
    const color = PALETTE[index % PALETTE.length];
    const displayName = `${entry.ticker} · ${entry.label}`;
    const lineData = entry.points.map((point) => ({
      value: [point.x, point.y],
      date: point.date,
      ticker: entry.ticker,
      label: entry.label,
      latest: entry.latest,
      quadrant: entry.quadrant,
    }));

    return [
      {
        name: displayName,
        type: "line",
        data: lineData,
        showSymbol: false,
        smooth: false,
        lineStyle: {
          color,
          width: 2,
          opacity: 0.7,
        },
        emphasis: {
          focus: "series",
        },
        animation: false,
        z: 3,
      },
      {
        name: displayName,
        type: "scatter",
        data: [
          {
            value: [entry.latest.x, entry.latest.y],
            date: entry.latest.date,
            ticker: entry.ticker,
            label: entry.label,
            latest: entry.latest,
            quadrant: entry.quadrant,
          },
        ],
        symbolSize: 14,
        itemStyle: {
          color,
          borderColor: "#fafafa",
          borderWidth: 1.5,
          shadowBlur: 14,
          shadowColor: color,
        },
        emphasis: {
          scale: true,
        },
        animation: false,
        z: 5,
      },
    ];
  });

  return {
    animation: false,
    backgroundColor: "transparent",
    color: PALETTE,
    grid: {
      left: compact ? 42 : 64,
      right: compact ? 14 : 28,
      top: compact ? 14 : 24,
      bottom: compact ? 32 : 56,
      containLabel: false,
    },
    legend: {
      show: showLegend,
      type: "scroll",
      top: 0,
      textStyle: {
        color: "#d4d4d8",
        fontSize: compact ? 10 : 11,
      },
      pageTextStyle: {
        color: "#d4d4d8",
      },
      itemWidth: 10,
      itemHeight: 10,
    },
    tooltip: {
      trigger: "item",
      backgroundColor: "rgba(17, 24, 39, 0.96)",
      borderColor: "#3f3f46",
      borderWidth: 1,
      textStyle: {
        color: "#f4f4f5",
      },
      formatter: (rawParams: unknown) => {
        const params = rawParams as {
          data?: {
            ticker?: string;
            label?: string;
            latest?: { x: number; y: number; date: string };
            quadrant?: string;
          };
        };
        const payload = params.data;
        if (!payload?.latest) {
          return "";
        }
        return [
          `<strong>${payload.ticker ?? ""}</strong>`,
          payload.label ?? "",
          `Benchmark: ${benchmark}`,
          `Latest X: ${payload.latest.x.toFixed(2)}`,
          `Latest Y: ${payload.latest.y.toFixed(2)}`,
          `Quadrant: ${payload.quadrant ?? ""}`,
          `Last date: ${payload.latest.date}`,
        ].join("<br/>");
      },
    },
    xAxis: {
      type: "value",
      min: bounds.xMin,
      max: bounds.xMax,
      axisLabel: {
        color: "#a1a1aa",
        formatter: (value: number) => value.toFixed(1),
        fontSize: compact ? 10 : 12,
      },
      axisLine: {
        lineStyle: {
          color: "#3f3f46",
        },
      },
      splitLine: {
        lineStyle: {
          color: "#27272a",
        },
      },
      name: compact ? "" : "RS Ratio",
      nameLocation: "middle",
      nameGap: 34,
      nameTextStyle: {
        color: "#a1a1aa",
        fontSize: compact ? 10 : 12,
      },
    },
    yAxis: {
      type: "value",
      min: bounds.yMin,
      max: bounds.yMax,
      axisLabel: {
        color: "#a1a1aa",
        formatter: (value: number) => value.toFixed(1),
        fontSize: compact ? 10 : 12,
      },
      axisLine: {
        lineStyle: {
          color: "#3f3f46",
        },
      },
      splitLine: {
        lineStyle: {
          color: "#27272a",
        },
      },
      name: compact ? "" : "RS Momentum",
      nameLocation: "middle",
      nameGap: 48,
      nameTextStyle: {
        color: "#a1a1aa",
        fontSize: compact ? 10 : 12,
      },
    },
    series: chartSeries,
  };
}

function applyQuadrantGraphics(chart: EChartsType, bounds: ChartBounds) {
  const topLeft = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [bounds.xMin, bounds.yMax]) as [number, number];
  const topCenter = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [100, bounds.yMax]) as [number, number];
  const center = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [100, 100]) as [number, number];
  const bottomLeft = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [bounds.xMin, bounds.yMin]) as [number, number];
  const bottomCenter = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [100, bounds.yMin]) as [number, number];
  const topRight = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [bounds.xMax, bounds.yMax]) as [number, number];
  const bottomRight = chart.convertToPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [bounds.xMax, bounds.yMin]) as [number, number];

  const graphics: GraphicComponentOption[] = [
    rectGraphic(topCenter[0], topCenter[1], topRight[0] - topCenter[0], center[1] - topCenter[1], QUADRANT_COLORS.Leading),
    rectGraphic(topCenter[0], center[1], topRight[0] - topCenter[0], bottomRight[1] - center[1], QUADRANT_COLORS.Weakening),
    rectGraphic(topLeft[0], center[1], topCenter[0] - topLeft[0], bottomCenter[1] - center[1], QUADRANT_COLORS.Lagging),
    rectGraphic(topLeft[0], topLeft[1], topCenter[0] - topLeft[0], center[1] - topLeft[1], QUADRANT_COLORS.Improving),
    lineGraphic(center[0], topLeft[1], center[0], bottomLeft[1]),
    lineGraphic(topLeft[0], center[1], topRight[0], center[1]),
    textGraphic(center[0] + 10, topLeft[1] + 12, "Leading", "#86efac"),
    textGraphic(center[0] + 10, bottomRight[1] - 26, "Weakening", "#fde68a"),
    textGraphic(topLeft[0] + 10, bottomLeft[1] - 26, "Lagging", "#fda4af"),
    textGraphic(topLeft[0] + 10, topLeft[1] + 12, "Improving", "#93c5fd"),
  ];

  chart.setOption({ graphic: graphics }, { replaceMerge: ["graphic"] });
}

function rectGraphic(x: number, y: number, width: number, height: number, fill: string): GraphicComponentOption {
  return {
    type: "rect",
    silent: true,
    shape: { x, y, width, height },
    style: { fill },
    z: 0,
  };
}

function lineGraphic(x1: number, y1: number, x2: number, y2: number): GraphicComponentOption {
  return {
    type: "line",
    silent: true,
    shape: { x1, y1, x2, y2 },
    style: {
      stroke: "rgba(250, 250, 250, 0.48)",
      lineWidth: 1,
    },
    z: 1,
  };
}

function textGraphic(x: number, y: number, text: string, fill: string): GraphicComponentOption {
  return {
    type: "text",
    silent: true,
    style: {
      x,
      y,
      text,
      fill,
      font: "12px Inter, system-ui, sans-serif",
    },
    z: 1,
  };
}

function computeBounds(series: RrgSeries[]): ChartBounds {
  const xValues = [100];
  const yValues = [100];
  for (const item of series) {
    for (const point of item.points) {
      xValues.push(point.x);
      yValues.push(point.y);
    }
  }
  const xMinRaw = Math.min(...xValues);
  const xMaxRaw = Math.max(...xValues);
  const yMinRaw = Math.min(...yValues);
  const yMaxRaw = Math.max(...yValues);
  const xPad = Math.max(2, (xMaxRaw - xMinRaw) * 0.12);
  const yPad = Math.max(2, (yMaxRaw - yMinRaw) * 0.12);

  return {
    xMin: Number((xMinRaw - xPad).toFixed(2)),
    xMax: Number((xMaxRaw + xPad).toFixed(2)),
    yMin: Number((yMinRaw - yPad).toFixed(2)),
    yMax: Number((yMaxRaw + yPad).toFixed(2)),
  };
}
