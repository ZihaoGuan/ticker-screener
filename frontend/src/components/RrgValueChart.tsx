import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import type { EChartsType } from "echarts/core";
import { LineChart } from "echarts/charts";
import { CanvasRenderer } from "echarts/renderers";
import { GridComponent, TooltipComponent } from "echarts/components";
import type { RrgSeries } from "../lib/types";

echarts.use([LineChart, GridComponent, TooltipComponent, CanvasRenderer]);

type RrgValueChartProps = {
  series: RrgSeries;
};

export function RrgValueChart({ series }: RrgValueChartProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<EChartsType | null>(null);

  useEffect(() => {
    if (!rootRef.current) {
      return;
    }
    const chart = echarts.init(rootRef.current, undefined, { renderer: "canvas" });
    chartRef.current = chart;

    const resizeObserver = new ResizeObserver(() => {
      chart.resize();
    });
    resizeObserver.observe(rootRef.current);

    return () => {
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

    chart.setOption(
      {
        animation: false,
        backgroundColor: "transparent",
        grid: {
          left: 42,
          right: 12,
          top: 12,
          bottom: 28,
        },
        tooltip: {
          trigger: "axis",
          backgroundColor: "rgba(17, 24, 39, 0.96)",
          borderColor: "#3f3f46",
          borderWidth: 1,
          textStyle: {
            color: "#f4f4f5",
          },
        },
        xAxis: {
          type: "category",
          data: series.points.map((point) => point.date),
          axisLabel: {
            color: "#71717a",
            showMaxLabel: true,
            showMinLabel: true,
            formatter: (value: string) => value.slice(5),
          },
          axisLine: {
            lineStyle: {
              color: "#27272a",
            },
          },
          axisTick: {
            show: false,
          },
        },
        yAxis: {
          type: "value",
          scale: true,
          axisLabel: {
            color: "#a1a1aa",
            formatter: (value: number) => value.toFixed(1),
          },
          axisLine: {
            show: false,
          },
          splitLine: {
            lineStyle: {
              color: "#202024",
            },
          },
        },
        series: [
          {
            type: "line",
            smooth: false,
            showSymbol: false,
            lineStyle: {
              color: "#60a5fa",
              width: 2,
            },
            areaStyle: {
              color: "rgba(96, 165, 250, 0.10)",
            },
            data: series.points.map((point) => point.x),
          },
        ],
      },
      true,
    );
  }, [series]);

  return <div ref={rootRef} className="rrg-value-chart-root" />;
}
