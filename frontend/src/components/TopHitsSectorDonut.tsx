import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts/core";
import type { EChartsType } from "echarts/core";
import { PieChart } from "echarts/charts";
import { GraphicComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { ScannerTopHitRow } from "../lib/types";

echarts.use([PieChart, GraphicComponent, TooltipComponent, CanvasRenderer]);

const SECTOR_COLORS: Record<string, string> = {
  "Communication Services": "#38bdf8",
  "Consumer Cyclical": "#f59e0b",
  "Consumer Defensive": "#a3e635",
  Energy: "#fb7185",
  Financial: "#a78bfa",
  Healthcare: "#34d399",
  Industrials: "#60a5fa",
  "Real Estate": "#f472b6",
  "Basic Materials": "#f97316",
  Technology: "#22d3ee",
  Utilities: "#eab308",
  Unknown: "#71717a",
};

export function TopHitsSectorDonut({ rows }: { rows: ScannerTopHitRow[] }) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<EChartsType | null>(null);
  const sectors = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of rows) {
      const sector = row.sector?.trim();
      const name = !sector || sector.toLowerCase() === "unknown" ? "Unknown" : sector;
      counts.set(name, (counts.get(name) ?? 0) + 1);
    }
    return [...counts].map(([name, value]) => ({ name, value, itemStyle: { color: sectorColor(name) } }))
      .sort((left, right) => right.value - left.value || left.name.localeCompare(right.name));
  }, [rows]);
  const total = rows.length;
  const hasData = total > 0;

  useEffect(() => {
    if (!hasData || !rootRef.current) return;
    const chart = echarts.init(rootRef.current, undefined, { renderer: "canvas" });
    chartRef.current = chart;
    const resizeObserver = new ResizeObserver(() => chart.resize());
    resizeObserver.observe(rootRef.current);
    return () => {
      resizeObserver.disconnect();
      chart.dispose();
      chartRef.current = null;
    };
  }, [hasData]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    chart.setOption({
      animation: false,
      backgroundColor: "transparent",
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(17, 24, 39, 0.96)",
        borderColor: "#3f3f46",
        textStyle: { color: "#f4f4f5" },
        formatter: (raw: unknown) => {
          const item = raw as { name?: string; value?: number; percent?: number };
          return `${escapeHtml(item.name ?? "Unknown")}<br/>${item.value ?? 0} tickers · ${item.percent ?? 0}%`;
        },
      },
      graphic: [{
        type: "text",
        left: "center",
        top: "middle",
        style: { text: String(total), fill: "#f4f4f5", font: "700 22px sans-serif", textAlign: "center" },
      }],
      series: [{
        type: "pie",
        radius: ["58%", "82%"],
        center: ["50%", "50%"],
        clockwise: true,
        avoidLabelOverlap: true,
        label: { show: false },
        emphasis: { scale: false },
        selectedMode: false,
        data: sectors,
      }],
    }, true);
  }, [sectors, total]);

  return (
    <section className="top-hits-sector-mix panel" aria-label={`Sector mix for ${total} filtered Top Hits tickers`}>
      <div className="top-hits-sector-heading">
        <span className="eyebrow">Sector Mix</span>
        <span className="panel-copy">{total.toLocaleString()} tickers</span>
      </div>
      {!hasData ? <p className="panel-copy">No tickers to chart.</p> : (
        <>
          <div ref={rootRef} className="top-hits-sector-chart" role="img" aria-label={`Donut chart: ${sectors.map(({ name, value }) => `${name}, ${value}`).join("; ")}`} />
          <ul className="top-hits-sector-legend" aria-label="Sector ticker counts">
            {sectors.map(({ name, value, itemStyle }) => (
              <li key={name}>
                <span className="top-hits-sector-swatch" style={{ backgroundColor: itemStyle.color }} aria-hidden="true" />
                <span>{name}</span><strong>{value.toLocaleString()}</strong>
              </li>
            ))}
          </ul>
        </>
      )}
    </section>
  );
}

function sectorColor(name: string): string {
  if (SECTOR_COLORS[name]) return SECTOR_COLORS[name];
  let hash = 0;
  for (const character of name) hash = (hash * 31 + character.charCodeAt(0)) | 0;
  return `hsl(${Math.abs(hash) % 360} 68% 62%)`;
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[character] ?? character);
}
