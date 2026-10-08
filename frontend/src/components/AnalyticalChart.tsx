import { Component, lazy, Suspense, type ErrorInfo, type ReactNode } from "react";
import { LoadingBlock } from "./LoadingBlock";
import { PriceChart, type PriceChartProps } from "./PriceChart";

export type ChartEngine = "current" | "vela";

type AnalyticalChartProps = PriceChartProps & {
  engine: ChartEngine;
  onVelaUnavailable: (message: string) => void;
};

const VelaPriceChart = lazy(() => import("./VelaPriceChart").then((module) => ({ default: module.VelaPriceChart })));

class VelaPreviewBoundary extends Component<
  { fallback: ReactNode; onUnavailable: (message: string) => void; children: ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, _: ErrorInfo) {
    this.props.onUnavailable(error.message || "Vela preview could not load.");
  }

  render() {
    return this.state.error ? this.props.fallback : this.props.children;
  }
}

export function AnalyticalChart({ engine, onVelaUnavailable, ...priceChartProps }: AnalyticalChartProps) {
  if (engine === "current") {
    return <PriceChart {...priceChartProps} />;
  }

  return (
    <VelaPreviewBoundary key={priceChartProps.ticker} fallback={<PriceChart {...priceChartProps} />} onUnavailable={onVelaUnavailable}>
      <Suspense fallback={<LoadingBlock label="Loading Vela preview…" />}>
        <VelaPriceChart ticker={priceChartProps.ticker} candles={priceChartProps.candles} overlays={priceChartProps.overlays} onUnavailable={onVelaUnavailable} />
      </Suspense>
    </VelaPreviewBoundary>
  );
}
