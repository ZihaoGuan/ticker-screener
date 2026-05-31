import { useEffect, useState } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { BacktestsResponse } from "../lib/types";

export function BacktestsPage() {
  const [payload, setPayload] = useState<BacktestsResponse>({ backtest_templates: [] });
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    void fetchJson<BacktestsResponse>("/api/backtests")
      .then(setPayload)
      .catch(() => setPayload({ backtest_templates: [] }))
      .finally(() => setIsLoading(false));
  }, []);

  return (
    <div className="page-grid">
      <Panel title="Backtest Templates">
        {isLoading ? <LoadingBlock label="Loading backtest templates…" compact /> : null}
        <div className="template-list">
          {payload.backtest_templates.map((item) => (
            <article key={item.label} className="metric-card">
              <h3>{item.label}</h3>
              <p className="panel-copy">{item.description}</p>
              <code className="inline-code">{item.command}</code>
            </article>
          ))}
        </div>
      </Panel>
    </div>
  );
}
