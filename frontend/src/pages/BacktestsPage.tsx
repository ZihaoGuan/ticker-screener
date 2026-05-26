import { Panel } from "../components/Panel";

export function BacktestsPage() {
  return (
    <div className="page-grid">
      <Panel title="Backtest Templates">
        <div className="template-list">
          <article className="metric-card">
            <h3>Overlap Count Backtest</h3>
            <p className="panel-copy">Historical overlap summary forward-return study.</p>
            <code className="inline-code">python scripts/build_overlap_backtest_report.py --start-date 2024-01-01 --end-date 2026-05-01</code>
          </article>
        </div>
      </Panel>
    </div>
  );
}
