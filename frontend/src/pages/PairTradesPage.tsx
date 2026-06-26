import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { PairTradeReportDetail, PairTradeReportSummary } from "../lib/types";

export function PairTradesPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [reports, setReports] = useState<PairTradeReportSummary[]>([]);
  const [selectedStem, setSelectedStem] = useState("");
  const [detail, setDetail] = useState<PairTradeReportDetail | null>(null);
  const [pairSearch, setPairSearch] = useState("");
  const [isReportsLoading, setIsReportsLoading] = useState(true);
  const [isDetailLoading, setIsDetailLoading] = useState(false);
  const requestedStem = searchParams.get("stem") ?? "";

  useEffect(() => {
    setIsReportsLoading(true);
    void fetchJson<{ reports: PairTradeReportSummary[] }>("/api/pair-trades")
      .then((payload) => {
        setReports(payload.reports);
        setSelectedStem((current) => {
          return (
            payload.reports.find((item) => item.stem === current)?.stem ??
            payload.reports.find((item) => item.stem === requestedStem)?.stem ??
            payload.reports[0]?.stem ??
            ""
          );
        });
      })
      .finally(() => setIsReportsLoading(false));
  }, [requestedStem]);

  useEffect(() => {
    if (!selectedStem) {
      setDetail(null);
      return;
    }
    setIsDetailLoading(true);
    void fetchJson<PairTradeReportDetail>(`/api/pair-trades/${selectedStem}`)
      .then((payload) => setDetail(payload))
      .finally(() => setIsDetailLoading(false));
  }, [selectedStem]);

  useEffect(() => {
    if (!selectedStem) {
      return;
    }
    const nextParams = new URLSearchParams(searchParams);
    nextParams.set("stem", selectedStem);
    if (nextParams.toString() !== searchParams.toString()) {
      setSearchParams(nextParams, { replace: true });
    }
  }, [searchParams, selectedStem, setSearchParams]);

  const filteredPairs = useMemo(() => {
    const query = pairSearch.trim().toLowerCase();
    const pairs = detail?.pairs ?? [];
    if (!query) {
      return pairs;
    }
    return pairs.filter((item) =>
      [
        item.pair,
        item.stock_a,
        item.stock_b,
        item.company_a,
        item.company_b,
        item.sector,
        item.industry,
        item.group_name,
        item.signal,
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(query),
    );
  }, [detail?.pairs, pairSearch]);

  const selectedReport = useMemo(
    () => reports.find((item) => item.stem === selectedStem) ?? null,
    [reports, selectedStem],
  );

  return (
    <div className="watchlists-layout">
      <aside className="panel files-pane">
        <div className="panel-head">
          <h2>Pair Trade Runs</h2>
          <span className="eyebrow">{reports.length} reports</span>
        </div>
        {isReportsLoading ? <LoadingBlock label="Loading pair trade reports…" compact /> : null}
        <div className="file-list">
          {reports.map((report) => (
            <button
              key={report.stem}
              className={`file-row file-button${selectedStem === report.stem ? " is-selected" : ""}`}
              onClick={() => setSelectedStem(report.stem)}
              type="button"
            >
              <div className="file-name">{report.top_pair || report.stem}</div>
              <div className="file-meta">
                {formatLocalDateTime(report.captured_at)} · {report.group_mode} · {report.actionable_pairs ?? 0} actionable
              </div>
            </button>
          ))}
        </div>
      </aside>

      <div className="watchlists-main">
        <Panel
          title={selectedReport?.top_pair || "Pair Trade Report"}
          aside={
            <div className="watchlist-panel-aside">
              <span className="eyebrow">{detail?.summary.pairs_analyzed ?? 0} pairs analyzed</span>
              <span className="eyebrow">{detail?.summary.cointegrated_pairs ?? 0} cointegrated</span>
            </div>
          }
        >
          <div className="watchlist-board-copy">
            <p className="panel-copy">FINVIZ builds liquid same-group universe. Local `daily_bars` drives correlation, cointegration, half-life, and z-score.</p>
            {selectedReport ? (
              <p className="panel-copy">
                Captured {formatLocalDateTime(selectedReport.captured_at)} · As of {formatLocalDate(selectedReport.as_of_date)} · Groups{" "}
                {selectedReport.included_groups.join(", ") || "Default sectors"}
              </p>
            ) : null}
          </div>
          <div className="ticker-list-toolbar watchlist-board-toolbar">
            <label className="field">
              <span>Filter pairs</span>
              <input
                type="text"
                value={pairSearch}
                onChange={(event) => setPairSearch(event.target.value)}
                placeholder="Pair, ticker, sector, industry, signal"
              />
            </label>
            <span className="eyebrow">{filteredPairs.length} matched</span>
          </div>
          {isDetailLoading ? <LoadingBlock label="Loading pair trade detail…" compact /> : null}
          {!isDetailLoading && detail ? (
            <>
              <div className="card-grid">
                <article className="metric-card">
                  <div className="metric-card-head">
                    <h3>Universe</h3>
                    <span className="accent-mark accent-neutral" />
                  </div>
                  <div className="metric-value">
                    {detail.summary.universe_size} <span>tickers</span>
                  </div>
                  <p className="card-meta">Mode {detail.metadata.group_mode} · Groups {detail.metadata.included_groups.join(", ") || "Default sectors"}</p>
                  <p className="card-meta">Lookback {detail.metadata.lookback_days} days · Min history {detail.metadata.min_history_days}</p>
                </article>
                <article className="metric-card">
                  <div className="metric-card-head">
                    <h3>Stat Filter</h3>
                    <span className="accent-mark accent-neutral" />
                  </div>
                  <div className="metric-value">
                    {detail.summary.cointegrated_pairs} <span>cointegrated</span>
                  </div>
                  <p className="card-meta">Min corr {detail.metadata.min_correlation} · Max half-life {detail.metadata.max_half_life}</p>
                  <p className="card-meta">Entry z-score {detail.metadata.entry_zscore} · Test mode {detail.metadata.stats_test_mode || "cointegration"}</p>
                </article>
                <article className="metric-card">
                  <div className="metric-card-head">
                    <h3>Actionable</h3>
                    <span className={`accent-mark ${detail.summary.actionable_pairs > 0 ? "accent-bull" : "accent-neutral"}`} />
                  </div>
                  <div className="metric-value">
                    {detail.summary.actionable_pairs} <span>signals</span>
                  </div>
                  <p className="card-meta">Top pair {detail.summary.top_pair || "--"}</p>
                  <p className="card-meta">Corr pass {detail.summary.correlation_pass} · Pairs analyzed {detail.summary.pairs_analyzed}</p>
                </article>
              </div>

              <div className="data-table-wrap" style={{ marginTop: 16 }}>
                <table className="data-table scanner-result-table">
                  <thead>
                    <tr>
                      <th>Pair</th>
                      <th>Group</th>
                      <th>Signal</th>
                      <th>Corr</th>
                      <th>P-Value</th>
                      <th>Half-Life</th>
                      <th>Z-Score</th>
                      <th>Score</th>
                      <th>Charts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredPairs.map((item) => (
                      <tr key={item.pair}>
                        <td>
                          <div className="scanner-result-company">
                            <strong>{item.pair}</strong>
                            <span>{item.company_a || item.stock_a} vs {item.company_b || item.stock_b}</span>
                          </div>
                        </td>
                        <td>
                          <div className="scanner-result-sector">
                            <strong>{item.group_name}</strong>
                            <span>{item.industry || item.sector || "--"}</span>
                          </div>
                        </td>
                        <td><span className={`scanner-inline-badge${item.actionable ? " is-new" : ""}`}>{item.signal}</span></td>
                        <td>{formatDecimal(item.correlation)}</td>
                        <td>{formatDecimal(item.cointegration_pvalue, 4)}</td>
                        <td>{formatDecimal(item.half_life_days, 1)}</td>
                        <td>{formatSigned(item.current_zscore)}</td>
                        <td>{formatDecimal(item.opportunity_score, 1)}</td>
                        <td>
                          <div className="scanner-result-view-actions">
                            <Link className="ghost-button" to={`/charts?ticker=${encodeURIComponent(item.stock_a)}`}>{item.stock_a}</Link>
                            <Link className="ghost-button" to={`/charts?ticker=${encodeURIComponent(item.stock_b)}`}>{item.stock_b}</Link>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : null}
        </Panel>
      </div>
    </div>
  );
}

function formatDecimal(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  return value.toFixed(digits);
}

function formatSigned(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) {
    return "--";
  }
  return value > 0 ? `+${value.toFixed(2)}` : value.toFixed(2);
}
