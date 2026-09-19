import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { DailyReportCandidate, DailyReportDetail, DailyReportSummary } from "../lib/types";

const groupLabels: Record<string, string> = {
  A: "Actionable",
  B: "Ready",
  C: "Developing",
  D: "Extended",
  E: "Avoid",
  U: "Insufficient data",
};

export function DailyReportsPage() {
  const { reportDate = "", agentId = "" } = useParams();
  const navigate = useNavigate();
  const [reports, setReports] = useState<DailyReportSummary[]>([]);
  const [detail, setDetail] = useState<DailyReportDetail | null>(null);
  const [loadingReports, setLoadingReports] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    const controller = new AbortController();
    setLoadingReports(true);
    fetchJson<{ reports: DailyReportSummary[] }>("/api/daily-reports", { signal: controller.signal })
      .then((payload) => {
        setReports(payload.reports);
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Unable to load daily reports.");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadingReports(false);
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (reports.length === 0) return;
    const selected = reportDate
      ? reports.find((report) => report.report_date === reportDate && (!agentId || report.agent_id === agentId))
      : reports[0];
    if (selected && (!reportDate || !agentId)) {
      navigate(`/daily-reports/${selected.report_date}/${selected.agent_id}`, { replace: true });
    }
  }, [agentId, navigate, reportDate, reports]);

  useEffect(() => {
    if (!reportDate || !agentId) {
      setDetail(null);
      return;
    }
    const controller = new AbortController();
    setLoadingDetail(true);
    setError("");
    fetchJson<DailyReportDetail>(
      `/api/daily-reports/${encodeURIComponent(reportDate)}/${encodeURIComponent(agentId)}`,
      { signal: controller.signal },
    )
      .then(setDetail)
      .catch((reason) => {
        if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Unable to load this report.");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadingDetail(false);
      });
    return () => controller.abort();
  }, [agentId, reportDate]);

  const orderedCandidates = useMemo(
    () => [...(detail?.candidates ?? [])].sort((left, right) => left.group.localeCompare(right.group) || (right.score ?? -1) - (left.score ?? -1)),
    [detail],
  );
  const reportsByDate = useMemo(() => {
    const grouped = new Map<string, DailyReportSummary[]>();
    reports.forEach((report) => grouped.set(report.report_date, [...(grouped.get(report.report_date) ?? []), report]));
    return [...grouped.entries()];
  }, [reports]);

  if (loadingReports && reports.length === 0) return <LoadingBlock label="Loading daily reports…" />;

  return (
    <div className="page-grid daily-reports-page">
      <Panel title="Daily VCP Reports" aside={<span className="eyebrow">After-close review</span>}>
        <p className="panel-copy">
          Ranked volatility-contraction setups with pivots, entry conditions, risk, and explicit invalidation levels.
        </p>
        {error ? <div className="notice-banner">{error}</div> : null}
      </Panel>

      {reports.length === 0 ? (
        <div className="empty-state">No daily reports have been published yet.</div>
      ) : (
        <div className="daily-report-layout">
          <Panel title="Archive" aside={<span className="eyebrow">{reports.length} reports</span>}>
            <div className="daily-report-archive">
              {reportsByDate.map(([date, dateReports]) => (
                <div className="daily-report-date-group" key={date}>
                  <div className="eyebrow">{formatLocalDate(date)}</div>
                  {dateReports.map((report) => (
                    <Link
                      key={`${report.report_date}:${report.agent_id}`}
                      className={`daily-report-link${report.report_date === reportDate && report.agent_id === agentId ? " is-active" : ""}`}
                      to={`/daily-reports/${report.report_date}/${report.agent_id}`}
                    >
                      <strong>{report.agent_name}</strong>
                      <span>{report.analyzed_count} analyzed · {report.group_counts.A + report.group_counts.B} ready</span>
                      <small>{report.model || report.top_tickers.join(" · ") || "No actionable candidates"}</small>
                    </Link>
                  ))}
                </div>
              ))}
            </div>
          </Panel>

          <div className="daily-report-detail">
            {loadingDetail ? <LoadingBlock label="Loading report…" /> : null}
            {!loadingDetail && detail ? (
              <>
                <Panel
                  title={detail.title}
                  aside={<span className="eyebrow">Trading date {formatLocalDate(detail.target_trading_date)}</span>}
                >
                  <div className="daily-report-meta">
                    <span>{detail.agent_name}</span>
                    {detail.model ? <span>{detail.model}</span> : null}
                    <span>{detail.candidate_count} candidates</span>
                    <span>{detail.analyzed_count} analyzed</span>
                    <span>Generated {formatLocalDateTime(detail.generated_at)}</span>
                  </div>
                  <p className="panel-copy">{detail.market_context || "No market context was included."}</p>
                </Panel>

                <Panel title="Ranked setups" aside={<span className="eyebrow">High to low conviction</span>}>
                  <div className="data-table-responsive daily-report-table-wrap">
                    <table className="data-table daily-report-table">
                      <thead>
                        <tr>
                          <th>Group</th><th>Ticker</th><th>Score</th><th>Last</th><th>Pivot</th><th>Stop</th><th>Risk</th><th>Reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {orderedCandidates.map((candidate) => (
                          <CandidateRow key={candidate.ticker} candidate={candidate} />
                        ))}
                      </tbody>
                    </table>
                  </div>
                </Panel>

                <div className="daily-report-cards">
                  {orderedCandidates.slice(0, 5).map((candidate) => (
                    <Panel
                      key={candidate.ticker}
                      title={candidate.ticker}
                      aside={<span className={`daily-report-group group-${candidate.group.toLowerCase()}`}>{candidate.group} · {groupLabels[candidate.group]}</span>}
                    >
                      <p className="panel-copy">{candidate.main_reason || "No setup summary supplied."}</p>
                      <ReportField label="Entry trigger" value={candidate.entry_trigger} />
                      <ReportField label="Pre-entry invalidation" value={candidate.pre_entry_invalidation} />
                      <ReportField label="Post-entry failure" value={candidate.post_entry_failure} />
                      <ReportField label="Next event" value={candidate.next_event} />
                      {candidate.counterargument ? <ReportField label="Strongest counterargument" value={candidate.counterargument} /> : null}
                      <Link className="text-link" to={`/charts?ticker=${encodeURIComponent(candidate.ticker)}`}>Open chart</Link>
                    </Panel>
                  ))}
                </div>

                <Panel title="Next-session watch plan" aside={<span className="eyebrow">Conditional actions</span>}>
                  {detail.watch_plan.length ? (
                    <ol className="daily-report-watch-plan">
                      {detail.watch_plan.map((item) => <li key={item}>{item}</li>)}
                    </ol>
                  ) : <div className="empty-state">No watch plan was included.</div>}
                </Panel>
              </>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}

function CandidateRow({ candidate }: { candidate: DailyReportCandidate }) {
  return (
    <tr>
      <td><span className={`daily-report-group group-${candidate.group.toLowerCase()}`}>{candidate.group}</span></td>
      <td><Link className="ticker-link" to={`/charts?ticker=${encodeURIComponent(candidate.ticker)}`}>{candidate.ticker}</Link></td>
      <td>{formatNumber(candidate.score)}</td>
      <td>{formatPrice(candidate.last_price)}</td>
      <td>{formatPrice(candidate.pivot)}</td>
      <td>{formatPrice(candidate.stop)}</td>
      <td>{candidate.risk_pct == null ? "—" : `${candidate.risk_pct.toFixed(1)}%`}</td>
      <td>{candidate.main_reason || "—"}</td>
    </tr>
  );
}

function ReportField({ label, value }: { label: string; value?: string | null }) {
  if (!value) return null;
  return <div className="daily-report-field"><span className="eyebrow">{label}</span><p>{value}</p></div>;
}

function formatPrice(value?: number | null) {
  return value == null ? "—" : `$${value.toFixed(2)}`;
}

function formatNumber(value?: number | null) {
  return value == null ? "—" : value.toFixed(0);
}
