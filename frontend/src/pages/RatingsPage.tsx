import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate } from "../lib/format";
import type { TopRatingEntry, TopRatingsResponse } from "../lib/types";

function formatScore(value: number | null | undefined): string {
  if (value == null) {
    return "--";
  }
  return value.toFixed(2);
}

function statusOptions(response: TopRatingsResponse | null) {
  const counts = response?.status_counts ?? {};
  return ["ok", ...Object.keys(counts).filter((key) => key !== "ok").sort()];
}

function buildRequestPath(asOfDate: string, limit: number, ratingStatus: string) {
  const query = new URLSearchParams();
  if (asOfDate.trim()) {
    query.set("asOfDate", asOfDate.trim());
  }
  query.set("limit", String(limit));
  if (ratingStatus.trim()) {
    query.set("ratingStatus", ratingStatus.trim());
  }
  return `/api/ratings/top?${query.toString()}`;
}

export function RatingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const requestedStatus = (searchParams.get("status") ?? "ok").trim().toLowerCase() || "ok";
  const requestedLimit = Math.min(500, Math.max(1, Number(searchParams.get("limit") ?? "100") || 100));
  const [payload, setPayload] = useState<TopRatingsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<TopRatingsResponse>(buildRequestPath(requestedDate, requestedLimit, requestedStatus))
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load top ratings.");
      })
      .finally(() => setIsLoading(false));
  }, [requestedDate, requestedLimit, requestedStatus]);

  const rows = payload?.rows ?? [];
  const bestOverall = useMemo(
    () => rows.reduce<number | null>((best, row) => (row.overall_rating != null && (best == null || row.overall_rating > best) ? row.overall_rating : best), null),
    [rows],
  );
  const visibleStatuses = statusOptions(payload);

  return (
    <div className="page-grid earnings-board weekly-watchlist-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Ratings Leaderboard</span>
          <h1>Top rated tickers</h1>
          <p className="panel-copy">
            Fast review board for the latest ticker ratings snapshots. Open charts from here, inspect grade balance, and sanity-check which names rise to the top.
          </p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Ratings Date</span>
            <strong>{formatLocalDate(payload?.as_of_date)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Status Filter</span>
            <strong>{requestedStatus || "all"}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Rows</span>
            <strong>{formatCount(rows.length)}</strong>
          </div>
          <div className="earnings-metric earnings-metric-highlight">
            <span className="eyebrow">Best Overall</span>
            <strong>{formatScore(bestOverall)}</strong>
          </div>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <div className="earnings-filter-console-row weekly-watchlist-console-row">
          <label className="field">
            <span>As Of Date</span>
            <input
              type="date"
              value={requestedDate}
              onChange={(event) => {
                const next = new URLSearchParams(searchParams);
                if (event.target.value) {
                  next.set("date", event.target.value);
                } else {
                  next.delete("date");
                }
                setSearchParams(next, { replace: true });
              }}
            />
          </label>
          <label className="field">
            <span>Status</span>
            <select
              value={requestedStatus}
              onChange={(event) => {
                const next = new URLSearchParams(searchParams);
                next.set("status", event.target.value);
                setSearchParams(next, { replace: true });
              }}
            >
              {visibleStatuses.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Limit</span>
            <select
              value={String(requestedLimit)}
              onChange={(event) => {
                const next = new URLSearchParams(searchParams);
                next.set("limit", event.target.value);
                setSearchParams(next, { replace: true });
              }}
            >
              {[25, 50, 100, 200].map((value) => (
                <option key={value} value={value}>
                  Top {value}
                </option>
              ))}
            </select>
          </label>
          <div className="weekly-watchlist-actions">
            <Link className="ghost-button" to="/scanner">
              Back To Scanner
            </Link>
          </div>
        </div>
        <p className="panel-copy earnings-console-note">
          Current dataset status mix:
          {" "}
          {Object.entries(payload?.status_counts ?? {})
            .map(([status, count]) => `${status} ${formatCount(count)}`)
            .join(" · ") || "No ratings status counts yet."}
        </p>
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>Leaderboard</h2>
            <span className="eyebrow">{rows.length} names</span>
          </div>
        </div>
        {isLoading ? <LoadingBlock label="Loading top ratings…" /> : null}
        {!isLoading && rows.length === 0 ? <p className="panel-copy">No ratings found for this date or status filter.</p> : null}
        {rows.length > 0 ? (
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>Overall</th>
                  <th>Valuation</th>
                  <th>Profitability</th>
                  <th>Growth</th>
                  <th>Performance</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row: TopRatingEntry, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{index + 1}</td>
                    <td data-label="Ticker">
                      <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="Overall">{formatScore(row.overall_rating)}</td>
                    <td data-label="Valuation">{row.valuation_grade ?? "-"} ({formatScore(row.valuation_score)})</td>
                    <td data-label="Profitability">{row.profitability_grade ?? "-"} ({formatScore(row.profitability_score)})</td>
                    <td data-label="Growth">{row.growth_grade ?? "-"} ({formatScore(row.growth_score)})</td>
                    <td data-label="Performance">{row.performance_grade ?? "-"} ({formatScore(row.performance_score)})</td>
                    <td data-label="Status">{row.rating_status ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
      </section>
    </div>
  );
}
