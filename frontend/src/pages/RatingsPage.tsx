import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate } from "../lib/format";
import type { TopRatingEntry, TopRatingsResponse, TopTechnicalRatingEntry, TopTechnicalRatingsResponse } from "../lib/types";

type RatingsMode = "fundamental" | "technical";

function formatScore(value: number | null | undefined): string {
  if (value == null) {
    return "--";
  }
  return value.toFixed(2);
}

function formatPercent(value: number | null | undefined): string {
  if (value == null) {
    return "--";
  }
  return `${value.toFixed(2)}%`;
}

function buildFundamentalRequestPath(asOfDate: string, limit: number, ratingStatus: string, sector: string) {
  const query = new URLSearchParams();
  if (asOfDate.trim()) {
    query.set("asOfDate", asOfDate.trim());
  }
  query.set("limit", String(limit));
  if (ratingStatus.trim()) {
    query.set("ratingStatus", ratingStatus.trim());
  }
  if (sector.trim()) {
    query.set("sector", sector.trim());
  }
  return `/api/ratings/top?${query.toString()}`;
}

function buildTechnicalRequestPath(asOfDate: string, limit: number, technicalStatus: string, sector: string) {
  const query = new URLSearchParams();
  if (asOfDate.trim()) {
    query.set("asOfDate", asOfDate.trim());
  }
  query.set("limit", String(limit));
  if (technicalStatus.trim()) {
    query.set("technicalStatus", technicalStatus.trim());
  }
  if (sector.trim()) {
    query.set("sector", sector.trim());
  }
  return `/api/ratings/technical/top?${query.toString()}`;
}

function statusOptions(response: TopRatingsResponse | TopTechnicalRatingsResponse | null, preferredKey: "ok" = "ok") {
  const counts = response?.status_counts ?? {};
  return [preferredKey, ...Object.keys(counts).filter((key) => key !== preferredKey).sort()];
}

function normalizeMode(value: string | null): RatingsMode {
  return value === "technical" ? "technical" : "fundamental";
}

function formatRankChange(row: Pick<TopRatingEntry, "current_rank" | "previous_rank" | "rank_change" | "rank_delta">): string {
  if (row.rank_change === "new") {
    return "New";
  }
  if (row.rank_change === "same") {
    return "Same";
  }
  return `${row.rank_change === "up" ? "Up" : "Down"} ${Math.abs(row.rank_delta ?? 0)}`;
}

function rankChangeTitle(row: Pick<TopRatingEntry, "current_rank" | "previous_rank" | "rank_change" | "rank_delta">): string {
  if (row.rank_change === "new") {
    return `New on board at #${row.current_rank}`;
  }
  if (row.rank_change === "same") {
    return `Held rank #${row.current_rank}`;
  }
  return `Moved from #${row.previous_rank ?? "-"} to #${row.current_rank}`;
}

export function RatingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const mode = normalizeMode(searchParams.get("mode"));
  const requestedDate = (searchParams.get("date") ?? "").trim();
  const requestedStatus = (searchParams.get("status") ?? "ok").trim().toLowerCase() || "ok";
  const requestedSector = (searchParams.get("sector") ?? "").trim();
  const requestedLimit = Math.min(500, Math.max(1, Number(searchParams.get("limit") ?? "100") || 100));
  const [fundamentalPayload, setFundamentalPayload] = useState<TopRatingsResponse | null>(null);
  const [technicalPayload, setTechnicalPayload] = useState<TopTechnicalRatingsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    const request =
      mode === "technical"
        ? fetchJson<TopTechnicalRatingsResponse>(buildTechnicalRequestPath(requestedDate, requestedLimit, requestedStatus, requestedSector))
        : fetchJson<TopRatingsResponse>(buildFundamentalRequestPath(requestedDate, requestedLimit, requestedStatus, requestedSector));
    void request
      .then((response) => {
        if (mode === "technical") {
          setTechnicalPayload(response as TopTechnicalRatingsResponse);
        } else {
          setFundamentalPayload(response as TopRatingsResponse);
        }
      })
      .catch((error) => {
        if (mode === "technical") {
          setTechnicalPayload(null);
        } else {
          setFundamentalPayload(null);
        }
        setNotice(error instanceof Error ? error.message : `Failed to load ${mode} ratings.`);
      })
      .finally(() => setIsLoading(false));
  }, [mode, requestedDate, requestedLimit, requestedSector, requestedStatus]);

  const payload = mode === "technical" ? technicalPayload : fundamentalPayload;
  const rows = mode === "technical" ? (technicalPayload?.rows ?? []) : (fundamentalPayload?.rows ?? []);
  const visibleSectors = payload?.sector_options ?? [];
  const bestOverall = useMemo(
    () => rows.reduce<number | null>((best, row) => (row.overall_rating != null && (best == null || row.overall_rating > best) ? row.overall_rating : best), null),
    [rows],
  );
  const visibleStatuses = statusOptions(payload);
  const heroTitle = mode === "technical" ? "Top technical rated tickers" : "Top rated tickers";
  const heroCopy =
    mode === "technical"
      ? "Fast review board for technical leadership snapshots. Focus on trend health, MA behavior, RS leadership, and extension risk."
      : "Fast review board for the latest ticker ratings snapshots. Open charts from here, inspect grade balance, and sanity-check which names rise to the top.";

  function updateParam(key: string, value: string | null) {
    const next = new URLSearchParams(searchParams);
    if (value && value.trim()) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    setSearchParams(next, { replace: true });
  }

  return (
    <div className="page-grid earnings-board weekly-watchlist-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Ratings Leaderboard</span>
          <h1>{heroTitle}</h1>
          <p className="panel-copy">{heroCopy}</p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Ratings Date</span>
            <strong>{formatLocalDate(payload?.as_of_date)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Mode</span>
            <strong>{mode}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Rows</span>
            <strong>{formatCount(rows.length)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Prev Compare</span>
            <strong>{formatLocalDate(payload?.previous_as_of_date)}</strong>
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
            <span>Mode</span>
            <select value={mode} onChange={(event) => updateParam("mode", event.target.value === "fundamental" ? null : event.target.value)}>
              <option value="fundamental">fundamental</option>
              <option value="technical">technical</option>
            </select>
          </label>
          <label className="field">
            <span>As Of Date</span>
            <input type="date" value={requestedDate} onChange={(event) => updateParam("date", event.target.value || null)} />
          </label>
          <label className="field">
            <span>Status</span>
            <select value={requestedStatus} onChange={(event) => updateParam("status", event.target.value)}>
              {visibleStatuses.map((status) => (
                <option key={status} value={status}>
                  {status}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Limit</span>
            <select value={String(requestedLimit)} onChange={(event) => updateParam("limit", event.target.value)}>
              {[25, 50, 100, 200].map((value) => (
                <option key={value} value={value}>
                  Top {value}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Sector</span>
            <select value={requestedSector} onChange={(event) => updateParam("sector", event.target.value || null)}>
              <option value="">All sectors</option>
              {visibleSectors.map((sector) => (
                <option key={sector} value={sector}>
                  {sector}
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
        {requestedSector ? <p className="panel-copy earnings-console-note">Sector filter: {requestedSector}</p> : null}
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>{mode === "technical" ? "Technical Leaderboard" : "Leaderboard"}</h2>
            <span className="eyebrow">{rows.length} names</span>
          </div>
        </div>
        {isLoading ? <LoadingBlock label={mode === "technical" ? "Loading top technical ratings…" : "Loading top ratings…"} /> : null}
        {!isLoading && rows.length === 0 ? <p className="panel-copy">No {mode} ratings found for this date or status filter.</p> : null}
        {rows.length > 0 && mode === "fundamental" ? (
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Rank Change</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>1Y %</th>
                  <th>YTD %</th>
                  <th>Overall</th>
                  <th>Valuation</th>
                  <th>Profitability</th>
                  <th>Growth</th>
                  <th>Performance</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {(rows as TopRatingEntry[]).map((row, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{row.current_rank ?? index + 1}</td>
                    <td data-label="Rank Change" title={rankChangeTitle(row)}>{formatRankChange(row)}</td>
                    <td data-label="Ticker">
                      <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="1Y %">{formatPercent(row.perf_year_pct)}</td>
                    <td data-label="YTD %">{formatPercent(row.perf_ytd_pct)}</td>
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
        {rows.length > 0 && mode === "technical" ? (
          <div className="data-table-responsive">
            <table className="data-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Rank Change</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>Overall</th>
                  <th>Band</th>
                  <th>Trend</th>
                  <th>DMA Speed</th>
                  <th>Divergence</th>
                  <th>Leadership</th>
                  <th>Structure / Volume</th>
                  <th>Flags</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {(rows as TopTechnicalRatingEntry[]).map((row, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{row.current_rank ?? index + 1}</td>
                    <td data-label="Rank Change" title={rankChangeTitle(row)}>{formatRankChange(row)}</td>
                    <td data-label="Ticker">
                      <Link to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="Overall">{formatScore(row.overall_rating)}</td>
                    <td data-label="Band">{row.rating_band ?? "-"}</td>
                    <td data-label="Trend">{formatScore(row.trend_regime_score)}</td>
                    <td data-label="DMA Speed">{formatScore(row.dma_speed_score)}</td>
                    <td data-label="Divergence">{formatScore(row.divergence_health_score)}</td>
                    <td data-label="Leadership">{formatScore(row.leadership_score)}</td>
                    <td data-label="Structure / Volume">{formatScore(row.structure_volume_score)}</td>
                    <td data-label="Flags">{row.flags.length > 0 ? row.flags.join(", ") : "-"}</td>
                    <td data-label="Status">{row.technical_status ?? "-"}</td>
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
