import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate } from "../lib/format";
import type {
  TopRatingEntry,
  TopRatingsResponse,
  TopTechnicalIndicatorRatingEntry,
  TopTechnicalIndicatorRatingsResponse,
  TopTechnicalRatingEntry,
  TopTechnicalRatingsResponse,
} from "../lib/types";

type RatingsMode = "fundamental" | "technical" | "technical-indicator";

type RatingRow = TopRatingEntry | TopTechnicalRatingEntry | TopTechnicalIndicatorRatingEntry;

const ALLOWED_LIMITS = [25, 50, 100, 200] as const;

const MODE_LABELS: Record<RatingsMode, string> = {
  fundamental: "Fundamentals",
  technical: "Technical leadership",
  "technical-indicator": "Multi-timeframe signals",
};

const REQUEST_PATHS: Record<RatingsMode, string> = {
  fundamental: "/api/ratings/top",
  technical: "/api/ratings/technical/top",
  "technical-indicator": "/api/ratings/technical-indicator/top",
};

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

function formatCanslimScore(score: number | null | undefined, maxScore: number | null | undefined): string {
  if (score == null) {
    return "--";
  }
  return `${score}/${maxScore ?? 14}`;
}

function formatMetricCoverage(coverage: TopRatingEntry["metric_coverage"]): string {
  return coverage ? `${coverage.available}/${coverage.total}` : "--";
}

function normalizeLimit(value: string | null): number {
  const candidate = Number(value ?? "100");
  return ALLOWED_LIMITS.includes(candidate as (typeof ALLOWED_LIMITS)[number]) ? candidate : 100;
}

function rowOverallScore(row: RatingRow, mode: RatingsMode): number | null | undefined {
  return mode === "technical-indicator"
    ? (row as TopTechnicalIndicatorRatingEntry).daily.overall_score
    : (row as TopRatingEntry | TopTechnicalRatingEntry).overall_rating;
}

function formatStatusLabel(value: string): string {
  return value.replace(/_/g, " ");
}

function formatDataQualityLabel(value: string): string {
  if (value === "ok") {
    return "rated";
  }
  return formatStatusLabel(value);
}

function buildRequestPath(mode: RatingsMode, limit: number, sectors: string[]) {
  const query = new URLSearchParams();
  query.set("limit", String(limit));
  if (sectors.length) {
    query.set("sector", sectors.join(","));
  }
  return `${REQUEST_PATHS[mode]}?${query.toString()}`;
}

function parseSectors(value: string | null): string[] {
  return [...new Set((value ?? "").split(",").map((sector) => sector.trim().toLowerCase()).filter(Boolean))];
}

function isSectorOption(value: string): boolean {
  return !/^\(?[+-]?\d+(?:\.\d+)?%\)?$/.test(value.trim());
}

function normalizeMode(value: string | null): RatingsMode {
  if (value === "technical") {
    return "technical";
  }
  if (value === "technical-indicator") {
    return "technical-indicator";
  }
  return "fundamental";
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
  const requestedMode = searchParams.get("mode");
  const requestedLimitParam = searchParams.get("limit");
  const requestedSort = searchParams.get("sort");
  const requestedSectorParam = searchParams.get("sector");
  const mode = normalizeMode(requestedMode);
  const requestedSectors = useMemo(() => parseSectors(requestedSectorParam), [requestedSectorParam]);
  const requestedLimit = normalizeLimit(requestedLimitParam);
  const tickerQuery = (searchParams.get("q") ?? "").trim();
  const sortBy = requestedSort === "overall" ? "overall" : "rank";
  const [fundamentalPayload, setFundamentalPayload] = useState<TopRatingsResponse | null>(null);
  const [technicalPayload, setTechnicalPayload] = useState<TopTechnicalRatingsResponse | null>(null);
  const [technicalIndicatorPayload, setTechnicalIndicatorPayload] = useState<TopTechnicalIndicatorRatingsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [announcement, setAnnouncement] = useState("");
  const [retryKey, setRetryKey] = useState(0);

  useEffect(() => {
    if (
      (requestedMode && requestedMode !== mode)
      || (requestedLimitParam && requestedLimitParam !== String(requestedLimit))
      || (requestedSort && requestedSort !== sortBy)
    ) {
      const next = new URLSearchParams(searchParams);
      requestedMode !== mode && next.delete("mode");
      requestedLimitParam !== String(requestedLimit) && next.delete("limit");
      requestedSort !== sortBy && next.delete("sort");
      setSearchParams(next, { replace: true });
    }
  }, [mode, requestedLimit, requestedLimitParam, requestedMode, requestedSort, searchParams, setSearchParams, sortBy]);

  useEffect(() => {
    let ignore = false;
    setIsLoading(true);
    setNotice("");
    const primaryRequest =
      mode === "technical"
        ? fetchJson<TopTechnicalRatingsResponse>(buildRequestPath(mode, requestedLimit, requestedSectors))
        : mode === "technical-indicator"
          ? fetchJson<TopTechnicalIndicatorRatingsResponse>(buildRequestPath(mode, requestedLimit, requestedSectors))
          : fetchJson<TopRatingsResponse>(buildRequestPath(mode, requestedLimit, requestedSectors));
    void primaryRequest
      .then((response) => {
        if (ignore) {
          return;
        }
        if (mode === "technical") {
          setTechnicalPayload(response as TopTechnicalRatingsResponse);
        } else if (mode === "technical-indicator") {
          setTechnicalIndicatorPayload(response as TopTechnicalIndicatorRatingsResponse);
        } else {
          setFundamentalPayload(response as TopRatingsResponse);
        }
        setAnnouncement(`${formatCount(response.rows.length)} ${MODE_LABELS[mode].toLowerCase()} ratings loaded.`);
      })
      .catch(() => {
        if (ignore) {
          return;
        }
        setNotice(`We couldn't load ${MODE_LABELS[mode].toLowerCase()} ratings. Check the connection and try again.`);
      })
      .finally(() => {
        if (!ignore) {
          setIsLoading(false);
        }
      });
    return () => {
      ignore = true;
    };
  }, [mode, requestedLimit, requestedSectors, retryKey]);

  const payload = mode === "technical" ? technicalPayload : mode === "technical-indicator" ? technicalIndicatorPayload : fundamentalPayload;
  const rows = mode === "technical" ? (technicalPayload?.rows ?? []) : mode === "technical-indicator" ? (technicalIndicatorPayload?.rows ?? []) : (fundamentalPayload?.rows ?? []);
  const visibleSectors = (payload?.sector_options ?? []).filter(isSectorOption);
  const visibleRows = useMemo(() => {
    const normalizedQuery = tickerQuery.toUpperCase();
    const filtered = normalizedQuery
      ? rows.filter((row) => row.ticker.toUpperCase().includes(normalizedQuery))
      : rows;
    if (sortBy === "rank") {
      return filtered;
    }
    return [...filtered].sort((left, right) => (rowOverallScore(right, mode) ?? -Infinity) - (rowOverallScore(left, mode) ?? -Infinity));
  }, [mode, rows, sortBy, tickerQuery]);
  const bestOverall = useMemo(
    () =>
      rows.reduce<number | null>((best, row) => {
        const candidate = rowOverallScore(row, mode);
        return candidate != null && (best == null || candidate > best) ? candidate : best;
      }, null),
    [mode, rows],
  );
  const heroTitle =
    mode === "technical"
      ? "Top technical rated tickers"
      : mode === "technical-indicator"
        ? "Multi-timeframe technical ratings"
        : "Top rated tickers";
  const heroCopy =
    mode === "technical"
      ? "Fast review board for technical leadership snapshots. Focus on trend health, MA behavior, RS leadership, and extension risk."
      : mode === "technical-indicator"
        ? "TradingView-style composite ratings across daily, weekly, and monthly timeframes. Review labels and raw scores side by side."
      : "Fast review board for the latest ticker ratings snapshots. Open charts from here, inspect grade balance, and sanity-check which names rise to the top.";
  const isRefreshing = isLoading && rows.length > 0;
  const activeFilterCount = Number(requestedSectors.length > 0) + Number(Boolean(tickerQuery)) + Number(sortBy !== "rank") + Number(mode !== "fundamental") + Number(requestedLimit !== 100);

  function updateParam(key: string, value: string | null) {
    const next = new URLSearchParams(searchParams);
    if (value && value.trim()) {
      next.set(key, value);
    } else {
      next.delete(key);
    }
    setSearchParams(next, { replace: true });
  }

  function resetFilters() {
    setSearchParams(new URLSearchParams(), { replace: true });
  }

  function toggleSector(sector: string) {
    const normalizedSector = sector.toLowerCase();
    const next = new Set(requestedSectors);
    next.has(normalizedSector) ? next.delete(normalizedSector) : next.add(normalizedSector);
    updateParam("sector", [...next].sort().join(",") || null);
  }

  return (
    <div className="page-grid earnings-board weekly-watchlist-board">
      <section className="earnings-board-hero ratings-hero">
        <div className="earnings-board-hero-copy">
          <h1>{heroTitle}</h1>
          <p className="panel-copy">{heroCopy}</p>
          <p className="ratings-freshness">
            Data through <strong>{formatLocalDate(payload?.as_of_date)}</strong>
            {payload?.previous_as_of_date ? <> · compared with <strong>{formatLocalDate(payload.previous_as_of_date)}</strong></> : null}
            {rows.length > 0 ? <> · {formatCount(rows.length)} ranked names</> : null}
          </p>
        </div>
        <div className="ratings-best-score" aria-label={`Highest overall score ${formatScore(bestOverall)}`}>
          <span>Highest score</span>
          <strong>{formatScore(bestOverall)}</strong>
          <small>{MODE_LABELS[mode]}</small>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <div className="ratings-filter-grid">
          <label className="field">
            <span>Mode</span>
            <select value={mode} onChange={(event) => updateParam("mode", event.target.value === "fundamental" ? null : event.target.value)}>
              <option value="fundamental">Fundamentals</option>
              <option value="technical">Technical leadership</option>
              <option value="technical-indicator">Multi-timeframe signals</option>
            </select>
          </label>
          <label className="field">
            <span>Limit</span>
            <select value={String(requestedLimit)} onChange={(event) => updateParam("limit", event.target.value)}>
              {ALLOWED_LIMITS.map((value) => (
                <option key={value} value={value}>
                  Top {value}
                </option>
              ))}
            </select>
          </label>
          <div className="field ratings-sector-filter">
            <span>Sector</span>
            <details>
              <summary>{requestedSectors.length ? `${requestedSectors.length} sector${requestedSectors.length === 1 ? "" : "s"} selected` : "All sectors"}</summary>
              <div className="ratings-sector-options">
                {visibleSectors.map((sector) => (
                  <label key={sector} className="ratings-sector-option">
                    <input type="checkbox" checked={requestedSectors.includes(sector.toLowerCase())} onChange={() => toggleSector(sector)} />
                    {sector}
                  </label>
                ))}
              </div>
            </details>
          </div>
          <label className="field">
            <span>Find ticker</span>
            <input
              type="search"
              value={tickerQuery}
              placeholder="e.g. NVDA"
              onChange={(event) => updateParam("q", event.target.value || null)}
            />
          </label>
          <label className="field">
            <span>Sort</span>
            <select value={sortBy} onChange={(event) => updateParam("sort", event.target.value === "rank" ? null : event.target.value)}>
              <option value="rank">Leaderboard rank</option>
              <option value="overall">Overall score</option>
            </select>
          </label>
        </div>
        <div className="ratings-filter-footer">
          <details className="ratings-quality-disclosure">
            <summary>About ratings and data quality</summary>
            <div className="ratings-quality-copy">
              <p><strong>Overall</strong> combines the factors shown in each row. <strong>Coverage</strong> is the number of available fundamental inputs. 1D, 1W, and 1M are daily, weekly, and monthly technical signals.</p>
              <p>
                Data quality:{" "}
                {Object.entries(payload?.status_counts ?? {})
                  .map(([status, count]) => `${formatCount(count)} ${formatDataQualityLabel(status)}`)
                  .join(" · ") || "Waiting for rating status counts."}
              </p>
            </div>
          </details>
          <div className="ratings-filter-actions">
            {activeFilterCount > 0 ? (
              <button className="ghost-button" type="button" onClick={resetFilters}>
                Reset filters ({activeFilterCount})
              </button>
            ) : null}
            <Link className="ratings-back-link" to="/scanner">Back to scanner</Link>
          </div>
        </div>
        {notice ? (
          <div className="ratings-error" role="alert">
            <span>{notice}</span>
            <button className="ghost-button" type="button" onClick={() => setRetryKey((value) => value + 1)}>Try again</button>
          </div>
        ) : null}
      </section>

      <section className="panel earnings-calendar-panel ratings-leaderboard" aria-busy={isLoading}>
        <div className="panel-head earnings-calendar-head">
          <div>
            <h2>{mode === "technical" ? "Technical Leaderboard" : mode === "technical-indicator" ? "Multi-Timeframe Technical Ratings" : "Leaderboard"}</h2>
            <span className="eyebrow">{visibleRows.length} of {rows.length} names</span>
          </div>
        </div>
        <span className="visually-hidden" role="status" aria-live="polite">{announcement}</span>
        {isLoading && rows.length === 0 ? <LoadingBlock label={mode === "technical" ? "Loading technical leaders…" : mode === "technical-indicator" ? "Loading multi-timeframe signals…" : "Loading fundamental leaders…"} /> : null}
        {isRefreshing ? <div className="ratings-refreshing" role="status">Updating leaderboard… Current results remain visible until the refresh finishes.</div> : null}
        {!isLoading && rows.length === 0 && !notice ? (
          <div className="ratings-empty-state">
            <strong>No {MODE_LABELS[mode].toLowerCase()} ratings match these filters.</strong>
            <span>Reset the filters or choose a broader sector to see ranked names.</span>
            <button className="ghost-button" type="button" onClick={resetFilters}>Reset filters</button>
          </div>
        ) : null}
        {!isLoading && rows.length > 0 && visibleRows.length === 0 ? (
          <div className="ratings-empty-state">
            <strong>No ticker matches “{tickerQuery}”.</strong>
            <span>Try another symbol or clear the ticker search.</span>
            <button className="ghost-button" type="button" onClick={() => updateParam("q", null)}>Clear ticker search</button>
          </div>
        ) : null}
        {visibleRows.length > 0 && mode === "fundamental" ? (
          <div className="data-table-responsive ratings-table-wrap">
            <table className="data-table ratings-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>Overall</th>
                  <th>Rank change</th>
                  <th>Coverage</th>
                  <th>1Y return</th>
                  <th>Factors</th>
                  <th>Chart</th>
                </tr>
              </thead>
              <tbody>
                {(visibleRows as TopRatingEntry[]).map((row, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{row.current_rank ?? index + 1}</td>
                    <td data-label="Ticker">
                      <Link className="ratings-ticker-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="Overall">{formatScore(row.overall_rating)}</td>
                    <td data-label="Rank Change" title={rankChangeTitle(row)}>{formatRankChange(row)}</td>
                    <td data-label="Coverage" title="Available fundamental metrics / total rating metrics">{formatMetricCoverage(row.metric_coverage)}</td>
                    <td data-label="1Y Return">{formatPercent(row.perf_year_pct)}</td>
                    <td data-label="Factors">
                      <details className="ratings-factor-disclosure">
                        <summary>View factors</summary>
                        <dl>
                          <div><dt>YTD return</dt><dd>{formatPercent(row.perf_ytd_pct)}</dd></div>
                          <div><dt>Scanner hits</dt><dd>{formatCount(row.latest_scanner_hit_count ?? 0)}</dd></div>
                          <div><dt>Daily signal</dt><dd>{row.technical_indicator_ratings?.["1d"]?.rating_label ?? "-"}</dd></div>
                          <div><dt>Weekly signal</dt><dd>{row.technical_indicator_ratings?.["1w"]?.rating_label ?? "-"}</dd></div>
                          <div><dt>CANSLIM</dt><dd>{formatCanslimScore(row.canslim_score, row.canslim_max_score)}</dd></div>
                          <div><dt>Valuation</dt><dd>{row.valuation_grade ?? "-"} ({formatScore(row.valuation_score)})</dd></div>
                          <div><dt>Profitability</dt><dd>{row.profitability_grade ?? "-"} ({formatScore(row.profitability_score)})</dd></div>
                          <div><dt>Growth</dt><dd>{row.growth_grade ?? "-"} ({formatScore(row.growth_score)})</dd></div>
                          <div><dt>Performance</dt><dd>{row.performance_grade ?? "-"} ({formatScore(row.performance_score)})</dd></div>
                          <div><dt>Data status</dt><dd>{formatStatusLabel(row.rating_status ?? "unknown")}</dd></div>
                        </dl>
                      </details>
                    </td>
                    <td data-label="Chart"><Link className="ratings-chart-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>Open chart</Link></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
        {visibleRows.length > 0 && mode === "technical" ? (
          <div className="data-table-responsive ratings-table-wrap">
            <table className="data-table ratings-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>Overall</th>
                  <th>Rank change</th>
                  <th>Band</th>
                  <th>Status</th>
                  <th>Factors</th>
                  <th>Chart</th>
                </tr>
              </thead>
              <tbody>
                {(visibleRows as TopTechnicalRatingEntry[]).map((row, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{row.current_rank ?? index + 1}</td>
                    <td data-label="Ticker">
                      <Link className="ratings-ticker-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="Overall">{formatScore(row.overall_rating)}</td>
                    <td data-label="Rank Change" title={rankChangeTitle(row)}>{formatRankChange(row)}</td>
                    <td data-label="Band">{row.rating_band ?? "-"}</td>
                    <td data-label="Status">{formatStatusLabel(row.technical_status ?? "unknown")}</td>
                    <td data-label="Factors">
                      <details className="ratings-factor-disclosure">
                        <summary>View factors</summary>
                        <dl>
                          <div><dt>Daily signal</dt><dd>{row.technical_indicator_ratings?.["1d"]?.rating_label ?? "-"}</dd></div>
                          <div><dt>Weekly signal</dt><dd>{row.technical_indicator_ratings?.["1w"]?.rating_label ?? "-"}</dd></div>
                          <div><dt>CANSLIM</dt><dd>{formatCanslimScore(row.canslim_score, row.canslim_max_score)}</dd></div>
                          <div><dt>Trend</dt><dd>{formatScore(row.trend_regime_score)}</dd></div>
                          <div><dt>DMA speed</dt><dd>{formatScore(row.dma_speed_score)}</dd></div>
                          <div><dt>Divergence</dt><dd>{formatScore(row.divergence_health_score)}</dd></div>
                          <div><dt>Leadership</dt><dd>{formatScore(row.leadership_score)}</dd></div>
                          <div><dt>Structure / volume</dt><dd>{formatScore(row.structure_volume_score)}</dd></div>
                          <div><dt>Flags</dt><dd>{row.flags.length > 0 ? row.flags.join(", ") : "None"}</dd></div>
                        </dl>
                      </details>
                    </td>
                    <td data-label="Chart"><Link className="ratings-chart-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>Open chart</Link></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
        {visibleRows.length > 0 && mode === "technical-indicator" ? (
          <div className="data-table-responsive ratings-table-wrap">
            <table className="data-table ratings-table">
              <thead>
                <tr>
                  <th>#</th>
                  <th>Ticker</th>
                  <th>Sector / Industry</th>
                  <th>1D</th>
                  <th>1W</th>
                  <th>1M</th>
                  <th>Status</th>
                  <th>Factors</th>
                  <th>Chart</th>
                </tr>
              </thead>
              <tbody>
                {(visibleRows as TopTechnicalIndicatorRatingEntry[]).map((row, index) => (
                  <tr key={`${row.ticker}-${row.as_of_date}`}>
                    <td data-label="#">{row.current_rank ?? index + 1}</td>
                    <td data-label="Ticker">
                      <Link className="ratings-ticker-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>{row.ticker}</Link>
                    </td>
                    <td data-label="Sector / Industry">
                      {[row.sector, row.industry].filter(Boolean).join(" / ") || "-"}
                    </td>
                    <td data-label="1D">{row.daily.rating_label ?? "-"}</td>
                    <td data-label="1W">{row.weekly.rating_label ?? "-"}</td>
                    <td data-label="1M">{row.monthly.rating_label ?? "-"}</td>
                    <td data-label="Status">{formatStatusLabel(row.combined_status)}</td>
                    <td data-label="Factors">
                      <details className="ratings-factor-disclosure">
                        <summary>View scores</summary>
                        <dl>
                          <div><dt>Daily score</dt><dd>{formatScore(row.daily.overall_score)}</dd></div>
                          <div><dt>Weekly score</dt><dd>{formatScore(row.weekly.overall_score)}</dd></div>
                          <div><dt>Monthly score</dt><dd>{formatScore(row.monthly.overall_score)}</dd></div>
                          <div><dt>CANSLIM</dt><dd>{formatCanslimScore(row.canslim_score, row.canslim_max_score)}</dd></div>
                          <div><dt>Rank change</dt><dd title={rankChangeTitle(row)}>{formatRankChange(row)}</dd></div>
                        </dl>
                      </details>
                    </td>
                    <td data-label="Chart"><Link className="ratings-chart-link" to={`/charts?ticker=${encodeURIComponent(row.ticker)}`}>Open chart</Link></td>
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
