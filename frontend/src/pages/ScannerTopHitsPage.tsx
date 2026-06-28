import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { LoadingBlock } from "../components/LoadingBlock";
import { PaginationControls } from "../components/PaginationControls";
import { fetchJson } from "../lib/api";
import { formatCount, formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { MyPicksContextResponse, ScannerTopHitRow, ScannerTopHitsResponse, TechnicalIndicatorRatingCell } from "../lib/types";

type SortKey = "hits" | "ticker" | "sector" | "sectorTopHit" | "industryTopHit" | "close" | "change" | "rs" | "ta" | "fa";
type SortDirection = "asc" | "desc";
const PAGE_SIZE = 50;

export function ScannerTopHitsPage() {
  const auth = useAuth();
  const [payload, setPayload] = useState<ScannerTopHitsResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [myPicksNotice, setMyPicksNotice] = useState("");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("all");
  const [eliteOnly, setEliteOnly] = useState(false);
  const [sortBy, setSortBy] = useState<SortKey>("hits");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [currentPage, setCurrentPage] = useState(1);
  const [myPickTickers, setMyPickTickers] = useState<Set<string>>(new Set());
  const [savingMyPickTickers, setSavingMyPickTickers] = useState<Record<string, boolean>>({});
  const canManageMyPicks = auth.hasCapability("manage_exclusions");

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<ScannerTopHitsResponse>("/api/scanner-board/top-hits")
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load scanner top hits.");
      })
      .finally(() => setIsLoading(false));
  }, []);

  useEffect(() => {
    if (!canManageMyPicks) {
      setMyPickTickers(new Set());
      return;
    }
    void fetchJson<MyPicksContextResponse>("/api/admin/my-picks")
      .then((response) => {
        setMyPickTickers(new Set(response.rows.map((row) => row.ticker.toUpperCase())));
      })
      .catch(() => {
        setMyPickTickers(new Set());
      });
  }, [canManageMyPicks]);

  const rows = payload?.rows ?? [];
  const sectors = useMemo(
    () => Array.from(new Set(rows.map((row) => row.sector).filter((sector) => sector && sector !== "Unknown sector"))).sort(),
    [rows],
  );

  const filteredRows = useMemo(() => {
    const query = search.trim().toLowerCase();
    let nextRows = rows;
    if (sectorFilter !== "all") {
      nextRows = nextRows.filter((row) => row.sector === sectorFilter);
    }
    if (query) {
      nextRows = nextRows.filter((row) =>
        [row.ticker, row.company, row.sector, row.industry, row.scanner_labels.join(" ")].join(" ").toLowerCase().includes(query),
      );
    }
    if (eliteOnly) {
      nextRows = nextRows.filter(isElitePick);
    }
    return [...nextRows].sort((left, right) => compareRows(left, right, sortBy, sortDirection, {
      sectorLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeSectorKey(item.sector)) : new Map<string, string>(),
      industryLeaders: eliteOnly ? buildEliteLeaderMap(nextRows, (item) => normalizeIndustryKey(item.industry)) : new Map<string, string>(),
    }));
  }, [eliteOnly, rows, search, sectorFilter, sortBy, sortDirection]);

  useEffect(() => {
    setCurrentPage(1);
  }, [eliteOnly, search, sectorFilter, sortBy, sortDirection]);

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / PAGE_SIZE));
  const normalizedPage = Math.min(currentPage, totalPages);
  const pagedRows = useMemo(() => {
    const startIndex = (normalizedPage - 1) * PAGE_SIZE;
    return filteredRows.slice(startIndex, startIndex + PAGE_SIZE);
  }, [filteredRows, normalizedPage]);

  const eliteIndustryLeaders = useMemo(() => {
    if (!eliteOnly) {
      return new Map<string, string>();
    }
    return buildEliteLeaderMap(filteredRows, (row) => normalizeIndustryKey(row.industry));
  }, [eliteOnly, filteredRows]);

  const eliteSectorLeaders = useMemo(() => {
    if (!eliteOnly) {
      return new Map<string, string>();
    }
    return buildEliteLeaderMap(filteredRows, (row) => normalizeSectorKey(row.sector));
  }, [eliteOnly, filteredRows]);

  useEffect(() => {
    if (currentPage !== normalizedPage) {
      setCurrentPage(normalizedPage);
    }
  }, [currentPage, normalizedPage]);

  const handleAddToMyPicks = async (ticker: string) => {
    const normalizedTicker = ticker.trim().toUpperCase();
    if (!normalizedTicker || myPickTickers.has(normalizedTicker) || savingMyPickTickers[normalizedTicker]) {
      return;
    }
    setSavingMyPickTickers((current) => ({ ...current, [normalizedTicker]: true }));
    setMyPicksNotice("");
    try {
      await fetchJson<{ ok: boolean; pick: { ticker: string } }>("/api/admin/my-picks", {
        method: "POST",
        body: JSON.stringify({
          ticker: normalizedTicker,
          notes: "Added from scanner top hits.",
        }),
      });
      setMyPickTickers((current) => new Set([...current, normalizedTicker]));
      setMyPicksNotice(`${normalizedTicker} added to My Picks.`);
    } catch (error) {
      setMyPicksNotice(error instanceof Error ? error.message : "Failed to add ticker to My Picks.");
    } finally {
      setSavingMyPickTickers((current) => {
        const next = { ...current };
        delete next[normalizedTicker];
        return next;
      });
    }
  };

  return (
    <div className="page-grid scanner-top-hits-page">
      <section className="scanner-result-hero panel">
        <div className="scanner-result-breadcrumbs">
          <Link to="/">Dashboard</Link>
          <span>›</span>
          <Link to="/scanner">Stock Scanner</Link>
          <span>›</span>
          <span>Top Hits</span>
        </div>
        <div className="scanner-result-title-row">
          <div>
            <span className="scanner-result-kicker">Overlap Radar</span>
            <h1>Scanner top hits</h1>
          </div>
          <span className={`scanner-result-status${rows.length > 0 ? " is-live" : ""}`}>{rows.length > 0 ? "Overlap Found" : "No Overlap"}</span>
        </div>
        <p className="scanner-result-copy">Top hit = same ticker flagged by multiple live daily scanner boards. Sector momentum uses weekly sector RRG snapshot.</p>
        <div className="scanner-result-metrics">
          <div className="scanner-result-metric">
            <span className="eyebrow">Unique Tickers</span>
            <strong>{formatCount(payload?.total_unique_tickers ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Overlap Names</span>
            <strong>{formatCount(payload?.overlapping_ticker_count ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Daily Scanners</span>
            <strong>{formatCount(payload?.total_live_scanners ?? 0)}</strong>
          </div>
          <div className="scanner-result-metric">
            <span className="eyebrow">Signal Date</span>
            <strong>{formatLocalDate(payload?.latest_signal_date)}</strong>
          </div>
        </div>
      </section>

      <section className="scanner-result-filter-grid">
        <label className="scanner-result-filter panel">
          <span>Search</span>
          <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Ticker, sector, scanner…" />
        </label>
        <label className="scanner-result-filter panel">
          <span>Sector</span>
          <select value={sectorFilter} onChange={(event) => setSectorFilter(event.target.value)}>
            <option value="all">All sectors</option>
            {sectors.map((sector) => (
              <option key={sector} value={sector}>
                {sector}
              </option>
            ))}
          </select>
        </label>
        <label className="scanner-result-filter panel">
          <span>Elite Pick</span>
          <span className="scanner-result-check">
            <input type="checkbox" checked={eliteOnly} onChange={(event) => setEliteOnly(event.target.checked)} />
            <span>Only show elite candidates</span>
          </span>
          <span className="panel-copy">1D + 1W Strong Buy, and FA rank top 200 when present.</span>
        </label>
        <div className="scanner-result-filter panel scanner-result-filter-actions">
          <span>Board Snapshot</span>
          <div className="scanner-result-view-actions">
            <Link className="ghost-button" to="/scanner">
              Back to board
            </Link>
          </div>
          <span className="panel-copy">Updated {formatLocalDateTime(payload?.latest_update_at)}.</span>
        </div>
      </section>

      <section className="scanner-result-table-shell panel">
        {isLoading ? <LoadingBlock label="Loading scanner top hits…" /> : null}
        {notice ? <p className="panel-copy">{notice}</p> : null}
        {!notice && myPicksNotice ? <p className="panel-copy earnings-console-note">{myPicksNotice}</p> : null}
        {!isLoading && !notice && filteredRows.length === 0 ? <p className="panel-copy">No tickers match current filters.</p> : null}
        {!isLoading && !notice && filteredRows.length > 0 ? (
          <>
            <div className="scanner-top-hits-toolbar">
              <span>{formatCount(filteredRows.length)} names</span>
              <span>Latest board date {formatLocalDate(payload?.target_trading_date)}</span>
            </div>
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={PAGE_SIZE}
              onPageChange={setCurrentPage}
            />
            <div className="data-table-responsive scanner-result-table-wrap">
              <table className="data-table scanner-result-table scanner-top-hits-table">
                <thead>
                  <tr>
                    {canManageMyPicks ? <th>My Pick</th> : null}
                    <th>{renderSortButton("Ticker", "ticker", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Hits", "hits", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>Scanners</th>
                    <th>{renderSortButton("Sector", "sector", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>Sector Momentum</th>
                    <th>{renderSortButton("Close", "close", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("Change", "change", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>1Y %</th>
                    <th>YTD %</th>
                    <th>CAN V2</th>
                    <th>VCP</th>
                    <th>{renderSortButton("RS", "rs", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>{renderSortButton("TA", "ta", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>1D</th>
                    <th>1W</th>
                    <th>{renderSortButton("FA", "fa", sortBy, sortDirection, setSortBy, setSortDirection)}</th>
                    <th>FA Rank</th>
                    {eliteOnly ? <th>{renderSortButton("Sector Top Hit", "sectorTopHit", sortBy, sortDirection, setSortBy, setSortDirection)}</th> : null}
                    {eliteOnly ? <th>{renderSortButton("Industry Top Hit", "industryTopHit", sortBy, sortDirection, setSortBy, setSortDirection)}</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row) => (
                    <tr key={row.ticker}>
                      {canManageMyPicks ? (
                        <td data-label="My Pick">
                          <input
                            type="checkbox"
                            checked={myPickTickers.has(row.ticker)}
                            disabled={myPickTickers.has(row.ticker) || Boolean(savingMyPickTickers[row.ticker])}
                            aria-label={myPickTickers.has(row.ticker) ? `${row.ticker} already in My Picks` : `Add ${row.ticker} to My Picks`}
                            onChange={(event) => {
                              if (event.target.checked) {
                                void handleAddToMyPicks(row.ticker);
                              }
                            }}
                          />
                        </td>
                      ) : null}
                      <td data-label="Ticker">
                        <div className="scanner-result-company">
                          <Link className="scanner-result-symbol" to={buildChartHref(row.ticker)}>
                            {row.ticker}
                          </Link>
                          <span>{row.company || row.industry || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Hits">
                        <strong>{formatCount(row.scanner_count)}</strong>
                      </td>
                      <td data-label="Scanners">
                        <div className="scanner-top-hit-pills">
                          {row.scanners.map((scanner) => (
                            <Link key={`${row.ticker}-${scanner.id}`} className="scanner-card-pill" to={`/scanner/${encodeURIComponent(scanner.id)}`}>
                              {scanner.label}
                            </Link>
                          ))}
                        </div>
                      </td>
                      <td data-label="Sector">
                        <div className="scanner-result-sector">
                          <strong>{row.sector || "Unknown sector"}</strong>
                          <span>{row.industry || "-"}</span>
                        </div>
                      </td>
                      <td data-label="Sector Momentum">
                        <SectorMomentumCell row={row} />
                      </td>
                      <td data-label="Close">{formatPrice(row.day_close)}</td>
                      <td data-label="Change">{renderChange(row.change_pct)}</td>
                      <td data-label="1Y %">{renderChange(row.perf_year_pct)}</td>
                      <td data-label="YTD %">{renderChange(row.perf_ytd_pct)}</td>
                      <td data-label="CAN V2">{formatCanslimScore(row.canslim_score, row.canslim_max_score)}</td>
                      <td data-label="VCP">{formatVcpScore(row.vcp_score, row.vcp_rating)}</td>
                      <td data-label="RS">{formatRating(row.rs_rating)}</td>
                      <td data-label="TA">{formatRating(row.ta_rating)}</td>
                      <td data-label="1D">{formatTechnicalIndicatorLabel(row.technical_indicator_ratings?.["1d"])}</td>
                      <td data-label="1W">{formatTechnicalIndicatorLabel(row.technical_indicator_ratings?.["1w"])}</td>
                      <td data-label="FA">{formatRating(row.fa_rating)}</td>
                      <td data-label="FA Rank">{row.fa_current_rank != null ? `#${formatCount(row.fa_current_rank)}` : "--"}</td>
                      {eliteOnly ? (
                        <td data-label="Sector Top Hit">
                          {eliteSectorLeaders.get(normalizeSectorKey(row.sector)) === row.ticker ? (
                            <span className="scanner-score-pill is-strong">Top Hit</span>
                          ) : (
                            "--"
                          )}
                        </td>
                      ) : null}
                      {eliteOnly ? (
                        <td data-label="Industry Top Hit">
                          {eliteIndustryLeaders.get(normalizeIndustryKey(row.industry)) === row.ticker ? (
                            <span className="scanner-score-pill is-strong">Top Hit</span>
                          ) : (
                            "--"
                          )}
                        </td>
                      ) : null}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <PaginationControls
              currentPage={normalizedPage}
              totalItems={filteredRows.length}
              totalPages={totalPages}
              pageSize={PAGE_SIZE}
              onPageChange={setCurrentPage}
            />
          </>
        ) : null}
      </section>
    </div>
  );
}

function SectorMomentumCell({ row }: { row: ScannerTopHitRow }) {
  const momentum = row.sector_momentum;
  if (!momentum) {
    return <span className="panel-copy">--</span>;
  }
  const tone = toneForQuadrant(momentum.quadrant);
  return (
    <div className="scanner-top-hits-momentum">
      <span className={`scanner-score-pill ${tone}`}>{momentum.quadrant || "--"}</span>
      <span>{momentum.etf_ticker || momentum.sector}</span>
      <span>
        {formatCompactNumber(momentum.rs_ratio)} / {formatCompactNumber(momentum.momentum)}
      </span>
    </div>
  );
}

function renderSortButton(
  label: string,
  column: SortKey,
  sortBy: SortKey,
  sortDirection: SortDirection,
  setSortBy: (value: SortKey) => void,
  setSortDirection: (value: SortDirection) => void,
) {
  const isActive = sortBy === column;
  const indicator = !isActive ? "" : sortDirection === "asc" ? " ↑" : " ↓";
  return (
    <button
      className={`ghost-button scanner-result-sort-button${isActive ? " is-active" : ""}`}
      type="button"
      onClick={() => {
        if (isActive) {
          setSortDirection(sortDirection === "asc" ? "desc" : "asc");
          return;
        }
        setSortBy(column);
        setSortDirection(column === "ticker" || column === "sector" ? "asc" : "desc");
      }}
    >
      {label}
      {indicator}
    </button>
  );
}

function compareRows(
  left: ScannerTopHitRow,
  right: ScannerTopHitRow,
  sortBy: SortKey,
  sortDirection: SortDirection,
  leaderMaps: { sectorLeaders: Map<string, string>; industryLeaders: Map<string, string> },
) {
  if (sortBy === "ticker") {
    return compareText(left.ticker, right.ticker, sortDirection);
  }
  if (sortBy === "sector") {
    return compareText(left.sector, right.sector, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "sectorTopHit") {
    return compareNullableNumber(
      leaderMaps.sectorLeaders.get(normalizeSectorKey(left.sector)) === left.ticker ? 1 : 0,
      leaderMaps.sectorLeaders.get(normalizeSectorKey(right.sector)) === right.ticker ? 1 : 0,
      sortDirection,
    ) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "industryTopHit") {
    return compareNullableNumber(
      leaderMaps.industryLeaders.get(normalizeIndustryKey(left.industry)) === left.ticker ? 1 : 0,
      leaderMaps.industryLeaders.get(normalizeIndustryKey(right.industry)) === right.ticker ? 1 : 0,
      sortDirection,
    ) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "hits") {
    return compareNullableNumber(left.scanner_count, right.scanner_count, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "close") {
    return compareNullableNumber(left.day_close, right.day_close, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "change") {
    return compareNullableNumber(left.change_pct, right.change_pct, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "rs") {
    return compareNullableNumber(left.rs_rating, right.rs_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  if (sortBy === "ta") {
    return compareNullableNumber(left.ta_rating, right.ta_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
  }
  return compareNullableNumber(left.fa_rating, right.fa_rating, sortDirection) || left.ticker.localeCompare(right.ticker);
}

function compareNullableNumber(left: number | null, right: number | null, sortDirection: SortDirection) {
  const missingSentinel = sortDirection === "asc" ? Number.POSITIVE_INFINITY : Number.NEGATIVE_INFINITY;
  const normalizedLeft = typeof left === "number" ? left : missingSentinel;
  const normalizedRight = typeof right === "number" ? right : missingSentinel;
  return sortDirection === "asc" ? normalizedLeft - normalizedRight : normalizedRight - normalizedLeft;
}

function compareText(left: string, right: string, sortDirection: SortDirection) {
  return sortDirection === "asc" ? left.localeCompare(right) : right.localeCompare(left);
}

function buildChartHref(ticker: string) {
  const params = new URLSearchParams();
  params.set("ticker", ticker);
  return `/charts?${params.toString()}`;
}

function formatPrice(value: number | null) {
  return value == null ? "--" : `$${value.toFixed(2)}`;
}

function formatRating(value: number | null) {
  return value == null ? "--" : value.toFixed(1);
}

function formatCanslimScore(score: number | null | undefined, maxScore: number | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "--";
  }
  if (maxScore == null || Number.isNaN(maxScore)) {
    return `${Math.round(score)}`;
  }
  return `${Math.round(score)}/${Math.round(maxScore)}`;
}

function formatVcpScore(score: number | null | undefined, rating: string | null | undefined) {
  if (score == null || Number.isNaN(score)) {
    return "--";
  }
  const base = score.toFixed(1);
  return rating ? `${base} ${rating}` : base;
}

function formatCompactNumber(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(1);
}

function formatTechnicalIndicatorLabel(value: TechnicalIndicatorRatingCell | undefined) {
  return value?.rating_label ?? "--";
}

function isElitePick(row: ScannerTopHitRow) {
  const dailyLabel = normalizeIndicatorLabel(row.technical_indicator_ratings?.["1d"]);
  const weeklyLabel = normalizeIndicatorLabel(row.technical_indicator_ratings?.["1w"]);
  const faRankOk = row.fa_current_rank == null || row.fa_current_rank <= 200;
  return dailyLabel === "strong buy" && weeklyLabel === "strong buy" && faRankOk;
}

function normalizeIndicatorLabel(value: TechnicalIndicatorRatingCell | undefined) {
  return String(value?.rating_label || "").trim().toLowerCase();
}

function normalizeIndustryKey(industry: string | null | undefined) {
  return String(industry || "").trim().toLowerCase();
}

function normalizeSectorKey(sector: string | null | undefined) {
  return String(sector || "").trim().toLowerCase();
}

function buildEliteLeaderMap(
  rows: ScannerTopHitRow[],
  resolveKey: (row: ScannerTopHitRow) => string,
) {
  const leaders = new Map<string, ScannerTopHitRow>();
  for (const row of rows) {
    const key = resolveKey(row);
    if (!key) {
      continue;
    }
    const currentLeader = leaders.get(key);
    if (!currentLeader || compareEliteIndustryLeader(row, currentLeader) < 0) {
      leaders.set(key, row);
    }
  }
  return new Map(Array.from(leaders.entries()).map(([key, row]) => [key, row.ticker]));
}

function compareEliteIndustryLeader(left: ScannerTopHitRow, right: ScannerTopHitRow) {
  if (left.scanner_count !== right.scanner_count) {
    return right.scanner_count - left.scanner_count;
  }
  const leftFaRank = left.fa_current_rank ?? Number.POSITIVE_INFINITY;
  const rightFaRank = right.fa_current_rank ?? Number.POSITIVE_INFINITY;
  if (leftFaRank !== rightFaRank) {
    return leftFaRank - rightFaRank;
  }
  if ((left.fa_rating ?? Number.NEGATIVE_INFINITY) !== (right.fa_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.fa_rating ?? Number.NEGATIVE_INFINITY) - (left.fa_rating ?? Number.NEGATIVE_INFINITY);
  }
  if ((left.ta_rating ?? Number.NEGATIVE_INFINITY) !== (right.ta_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.ta_rating ?? Number.NEGATIVE_INFINITY) - (left.ta_rating ?? Number.NEGATIVE_INFINITY);
  }
  if ((left.rs_rating ?? Number.NEGATIVE_INFINITY) !== (right.rs_rating ?? Number.NEGATIVE_INFINITY)) {
    return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
  }
  return left.ticker.localeCompare(right.ticker);
}

function renderChange(value: number | null) {
  if (value == null) {
    return <span className="ticker-change neutral">--</span>;
  }
  const tone = value >= 0 ? "up" : "down";
  return (
    <span className={`ticker-change ${tone}`}>
      {value >= 0 ? "+" : ""}
      {value.toFixed(2)}%
    </span>
  );
}

function toneForQuadrant(value: string) {
  if (value === "Leading") {
    return "is-strong";
  }
  if (value === "Improving") {
    return "is-warm";
  }
  return "is-neutral";
}
