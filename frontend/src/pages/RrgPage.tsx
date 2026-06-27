import { useEffect, useMemo, useState } from "react";
import { NavLink, useParams } from "react-router-dom";
import { Panel } from "../components/Panel";
import { RrgChart } from "../components/RrgChart";
import { fetchJson } from "../lib/api";
import type { RrgCadence, RrgResponse, RrgSeries, RrgUniverse } from "../lib/types";

const QUADRANT_ORDER = ["Leading", "Improving", "Lagging", "Weakening"] as const;

const DEFAULT_RESPONSE: RrgResponse = {
  universe: "sector",
  benchmark: "SPY",
  period: "3y",
  trail_weeks: 12,
  generated_at: "",
  series: [],
  cadence: "weekly",
  quadrants: {
    center_x: 100,
    center_y: 100,
    definitions: [],
  },
  meta: {
    count: 0,
    notes: [],
  },
};

const universeTabs: Array<{ value: RrgUniverse; label: string }> = [
  { value: "sector", label: "Sector" },
  { value: "industry", label: "Industry" },
  { value: "theme", label: "Theme" },
];

const cadenceTabs: Array<{ value: RrgCadence; label: string }> = [
  { value: "weekly", label: "Weekly" },
  { value: "daily-2m", label: "Daily 2M" },
];

export function RrgPage() {
  const params = useParams();
  const universe = normalizeUniverse(params.universe);
  const [cadence, setCadence] = useState<RrgCadence>("weekly");
  const [payload, setPayload] = useState<RrgResponse>(DEFAULT_RESPONSE);
  const [loading, setLoading] = useState(true);
  const [hasError, setHasError] = useState(false);
  const [activeGroupId, setActiveGroupId] = useState("");
  const [refreshNonce, setRefreshNonce] = useState(0);
  const [focusQuadrant, setFocusQuadrant] = useState<string>("All");

  useEffect(() => {
    clearAllRrgCache();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setHasError(false);
    void fetchJson<RrgResponse>(`/api/rrg/${universe}?benchmark=SPY&period=3y&trailWeeks=12&cadence=${cadence}`, {
      signal: controller.signal,
    })
      .then((response) => {
        setPayload(response);
        setActiveGroupId(response.groups?.[0]?.id ?? "");
        setLoading(false);
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") {
          return;
        }
        setPayload({ ...DEFAULT_RESPONSE, universe, cadence });
        setActiveGroupId("");
        setHasError(true);
        setLoading(false);
      });
    return () => controller.abort();
  }, [cadence, refreshNonce, universe]);

  useEffect(() => {
    setFocusQuadrant("All");
  }, [activeGroupId, cadence, universe]);

  const activeGroup = useMemo(() => {
    if (!payload.groups?.length) {
      return null;
    }
    return payload.groups.find((group) => group.id === activeGroupId) ?? payload.groups[0];
  }, [activeGroupId, payload.groups]);

  const groupSeries = useMemo<RrgSeries[]>(() => {
    if (payload.universe === "theme" && activeGroup) {
      return activeGroup.series;
    }
    return payload.series;
  }, [activeGroup, payload.series, payload.universe]);

  const visibleSeries = useMemo<RrgSeries[]>(() => {
    if (focusQuadrant === "All") {
      return groupSeries;
    }
    return groupSeries.filter((entry) => entry.quadrant === focusQuadrant);
  }, [focusQuadrant, groupSeries]);

  const quadrantCounts = useMemo(() => {
    return groupSeries.reduce<Record<string, number>>((acc, entry) => {
      acc[entry.quadrant] = (acc[entry.quadrant] ?? 0) + 1;
      return acc;
    }, {});
  }, [groupSeries]);

  const quadrantStats = useMemo(() => {
    const total = groupSeries.length || 1;
    return QUADRANT_ORDER.map((name) => {
      const count = quadrantCounts[name] ?? 0;
      return {
        name,
        count,
        share: Math.round((count / total) * 100),
        tone: quadrantTone(name),
      };
    });
  }, [groupSeries.length, quadrantCounts]);

  const leaders = useMemo(() => {
    return [...visibleSeries]
      .sort((left, right) => quadrantRank(right.quadrant) - quadrantRank(left.quadrant) || right.distance - left.distance)
      .slice(0, 6);
  }, [visibleSeries]);

  const title = `${labelForUniverse(universe)} Rotation Analysis`;
  const subtitle = `${labelForUniverse(universe)} relative strength rotation vs ${payload.benchmark}`;
  const cadenceLabel = cadence === "daily-2m" ? "Daily 2M" : "Weekly";
  const latestLabel = formatGeneratedAt(payload.generated_at);
  const selectedGroupTitle = activeGroup?.title ?? "Theme Focus";
  const operationalNotes = payload.meta.notes.length ? payload.meta.notes : ["Using the same RRG math as the static renderer."];

  return (
    <div className="page-grid rrg-page rrg-workbench">
      <Panel
        title={title}
        aside={
          <div className="rrg-panel-aside rrg-header-actions">
            <NavLink className="ghost-button" to="/guide">
              Open Guide
            </NavLink>
            {payload.static_report_url ? (
              <a className="ghost-button" href={payload.static_report_url} target="_blank" rel="noreferrer">
                Static Report
              </a>
            ) : (
              <span className="eyebrow">Interactive</span>
            )}
          </div>
        }
      >
        <div className="rrg-hero rrg-hero-compact">
          <div className="rrg-title-block">
            <p className="panel-copy">{subtitle}</p>
            <div className="rrg-meta-strip">
              <span className="rrg-meta-chip">Benchmark: {payload.benchmark}</span>
              <span className="rrg-meta-chip">Mode: {cadenceLabel}</span>
              <span className="rrg-meta-chip">Period: {payload.period}</span>
              <span className="rrg-meta-chip">Trail: {payload.trail_weeks} weeks</span>
              {payload.universe === "theme" && activeGroup ? <span className="rrg-meta-chip">Group: {selectedGroupTitle}</span> : null}
            </div>
          </div>
          <div className="rrg-tabs">
            {universeTabs.map((tab) => (
              <NavLink key={tab.value} className={({ isActive }) => `rrg-tab${isActive ? " is-active" : ""}`} to={`/rotation/${tab.value}`}>
                {tab.label}
              </NavLink>
            ))}
          </div>
        </div>
      </Panel>

      <section className="panel rrg-utility-strip">
        <div className="rrg-utility-left">
          <div className="rrg-utility-group">
            <span className="eyebrow">Cadence</span>
            <div className="rrg-cadence-toggle">
              {cadenceTabs.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  className={`rrg-cadence-chip${cadence === option.value ? " is-active" : ""}`}
                  onClick={() => setCadence(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="rrg-utility-right">
          <span className="rrg-latest-stamp">Latest: {latestLabel}</span>
          <button
            type="button"
            className="ghost-button"
            onClick={() => setRefreshNonce((current) => current + 1)}
          >
            Manual Refresh
          </button>
        </div>
      </section>

      {loading ? (
        <Panel title="Loading RRG">
          <p className="panel-copy">Pulling weekly closes and computing the latest rotation trails…</p>
        </Panel>
      ) : hasError ? (
        <Panel title="RRG Unavailable" aside={<span className="eyebrow">Retry Later</span>}>
          <p className="panel-copy">
            The interactive RRG request failed. This view computes fresh data on demand, so a temporary data-source
            miss will show up here before the static report does.
          </p>
        </Panel>
      ) : visibleSeries.length === 0 ? (
        <Panel title="No RRG Data" aside={<span className="eyebrow">Empty State</span>}>
          <p className="panel-copy">
            No rotation series could be computed for this universe right now. Check the notes below for skipped
            tickers or data gaps.
          </p>
        </Panel>
      ) : (
        <div className="rrg-layout">
          <Panel className="rrg-workspace-panel rrg-chart-panel">
            <div className="rrg-workspace-head">
              <div className="rrg-workspace-title">
                <span className="rrg-workspace-icon">◎</span>
                <div>
                  <h2>Main Rotation Workspace</h2>
                  <p className="panel-copy">
                    {focusQuadrant === "All"
                      ? `${groupSeries.length} symbols on current map`
                      : `${visibleSeries.length} symbols filtered to ${focusQuadrant}`}
                  </p>
                </div>
              </div>
              <div className="rrg-focus-strip">
                <span className="eyebrow">Legend Focus</span>
                <button
                  type="button"
                  className={`rrg-focus-chip${focusQuadrant === "All" ? " is-active is-neutral" : ""}`}
                  onClick={() => setFocusQuadrant("All")}
                >
                  All ({groupSeries.length})
                </button>
                {quadrantStats.map((item) => (
                  <button
                    key={item.name}
                    type="button"
                    className={`rrg-focus-chip ${item.tone}${focusQuadrant === item.name ? " is-active" : ""}`}
                    onClick={() => setFocusQuadrant(item.name)}
                  >
                    {item.name} ({item.count})
                  </button>
                ))}
              </div>
            </div>
            <div className="rrg-chart-frame">
              <RrgChart benchmark={payload.benchmark} series={visibleSeries} />
            </div>
            {payload.universe === "theme" && payload.groups?.length ? (
              <div className="rrg-theme-strip">
                {payload.groups.map((group) => (
                  <button
                    key={group.id}
                    type="button"
                    className={`rrg-theme-chip${(activeGroup?.id ?? "") === group.id ? " is-active" : ""}`}
                    onClick={() => setActiveGroupId(group.id)}
                  >
                    <span>{group.title}</span>
                    <strong>{group.series.length}</strong>
                  </button>
                ))}
              </div>
            ) : null}
          </Panel>

          <div className="rrg-sidecar">
            <Panel title="Quadrant Distribution">
              <div className="rrg-distribution-list">
                {quadrantStats.map((item) => (
                  <div key={item.name} className="rrg-distribution-row">
                    <div className="rrg-distribution-head">
                      <span className={`rrg-distribution-count ${item.tone}`}>{item.count}</span>
                      <span>{item.name}</span>
                      <span className="rrg-distribution-share">{item.share}%</span>
                    </div>
                    <div className="rrg-distribution-track">
                      <div className={`rrg-distribution-fill ${item.tone}`} style={{ width: `${item.share}%` }} />
                    </div>
                  </div>
                ))}
              </div>
            </Panel>

            <Panel title="Strongest Leaders" aside={<span className="eyebrow">{leaders.length} shown</span>}>
              <div className="rrg-leader-list rrg-leader-list-compact">
                {leaders.map((entry, index) => (
                  <article key={entry.ticker} className="rrg-leader-item">
                    <div className="rrg-leader-rank">{index + 1}</div>
                    <div className="rrg-leader-main">
                      <div className="ticker-symbol">{entry.ticker}</div>
                      <div className="ticker-company">{entry.label}</div>
                    </div>
                    <div className="rrg-leader-side">
                      <div className={`status-pill status-${badgeClass(entry.quadrant)}`}>{entry.quadrant}</div>
                      <div className="file-meta">
                        {entry.latest.x.toFixed(1)} / {entry.latest.y.toFixed(1)}
                      </div>
                    </div>
                  </article>
                ))}
              </div>
            </Panel>

            <Panel title="Operational Notes">
              <div className="rrg-notes">
                {payload.meta.failed_tickers?.length ? (
                  <div className="rrg-note-block">
                    <div className="eyebrow">Skipped Symbols ({payload.meta.failed_tickers.length})</div>
                    <ul className="rrg-note-list">
                      {payload.meta.failed_tickers.slice(0, 8).map((ticker) => (
                        <li key={ticker}>{ticker}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
                <div className="rrg-note-block">
                  <div className="eyebrow">Workflow Notes</div>
                  <ul className="rrg-note-list">
                    {operationalNotes.map((note) => (
                      <li key={note}>{note}</li>
                    ))}
                  </ul>
                </div>
                {activeGroup ? (
                  <div className="rrg-note-callout">
                    <span className="eyebrow">Theme Cluster</span>
                    <p>{selectedGroupTitle}</p>
                  </div>
                ) : null}
              </div>
            </Panel>
          </div>

          <Panel
            title={`${labelForUniverse(universe)} Components`}
            aside={<span className="eyebrow">{visibleSeries.length} drilldown cards</span>}
            className="rrg-components-panel"
          >
            <div className="rrg-item-grid">
              {visibleSeries.map((entry) => (
                <article key={entry.ticker} className="rrg-item-card">
                  <div className="rrg-item-head">
                    <div className="rrg-item-brand">
                      <div className="rrg-item-monogram">{entry.ticker.slice(0, 3)}</div>
                      <div>
                        <div className="rrg-item-symbol-row">
                          <div className="ticker-symbol">{entry.ticker}</div>
                          <a className="rrg-item-link" href={holdingsUrl(entry.ticker)} target="_blank" rel="noreferrer">
                            Holdings
                          </a>
                        </div>
                        <div className="ticker-company">{entry.label}</div>
                      </div>
                    </div>
                    <div className={`status-pill status-${badgeClass(entry.quadrant)}`}>{entry.quadrant}</div>
                  </div>
                  <div className="rrg-kpi-grid">
                    <div className="rrg-kpi-card">
                      <span className="eyebrow">RS Ratio</span>
                      <strong>{entry.latest.x.toFixed(1)}</strong>
                    </div>
                    <div className="rrg-kpi-card">
                      <span className="eyebrow">Momentum</span>
                      <strong>{entry.latest.y.toFixed(1)}</strong>
                    </div>
                    <div className="rrg-kpi-card">
                      <span className="eyebrow">Fearzone</span>
                      <strong className={entry.fearzone.active ? "rrg-kpi-positive" : "rrg-kpi-neutral"}>
                        {entry.fearzone.active ? "Active" : "Idle"}
                      </strong>
                    </div>
                  </div>
                  <div className="rrg-fearzone-card">
                    <div className="rrg-fearzone-head">
                      <div className="rrg-item-chart-label">Fearzone</div>
                      <div className={`status-pill status-${entry.fearzone.active ? "success" : "unknown"}`}>
                        {entry.fearzone.trigger_labels.length ? entry.fearzone.trigger_labels.join(", ") : "No trigger"}
                      </div>
                    </div>
                    <div className="rrg-mini-metrics">
                      <span className="file-meta">Signal {entry.fearzone.signal_date ?? "None"}</span>
                      <span className="file-meta">Age {entry.fearzone.signal_age_bars ?? "-"} bars</span>
                      <span className="file-meta">{entry.points.length} points</span>
                    </div>
                    <div className="rrg-fearzone-indicators">
                      {entry.fearzone.conditions.map((condition) => (
                        <span
                          key={`${entry.ticker}-${condition.key}`}
                          className={`rrg-fearzone-indicator${condition.active ? " is-active" : ""}`}
                        >
                          {condition.label}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div className="rrg-card-chart-grid">
                    <div className="rrg-item-chart">
                      <div className="rrg-item-chart-label">RRG Trail</div>
                      <RrgChart benchmark={payload.benchmark} series={[entry]} compact showLegend={false} />
                    </div>
                  </div>
                </article>
              ))}
            </div>
          </Panel>
        </div>
      )}
    </div>
  );
}

function formatGeneratedAt(value: string): string {
  if (!value) {
    return "Awaiting refresh";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(parsed);
}

function normalizeUniverse(value: string | undefined): RrgUniverse {
  if (value === "industry" || value === "theme") {
    return value;
  }
  return "sector";
}

function labelForUniverse(universe: RrgUniverse): string {
  if (universe === "industry") {
    return "Industry";
  }
  if (universe === "theme") {
    return "Theme";
  }
  return "Sector";
}

function quadrantRank(quadrant: string): number {
  if (quadrant === "Leading") {
    return 4;
  }
  if (quadrant === "Improving") {
    return 3;
  }
  if (quadrant === "Weakening") {
    return 2;
  }
  return 1;
}

function quadrantTone(quadrant: string): string {
  if (quadrant === "Leading") {
    return "tone-leading";
  }
  if (quadrant === "Improving") {
    return "tone-improving";
  }
  if (quadrant === "Lagging") {
    return "tone-lagging";
  }
  return "tone-weakening";
}

function badgeClass(quadrant: string): "success" | "running" | "failed" {
  if (quadrant === "Leading") {
    return "success";
  }
  if (quadrant === "Improving") {
    return "running";
  }
  return "failed";
}

function holdingsUrl(ticker: string): string {
  return `https://nz.finance.yahoo.com/quote/${encodeURIComponent(ticker)}/holdings/`;
}

function clearAllRrgCache() {
  try {
    for (let index = localStorage.length - 1; index >= 0; index -= 1) {
      const key = localStorage.key(index);
      if (!key || !key.startsWith("rrg-page-cache-v1:")) {
        continue;
      }
      localStorage.removeItem(key);
    }
  } catch {
    // Ignore cache clear failures.
  }
}
