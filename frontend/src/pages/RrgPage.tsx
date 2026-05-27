import { useEffect, useMemo, useState } from "react";
import { NavLink, useParams } from "react-router-dom";
import { Panel } from "../components/Panel";
import { RrgChart } from "../components/RrgChart";
import { fetchJson } from "../lib/api";
import type { RrgCadence, RrgResponse, RrgSeries, RrgUniverse } from "../lib/types";

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

  useEffect(() => {
    setLoading(true);
    setHasError(false);
    void fetchJson<RrgResponse>(`/api/rrg/${universe}?benchmark=SPY&period=3y&trailWeeks=12&cadence=${cadence}`)
      .then((response) => {
        setPayload(response);
        setActiveGroupId(response.groups?.[0]?.id ?? "");
        setLoading(false);
      })
      .catch(() => {
        setPayload({ ...DEFAULT_RESPONSE, universe, cadence });
        setActiveGroupId("");
        setHasError(true);
        setLoading(false);
      });
  }, [cadence, universe]);

  const activeGroup = useMemo(() => {
    if (!payload.groups?.length) {
      return null;
    }
    return payload.groups.find((group) => group.id === activeGroupId) ?? payload.groups[0];
  }, [activeGroupId, payload.groups]);

  const visibleSeries = useMemo<RrgSeries[]>(() => {
    if (payload.universe === "theme" && activeGroup) {
      return activeGroup.series;
    }
    return payload.series;
  }, [activeGroup, payload.series, payload.universe]);

  const quadrantCounts = useMemo(() => {
    return visibleSeries.reduce<Record<string, number>>((acc, entry) => {
      acc[entry.quadrant] = (acc[entry.quadrant] ?? 0) + 1;
      return acc;
    }, {});
  }, [visibleSeries]);

  const leaders = useMemo(() => {
    return [...visibleSeries]
      .sort((left, right) => quadrantRank(right.quadrant) - quadrantRank(left.quadrant) || right.distance - left.distance)
      .slice(0, 6);
  }, [visibleSeries]);

  const title = `${labelForUniverse(universe)} RRG`;
  const subtitle = `${labelForUniverse(universe)} rotation map vs ${payload.benchmark}`;
  const cadenceLabel = cadence === "daily-2m" ? "Daily 2M" : "Weekly";

  return (
    <div className="page-grid rrg-page">
      <Panel
        title={title}
        aside={
          payload.static_report_url ? (
            <a className="ghost-button" href={payload.static_report_url} target="_blank" rel="noreferrer">
              Open Static Report
            </a>
          ) : (
            <span className="eyebrow">Interactive</span>
          )
        }
      >
        <div className="rrg-hero">
          <div>
            <p className="panel-copy">{subtitle}</p>
            <div className="rrg-meta-row">
              <span className="eyebrow">Benchmark</span>
              <strong>{payload.benchmark}</strong>
              <span className="eyebrow">Mode</span>
              <strong>{cadenceLabel}</strong>
              <span className="eyebrow">Period</span>
              <strong>{payload.period}</strong>
              <span className="eyebrow">Trail</span>
              <strong>{payload.trail_weeks} weeks</strong>
            </div>
          </div>
          <div className="rrg-tabs">
            {universeTabs.map((tab) => (
              <NavLink key={tab.value} className={({ isActive }) => `rrg-tab${isActive ? " is-active" : ""}`} to={`/rrg/${tab.value}`}>
                {tab.label}
              </NavLink>
            ))}
          </div>
        </div>
      </Panel>

      <Panel title="RRG Mode">
        <div className="rrg-group-row">
          {cadenceTabs.map((option) => (
            <button
              key={option.value}
              type="button"
              className={`rrg-group-chip${cadence === option.value ? " is-active" : ""}`}
              onClick={() => setCadence(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </Panel>

      {payload.universe === "theme" && payload.groups?.length ? (
        <Panel title="Theme Groups">
          <div className="rrg-group-row">
            {payload.groups.map((group) => (
              <button
                key={group.id}
                type="button"
                className={`rrg-group-chip${(activeGroup?.id ?? "") === group.id ? " is-active" : ""}`}
                onClick={() => setActiveGroupId(group.id)}
              >
                {group.title}
              </button>
            ))}
          </div>
        </Panel>
      ) : null}

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
          <Panel title={`${labelForUniverse(universe)} Map`} className="rrg-chart-panel">
            <RrgChart benchmark={payload.benchmark} series={visibleSeries} />
          </Panel>

          <div className="rrg-sidecar">
            <Panel title="Summary" aside={<span className="eyebrow">{visibleSeries.length} symbols</span>}>
              <div className="rrg-summary-grid">
                <div className="rrg-summary-card">
                  <span className="eyebrow">Leading</span>
                  <strong>{quadrantCounts.Leading ?? 0}</strong>
                </div>
                <div className="rrg-summary-card">
                  <span className="eyebrow">Weakening</span>
                  <strong>{quadrantCounts.Weakening ?? 0}</strong>
                </div>
                <div className="rrg-summary-card">
                  <span className="eyebrow">Lagging</span>
                  <strong>{quadrantCounts.Lagging ?? 0}</strong>
                </div>
                <div className="rrg-summary-card">
                  <span className="eyebrow">Improving</span>
                  <strong>{quadrantCounts.Improving ?? 0}</strong>
                </div>
              </div>
            </Panel>

            <Panel title="Strongest Leaders">
              <div className="rrg-leader-list">
                {leaders.map((entry) => (
                  <article key={entry.ticker} className="rrg-leader-item">
                    <div>
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

            <Panel title="Notes">
              <div className="rrg-notes">
                {(payload.meta.notes.length ? payload.meta.notes : ["Using the same RRG math as the static renderer."]).map((note) => (
                  <p key={note} className="panel-copy">
                    {note}
                  </p>
                ))}
              </div>
            </Panel>
          </div>
        </div>
      )}
    </div>
  );
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

function badgeClass(quadrant: string): "success" | "running" | "failed" {
  if (quadrant === "Leading") {
    return "success";
  }
  if (quadrant === "Improving") {
    return "running";
  }
  return "failed";
}
