import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { EarningsCalendarDay, EarningsCalendarEntry, EarningsCalendarResponse } from "../lib/types";

function toggleSelection(current: string[], value: string) {
  return current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
}

function formatRange(start: string | undefined, end: string | undefined) {
  if (!start || !end) return "Next week";
  return `${start} to ${end}`;
}

function bucketLabel(key: "before_market" | "after_market" | "unknown") {
  if (key === "before_market") return "Before Market";
  if (key === "after_market") return "After Market";
  return "Unknown";
}

function bucketEntries(day: EarningsCalendarDay, key: "before_market" | "after_market" | "unknown") {
  return day[key] ?? [];
}

function EntryList({ entries }: { entries: EarningsCalendarEntry[] }) {
  if (entries.length === 0) {
    return <p className="panel-copy">None</p>;
  }
  return (
    <div className="earnings-entry-list">
      {entries.map((entry) => (
        <article key={`${entry.date}-${entry.ticker}-${entry.session ?? "unknown"}`} className="earnings-entry-card">
          <div className="earnings-entry-head">
            <Link className="rrg-item-link" to={`/charts?ticker=${encodeURIComponent(entry.ticker)}`}>
              {entry.ticker}
            </Link>
            <span className="eyebrow">{entry.exchange ?? "-"}</span>
          </div>
          <p className="panel-copy">
            {[entry.sector, entry.industry].filter(Boolean).join(" · ") || "No sector/industry"}
          </p>
          {entry.summary ? <p className="panel-copy">{entry.summary}</p> : null}
          {entry.criteria ? (
            <>
              <p className="panel-copy">
                Criteria {entry.criteria.passed ? "PASS" : "FAIL"}
                {entry.criteria.pass_mode ? ` (${entry.criteria.pass_mode})` : ""}
              </p>
              <p className="panel-copy">
                Matched: {entry.criteria.matched_criteria.length > 0 ? entry.criteria.matched_criteria.join(", ") : "-"}
              </p>
              <p className="panel-copy">
                Not pass: {entry.criteria.not_matched_criteria.length > 0 ? entry.criteria.not_matched_criteria.join(", ") : "-"}
              </p>
            </>
          ) : null}
        </article>
      ))}
    </div>
  );
}

export function EarningsPage() {
  const [payload, setPayload] = useState<EarningsCalendarResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [excludedSectors, setExcludedSectors] = useState<string[]>([]);
  const [excludedIndustries, setExcludedIndustries] = useState<string[]>([]);
  const [onlyCriteria, setOnlyCriteria] = useState(false);

  const queryString = useMemo(() => {
    const params = new URLSearchParams();
    for (const sector of excludedSectors) params.append("excludeSector", sector);
    for (const industry of excludedIndustries) params.append("excludeIndustry", industry);
    if (onlyCriteria) params.set("onlyCriteria", "true");
    return params.toString();
  }, [excludedIndustries, excludedSectors, onlyCriteria]);

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    void fetchJson<EarningsCalendarResponse>(`/api/earnings-calendar${queryString ? `?${queryString}` : ""}`)
      .then(setPayload)
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load next-week earnings calendar.");
      })
      .finally(() => setIsLoading(false));
  }, [queryString]);

  const days = payload?.days ?? [];

  return (
    <div className="page-grid">
      <section className="hero-card earnings-hero">
        <div>
          <span className="eyebrow">Next Week Earnings</span>
          <h1>Weekly Calendar</h1>
          <p className="panel-copy">Sunday to Saturday view, grouped by before-market and after-market sessions.</p>
        </div>
        <div className="hero-stats">
          <div>
            <span className="eyebrow">Week</span>
            <strong>{formatRange(payload?.week_start, payload?.week_end)}</strong>
          </div>
          <div>
            <span className="eyebrow">Days</span>
            <strong>{days.length || "-"}</strong>
          </div>
          <div>
            <span className="eyebrow">Sector Excludes</span>
            <strong>{excludedSectors.length}</strong>
          </div>
          <div>
            <span className="eyebrow">Industry Excludes</span>
            <strong>{excludedIndustries.length}</strong>
          </div>
          <div>
            <span className="eyebrow">Criteria Matches</span>
            <strong>{payload?.criteria_filter.matched_count ?? "-"}</strong>
          </div>
        </div>
      </section>

      <Panel title="Filters" aside={<span className="eyebrow">Exclude unwanted sectors or industries</span>}>
        {isLoading ? <LoadingBlock label="Loading earnings filters…" compact /> : null}
        <div className="earnings-filter-grid">
          <label className="chart-toggle">
            <input type="checkbox" checked={onlyCriteria} onChange={() => setOnlyCriteria((current) => !current)} />
            <span>Only show persisted criteria matches</span>
          </label>
          <div className="field">
            <span>Sectors</span>
            <div className="earnings-chip-grid">
              {(payload?.available_sectors ?? []).map((value) => (
                <button
                  key={value}
                  type="button"
                  className={`rrg-group-chip${excludedSectors.includes(value) ? " is-active" : ""}`}
                  onClick={() => setExcludedSectors((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
              ))}
            </div>
          </div>
          <div className="field">
            <span>Industries</span>
            <div className="earnings-chip-grid">
              {(payload?.available_industries ?? []).map((value) => (
                <button
                  key={value}
                  type="button"
                  className={`rrg-group-chip${excludedIndustries.includes(value) ? " is-active" : ""}`}
                  onClick={() => setExcludedIndustries((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
              ))}
            </div>
          </div>
        </div>
        {onlyCriteria ? (
          <p className="panel-copy">
            {payload?.criteria_filter.available
              ? `Using latest persisted criteria run ${payload.criteria_filter.run_date || ""} with ${payload.criteria_filter.matched_count} matches.`
              : "Criteria filter is on, but no persisted criteria run is available yet."}
          </p>
        ) : null}
        {notice ? <p className="panel-copy">{notice}</p> : null}
      </Panel>

      <Panel title="Calendar" aside={<span className="eyebrow">Grouped by earnings session</span>}>
        {isLoading ? <LoadingBlock label="Loading next-week earnings calendar…" /> : null}
        {!isLoading && days.length === 0 ? <p className="panel-copy">No earnings events returned for next week.</p> : null}
        {!isLoading && days.length > 0 ? (
          <div className="earnings-calendar-grid">
            {days.map((day) => (
              <section key={day.date} className="earnings-day-card">
                <div className="earnings-day-head">
                  <strong>{day.weekday}</strong>
                  <span className="eyebrow">{day.date}</span>
                </div>
                {(["before_market", "after_market", "unknown"] as const).map((bucketKey) => (
                  <div key={bucketKey} className="earnings-bucket">
                    <div className="earnings-bucket-head">
                      <span>{bucketLabel(bucketKey)}</span>
                      <span className="eyebrow">{bucketEntries(day, bucketKey).length}</span>
                    </div>
                    <EntryList entries={bucketEntries(day, bucketKey)} />
                  </div>
                ))}
              </section>
            ))}
          </div>
        ) : null}
      </Panel>
    </div>
  );
}
