import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import type { EarningsCalendarDay, EarningsCalendarEntry, EarningsCalendarResponse } from "../lib/types";

const CRITERIA_LABELS: Array<{ key: string; label: string; shortLabel: string }> = [
  { key: "institutional_ownership_ge_10", label: "Institutional ownership >= 10%", shortLabel: "Inst > 10%" },
  { key: "bullish_ma_stack", label: "MA20 > MA50 > MA200", shortLabel: "MA Stack" },
  { key: "revenue_yoy_ge_100", label: "Revenue YoY >= 100%", shortLabel: "Rev +100%" },
  { key: "latest_eps_negative", label: "Latest EPS negative", shortLabel: "EPS Neg" },
  { key: "eps_improving_last_4", label: "EPS improving last 4", shortLabel: "EPS Trend" },
];

const BUCKET_KEYS = ["before_market", "after_market", "during_market", "unknown"] as const;

function toggleSelection(current: string[], value: string) {
  return current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
}

function formatRange(start: string | undefined, end: string | undefined) {
  if (!start || !end) return "Next week";
  return `${formatMonthDay(start)} - ${formatMonthDay(end)}`;
}

function formatMonthDay(value: string) {
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(parsed).toUpperCase();
}

function formatDayHeading(day: EarningsCalendarDay) {
  const parsed = new Date(`${day.date}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return `${day.weekday} ${day.date}`.toUpperCase();
  return new Intl.DateTimeFormat("en-US", { weekday: "short", month: "short", day: "numeric" }).format(parsed).toUpperCase();
}

function bucketLabel(key: (typeof BUCKET_KEYS)[number]) {
  if (key === "before_market") return "Before Market";
  if (key === "after_market") return "After Market";
  if (key === "during_market") return "During Market";
  return "Unknown";
}

function bucketEntries(day: EarningsCalendarDay, key: (typeof BUCKET_KEYS)[number]) {
  return day[key] ?? [];
}

function countDayEntries(day: EarningsCalendarDay) {
  return BUCKET_KEYS.reduce((total, key) => total + bucketEntries(day, key).length, 0);
}

function countMatchedEntries(entries: EarningsCalendarEntry[]) {
  return entries.filter((entry) => entry.criteria?.passed).length;
}

function hasAnyEntries(day: EarningsCalendarDay) {
  return countDayEntries(day) > 0;
}

function EntryList({ entries }: { entries: EarningsCalendarEntry[] }) {
  if (entries.length === 0) {
    return <div className="earnings-empty-slot">Empty</div>;
  }
  return (
    <div className="earnings-entry-list">
      {entries.map((entry) => (
        <article key={`${entry.date}-${entry.ticker}-${entry.session ?? "unknown"}`} className="earnings-entry-card">
          <div className="earnings-entry-head">
            <Link className="earnings-entry-link" to={`/charts?ticker=${encodeURIComponent(entry.ticker)}`}>
              {entry.ticker}
            </Link>
            <span className="earnings-exchange-badge">{entry.exchange ?? "-"}</span>
          </div>
          <p className="earnings-entry-meta">{[entry.sector, entry.industry].filter(Boolean).join(" / ") || "No sector or industry"}</p>
          {entry.summary ? <p className="earnings-entry-summary">"{entry.summary}"</p> : null}
          {entry.criteria ? (
            <div className="earnings-criteria-block">
              <div className="earnings-criteria-summary">
                <span className={`earnings-pass-indicator${entry.criteria.passed ? " is-pass" : " is-fail"}`}>
                  {entry.criteria.passed ? "PASS" : "FAIL"} {entry.criteria.matched_criteria.length}/{CRITERIA_LABELS.length}
                </span>
                {entry.criteria.pass_mode ? <span className="earnings-pass-mode">{entry.criteria.pass_mode}</span> : null}
              </div>
              <div className="earnings-criteria-pill-row">
                {CRITERIA_LABELS.map((item) => {
                  const matched = entry.criteria?.criteria?.[item.key];
                  return (
                    <span
                      key={item.key}
                      className={`earnings-criteria-pill${matched ? " is-match" : " is-miss"}`}
                      title={item.label}
                    >
                      {item.shortLabel}
                    </span>
                  );
                })}
              </div>
            </div>
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
  const totalEntries = days.reduce((total, day) => total + countDayEntries(day), 0);
  const filteredExclusionCount = excludedSectors.length + excludedIndustries.length;

  return (
    <div className="page-grid earnings-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Operational Mode</span>
          <h1>Next Week&apos;s Earnings Calendar</h1>
          <p className="panel-copy">Command-board view for next week&apos;s earnings, grouped by trading session and tuned for fast weekly scanning.</p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Active Week</span>
            <strong>{formatRange(payload?.week_start, payload?.week_end)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Day Count</span>
            <strong>{String(days.length).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Event Count</span>
            <strong>{String(totalEntries).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Exclusion Count</span>
            <strong>{String(filteredExclusionCount).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric earnings-metric-highlight">
            <span className="eyebrow">Criteria Match</span>
            <strong>{payload?.criteria_filter.matched_count ?? "-"}</strong>
          </div>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <div className="earnings-filter-console-row">
          <div className="earnings-filter-toggle-group">
            <span className="eyebrow">Filters</span>
            <label className="earnings-toggle">
              <input type="checkbox" checked={onlyCriteria} onChange={() => setOnlyCriteria((current) => !current)} />
              <span className="earnings-toggle-track" aria-hidden="true">
                <span className="earnings-toggle-thumb" />
              </span>
              <span className="earnings-toggle-label">Only Criteria Matches</span>
            </label>
          </div>
          <div className="earnings-inline-filters">
            <div className="earnings-inline-filter-group">
              <span className="eyebrow">Sector Exclusions</span>
              <div className="earnings-inline-chip-row">
                {excludedSectors.length > 0 ? excludedSectors.map((value) => <span key={value} className="earnings-inline-chip">{value}</span>) : <span className="earnings-inline-empty">None</span>}
              </div>
            </div>
            <div className="earnings-inline-filter-group">
              <span className="eyebrow">Industry Exclusions</span>
              <div className="earnings-inline-chip-row">
                {excludedIndustries.length > 0 ? excludedIndustries.map((value) => <span key={value} className="earnings-inline-chip">{value}</span>) : <span className="earnings-inline-empty">None</span>}
              </div>
            </div>
          </div>
        </div>
        {isLoading ? <LoadingBlock label="Loading earnings filters…" compact /> : null}
        <div className="earnings-filter-grid">
          <div className="field earnings-filter-field">
            <span>Sector Universe</span>
            <div className="earnings-chip-grid">
              {(payload?.available_sectors ?? []).map((value) => (
                <button
                  key={value}
                  type="button"
                  className={`earnings-filter-chip${excludedSectors.includes(value) ? " is-active" : ""}`}
                  onClick={() => setExcludedSectors((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
              ))}
            </div>
          </div>
          <div className="field earnings-filter-field">
            <span>Industry Universe</span>
            <div className="earnings-chip-grid">
              {(payload?.available_industries ?? []).map((value) => (
                <button
                  key={value}
                  type="button"
                  className={`earnings-filter-chip${excludedIndustries.includes(value) ? " is-active" : ""}`}
                  onClick={() => setExcludedIndustries((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
              ))}
            </div>
          </div>
        </div>
        {onlyCriteria ? (
          <p className="panel-copy earnings-console-note">
            {payload?.criteria_filter.available
              ? `Using latest persisted criteria run ${payload.criteria_filter.run_date || ""} with ${payload.criteria_filter.matched_count} matches.`
              : "Criteria filter is on, but no persisted criteria run is available yet."}
          </p>
        ) : null}
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <h2>Calendar</h2>
          <span className="eyebrow">Grouped by earnings session</span>
        </div>
        {isLoading ? <LoadingBlock label="Loading next-week earnings calendar…" /> : null}
        {!isLoading && days.length === 0 ? <p className="panel-copy">No earnings events returned for next week.</p> : null}
        {!isLoading && days.length > 0 ? (
          <div className="earnings-calendar-grid earnings-command-grid">
            {days.map((day) => (
              <section key={day.date} className="earnings-day-card">
                <div className="earnings-day-head">
                  <strong>{formatDayHeading(day)}</strong>
                  <span className={`earnings-day-badge${countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key))) > 0 ? " is-active" : ""}`}>
                    {countDayEntries(day)} matches
                  </span>
                </div>
                {hasAnyEntries(day) ? (
                  BUCKET_KEYS.filter((bucketKey) => bucketKey !== "during_market" || bucketEntries(day, bucketKey).length > 0).map((bucketKey) => (
                    <div key={bucketKey} className="earnings-bucket">
                      <div className="earnings-bucket-head">
                        <span>{bucketLabel(bucketKey)}</span>
                        <span className="eyebrow">{bucketEntries(day, bucketKey).length}</span>
                      </div>
                      <EntryList entries={bucketEntries(day, bucketKey)} />
                    </div>
                  ))
                ) : (
                  <div className="earnings-day-empty">
                    <div className="earnings-day-empty-icon">▥</div>
                    <div className="earnings-day-empty-copy">Low Volume Day</div>
                  </div>
                )}
              </section>
            ))}
          </div>
        ) : null}
      </section>
    </div>
  );
}
