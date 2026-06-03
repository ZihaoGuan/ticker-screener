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
  { key: "implied_move_ge_7_near_earnings", label: "Implied move > 7% near earnings", shortLabel: "IV > 7%" },
];

const BUCKET_KEYS = ["before_market", "after_market", "during_market", "unknown"] as const;
const EARNINGS_CALENDAR_SESSION_CACHE_KEY_PREFIX = "earnings-calendar-cache-v2";
const WEEK_OPTIONS = [
  { value: 0, label: "This Week" },
  { value: 1, label: "Next Week" },
  { value: 2, label: "Week After" },
] as const;

function toggleSelection(current: string[], value: string) {
  return current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
}

function formatRange(start: string | undefined, end: string | undefined) {
  if (!start || !end) return "Selected week";
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

function normalizeFilterValue(value: string | null | undefined) {
  return String(value ?? "").trim().toLowerCase();
}

function filterEntries(
  entries: EarningsCalendarEntry[],
  {
    excludedSectors,
    excludedIndustries,
    onlyCriteria,
  }: {
    excludedSectors: string[];
    excludedIndustries: string[];
    onlyCriteria: boolean;
  },
) {
  const excludedSectorKeys = new Set(excludedSectors.map((value) => normalizeFilterValue(value)).filter(Boolean));
  const excludedIndustryKeys = new Set(excludedIndustries.map((value) => normalizeFilterValue(value)).filter(Boolean));
  return entries.filter((entry) => {
    if (excludedSectorKeys.has(normalizeFilterValue(entry.sector))) {
      return false;
    }
    if (excludedIndustryKeys.has(normalizeFilterValue(entry.industry))) {
      return false;
    }
    if (onlyCriteria && !entry.criteria?.passed) {
      return false;
    }
    return true;
  });
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
          {entry.implied_move_signal ? (
            <div className="earnings-criteria-pill-row">
              <span
                className={`earnings-criteria-pill${entry.implied_move_signal.matched ? " is-match" : " is-miss"}`}
                title={`Near earnings implied move threshold ${entry.implied_move_signal.threshold_pct.toFixed(0)}%`}
              >
                {entry.implied_move_signal.percent_move != null
                  ? `IV ${entry.implied_move_signal.percent_move.toFixed(2)}%`
                  : "IV --"}
              </span>
            </div>
          ) : null}
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
  const [weekOffset, setWeekOffset] = useState(0);
  const [excludedSectors, setExcludedSectors] = useState<string[]>([]);
  const [excludedIndustries, setExcludedIndustries] = useState<string[]>([]);
  const [onlyCriteria, setOnlyCriteria] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  useEffect(() => {
    setIsLoading(true);
    setNotice("");
    const cacheKey = buildEarningsCalendarCacheKey(weekOffset);
    if (refreshNonce === 0) {
      try {
        const cached = sessionStorage.getItem(cacheKey);
        if (cached) {
          const parsed = JSON.parse(cached) as EarningsCalendarResponse;
          setPayload(parsed);
          setIsLoading(false);
          return;
        }
      } catch {
        sessionStorage.removeItem(cacheKey);
      }
    }
    void fetchJson<EarningsCalendarResponse>(`/api/earnings-calendar?weekOffset=${weekOffset}`)
      .then((response) => {
        setPayload(response);
        sessionStorage.setItem(cacheKey, JSON.stringify(response));
      })
      .catch((error) => {
        setPayload(null);
        setNotice(error instanceof Error ? error.message : "Failed to load earnings calendar.");
      })
      .finally(() => setIsLoading(false));
  }, [refreshNonce, weekOffset]);

  const days = useMemo(() => {
    if (!payload) {
      return [];
    }
    return payload.days
      .map((day) => ({
        ...day,
        before_market: filterEntries(day.before_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        after_market: filterEntries(day.after_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        during_market: filterEntries(day.during_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        unknown: filterEntries(day.unknown, { excludedSectors, excludedIndustries, onlyCriteria }),
      }))
      .filter((day) => hasAnyEntries(day));
  }, [excludedIndustries, excludedSectors, onlyCriteria, payload]);
  const totalEntries = days.reduce((total, day) => total + countDayEntries(day), 0);
  const filteredExclusionCount = excludedSectors.length + excludedIndustries.length;
  const matchedCount = days.reduce(
    (total, day) => total + countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key))),
    0,
  );

  return (
    <div className="page-grid earnings-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <span className="earnings-board-kicker">Operational Mode</span>
          <h1>Earnings Calendar</h1>
          <p className="panel-copy">Command-board view for this week, next week, or the week after, grouped by trading session and tuned for fast weekly scanning.</p>
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
            <strong>{payload ? matchedCount : "-"}</strong>
          </div>
        </div>
      </section>

      <section className="panel earnings-filter-console">
        <div className="earnings-filter-console-row">
          <div className="earnings-filter-toggle-group">
            <span className="eyebrow">Filters</span>
            <label className="field earnings-filter-field">
              <span>Active Week</span>
              <select value={weekOffset} onChange={(event) => setWeekOffset(Number.parseInt(event.target.value, 10) || 0)}>
                {WEEK_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="earnings-toggle">
              <input type="checkbox" checked={onlyCriteria} onChange={() => setOnlyCriteria((current) => !current)} />
              <span className="earnings-toggle-track" aria-hidden="true">
                <span className="earnings-toggle-thumb" />
              </span>
              <span className="earnings-toggle-label">Only Criteria Matches</span>
            </label>
            <button
              type="button"
              className="ghost-button"
              onClick={() => {
                sessionStorage.removeItem(buildEarningsCalendarCacheKey(weekOffset));
                setRefreshNonce((current) => current + 1);
              }}
            >
              Refresh
            </button>
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
              ? `Using latest persisted criteria run ${payload.criteria_filter.run_date || ""}, including persisted IV > 7% near-earnings check. ${matchedCount} current matches.`
              : "Criteria filter is on, but no persisted criteria run is available yet."}
          </p>
        ) : null}
        {payload ? <p className="panel-copy earnings-console-note">Filter toggles use session-cached calendar data on this device per selected week. Refresh to pull newest server data.</p> : null}
        {notice ? <p className="panel-copy earnings-console-note">{notice}</p> : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <h2>Calendar</h2>
          <span className="eyebrow">Grouped by earnings session</span>
        </div>
        {isLoading ? <LoadingBlock label="Loading earnings calendar…" /> : null}
        {!isLoading && days.length === 0 ? <p className="panel-copy">No earnings events returned for selected week.</p> : null}
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

function buildEarningsCalendarCacheKey(weekOffset: number) {
  return `${EARNINGS_CALENDAR_SESSION_CACHE_KEY_PREFIX}:${weekOffset}`;
}
