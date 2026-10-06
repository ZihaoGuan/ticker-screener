import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { fetchJson } from "../lib/api";
import type { EarningsCalendarDay, EarningsCalendarEntry, EarningsCalendarResponse, TechnicalIndicatorRatingCell } from "../lib/types";

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

function formatRatingValue(value: number | null | undefined) {
  return value == null ? "--" : value.toFixed(1);
}

function formatDateLabel(value: string | null | undefined) {
  if (!value) return "--";
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(parsed).toUpperCase();
}

function orderedTechnicalIndicatorRatings(ratings: Record<string, TechnicalIndicatorRatingCell> | null | undefined) {
  return ["1d", "1w", "1m"]
    .map((timeframe) => ratings?.[timeframe] ?? null)
    .filter((item): item is TechnicalIndicatorRatingCell => item != null);
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
  return entries
    .filter((entry) => {
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
    })
    .sort((left, right) => {
      const leftMatches = left.criteria?.matched_criteria.length ?? 0;
      const rightMatches = right.criteria?.matched_criteria.length ?? 0;
      return Number(Boolean(right.criteria?.passed)) - Number(Boolean(left.criteria?.passed)) || rightMatches - leftMatches;
    });
}

function EntryList({ entries }: { entries: EarningsCalendarEntry[] }) {
  if (entries.length === 0) {
    return <div className="earnings-empty-slot">Empty</div>;
  }
  return (
    <div className="earnings-entry-list">
      {entries.map((entry) => {
        const technicalRatings = orderedTechnicalIndicatorRatings(entry.technical_indicator_ratings);
        const hasSecondaryDetails = Boolean(
          entry.fundamental_rating ||
            entry.technical_rating ||
            technicalRatings.length > 0 ||
            entry.summary ||
            entry.implied_move_signal ||
            entry.earnings_trade_analysis ||
            entry.pead_analysis ||
            entry.post_earnings_tracking ||
            entry.criteria,
        );

        return (
        <article key={`${entry.date}-${entry.ticker}-${entry.session ?? "unknown"}`} className="earnings-entry-card">
          <div className="earnings-entry-head">
            <Link className="earnings-entry-link" to={`/charts?ticker=${encodeURIComponent(entry.ticker)}`}>
              {entry.ticker}
            </Link>
            <span className="earnings-exchange-badge">{entry.exchange ?? "-"}</span>
          </div>
          <p className="earnings-entry-meta">{[entry.sector, entry.industry].filter(Boolean).join(" / ") || "No sector or industry"}</p>
          {entry.criteria ? (
            <div className="earnings-criteria-summary earnings-criteria-summary-primary">
              <span className={`earnings-pass-indicator${entry.criteria.passed ? " is-pass" : " is-fail"}`}>
                {entry.criteria.passed ? "Pass" : "No pass"} · {entry.criteria.matched_criteria.length}/{CRITERIA_LABELS.length} criteria
              </span>
              {entry.criteria.pass_mode ? <span className="earnings-pass-mode">{entry.criteria.pass_mode}</span> : null}
            </div>
          ) : null}
          {hasSecondaryDetails ? (
            <details className="earnings-entry-details">
              <summary>Signals and analysis</summary>
              <div className="earnings-entry-details-body">
          {entry.fundamental_rating || entry.technical_rating || technicalRatings.length > 0 ? (
            <div className="earnings-criteria-pill-row">
              {entry.fundamental_rating ? (
                <span
                  className={`earnings-criteria-pill${entry.fundamental_rating.rating_status === "ok" ? " is-match" : " is-miss"}`}
                  title={`Fundamental rating ${entry.fundamental_rating.as_of_date}${entry.fundamental_rating.rating_status ? ` · ${entry.fundamental_rating.rating_status}` : ""}`}
                >
                  {`F ${formatRatingValue(entry.fundamental_rating.overall_rating)}`}
                </span>
              ) : null}
              {entry.technical_rating ? (
                <span
                  className={`earnings-criteria-pill${entry.technical_rating.technical_status === "ok" ? " is-match" : " is-miss"}`}
                  title={`Technical rating ${entry.technical_rating.as_of_date}${entry.technical_rating.rating_band ? ` · ${entry.technical_rating.rating_band}` : ""}${entry.technical_rating.technical_status ? ` · ${entry.technical_rating.technical_status}` : ""}`}
                >
                  {`T ${formatRatingValue(entry.technical_rating.overall_rating)}${entry.technical_rating.rating_band ? ` ${entry.technical_rating.rating_band}` : ""}`}
                </span>
              ) : null}
              {technicalRatings.map((rating) => (
                <span
                  key={`${entry.ticker}-${rating.timeframe}`}
                  className={`earnings-criteria-pill${rating.technical_status === "ok" ? " is-match" : " is-miss"}`}
                  title={`Tech rating ${rating.timeframe.toUpperCase()} ${rating.as_of_date}${rating.overall_score != null ? ` · ${rating.overall_score.toFixed(2)}` : ""}${rating.technical_status ? ` · ${rating.technical_status}` : ""}`}
                >
                  {`${rating.timeframe.toUpperCase()} ${rating.rating_label ?? "--"}`}
                </span>
              ))}
            </div>
          ) : null}
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
          {entry.earnings_trade_analysis || entry.pead_analysis || entry.post_earnings_tracking ? (
            <div className="earnings-analysis-stack">
              {entry.earnings_trade_analysis ? (
                <div className="earnings-analysis-block">
                  <div className="earnings-criteria-summary">
                    <span className="earnings-pass-mode">Analyzer</span>
                    <span className="earnings-pass-indicator is-pass">
                      {entry.earnings_trade_analysis.grade ?? "--"} {formatRatingValue(entry.earnings_trade_analysis.composite_score)}
                    </span>
                  </div>
                  <div className="earnings-criteria-pill-row">
                    <span className="earnings-criteria-pill is-match">
                      {`Gap ${entry.earnings_trade_analysis.gap_pct != null ? `${entry.earnings_trade_analysis.gap_pct >= 0 ? "+" : ""}${entry.earnings_trade_analysis.gap_pct.toFixed(1)}%` : "--"}`}
                    </span>
                    {entry.earnings_trade_analysis.strongest_component ? (
                      <span className="earnings-criteria-pill is-match">
                        {`Best ${entry.earnings_trade_analysis.strongest_component}`}
                      </span>
                    ) : null}
                    {entry.earnings_trade_analysis.weakest_component ? (
                      <span className="earnings-criteria-pill is-miss">
                        {`Weak ${entry.earnings_trade_analysis.weakest_component}`}
                      </span>
                    ) : null}
                  </div>
                  {entry.earnings_trade_analysis.guidance ? (
                    <p className="earnings-analysis-copy">{entry.earnings_trade_analysis.guidance}</p>
                  ) : null}
                </div>
              ) : entry.post_earnings_tracking?.eligible_on ? (
                <div className="earnings-analysis-block">
                  <div className="earnings-criteria-summary">
                    <span className="earnings-pass-mode">Analyzer</span>
                    <span className="earnings-pass-indicator is-fail">Pending</span>
                  </div>
                  <p className="earnings-analysis-copy">
                    {`Eligible after ${formatDateLabel(entry.post_earnings_tracking.eligible_on)}.`}
                  </p>
                </div>
              ) : null}
              {entry.pead_analysis ? (
                <div className="earnings-analysis-block">
                  <div className="earnings-criteria-summary">
                    <span className="earnings-pass-mode">PEAD</span>
                    <span className={`earnings-pass-indicator${entry.pead_analysis.stage === "BREAKOUT" ? " is-pass" : " is-fail"}`}>
                      {entry.pead_analysis.stage ?? "PENDING"}
                    </span>
                  </div>
                  <div className="earnings-criteria-pill-row">
                    <span className={`earnings-criteria-pill${entry.pead_analysis.stage === "BREAKOUT" ? " is-match" : " is-miss"}`}>
                      {`${entry.pead_analysis.rating ?? "--"} ${formatRatingValue(entry.pead_analysis.composite_score)}`}
                    </span>
                    {entry.pead_analysis.breakout_pct != null ? (
                      <span className={`earnings-criteria-pill${(entry.pead_analysis.breakout_pct ?? 0) > 0 ? " is-match" : " is-miss"}`}>
                        {`BO ${entry.pead_analysis.breakout_pct >= 0 ? "+" : ""}${entry.pead_analysis.breakout_pct.toFixed(1)}%`}
                      </span>
                    ) : null}
                    {entry.pead_analysis.risk_reward_ratio != null ? (
                      <span className="earnings-criteria-pill is-match">
                        {`R:R ${entry.pead_analysis.risk_reward_ratio.toFixed(1)}`}
                      </span>
                    ) : null}
                  </div>
                  {entry.pead_analysis.guidance ? <p className="earnings-analysis-copy">{entry.pead_analysis.guidance}</p> : null}
                </div>
              ) : null}
            </div>
          ) : null}
          {entry.criteria ? (
            <div className="earnings-criteria-block">
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
              </div>
            </details>
          ) : null}
        </article>
        );
      })}
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
  const [loadedWeekOffset, setLoadedWeekOffset] = useState<number | null>(null);
  const [refreshRequest, setRefreshRequest] = useState<{ id: number; weekOffset: number } | null>(null);
  const consumedRefreshRequest = useRef<number | null>(null);

  const currentPayload = loadedWeekOffset === weekOffset ? payload : null;
  const refreshCalendar = () => {
    sessionStorage.removeItem(buildEarningsCalendarCacheKey(weekOffset));
    setRefreshRequest({ id: Date.now(), weekOffset });
  };

  useEffect(() => {
    const controller = new AbortController();
    let isCurrentRequest = true;
    const forceRefresh = refreshRequest?.weekOffset === weekOffset && refreshRequest.id !== consumedRefreshRequest.current;
    const cacheKey = buildEarningsCalendarCacheKey(weekOffset);

    if (forceRefresh) consumedRefreshRequest.current = refreshRequest?.id ?? null;

    setIsLoading(true);
    setNotice("");
    if (!forceRefresh) {
      try {
        const cached = sessionStorage.getItem(cacheKey);
        if (cached) {
          const parsed = JSON.parse(cached) as EarningsCalendarResponse;
          setPayload(parsed);
          setLoadedWeekOffset(weekOffset);
          setIsLoading(false);
          return () => {
            isCurrentRequest = false;
            controller.abort();
          };
        }
      } catch {
        sessionStorage.removeItem(cacheKey);
      }
    }
    void fetchJson<EarningsCalendarResponse>(`/api/earnings-calendar?weekOffset=${weekOffset}`, { signal: controller.signal })
      .then((response) => {
        if (!isCurrentRequest) return;
        setPayload(response);
        setLoadedWeekOffset(weekOffset);
        try {
          sessionStorage.setItem(cacheKey, JSON.stringify(response));
        } catch {
          // Calendar data remains usable when browser storage is unavailable.
        }
      })
      .catch((error) => {
        if (!isCurrentRequest || (error instanceof DOMException && error.name === "AbortError")) return;
        if (loadedWeekOffset !== weekOffset) {
          setPayload(null);
          setLoadedWeekOffset(weekOffset);
        }
        setNotice(error instanceof Error ? error.message : "Failed to load earnings calendar.");
      })
      .finally(() => {
        if (!isCurrentRequest) return;
        setIsLoading(false);
      });

    return () => {
      isCurrentRequest = false;
      controller.abort();
    };
  }, [refreshRequest, weekOffset]);

  const days = useMemo(() => {
    if (!currentPayload) {
      return [];
    }
    return currentPayload.days
      .map((day) => ({
        ...day,
        before_market: filterEntries(day.before_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        after_market: filterEntries(day.after_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        during_market: filterEntries(day.during_market, { excludedSectors, excludedIndustries, onlyCriteria }),
        unknown: filterEntries(day.unknown, { excludedSectors, excludedIndustries, onlyCriteria }),
      }));
  }, [currentPayload, excludedIndustries, excludedSectors, onlyCriteria]);
  const totalEntries = days.reduce((total, day) => total + countDayEntries(day), 0);
  const activeDayCount = days.filter(hasAnyEntries).length;
  const filteredExclusionCount = excludedSectors.length + excludedIndustries.length;
  const isRefreshing = isLoading && currentPayload != null;
  const matchedCount = days.reduce(
    (total, day) => total + countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key))),
    0,
  );

  return (
    <div className="page-grid earnings-board">
      <section className="earnings-board-hero">
        <div className="earnings-board-hero-copy">
          <h1>Earnings Calendar</h1>
          <p className="panel-copy">Plan the week by trading session, surface the strongest criteria matches first, then open a chart when a name deserves a closer look.</p>
        </div>
        <div className="earnings-board-metrics">
          <div className="earnings-metric">
            <span className="eyebrow">Active Week</span>
            <strong>{formatRange(currentPayload?.week_start, currentPayload?.week_end)}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Visible Days</span>
            <strong>{String(activeDayCount).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Visible Events</span>
            <strong>{String(totalEntries).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric">
            <span className="eyebrow">Exclusion Count</span>
            <strong>{String(filteredExclusionCount).padStart(2, "0")}</strong>
          </div>
          <div className="earnings-metric earnings-metric-highlight">
            <span className="eyebrow">Criteria Passes</span>
            <strong>{currentPayload ? matchedCount : "-"}</strong>
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
              <input className="visually-hidden" type="checkbox" checked={onlyCriteria} onChange={() => setOnlyCriteria((current) => !current)} />
              <span className="earnings-toggle-track" aria-hidden="true">
                <span className="earnings-toggle-thumb" />
              </span>
              <span className="earnings-toggle-label">Only Criteria Matches</span>
            </label>
            <button type="button" className="ghost-button" disabled={isLoading} onClick={refreshCalendar}>Refresh calendar</button>
          </div>
        </div>
        <details className="earnings-exclusion-disclosure">
          <summary>
            <span>Exclude sectors or industries</span>
            <span>{filteredExclusionCount === 0 ? "None selected" : `${filteredExclusionCount} selected`}</span>
          </summary>
          <div className="earnings-filter-grid">
          <div className="field earnings-filter-field">
            <span>Sectors to exclude</span>
            <div className="earnings-chip-grid">
              {(currentPayload?.available_sectors ?? []).map((value) => {
                const isExcluded = excludedSectors.includes(value);
                return (
                <button
                  key={value}
                  type="button"
                  aria-pressed={isExcluded}
                  aria-label={`${isExcluded ? "Include" : "Exclude"} ${value}`}
                  className={`earnings-filter-chip${isExcluded ? " is-excluded" : ""}`}
                  onClick={() => setExcludedSectors((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
                );
              })}
            </div>
          </div>
          <div className="field earnings-filter-field">
            <span>Industries to exclude</span>
            <div className="earnings-chip-grid">
              {(currentPayload?.available_industries ?? []).map((value) => {
                const isExcluded = excludedIndustries.includes(value);
                return (
                <button
                  key={value}
                  type="button"
                  aria-pressed={isExcluded}
                  aria-label={`${isExcluded ? "Include" : "Exclude"} ${value}`}
                  className={`earnings-filter-chip${isExcluded ? " is-excluded" : ""}`}
                  onClick={() => setExcludedIndustries((current) => toggleSelection(current, value))}
                >
                  {value}
                </button>
                );
              })}
            </div>
          </div>
          </div>
          {filteredExclusionCount > 0 ? (
            <button type="button" className="earnings-clear-filters" onClick={() => { setExcludedSectors([]); setExcludedIndustries([]); }}>
              Clear exclusions
            </button>
          ) : null}
        </details>
        {onlyCriteria ? (
          <p className="panel-copy earnings-console-note">
            {currentPayload?.criteria_filter.available
              ? `Showing ${matchedCount} criteria passes from the latest persisted run${currentPayload.criteria_filter.run_date ? ` on ${currentPayload.criteria_filter.run_date}` : ""}.`
              : "Criteria filter is on, but no persisted criteria run is available yet."}
          </p>
        ) : null}
        {currentPayload ? <p className="panel-copy earnings-console-note">This week is cached for this browser session. Refresh calendar to request the newest server data.</p> : null}
        {notice ? (
          <div className="earnings-error" role="alert">
            <span>Could not load the earnings calendar. {notice}</span>
            <button type="button" className="ghost-button" onClick={refreshCalendar}>Try again</button>
          </div>
        ) : null}
      </section>

      <section className="panel earnings-calendar-panel">
        <div className="panel-head earnings-calendar-head">
          <h2>Calendar</h2>
          <span className="eyebrow">Grouped by earnings session</span>
        </div>
        {isLoading && !currentPayload ? <LoadingBlock label="Loading earnings calendar…" /> : null}
        {isRefreshing ? <p className="earnings-refreshing" role="status">Refreshing calendar…</p> : null}
        {!isLoading && !notice && days.length === 0 ? <p className="panel-copy">No earnings events returned for selected week.</p> : null}
        {days.length > 0 ? (
          <div className="earnings-calendar-grid earnings-command-grid">
            {days.map((day) => (
              <section key={day.date} className="earnings-day-card">
                <div className="earnings-day-head">
                  <h3>{formatDayHeading(day)}</h3>
                  <span className={`earnings-day-badge${countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key))) > 0 ? " is-active" : ""}`}>
                    {countDayEntries(day)} events{countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key))) > 0 ? ` · ${countMatchedEntries(BUCKET_KEYS.flatMap((key) => bucketEntries(day, key)))} pass` : ""}
                  </span>
                </div>
                {hasAnyEntries(day) ? (
                  BUCKET_KEYS.filter((bucketKey) => bucketEntries(day, bucketKey).length > 0).map((bucketKey) => (
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
