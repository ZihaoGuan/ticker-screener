import assert from "node:assert/strict";
import { chromium } from "playwright";

const accountTickers = ["EXEL", "CAKE", "JOYY", "GKOS", "GTLB"];
const sectorTickers = ["EXEL", "CAKE", "JOYY"];
const candidates = (pending, prefix) => [
  ...pending.map((ticker, index) => ({ ticker, status: "pending", selected_for_book: true, selection_tier: "A", limit_price: 25 + index, stop: 22, shares: 40, reasons: [], sector: "Health Care", trigger: 25, max_entry: 27, adr14_pct: 4 })),
  { ticker: `${prefix}AUDIT`, status: "not_selected", selected_for_book: false, reasons: ["sector_quota_or_session_limit"] },
  { ticker: `${prefix}DATA`, status: "blocked_data", selected_for_book: false, reasons: ["missing_adr_or_stop"] },
  { ticker: `${prefix}CAP`, status: "blocked_capacity", selected_for_book: false, reasons: ["position_or_allocation_capacity"] },
];
const accountCandidates = [...candidates(accountTickers, "ACCOUNT"), { ticker: "ACCOUNTWAIT", status: "waiting_capacity", selected_for_book: true, queue_rank: 6, source_snapshot_rank: 6, selection_date: "2026-10-19", reasons: ["position_or_allocation_capacity"] }];
const position = (ticker, status, id) => ({
  id, ticker, status, payload_json: { opened_on: "2026-10-12", entry_price: 25, stop: 22, initial_shares: 40, remaining_shares: status === "open" ? 25 : 0, realized_r: status === "closed" ? 1.5 : 0.25, realized_pnl: status === "closed" ? 150 : 25, exit_next_open: status === "open", holding_sessions: 4, exit_type: status === "closed" ? "target" : null },
});
const book = (rows, positions = [], outstanding_orders = []) => ({
  strategy_id: "martin_focus",
  runs: [{ session_date: "2026-10-19", summary_json: { partial: true, source_run_id: 42, action_counts: { missing_bars: 1 } } }],
  candidates: rows, outstanding_orders,
  positions, events: [],
  history: [{ date: "2026-10-16", cumulative_r: 0.5, account_equity: 100250, equity_drawdown_pct: 0 }, { date: "2026-10-19", cumulative_r: 1.5, account_equity: 100750, equity_drawdown_pct: 0.2 }],
  attribution: {},
  metrics: { closed_count: 1, cumulative_r: 1.5, realized_r: 1.5, unrealized_r: 0.25, realized_pnl: 150, expectancy_r: 1.5, win_rate_pct: 100, profit_factor_r: 2, max_r_drawdown: 0.2, account_equity: 100750, equity_drawdown_pct: 0.2 },
});

const browser = await chromium.launch({ headless: true, channel: "chrome" });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(5000);
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"], { origin: "http://127.0.0.1:5173" });
  page.on("pageerror", (error) => { throw error; });
  await page.route("**/api/auth/me", (route) => route.fulfill({ json: {
    authenticated: true, role: "admin", user: null, capabilities: ["view_results", "run_screeners"],
  } }));
  const dailyTop5Candidates = candidates(sectorTickers, "TOP5").map((row, index) => ({ ...row, shares: 100, selection_date: "2026-10-19", order_date: "2026-10-19", queue_rank: index + 1, entry_policy_version: "top_hits_max_entry_5pct_v1" }));
  let apiPayload = { configured: true, books: { account: book(accountCandidates, [position("OPEN1", "open", 1), position("CLOSED1", "closed", 2)], [{ id: 44, ticker: "OLDWAIT", session_date: "2026-10-16", status: "pending", payload_json: { market_data_state: "awaiting_data", market_data_reason: "first_session_bar_missed", limit_price: 25, stop: 22 } }]), sector_opportunity: book(candidates(sectorTickers, "SECTOR"), [position("OPEN2", "open", 3)]), daily_top5: book(dailyTop5Candidates) }, comparison: {} };
  let apiStatus = 200;
  let martinForwardRequests = 0;
  await page.route("**/api/admin/martin-forward-test", (route) => { martinForwardRequests += 1; return route.fulfill({ status: apiStatus, json: apiPayload }); });
  await page.goto(process.env.MARTIN_FORWARD_URL || "http://127.0.0.1:5173/backtests/martin-forward");
  await page.getByRole("heading", { name: "Account book · Pending candidates" }).waitFor();
  await page.getByRole("heading", { name: "Earlier pending orders awaiting market data" }).waitFor();
  await page.getByText("OLDWAIT", { exact: true }).waitFor();
  assert.equal(await page.getByText("Partial session: market bars were unavailable for 1 pending orders or open positions.").count(), 1);

  const candidateTickers = async (heading) => {
    const panel = page.getByRole("heading", { name: heading }).locator("xpath=../../..");
    return panel.locator("tbody tr").evaluateAll((rows) => rows.map((row) => row.querySelector("th")?.textContent?.trim()).filter(Boolean));
  };
  assert.deepEqual(await candidateTickers("Account book · Pending candidates"), accountTickers);
  assert.equal(await page.locator("th").filter({ hasText: "Entry limit" }).count(), 1);
  await page.getByRole("button", { name: "Selected · latest session" }).click();
  assert.deepEqual(await candidateTickers("Account book · Selected candidates"), [...accountTickers, "ACCOUNTWAIT"]);
  await page.getByRole("button", { name: "Waiting capacity" }).click();
  assert.deepEqual(await candidateTickers("Account book · Waiting candidates"), ["ACCOUNTWAIT"]);
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  assert.deepEqual(await candidateTickers("Account book · All candidates"), [...accountTickers, "ACCOUNTAUDIT", "ACCOUNTDATA", "ACCOUNTCAP", "ACCOUNTWAIT"]);
  await page.getByText("Excluded candidates and exceptions").click();
  await page.getByText("ADR or stop level is missing", { exact: true }).waitFor();
  const attentionTickers = await page.locator(".martin-exclusions h3").filter({ hasText: "Needs attention" }).evaluate((heading) => Array.from(heading.nextElementSibling?.querySelectorAll("tbody td:first-child") ?? []).map((cell) => cell.textContent?.trim()));
  assert.deepEqual(attentionTickers, ["ACCOUNTDATA"]);
  await page.getByText("Stored reason code", { exact: true }).first().click();
  assert.equal((await page.locator(".martin-code-detail code").first().textContent())?.includes("sector_quota_or_session_limit"), true);

  await page.getByRole("button", { name: "Open · all sessions" }).click();
  await page.getByText("OPEN1", { exact: true }).waitFor();
  await page.getByText("Scheduled exit Next open", { exact: false }).waitFor();
  await page.getByRole("tab", { name: "Positions", exact: true }).click();
  await page.getByRole("heading", { name: "Positions" }).waitFor();
  await page.getByText("Remaining 25", { exact: false }).first().waitFor();
  await page.getByRole("tab", { name: "Performance", exact: true }).click();
  await page.getByRole("heading", { name: "Book comparison" }).waitFor();
  await page.getByRole("heading", { name: "Account book equity and drawdown" }).waitFor();
  await page.getByText("Data & audit", { exact: true }).click();
  await page.getByRole("button", { name: "Copy session JSON" }).click();
  await page.getByText("Session data copied.", { exact: true }).waitFor();
  await page.getByText("Data & audit", { exact: true }).click();
  assert.equal(await page.locator(".martin-chart-scale").count(), 3);
  await page.screenshot({ path: "/tmp/martin-forward-desktop.png", fullPage: true });

  await page.setViewportSize({ width: 390, height: 844 });
  const closeDrawer = page.getByRole("button", { name: /close/i });
  if (await closeDrawer.count()) await closeDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  await page.waitForTimeout(400);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
  await page.screenshot({ path: "/tmp/martin-forward-mobile.png" });
  await page.locator(".martin-comparison-chart").screenshot({ path: "/tmp/martin-forward-mobile-chart.png" });
  const legendRects = await page.locator(".martin-comparison-chart > div:last-child > span").evaluateAll((items) => items.map((item) => { const rect = item.getBoundingClientRect(); return { top: rect.top, bottom: rect.bottom, left: rect.left, right: rect.right }; }));
  const chartRect = await page.locator(".martin-comparison-chart").boundingBox();
  assert.equal(legendRects.every((rect) => rect.bottom <= chartRect.y + chartRect.height + 1), true, "All three book labels fit inside the mobile chart");
  assert.equal(legendRects.every((rect, index) => legendRects.slice(index + 1).every((other) => rect.bottom <= other.top || other.bottom <= rect.top || rect.right <= other.left || other.right <= rect.left)), true, `Chart labels overlap: ${JSON.stringify(legendRects)}`);
  const overflow = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth, main: document.querySelector(".martin-page")?.scrollWidth }));
  assert.equal(overflow.body <= overflow.viewport, true, `Unexpected page overflow: ${JSON.stringify(overflow)}`);
  await page.getByRole("tab", { name: "Today", exact: true }).click();
  await page.getByRole("button", { name: "Sector opportunity", exact: true }).click();
  await page.getByRole("heading", { name: "Sector opportunity · Pending candidates" }).waitFor();
  assert.deepEqual(await candidateTickers("Sector opportunity · Pending candidates"), sectorTickers);
  assert.match(page.url(), /book=sector_opportunity/);
  assert.match(page.url(), /view=today/);

  await page.getByRole("tab", { name: "Today", exact: true }).click();
  await page.getByRole("button", { name: "Daily Top 5", exact: true }).click();
  await page.getByRole("heading", { name: "Daily Top 5 · Pending candidates" }).waitFor();
  assert.deepEqual(await candidateTickers("Daily Top 5 · Pending candidates"), sectorTickers);
  await page.getByRole("button", { name: "Details", exact: true }).first().click();
  await page.getByText("selection date", { exact: true }).waitFor();
  await page.getByText("order date", { exact: true }).waitFor();

  const legacyWatch = (ticker, readiness, entry, stop) => ({ ticker, status: "watch", trade_readiness: readiness, entry, stop,
    reasons: ["trade_readiness_not_defined_risk"], selected_for_book: false });
  apiPayload = { configured: true, books: { account: book([
    legacyWatch("LEGACYSUPPORT", "pullback_zone", 57.018, 55.362149543412976),
    legacyWatch("LEGACYWIDE", "reclaim_trigger", 67.0768, 63.12980351389087),
    legacyWatch("LEGACYBOTH", "pullback_zone", 50, 45),
    legacyWatch("LEGACYLEADER", "leader", null, null),
    legacyWatch("LEGACYMISSING", "reclaim_trigger", null, null),
    { ticker: "NEWCONFIRM", status: "watch", reasons: ["awaiting_confirmation"], selected_for_book: false },
    { ticker: "NEWWIDE", status: "watch", reasons: ["risk_exceeds_5pct"], selected_for_book: false },
    { ticker: "NEWNOPLAN", status: "watch", reasons: ["no_supported_plan"], selected_for_book: false },
    { ticker: "NEWMISSING", status: "watch", reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "TBBB", status: "watch", entry: 57.018, stop: 55.362149543412976, max_entry: 58.27594688780314, reasons: ["awaiting_confirmation"], selected_for_book: false },
    { ticker: "CDN", status: "watch", entry: 67.0768, stop: 63.12980351389087, max_entry: 66.45242475146408, reference_risk_pct: 5.88, risk_status: "over_limit", plan_state: "watch", source_strategy_id: "ma_pullback_retest", signal_date: "2026-10-19", selection_date: "2026-10-20", reasons: ["risk_exceeds_5pct"], selected_for_book: false },
    { ticker: "AVPT", status: "watch", reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "BADLEVELS", status: "watch", entry: -2, stop: 0, max_entry: -1, reference_risk_pct: "5.88", reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "PARTIAL", status: "watch", entry: 12, max_entry: 13, reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "ZEROLEVEL", status: "watch", reference_risk_pct: 0, reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "NEGLEVEL", status: "watch", reference_risk_pct: -1, reasons: ["plan_levels_missing"], selected_for_book: false },
    { ticker: "RISKBOUNDARY", status: "watch", entry: 100, stop: 94.998, reasons: ["risk_exceeds_5pct"], selected_for_book: false },
  ]) } };
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=account&view=today");
  await page.getByText("Excluded candidates and exceptions").click();
  const expectedReasons = {
    LEGACYSUPPORT: "Awaiting confirmation", LEGACYWIDE: "Risk exceeds 5%", LEGACYBOTH: "Awaiting confirmation; Risk exceeds 5%",
    LEGACYLEADER: "No supported plan", LEGACYMISSING: "Plan levels missing", NEWCONFIRM: "Awaiting confirmation",
    NEWWIDE: "Risk exceeds 5%", NEWNOPLAN: "No supported plan", NEWMISSING: "Plan levels missing", TBBB: "Awaiting confirmation",
    CDN: "Risk exceeds 5%", AVPT: "Plan levels missing", BADLEVELS: "Plan levels missing", PARTIAL: "Plan levels missing",
    ZEROLEVEL: "Plan levels missing", NEGLEVEL: "Plan levels missing", RISKBOUNDARY: "Risk exceeds 5%",
  };
  for (const [ticker, reason] of Object.entries(expectedReasons)) {
    const row = page.locator(".martin-exclusions tbody tr").filter({ has: page.getByRole("cell", { name: ticker, exact: true }) });
    assert.equal(await row.locator("td").nth(2).locator("span").innerText(), reason);
  }
  const watchRow = (ticker) => page.locator(".martin-exclusions tbody tr").filter({ has: page.getByRole("cell", { name: ticker, exact: true }) });
  const previewFor = (ticker) => watchRow(ticker).getByText("View trade plan", { exact: true });
  const previewToggleFor = (ticker) => watchRow(ticker).getByRole("button", { name: /(?:view|hide) trade plan/i });
  const previewPanelFor = (ticker) => watchRow(ticker).locator("xpath=following-sibling::tr[1]").locator(".martin-trade-plan");
  const requestsBeforePreviewInteractions = martinForwardRequests;
  await previewFor("TBBB").focus();
  await page.keyboard.press("Enter");
  assert.equal(await watchRow("TBBB").getByRole("button", { name: "Hide trade plan", exact: true }).getAttribute("aria-expanded"), "true", "Keyboard activation opens the trade plan disclosure");
  const tbbbPreview = previewPanelFor("TBBB");
  await tbbbPreview.getByText("$57.02", { exact: true }).waitFor();
  await tbbbPreview.getByText("$55.36", { exact: true }).waitFor();
  await tbbbPreview.getByText("$58.28", { exact: true }).waitFor();
  await tbbbPreview.getByText("Calculated from saved levels", { exact: false }).waitFor();
  await tbbbPreview.getByText("Plan preview · not an active order", { exact: true }).waitFor();
  await previewFor("CDN").click();
  const cdnPreview = previewPanelFor("CDN");
  await cdnPreview.getByText("5.88%", { exact: true }).waitFor();
  await cdnPreview.getByText("MA Pullback & Retest", { exact: true }).waitFor();
  await cdnPreview.getByText("$66.45", { exact: true }).waitFor();
  await cdnPreview.getByText("2026-10-19", { exact: true }).waitFor();
  await cdnPreview.getByText("2026-10-20", { exact: true }).waitFor();
  await cdnPreview.getByText("Saved plan risk exceeds the 5% limit.", { exact: true }).waitFor();
  await previewFor("AVPT").click();
  const avptPreview = previewPanelFor("AVPT");
  assert.equal(await avptPreview.getByText("Unavailable", { exact: true }).count(), 8);
  await previewFor("BADLEVELS").click();
  const invalidPreview = previewPanelFor("BADLEVELS");
  assert.equal(await invalidPreview.locator("dd").nth(0).innerText(), "Unavailable");
  assert.equal(await invalidPreview.locator("dd").nth(1).innerText(), "Unavailable");
  assert.equal(await invalidPreview.locator("dd").nth(2).innerText(), "Unavailable");
  await previewFor("PARTIAL").click();
  const partialPreview = previewPanelFor("PARTIAL");
  assert.equal(await partialPreview.locator("dd").nth(0).innerText(), "$12");
  assert.equal(await partialPreview.locator("dd").nth(1).innerText(), "Unavailable");
  assert.equal(await partialPreview.locator("dd").nth(3).innerText(), "Unavailable");
  for (const ticker of ["ZEROLEVEL", "NEGLEVEL"]) {
    await previewFor(ticker).click();
    const boundaryPreview = previewPanelFor(ticker);
    assert.equal(await boundaryPreview.locator("dd").nth(3).innerText(), "Unavailable");
  }
  await previewFor("RISKBOUNDARY").click();
  const riskBoundaryPreview = previewPanelFor("RISKBOUNDARY");
  await riskBoundaryPreview.getByText("5.002% · Calculated from saved levels", { exact: true }).waitFor();
  await riskBoundaryPreview.getByText("Saved plan risk exceeds the 5% limit.", { exact: true }).waitFor();
  assert.equal(martinForwardRequests, requestsBeforePreviewInteractions, "Opening saved-row previews makes no additional endpoint requests");
  await page.setViewportSize({ width: 1440, height: 1000 });
  await previewToggleFor("RISKBOUNDARY").click();
  await previewFor("TBBB").click();
  await tbbbPreview.screenshot({ path: "/tmp/martin-trade-plan-preview-desktop.png" });
  await page.setViewportSize({ width: 390, height: 844 });
  if (await closeDrawer.count()) await closeDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  await tbbbPreview.scrollIntoViewIfNeeded();
  assert.equal(await tbbbPreview.isVisible(), true, "The preview remains visible at mobile width");
  assert.equal(await watchRow("TBBB").getByRole("button", { name: "Hide trade plan", exact: true }).getAttribute("aria-expanded"), "true");
  const mobilePreviewBounds = await tbbbPreview.evaluate((element) => { const rect = element.getBoundingClientRect(); return { left: rect.left, right: rect.right, width: rect.width }; });
  assert.equal(mobilePreviewBounds.left >= 0 && mobilePreviewBounds.right <= 390, true, `Preview exceeds the mobile viewport: ${JSON.stringify(mobilePreviewBounds)}`);
  assert.equal(await page.evaluate(() => document.body.scrollWidth <= window.innerWidth), true, "Watch explanations fit the 390px mobile viewport");
  await page.screenshot({ path: "/tmp/martin-trade-plan-preview-mobile.png" });
  await page.setViewportSize({ width: 195, height: 844 });
  await tbbbPreview.scrollIntoViewIfNeeded();
  const zoomOverflow = await tbbbPreview.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    const viewport = window.innerWidth;
    const result = { body: document.body.scrollWidth, viewport, previewLeft: rect.left, previewRight: rect.right };
    return result;
  });
  assert.equal(zoomOverflow.previewLeft >= 0 && zoomOverflow.previewRight <= zoomOverflow.viewport, true, `Preview exceeds the effective viewport at 200% zoom: ${JSON.stringify(zoomOverflow)}`);
  const missingTickers = await page.locator(".martin-exclusions h3").filter({ hasText: "Needs attention" }).evaluate((h) =>
    Array.from(h.nextElementSibling.querySelectorAll("tbody tr.martin-exclusion-row td:first-child"), cell => cell.textContent.trim()));
  assert.deepEqual(missingTickers, ["LEGACYMISSING", "NEWMISSING", "AVPT", "BADLEVELS", "PARTIAL", "ZEROLEVEL", "NEGLEVEL"], "Only missing or invalid levels are data exceptions; waiting and wide risk stay routine");
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  const supportRow = page.locator(".martin-candidate-table tbody tr").filter({ has: page.getByRole("rowheader", { name: "LEGACYSUPPORT", exact: true }) });
  assert.match(await supportRow.innerText(), /No order: Awaiting confirmation/);
  await supportRow.getByRole("button", { name: "Details", exact: true }).click();
  await page.locator(".martin-raw-code").filter({ hasText: "trade_readiness_not_defined_risk" }).waitFor();
  const detailPreview = supportRow.locator("xpath=following-sibling::tr[1]").locator(".martin-trade-plan");
  await detailPreview.getByText("Trade plan preview", { exact: true }).waitFor();
  assert.equal(await supportRow.getByText("View trade plan", { exact: true }).count(), 0, "The expanded candidate details reuse the same preview without duplicate controls");
  assert.equal(await page.getByText("Trade risk is not defined", { exact: true }).count(), 0);
  await page.screenshot({ path: "/tmp/martin-watch-reasons-mobile.png" });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.screenshot({ path: "/tmp/martin-watch-reasons-desktop.png" });

  apiPayload = { configured: true, books: { account: book([], []) } };
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=account&view=today");
  await page.getByRole("heading", { name: "Account book · Closed positions" }).waitFor();
  await page.getByRole("button", { name: "Open · all sessions" }).click();
  await page.getByText("No simulated entries have filled yet.", { exact: false }).waitFor();
  await page.getByRole("button", { name: "Closed · all sessions" }).click();
  await page.getByText("No simulated positions have closed yet.", { exact: true }).waitFor();

  apiPayload.books.account.runs = [];
  await page.reload();
  await page.getByRole("heading", { name: "No session yet" }).waitFor();
  apiPayload = { configured: false, books: {} };
  await page.reload();
  await page.getByRole("heading", { name: "Database unavailable" }).waitFor();
  apiStatus = 500;
  await page.reload();
  await page.getByRole("heading", { name: "Database error" }).waitFor();
  console.log("Martin lifecycle probe passed: candidate scopes/details, data vs routine exclusions and raw codes, populated/empty positions, no-session/unavailable/error states, account-only performance, query retention, and 390px overflow check. Screenshots saved to /tmp/martin-forward-desktop.png, /tmp/martin-forward-mobile.png, and /tmp/martin-forward-mobile-chart.png.");
} finally {
  await browser.close();
}
