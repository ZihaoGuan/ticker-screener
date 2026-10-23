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
accountCandidates.push({ ticker: "AVOIDSNAP", status: "avoid", selected_for_book: false, reasons: ["marked_avoid"], readiness_explanation: "Weinstein Stage 4 is not a preferred entry regime.", readiness_details: ["Weinstein Stage 4 is not a preferred entry regime.", "Earnings are in 7 days.", "Pullback quality failed: support broke with expanding selling pressure."] });
accountCandidates.push({ ticker: "AVOIDLEGACY", status: "avoid", selected_for_book: false, reasons: ["marked_avoid"] });
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
  const benchmarkComparison = { status: "available", basis: "price_return_close", baseline_date: "2026-10-16", comparison_date: "2026-10-20", account_history_start: "2026-10-16", account_history_end: "2026-10-21", expected_sessions: 6, included_sessions: 3, missing_dates: ["2026-10-17", "2026-10-18", "2026-10-21"], series: {
    Account: [{ date: "2026-10-16", value: 100, return_pct: 0, drawdown_pct: 0 }, { date: "2026-10-17", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-18", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-19", value: 110, return_pct: 10, drawdown_pct: 0 }, { date: "2026-10-20", value: 105, return_pct: 5, drawdown_pct: -4.5454545 }, { date: "2026-10-21", value: null, return_pct: null, drawdown_pct: null }],
    SPY: [{ date: "2026-10-16", value: 100, return_pct: 0, drawdown_pct: 0 }, { date: "2026-10-17", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-18", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-19", value: 120, return_pct: 20, drawdown_pct: 0 }, { date: "2026-10-20", value: 110, return_pct: 10, drawdown_pct: -8.3333333 }, { date: "2026-10-21", value: null, return_pct: null, drawdown_pct: null }],
    QQQ: [{ date: "2026-10-16", value: 100, return_pct: 0, drawdown_pct: 0 }, { date: "2026-10-17", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-18", value: null, return_pct: null, drawdown_pct: null }, { date: "2026-10-19", value: 110, return_pct: 10, drawdown_pct: 0 }, { date: "2026-10-20", value: 105, return_pct: 5, drawdown_pct: -4.5454545 }, { date: "2026-10-21", value: null, return_pct: null, drawdown_pct: null }],
  } };
  const accountBook = book(accountCandidates, [position("OPEN1", "open", 1), position("CLOSED1", "closed", 2)], [{ id: 44, ticker: "OLDWAIT", session_date: "2026-10-16", status: "pending", payload_json: { market_data_state: "awaiting_data", market_data_reason: "first_session_bar_missed", limit_price: 25, stop: 22 } }]);
  accountBook.benchmark_comparison = benchmarkComparison;
  let apiPayload = { configured: true, books: { account: accountBook, sector_opportunity: book(candidates(sectorTickers, "SECTOR"), [position("OPEN2", "open", 3)]), daily_top5: book(dailyTop5Candidates) }, comparison: {} };
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
  assert.deepEqual(await candidateTickers("Account book · All candidates"), [...accountTickers, "ACCOUNTAUDIT", "ACCOUNTDATA", "ACCOUNTCAP", "ACCOUNTWAIT", "AVOIDSNAP", "AVOIDLEGACY"]);
  await page.getByText("Excluded candidates and exceptions").click();
  await page.getByText("ADR or stop level is missing", { exact: true }).waitFor();
  const attentionTickers = await page.locator(".martin-exclusions h3").filter({ hasText: "Needs attention" }).evaluate((heading) => Array.from(heading.nextElementSibling?.querySelectorAll("tbody td:first-child") ?? []).map((cell) => cell.textContent?.trim()));
  assert.deepEqual(attentionTickers, ["ACCOUNTDATA"]);
  await page.getByText("Stored reason code", { exact: true }).first().click();
  assert.equal((await page.locator(".martin-code-detail code").first().textContent())?.includes("sector_quota_or_session_limit"), true);
  const routineExclusions = page.locator(".martin-exclusions");
  const savedAvoidRow = routineExclusions.locator("tbody tr").filter({ hasText: "AVOIDSNAP" });
  assert.equal(await savedAvoidRow.locator("td").nth(2).locator("span").innerText(), "Weinstein Stage 4 is not a preferred entry regime.");
  await savedAvoidRow.getByText("Why this was avoided", { exact: true }).click();
  assert.match(await savedAvoidRow.innerText(), /Earnings are in 7 days\.[\s\S]*Pullback quality failed: support broke with expanding selling pressure\./);
  await savedAvoidRow.getByText("Stored reason code", { exact: true }).click();
  assert.equal(await savedAvoidRow.locator("code").innerText(), '["marked_avoid"]', "Saved explanation leaves the raw reason code unchanged");
  const legacyAvoidRow = routineExclusions.locator("tbody tr").filter({ hasText: "AVOIDLEGACY" });
  await legacyAvoidRow.getByText("Why this was avoided", { exact: true }).click();
  assert.match(await legacyAvoidRow.innerText(), /Saved explanation details are unavailable for this historical row\./);
  await routineExclusions.screenshot({ path: "/tmp/martin-detailed-avoid-desktop.png" });
  await page.setViewportSize({ width: 390, height: 844 });
  const avoidCloseDrawer = page.getByRole("button", { name: /close/i });
  if (await avoidCloseDrawer.count()) await avoidCloseDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  await routineExclusions.screenshot({ path: "/tmp/martin-detailed-avoid-mobile.png" });
  assert.equal(await page.evaluate(() => document.body.scrollWidth <= innerWidth), true, "Expanded Avoid details fit the 390px mobile viewport");
  await page.setViewportSize({ width: 1440, height: 1000 });

  await page.getByRole("button", { name: "Open · all sessions" }).click();
  await page.getByText("OPEN1", { exact: true }).waitFor();
  await page.getByText("Scheduled exit Next open", { exact: false }).waitFor();
  await page.getByRole("tab", { name: "Positions", exact: true }).click();
  await page.getByRole("heading", { name: "Positions" }).waitFor();
  await page.getByText("Remaining 25", { exact: false }).first().waitFor();
  await page.getByRole("tab", { name: "Performance", exact: true }).click();
  await page.getByRole("heading", { name: "Book comparison" }).waitFor();
  await page.getByRole("heading", { name: "Account book equity and drawdown" }).waitFor();
  const benchmarkPanel = page.getByRole("heading", { name: "Account vs SPY and QQQ" }).locator("xpath=..");
  await benchmarkPanel.waitFor();
  assert.match(await benchmarkPanel.innerText(), /Rebased to 100 on 2026-10-16/);
  assert.match(await benchmarkPanel.innerText(), /persisted simulated NAV[\s\S]*stored Close price returns, excluding distributions/);
  assert.match(await benchmarkPanel.innerText(), /through the latest common session, 2026-10-20[\s\S]*history through 2026-10-21/);
  const benchmarkRows = benchmarkPanel.locator("tbody tr");
  assert.equal(await benchmarkRows.nth(0).locator("td").nth(0).innerText(), "5%", "Account return is taken at the fixed latest common date");
  assert.equal(await benchmarkRows.nth(0).locator("td").nth(1).innerText(), "-4.55%", "Maximum drawdown is calculated through that same date");
  assert.equal(await benchmarkRows.nth(1).locator("td").nth(0).innerText(), "10%");
  assert.equal(await benchmarkRows.nth(1).locator("td").nth(1).innerText(), "-8.33%");
  assert.equal(await benchmarkRows.nth(1).locator("td").nth(2).innerText(), "-5 pp");
  assert.equal(await benchmarkRows.nth(2).locator("td").nth(2).innerText(), "+0 pp");
  assert.match(await benchmarkPanel.innerText(), /Coverage: 3 of 6 sampled saved sessions/);
  assert.match(await benchmarkPanel.innerText(), /Missing sessions \(3\)/);
  assert.equal(await benchmarkPanel.locator("svg path").evaluateAll((paths) => paths.every((path) => (path.getAttribute("d")?.match(/M/g) || []).length === 2)), true, "All benchmark series break at missing sessions");
  await benchmarkPanel.screenshot({ path: "/tmp/martin-benchmark-desktop.png" });
  for (const bookName of ["Sector opportunity", "Daily Top 5"]) {
    await page.getByRole("button", { name: bookName, exact: true }).click();
    await page.getByRole("tab", { name: "Performance", exact: true }).click();
    await page.getByRole("heading", { name: "Book comparison" }).waitFor();
    assert.equal(await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).count(), 0, `${bookName} does not display the Account ETF benchmark comparison`);
  }
  await page.getByRole("button", { name: "Account book", exact: true }).click();
  await page.getByRole("tab", { name: "Performance", exact: true }).click();
  await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).waitFor();
  benchmarkComparison.status = "unavailable";
  benchmarkComparison.reason = "Common baseline 2026-10-16 is unavailable: QQQ.";
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=account&view=performance");
  const unavailableBenchmarkPanel = page.getByRole("heading", { name: "Account vs SPY and QQQ" }).locator("xpath=..");
  await unavailableBenchmarkPanel.getByText("Common baseline 2026-10-16 is unavailable: QQQ.", { exact: true }).waitFor();
  assert.equal(await unavailableBenchmarkPanel.locator("table").count(), 0, "Unavailable baseline is not replaced with a shifted start or partial metric table");
  benchmarkComparison.status = "available";
  delete benchmarkComparison.reason;
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=account&view=performance");
  await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).waitFor();
  await page.getByText("Data & audit", { exact: true }).click();
  await page.getByRole("button", { name: "Copy session JSON" }).click();
  await page.getByText("Session data copied.", { exact: true }).waitFor();
  await page.getByText("Data & audit", { exact: true }).click();
  assert.equal(await page.locator(".martin-chart-scale").count(), 4);
  assert.equal(await page.getByRole("img", { name: /^Cumulative R comparison/ }).locator("svg path").count(), 3, "The existing three R curves are unchanged before experiment history exists");
  await page.screenshot({ path: "/tmp/martin-forward-desktop.png", fullPage: true });

  await page.setViewportSize({ width: 390, height: 844 });
  const closeDrawer = page.getByRole("button", { name: /close/i });
  if (await closeDrawer.count()) await closeDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  await page.waitForTimeout(400);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
  await page.screenshot({ path: "/tmp/martin-forward-mobile.png" });
  const mobileBenchmarkPanel = page.getByRole("heading", { name: "Account vs SPY and QQQ" }).locator("xpath=..");
  await mobileBenchmarkPanel.scrollIntoViewIfNeeded();
  const benchmarkTableBounds = await mobileBenchmarkPanel.locator('.martin-benchmark-table').evaluate((element) => ({ width: element.clientWidth, scroll: element.scrollWidth }));
  assert.ok(benchmarkTableBounds.scroll <= benchmarkTableBounds.width + 1, 'All four benchmark metric columns fit without horizontal scrolling on mobile');
  const metricCells = await mobileBenchmarkPanel.locator('tbody tr').first().locator('th, td').evaluateAll((cells) => cells.map((cell) => { const rect = cell.getBoundingClientRect(); return { left: rect.left, right: rect.right }; }));
  assert.equal(metricCells.length, 4);
  assert.ok(metricCells.every((cell) => cell.left >= 0 && cell.right <= 390), 'Every benchmark metric is visible within the phone viewport');
  await mobileBenchmarkPanel.screenshot({ path: "/tmp/martin-benchmark-mobile.png" });
  const benchmarkChart = mobileBenchmarkPanel.locator(".martin-comparison-chart");
  await benchmarkChart.screenshot({ path: "/tmp/martin-forward-mobile-chart.png" });
  const legendRects = await benchmarkChart.locator("> div:last-child > span").evaluateAll((items) => items.map((item) => { const rect = item.getBoundingClientRect(); return { top: rect.top, bottom: rect.bottom, left: rect.left, right: rect.right }; }));
  const chartRect = await benchmarkChart.boundingBox();
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
  const savedConfirmation = "Saved MA Retest requires a rising support average and a held support touch; this snapshot does not record each individual check result.";
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
    { ticker: "TBBB", status: "watch", entry: 57.018, stop: 55.362149543412976, max_entry: 58.27594688780314, reasons: ["awaiting_confirmation"], confirmation_rules: savedConfirmation, source_strategy_id: "ma_pullback_retest", selected_for_book: false },
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
  const previewConfirmation = tbbbPreview.locator("details").filter({ hasText: "Confirmation rule" });
  await previewConfirmation.locator("summary").click();
  assert.match(await previewConfirmation.innerText(), /Saved MA Retest requires a rising support average[\s\S]*does not record each individual check result/);
  const tbbbRow = watchRow("TBBB");
  const rowConfirmation = tbbbRow.locator("details").filter({ hasText: "Confirmation rule" });
  await rowConfirmation.locator("summary").click();
  assert.match(await rowConfirmation.innerText(), /Saved MA Retest requires a rising support average[\s\S]*does not record each individual check result/);
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
  const newConfirmRow = watchRow("NEWCONFIRM");
  await newConfirmRow.getByText("Confirmation rule", { exact: true }).click();
  assert.match(await newConfirmRow.innerText(), /Scanner confirmation details are unavailable in this saved snapshot\./, "Legacy confirmation rows explain missing saved details without reconstructing them");
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

  const preservedAccount = apiPayload.books.account;
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=rs_expanded&view=performance");
  await page.getByRole("heading", { name: "No session yet" }).waitFor();
  await page.getByText("No RS Expanded session has been saved yet.", { exact: false }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).count(), 0, "An absent experimental book never borrows baseline performance");
  const emergingChecks = { fresh_completed_session: true, history_complete: true, trend: true, relative_strength_improving: true, liquidity: true, supported_plan: true, confirmed: false, pullback_quality: true, valid_plan_levels: true, risk_within_5pct: true, order_notional_within_liquidity: null };
  const expandedBook = book([
    { ticker: "EMERGE85", status: "watch", rs_tier: "emerging", rs_rating: 85, reasons: ["rs_confirmation_required"], entry: 100, stop: 97, max_entry: 102, rs_exception_checks: emergingChecks, rs_exception_metrics: { as_of_date: "2026-10-19", close: 100, sma50: 95, sma200: 80, adv20_dollars: 25000000, stock_spy_ratio: 0.152345, stock_spy_ratio_20_sessions_ago: 0.14, actual_order_notional: null } },
    { ticker: "PREFER95", status: "pending", rs_tier: "preferred", selected_for_book: true, reasons: [], shares: 40, entry: 100, stop: 97 },
    { ticker: "RESEARCH70", status: "watch", rs_tier: "research", reasons: ["rs_below_80_research"] },
    { ticker: "NORS", status: "blocked_data", rs_tier: "missing", reasons: ["rs_rating_missing"] },
  ]);
  expandedBook.strategy_id = "martin_rs_expanded_v1";
  expandedBook.cost_model = "10 bps adverse slippage on stops and market-open exits; no commissions modeled.";
  expandedBook.history = [{ date: "2026-10-19", cumulative_r: 0, account_equity: 100000, equity_drawdown_pct: 0 }];
  expandedBook.rs_tier_metrics = {
    preferred: { closed_count: 2, realized_r: 1, realized_pnl: 500, win_rate_pct: 50, expectancy_r: 0.5, stop_out_rate_pct: 50, average_holding_sessions: 4, max_closed_drawdown_r: -1 },
    emerging: { closed_count: 0, realized_r: 0, realized_pnl: 0, win_rate_pct: null, expectancy_r: null, stop_out_rate_pct: null, average_holding_sessions: null, max_closed_drawdown_r: 0 },
  };
  apiPayload.books.rs_expanded = expandedBook;
  await page.goto("http://127.0.0.1:5173/backtests/martin-forward?book=rs_expanded&view=today");
  await page.getByRole("heading", { name: "RS Expanded · experimental · Pending candidates" }).waitFor();
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  const emergingRow = page.locator(".martin-candidate-table tbody tr").filter({ has: page.getByRole("rowheader", { name: "EMERGE85", exact: true }) });
  await emergingRow.getByRole("button", { name: "Details", exact: true }).click();
  const emergingEvidence = emergingRow.locator("xpath=following-sibling::tr[1]").getByRole("region", { name: "Saved RS eligibility evidence" });
  await emergingEvidence.getByRole("heading", { name: "RS tier: Emerging · RS 80–89" }).waitFor();
  assert.equal(await emergingEvidence.locator("dd").filter({ hasText: /^Passed$/ }).count(), 9);
  assert.equal(await emergingEvidence.locator("dd").filter({ hasText: /^Not passed$/ }).count(), 1);
  assert.equal(await emergingEvidence.locator(":scope > dl dd").filter({ hasText: /^Unavailable$/ }).count(), 1);
  await emergingEvidence.getByText("Saved RS measurements", { exact: true }).click();
  await emergingEvidence.getByText("0.152345", { exact: true }).waitFor();
  await page.getByText("Excluded candidates and exceptions").click();
  const expandedExclusions = page.locator(".martin-exclusions");
  await expandedExclusions.getByText("RS below 80 · research only, no entry", { exact: true }).waitFor();
  await expandedExclusions.getByText("RS rating is missing; data is required", { exact: true }).waitFor();
  const emergingWatch = expandedExclusions.locator("tr.martin-exclusion-row").filter({ hasText: "EMERGE85" });
  await emergingWatch.getByRole("button", { name: "View trade plan", exact: true }).click();
  const watchEvidence = emergingWatch.locator("xpath=following-sibling::tr[1]").getByRole("region", { name: "Saved RS eligibility evidence" });
  await watchEvidence.getByRole("heading", { name: "RS tier: Emerging · RS 80–89" }).waitFor();
  await watchEvidence.screenshot({ path: "/tmp/martin-rs-expanded-evidence-desktop.png" });
  await page.setViewportSize({ width: 390, height: 844 });
  const rsCloseDrawer = page.getByRole("button", { name: /close/i });
  if (await rsCloseDrawer.count()) await rsCloseDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  await watchEvidence.scrollIntoViewIfNeeded();
  const rsBounds = await watchEvidence.evaluate((element) => { const rect = element.getBoundingClientRect(); return { left: rect.left, right: rect.right }; });
  assert.ok(rsBounds.left >= 0 && rsBounds.right <= 390, "RS evidence fits the phone viewport");
  await page.screenshot({ path: "/tmp/martin-rs-expanded-evidence-mobile.png" });
  await page.getByRole("tab", { name: "Performance", exact: true }).click();
  const cohortPanel = page.getByRole("heading", { name: "RS cohort outcomes" }).locator("xpath=..");
  await cohortPanel.waitFor();
  assert.match(await cohortPanel.innerText(), /Preferred · RS 90–99[\s\S]*Emerging · RS 80–89/);
  assert.match(await cohortPanel.innerText(), /Drawdown is closed-trade R, not account NAV/);
  assert.match(await cohortPanel.innerText(), /Cost model: 10 bps adverse slippage on stops and market-open exits; no commissions modeled/);
  assert.equal(await cohortPanel.locator("tbody tr").nth(1).locator("td").nth(1).innerText(), "0", "Zero-trade emerging cohort is explicit");
  assert.equal(await cohortPanel.locator("tbody tr").nth(1).locator("td").nth(4).innerText(), "—", "No trades do not invent a win rate");
  await page.getByText("Books can start on different dates;", { exact: false }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).count(), 0, "Expanded book does not substitute the baseline ETF results");
  const rsComparisonChart = page.getByRole("img", { name: /^Cumulative R comparison/ });
  assert.equal(await rsComparisonChart.locator("svg path").count(), 4, "The experimental R series is included only when saved history exists");
  await rsComparisonChart.getByText("RS Expanded", { exact: true }).waitFor();
  assert.equal(await rsComparisonChart.locator("path.rs-expanded-series").count(), 1);
  const rsLegendBounds = await rsComparisonChart.locator(".rs-expanded-legend").evaluate((element) => { const rect = element.getBoundingClientRect(); return { left: rect.left, right: rect.right }; });
  assert.ok(rsLegendBounds.left >= 0 && rsLegendBounds.right <= 390, "The fourth R-chart legend fits the phone viewport");
  assert.equal(await page.evaluate(() => document.body.scrollWidth <= innerWidth), true, "The expanded performance view has no page-level overflow at 390px");
  await rsComparisonChart.screenshot({ path: "/tmp/martin-rs-comparison-mobile.png" });
  await cohortPanel.screenshot({ path: "/tmp/martin-rs-cohort-mobile.png" });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await cohortPanel.screenshot({ path: "/tmp/martin-rs-cohort-desktop.png" });
  await page.getByRole("button", { name: "Account book", exact: true }).click();
  assert.match(page.url(), /book=account/);
  assert.match(page.url(), /view=performance/);
  await page.getByRole("heading", { name: "Account vs SPY and QQQ" }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "RS cohort outcomes" }).count(), 0);
  assert.equal(apiPayload.books.account, preservedAccount, "The experiment leaves baseline saved data untouched");

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
