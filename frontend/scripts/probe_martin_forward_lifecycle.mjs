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
const position = (ticker, status, id) => ({
  id, ticker, status, payload_json: { opened_on: "2026-10-12", entry_price: 25, stop: 22, initial_shares: 40, remaining_shares: status === "open" ? 25 : 0, realized_r: status === "closed" ? 1.5 : 0.25, realized_pnl: status === "closed" ? 150 : 25, exit_next_open: status === "open", holding_sessions: 4, exit_type: status === "closed" ? "target" : null },
});
const book = (rows, positions = []) => ({
  strategy_id: "martin_focus",
  runs: [{ session_date: "2026-10-19", summary_json: { partial: true, source_run_id: 42, action_counts: { missing_bars: 1 } } }],
  candidates: rows,
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
  let apiPayload = { configured: true, books: { account: book(candidates(accountTickers, "ACCOUNT"), [position("OPEN1", "open", 1), position("CLOSED1", "closed", 2)]), sector_opportunity: book(candidates(sectorTickers, "SECTOR"), [position("OPEN2", "open", 3)]) }, comparison: {} };
  let apiStatus = 200;
  await page.route("**/api/admin/martin-forward-test", (route) => route.fulfill({ status: apiStatus, json: apiPayload }));
  await page.goto(process.env.MARTIN_FORWARD_URL || "http://127.0.0.1:5173/backtests/martin-forward");
  await page.getByRole("heading", { name: "Account book · Pending candidates" }).waitFor();
  assert.equal(await page.getByText("Partial session: market bars were unavailable for 1 pending orders or open positions.").count(), 1);

  const candidateTickers = async (heading) => {
    const panel = page.getByRole("heading", { name: heading }).locator("xpath=../../..");
    return panel.locator("tbody tr").evaluateAll((rows) => rows.map((row) => row.querySelector("th")?.textContent?.trim()).filter(Boolean));
  };
  assert.deepEqual(await candidateTickers("Account book · Pending candidates"), accountTickers);
  assert.equal(await page.locator("th").filter({ hasText: "Entry limit" }).count(), 1);
  await page.getByRole("button", { name: "Selected · latest session" }).click();
  assert.deepEqual(await candidateTickers("Account book · Selected candidates"), accountTickers);
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  assert.deepEqual(await candidateTickers("Account book · All candidates"), [...accountTickers, "ACCOUNTAUDIT", "ACCOUNTDATA", "ACCOUNTCAP"]);
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
  await page.waitForTimeout(400);
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(150);
  await page.screenshot({ path: "/tmp/martin-forward-mobile.png" });
  await page.locator(".martin-comparison-chart").screenshot({ path: "/tmp/martin-forward-mobile-chart.png" });
  const legendRects = await page.locator(".martin-comparison-chart > div:last-child > span").evaluateAll((items) => items.map((item) => { const rect = item.getBoundingClientRect(); return { top: rect.top, bottom: rect.bottom, left: rect.left, right: rect.right }; }));
  assert.equal(legendRects.every((rect, index) => legendRects.slice(index + 1).every((other) => rect.bottom <= other.top || other.bottom <= rect.top || rect.right <= other.left || other.right <= rect.left)), true, `Chart labels overlap: ${JSON.stringify(legendRects)}`);
  const overflow = await page.evaluate(() => ({ body: document.body.scrollWidth, viewport: window.innerWidth, main: document.querySelector(".martin-page")?.scrollWidth }));
  assert.equal(overflow.body <= overflow.viewport, true, `Unexpected page overflow: ${JSON.stringify(overflow)}`);
  await page.getByRole("tab", { name: "Today", exact: true }).click();
  await page.getByRole("button", { name: "Sector opportunity", exact: true }).click();
  await page.getByRole("heading", { name: "Sector opportunity · Pending candidates" }).waitFor();
  assert.deepEqual(await candidateTickers("Sector opportunity · Pending candidates"), sectorTickers);
  assert.match(page.url(), /book=sector_opportunity/);
  assert.match(page.url(), /view=today/);

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
