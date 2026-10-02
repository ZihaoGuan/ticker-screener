import { chromium } from "playwright";

const baseUrl = process.env.MY_PICKS_TEST_URL || "http://127.0.0.1:5174";
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();
const chartRequests = new Map();
let contextRequests = 0;

const baseRow = {
  id: 1,
  ticker: "TEST",
  notes: "Watch the base",
  checklist: {},
  added_at: "2026-10-01T10:00:00Z",
  added_date: "2026-10-01",
  sector: "Technology",
  industry: "Software",
  latest_close: 101,
  change_1d_pct: 1.2,
  fundamental_rating: 90,
  leadership_score: 95,
  daily_rs_rating: 96,
  recent_signal_count: 1,
  recent_signals: [{ strategy_id: "qullamaggie", signal_date: "2026-10-01" }],
  position_bucket: "above_ema10",
  position_action: { action: "add_position", action_score: 88, reason_summary: "Constructive." },
  technical_indicator_ratings: {},
};
const nextRow = { ...baseRow, id: 2, ticker: "NEXT", latest_close: 202, position_bucket: "ema10_ema21" };
const addRow = { ...baseRow, id: 3, ticker: "ADD", notes: "New pick", latest_close: 303 };

await page.route("**/api/**", (route) => route.fulfill({ contentType: "application/json", body: "{}" }));
await page.route("**/api/auth/me", (route) => route.fulfill({
  contentType: "application/json",
  body: JSON.stringify({ authenticated: true, user: { id: 1, email: "admin@example.com" }, role: "admin", capabilities: ["view_results", "manage_exclusions"] }),
}));
await page.route("**/api/admin/my-picks", (route) => {
  if (route.request().method() === "POST") {
    return route.fulfill({ contentType: "application/json", body: JSON.stringify({ ok: true, pick: addRow }) });
  }
  contextRequests += 1;
  return route.fulfill({
    contentType: "application/json",
    body: JSON.stringify({ database_configured: true, total_count: 2, rows: [baseRow, nextRow], available_added_dates: ["2026-10-01"], fundamental_checklist: [], fundamental_summary: [] }),
  });
});
await page.route("**/api/charts/**", async (route) => {
  const url = new URL(route.request().url());
  const ticker = url.pathname.split("/")[3];
  const longHistory = url.searchParams.get("period") === "5y";
  const candles = longHistory
    ? Array.from({ length: 210 }, (_, index) => {
        const time = new Date(Date.UTC(2022, 0, 7 + (index * 7))).toISOString().slice(0, 10);
        return { time, open: 99 + index, high: 102 + index, low: 98 + index, close: 100 + index };
      })
    : [
        { time: "2026-09-30", open: 99, high: 102, low: 98, close: 100 },
        { time: "2026-10-01", open: 100, high: 103, low: 99, close: 101 },
      ];
  chartRequests.set(ticker, (chartRequests.get(ticker) ?? 0) + 1);
  await new Promise((resolve) => setTimeout(resolve, 100));
  await route.fulfill({
    contentType: "application/json",
    body: JSON.stringify({
      ticker,
      resolved_as_of_date: "2026-10-01",
      candles,
      volume: candles.map((item) => ({ time: item.time, value: 1_000_000 })),
      ma20: [], ma50: [], ma200: [], ema8: [], ema21: [], weekly_ema8: [], ipo_vwap: [], anchored_vwap_52w_low: [], rs_line: [],
    }),
  });
});

try {
  await page.goto(`${baseUrl}/my-picks?ticker=TEST`, { waitUntil: "networkidle" });
  await page.getByLabel("View").selectOption("sectors");
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Sector chart did not render");
  const weeklyButton = page.getByRole("button", { name: "W", exact: true });
  await weeklyButton.click();
  if (await weeklyButton.getAttribute("aria-pressed") !== "true") throw new Error("Weekly chart timeframe did not activate");
  await page.getByText("Loading TEST chart...").waitFor({ state: "visible", timeout: 3000 });
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (chartRequests.get("TEST") !== 2) throw new Error("Weekly EMA 200 did not request extended history");
  if (!(await page.getByLabel("Moving average legend").getByText("EMA 200").count())) throw new Error("EMA 200 legend is missing");
  await page.getByTitle("Select NEXT").click();
  await page.getByText("Loading NEXT chart...").waitFor({ state: "visible", timeout: 3000 });
  await page.getByText("Loading NEXT chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (chartRequests.get("NEXT") !== 1) throw new Error(`Expected one NEXT chart request, received ${chartRequests.get("NEXT") ?? 0}`);
  await page.getByLabel("View").selectOption("position");
  await page.locator('[aria-label="My Picks position map"]').waitFor({ state: "visible" });
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Position chart did not render");
  const contextRequestsBeforeDelete = contextRequests;
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Remove NEXT from My Picks" }).click();
  await page.getByText("Deleted NEXT.").waitFor({ state: "visible" });
  if (contextRequests !== contextRequestsBeforeDelete) throw new Error(`Delete refetched My Picks context ${contextRequests - contextRequestsBeforeDelete} time(s)`);
  if (await page.getByTitle("Select NEXT").count()) throw new Error("Deleted ticker remained in the current My Picks view");
  await page.getByRole("textbox", { name: "Ticker", exact: true }).fill("ADD");
  await page.getByRole("textbox", { name: "Notes", exact: true }).fill("New pick");
  await page.getByRole("button", { name: "Add Pick" }).click();
  await page.getByText("Added ADD to My Picks.").waitFor({ state: "visible" });
  if (contextRequests !== contextRequestsBeforeDelete) throw new Error("Add Pick refetched My Picks context");
  if (!(await page.getByTitle("Select ADD").count())) throw new Error("Added ticker did not appear in the current My Picks view");
  console.log("PASS My Picks chart, add, and delete interactions avoid context refetches");
} finally {
  await browser.close();
}
