import { chromium } from "playwright";

const baseUrl = process.env.MY_PICKS_TEST_URL || "http://127.0.0.1:5174";
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();
const chartRequests = new Map();

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

await page.route("**/api/**", (route) => route.fulfill({ contentType: "application/json", body: "{}" }));
await page.route("**/api/auth/me", (route) => route.fulfill({
  contentType: "application/json",
  body: JSON.stringify({ authenticated: true, user: { id: 1, email: "admin@example.com" }, role: "admin", capabilities: ["view_results", "manage_exclusions"] }),
}));
await page.route("**/api/admin/my-picks", (route) => route.fulfill({
  contentType: "application/json",
  body: JSON.stringify({ database_configured: true, total_count: 2, rows: [baseRow, nextRow], available_added_dates: ["2026-10-01"], fundamental_checklist: [], fundamental_summary: [] }),
}));
await page.route("**/api/charts/*?period=18mo", async (route) => {
  const ticker = new URL(route.request().url()).pathname.split("/")[3];
  chartRequests.set(ticker, (chartRequests.get(ticker) ?? 0) + 1);
  await new Promise((resolve) => setTimeout(resolve, 100));
  await route.fulfill({
    contentType: "application/json",
    body: JSON.stringify({
      ticker,
      resolved_as_of_date: "2026-10-01",
      candles: [
        { time: "2026-09-30", open: 99, high: 102, low: 98, close: 100 },
        { time: "2026-10-01", open: 100, high: 103, low: 99, close: 101 },
      ],
      volume: [{ time: "2026-09-30", value: 1_000_000 }, { time: "2026-10-01", value: 1_200_000 }],
      ma20: [], ma50: [], ma200: [], ema8: [], ema21: [], weekly_ema8: [], ipo_vwap: [], anchored_vwap_52w_low: [], rs_line: [],
    }),
  });
});

try {
  await page.goto(`${baseUrl}/my-picks?ticker=TEST`, { waitUntil: "networkidle" });
  await page.getByLabel("View").selectOption("sectors");
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Sector chart did not render");
  await page.getByTitle("Select NEXT").click();
  await page.getByText("Loading NEXT chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (chartRequests.get("NEXT") !== 1) throw new Error(`Expected one NEXT chart request, received ${chartRequests.get("NEXT") ?? 0}`);
  await page.getByLabel("View").selectOption("position");
  await page.locator('[aria-label="My Picks position map"]').waitFor({ state: "visible" });
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Position chart did not render");
  console.log("PASS My Picks sector and position columns share a selectable chart panel");
} finally {
  await browser.close();
}
