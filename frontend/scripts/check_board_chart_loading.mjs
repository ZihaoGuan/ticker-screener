import { chromium } from "playwright";

const baseUrl = process.env.TOP_HITS_TEST_URL || "http://127.0.0.1:5174";
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();
const chartRequests = new Map();

const row = {
  ticker: "TEST",
  company: "Test Leader",
  sector: "Technology",
  industry: "Software",
  market_cap: 10_000_000_000,
  day_close: 101,
  change_pct: 1.2,
  daily_rs_rating: 95,
  rs_rating: 95,
  scanner_count: 1,
  scanners: [{ id: "qullamaggie", label: "Qullamaggie" }],
  stage_analysis: { alias: "Stage 2A" },
  strike_zone: { state: "ready", label: "Ready", score: 88, reason: "Test" },
};
const nextRow = {
  ...row,
  ticker: "NEXT",
  company: "Next Leader",
  day_close: 202,
  scanners: [{ id: "qullamaggie", label: "Qullamaggie" }],
};

await page.route("**/api/**", (route) => route.fulfill({ contentType: "application/json", body: "{}" }));
await page.route("**/api/auth/me", (route) => route.fulfill({
  contentType: "application/json",
  body: JSON.stringify({ authenticated: false, user: null, role: "visitor", capabilities: ["view_results"] }),
}));
await page.route("**/api/scanner-board/top-hits", (route) => route.fulfill({
  contentType: "application/json",
  body: JSON.stringify({
    target_trading_date: "2026-10-01",
    latest_signal_date: "2026-10-01",
    total_live_scanners: 1,
    total_unique_tickers: 2,
    overlapping_ticker_count: 2,
    rows: [row, nextRow],
    guru_board: {
      definitions: [{ id: "qullamaggie", label: "Qullamaggie", accent: "amber", available: true }],
      total_unique_tickers: 2,
      total_scanner_matches: 2,
      confluence_ticker_count: 0,
      rows: [row, nextRow],
    },
  }),
}));
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
      benchmark_ticker: "SPY",
      resolved_as_of_date: "2026-10-01",
      candles,
      volume: candles.map((item) => ({ time: item.time, value: 1_000_000 })),
      ma20: [], ma50: [], ma200: [], ema8: [], ema21: [], weekly_ema8: [], ipo_vwap: [], anchored_vwap_52w_low: [], rs_line: [],
    }),
  });
});

try {
  await page.goto(`${baseUrl}/scanner/top-hits?ticker=TEST`, { waitUntil: "networkidle" });
  await page.getByRole("button", { name: "Guru Board" }).click();
  await page.getByText("Loading TEST chart...").waitFor({ state: "visible", timeout: 2000 });
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 2000 });
  if (chartRequests.get("TEST") !== 1) throw new Error(`Expected one TEST chart request, received ${chartRequests.get("TEST") ?? 0}`);
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Selected chart did not render");
  const weeklyButton = page.getByRole("button", { name: "W", exact: true });
  await weeklyButton.click();
  if (await weeklyButton.getAttribute("aria-pressed") !== "true") throw new Error("Weekly chart timeframe did not activate");
  await page.getByText("Loading TEST chart...").waitFor({ state: "visible", timeout: 2000 });
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 3000 });
  if (chartRequests.get("TEST") !== 2) throw new Error("Weekly EMA 200 did not request extended history");
  if (!(await page.getByLabel("Moving average legend").getByText("EMA 200").count())) throw new Error("EMA 200 legend is missing");
  await page.getByTitle("Select NEXT").click();
  await page.getByText("Loading NEXT chart...").waitFor({ state: "visible", timeout: 2000 });
  await page.getByText("Loading NEXT chart...").waitFor({ state: "hidden", timeout: 2000 });
  if (chartRequests.get("NEXT") !== 1) throw new Error(`Expected one NEXT chart request, received ${chartRequests.get("NEXT") ?? 0}`);
  const noData = page.getByText("No chart data.");
  if (await noData.count() && await noData.first().isVisible()) throw new Error("Second ticker remained in the no-data state");
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Second selected chart did not render");
  console.log("PASS selected board charts load initially and after card switch");
} finally {
  await browser.close();
}
