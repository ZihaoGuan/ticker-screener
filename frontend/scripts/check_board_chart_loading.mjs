import { chromium } from "playwright";

const baseUrl = process.env.TOP_HITS_TEST_URL || "http://127.0.0.1:5174";
const browser = await chromium.launch({ channel: "chrome", headless: true });
const page = await browser.newPage();
let chartRequests = 0;

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
    total_unique_tickers: 1,
    overlapping_ticker_count: 1,
    rows: [row],
    guru_board: {
      definitions: [{ id: "qullamaggie", label: "Qullamaggie", accent: "amber", available: true }],
      total_unique_tickers: 1,
      total_scanner_matches: 1,
      confluence_ticker_count: 0,
      rows: [row],
    },
  }),
}));
await page.route("**/api/charts/TEST/preview?period=18mo", async (route) => {
  chartRequests += 1;
  await new Promise((resolve) => setTimeout(resolve, 100));
  await route.fulfill({
    contentType: "application/json",
    body: JSON.stringify({
      ticker: "TEST",
      benchmark_ticker: "SPY",
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
  await page.goto(`${baseUrl}/scanner/top-hits?ticker=TEST`, { waitUntil: "networkidle" });
  await page.getByRole("button", { name: "Guru Board" }).click();
  await page.getByText("Loading TEST chart...").waitFor({ state: "visible", timeout: 2000 });
  await page.getByText("Loading TEST chart...").waitFor({ state: "hidden", timeout: 2000 });
  if (chartRequests !== 1) throw new Error(`Expected one chart request, received ${chartRequests}`);
  if (!(await page.locator(".board-chart-panel-chart canvas").count())) throw new Error("Selected chart did not render");
  console.log("PASS selected board chart leaves loading state");
} finally {
  await browser.close();
}
