import assert from "node:assert/strict";
import { chromium } from "playwright";

const accountTickers = ["EXEL", "CAKE", "JOYY", "GKOS", "GTLB"];
const sectorTickers = ["EXEL", "CAKE", "JOYY"];
const candidates = (pending, prefix) => [
  ...pending.map((ticker) => ({ ticker, status: "pending", selected_for_book: true, selection_tier: "A" })),
  { ticker: `${prefix}AUDIT`, status: "blocked", selected_for_book: false },
];
const book = (rows) => ({
  strategy_id: "martin_focus",
  runs: [{ session_date: "2026-10-19", summary_json: {} }],
  candidates: rows,
  positions: [], events: [], history: [], attribution: {},
  metrics: { closed_count: 0 },
});

const browser = await chromium.launch({ headless: true, channel: "chrome" });
try {
  const page = await browser.newPage();
  page.on("pageerror", (error) => { throw error; });
  await page.route("**/api/auth/me", (route) => route.fulfill({ json: {
    authenticated: true, role: "admin", user: null, capabilities: ["view_results", "run_screeners"],
  } }));
  await page.route("**/api/admin/martin-forward-test", (route) => route.fulfill({ json: {
    configured: true,
    books: { account: book(candidates(accountTickers, "ACCOUNT")), sector_opportunity: book(candidates(sectorTickers, "SECTOR")) },
    comparison: {},
  } }));
  await page.goto(process.env.MARTIN_FORWARD_URL || "http://127.0.0.1:5173/backtests/martin-forward");
  await page.getByRole("heading", { name: "Account book · Pending candidates" }).waitFor();

  const candidateTickers = async (heading) => {
    const panel = page.getByRole("heading", { name: heading }).locator("xpath=../../..");
    return panel.locator("tbody tr").evaluateAll((rows) => rows.map((row) => row.querySelector("th")?.textContent?.trim()).filter(Boolean));
  };
  assert.deepEqual(await candidateTickers("Account book · Pending candidates"), accountTickers);
  await page.getByRole("button", { name: "Selected 5" }).click();
  assert.deepEqual(await candidateTickers("Account book · Selected candidates"), accountTickers);
  await page.getByRole("button", { name: "Open 0" }).click();
  await page.getByText("No simulated entries have filled yet. Pending orders are evaluated against the next completed session.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Closed 0" }).click();
  await page.getByText("No simulated positions have closed yet.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  assert.deepEqual(await candidateTickers("Account book · All candidates"), [...accountTickers, "ACCOUNTAUDIT"]);

  await page.getByRole("button", { name: "Sector opportunity" }).click();
  await page.getByRole("heading", { name: "Sector opportunity · Pending candidates" }).waitFor();
  assert.deepEqual(await candidateTickers("Sector opportunity · Pending candidates"), sectorTickers);
  await page.getByRole("button", { name: "Selected 3" }).click();
  assert.deepEqual(await candidateTickers("Sector opportunity · Selected candidates"), sectorTickers);
  await page.getByRole("button", { name: "Open 0" }).click();
  await page.getByText("No simulated entries have filled yet. Pending orders are evaluated against the next completed session.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Closed 0" }).click();
  await page.getByText("No simulated positions have closed yet.", { exact: true }).waitFor();
  await page.getByRole("button", { name: "All candidates", exact: true }).click();
  assert.deepEqual(await candidateTickers("Sector opportunity · All candidates"), [...sectorTickers, "SECTORAUDIT"]);
  console.log("Martin lifecycle probe passed: account/sector Selected and Pending tickers, open/closed empty states, and All candidates restore.");
} finally {
  await browser.close();
}
