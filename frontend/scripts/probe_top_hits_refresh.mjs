import assert from "node:assert/strict";
import { chromium } from "playwright";

const browser = await chromium.launch({ headless: true, channel: "chrome" });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.setDefaultTimeout(8000);
  await page.clock.install();
  await page.addInitScript(() => {
    window.testHidden = false;
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => window.testHidden ? "hidden" : "visible" });
  });
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  let version = 1, fail = false, manualStatus = "waiting";
  let holdList = false, releaseList, onListHeld;
  const calls = { list: 0, guru: 0 };
  const fixture = section => {
    const ticker = `V${version}`;
    const row = { ticker, company: "Refresh fixture", sector: "Technology", day_close: 104,
      market_cap: 5_000_000_000, daily_rs_rating: 95, trade_readiness: "defined_risk", adr14_pct: 4,
      scanner_count: 1, scanners: [{ id: "rs", label: "RS", timeframe: "Daily" }], scanner_labels: ["rs"],
      trade_plan: { state: "active", source_strategy_id: "ma_pullback_retest", trigger_price: 103, stop_price: 100,
        max_entry_price: 105, risk_valid: true, reference_risk_pct: 3, execution_timeframe: "daily" } };
    return { generated_at: `2026-10-20T23:0${version}:00Z`, latest_signal_date: "2026-10-20", target_trading_date: "2026-10-20",
      latest_update_at: `2026-10-20T23:0${version}:00Z`, total_live_scanners: 1, total_unique_tickers: 1,
      snapshot: { snapshot_run_id: 42, snapshot_generated_at: `2026-10-20T23:0${version}:00Z`, freshness: "fresh" },
      cards: [], rows: section === "guru" ? [] : [row],
      guru_board: { definitions: [], total_unique_tickers: 1, total_scanner_matches: 1, confluence_ticker_count: 0, rows: [row] } };
  };
  await page.route("**/api/**", async route => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/auth/me") return route.fulfill({ json: { authenticated: true, role: "admin", user: null, capabilities: ["view_results", "manage_exclusions"] } });
    if (url.pathname === "/api/scanner-board/refresh") return route.fulfill({ json: { cards: [], snapshot_refresh: {
      status: manualStatus, target_date: "2026-10-20", message: manualStatus === "waiting" ? "Waiting for scanner prerequisites for 2026-10-20; retained previous snapshot." : "Top Hits snapshot rebuilt for 2026-10-20.",
    } } });
    if (url.pathname === "/api/scanner-board/top-hits") {
      const section = url.searchParams.get("section");
      calls[section] += 1;
      if (section === "list" && holdList) await new Promise(resolve => { releaseList = resolve; onListHeld(); });
      return route.fulfill({ status: fail ? 503 : 200, json: fail ? { detail: "Offline fixture" } : fixture(section) });
    }
    return route.fulfill({ json: { rows: [], cards: [] } });
  });
  await page.goto("http://127.0.0.1:5173/scanner/top-hits");
  await page.getByRole("button", { name: "Clear filters", exact: true }).click();
  await page.getByRole("button", { name: "Trade Readiness", exact: true }).click();
  await page.getByRole("article", { name: "V1 trade candidate" }).waitFor();
  await page.getByLabel("Sort readiness candidates").selectOption("risk");

  const initialGuruCalls = calls.guru;
  const unchangedResponse = page.waitForResponse(r => r.url().includes("section=list"));
  await page.clock.fastForward(5 * 60 * 1000);
  await unchangedResponse;
  assert.equal(calls.guru, initialGuruCalls, "Unchanged snapshot does not reload Guru data");
  version = 2;
  await page.clock.fastForward(5 * 60 * 1000);
  await page.getByRole("article", { name: "V2 trade candidate" }).waitFor();
  assert.equal(await page.getByRole("article", { name: "V1 trade candidate" }).count(), 0);
  assert.equal(await page.getByLabel("Sort readiness candidates").inputValue(), "risk", "Refresh preserves user filters");

  await page.evaluate(() => { window.testHidden = true; document.dispatchEvent(new Event("visibilitychange")); });
  const beforeHidden = calls.list;
  version = 3;
  await page.clock.fastForward(5 * 60 * 1000);
  assert.equal(calls.list, beforeHidden, "Hidden tab skips polling");
  await page.evaluate(() => { window.testHidden = false; document.dispatchEvent(new Event("visibilitychange")); });
  await page.getByRole("article", { name: "V3 trade candidate" }).waitFor();

  fail = true;
  await page.clock.fastForward(5 * 60 * 1000);
  await page.getByText("Automatic refresh failed.", { exact: false }).waitFor();
  await page.getByRole("article", { name: "V3 trade candidate" }).waitFor();
  fail = false;
  version = 4;
  await page.clock.fastForward(5 * 60 * 1000);
  await page.getByRole("article", { name: "V4 trade candidate" }).waitFor();
  assert.equal(await page.getByText("Automatic refresh failed.", { exact: false }).count(), 0);
  holdList = true;
  const heldRequest = new Promise(resolve => { onListHeld = resolve; });
  await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange")));
  await heldRequest;
  const duringSlowRequest = calls.list;
  await page.clock.fastForward(5 * 60 * 1000);
  await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange")));
  assert.equal(calls.list, duringSlowRequest, "Slow requests do not overlap polling or visibility refresh");
  holdList = false;
  const releasedResponse = page.waitForResponse(r => r.url().includes("section=list"));
  releaseList();
  await releasedResponse;
  await page.setViewportSize({ width: 390, height: 844 });
  assert.ok(await page.evaluate(() => document.body.scrollWidth <= window.innerWidth), "Mobile has no page overflow");
  await page.screenshot({ path: "/tmp/top-hits-auto-refresh-mobile.png" });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.screenshot({ path: "/tmp/top-hits-auto-refresh-desktop.png" });
  await page.getByRole("link", { name: "Back to board", exact: true }).click();
  await page.getByRole("heading", { name: "Find the strongest current setups" }).waitFor();
  const beforeUnmount = calls.list;
  await page.clock.fastForward(5 * 60 * 1000);
  assert.equal(calls.list, beforeUnmount, "Polling stops after leaving Top Hits");
  await page.getByRole("button", { name: "Refresh board", exact: true }).click();
  await page.getByRole("status").filter({ hasText: "Waiting for scanner prerequisites" }).waitFor();
  assert.equal(await page.getByText("Scanner board manually refreshed", { exact: false }).count(), 0);
  manualStatus = "rebuilt";
  await page.getByRole("button", { name: "Refresh board", exact: true }).click();
  await page.getByRole("status").filter({ hasText: "Top Hits snapshot rebuilt" }).waitFor();
  assert.deepEqual(errors, []);
  console.log("Top Hits refresh probe passed: same-ID snapshot updates, unchanged snapshot, filters, hidden/resumed tab, failure/recovery, mobile layout, and unmount cleanup.");
} finally {
  await browser.close();
}
