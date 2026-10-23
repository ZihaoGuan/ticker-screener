import assert from "node:assert/strict";
import { chromium } from "playwright";

// Synthetic saved-session fixtures: this probe does not execute a strategy or use production data.
const history = [{ date: "2026-10-22", cumulative_r: 0, account_equity: 100000, equity_drawdown_pct: 0, r_drawdown: 0 }, { date: "2026-10-23", cumulative_r: 1, account_equity: 100500, equity_drawdown_pct: 0, r_drawdown: 0 }];
const makeBook = (strategy_id) => ({ strategy_id,
  runs: [{ session_date: "2026-10-23", summary_json: { source_run_id: 42 } }], candidates: [], positions: [], events: [],
  history, metrics: { closed_count: 1, cumulative_r: 1, realized_pnl: 500, account_equity: 100500, equity_drawdown_pct: 0 }, attribution: {},
});
const rotation = makeBook("martin_rs_expanded_rotation_v1");
rotation.runs[0].summary_json = { source_run_id: 42, rotation_policy_version: "rotation_v1", committed_stop_risk: 2100, mark_to_stop_exposure: 3650,
  rotation: { status: "scheduled", reason: "stalled_relative_laggard_replaced_by_confirmed_leader", cooldown_sessions_remaining: 0, outgoing: { ticker: "OLD" }, incoming: { ticker: "NEW" } } };
rotation.candidates = [{ ticker: "NEW", status: "pending", payload_json: { selected_for_book: true, rotation_intent: { outgoing: { ticker: "OLD" }, incoming: { ticker: "NEW" }, decision_id: "synthetic-1" }, reasons: ["rotation_waiting_exit"], limit_price: 50, stop: 48, shares: 200 } }];
rotation.events = [{ event_date: "2026-10-23", ticker: "NEW", event_type: "rotation_scheduled", payload_json: { outgoing: { ticker: "OLD" }, incoming: { ticker: "NEW" }, decision_id: "synthetic-1", reason: "stalled_relative_laggard_replaced_by_confirmed_leader" } }];
rotation.rs_tier_metrics = { preferred: { closed_count: 1, realized_pnl: 500, realized_r: 1 } };
rotation.benchmark_comparison = { status: "available", basis: "price_return_close", baseline_date: "2026-10-22", comparison_date: "2026-10-23", included_sessions: 2, expected_sessions: 2,
  series: Object.fromEntries(["Account", "SPY", "QQQ"].map((name) => [name, [{ date: "2026-10-22", value: 100, return_pct: 0, drawdown_pct: 0 }, { date: "2026-10-23", value: 100.5, return_pct: 0.5, drawdown_pct: 0 }]])) };
const account = makeBook("martin_daily_v1"), expanded = makeBook("martin_rs_expanded_v1");
let payload = { configured: true, books: { account, rs_expanded: expanded, rs_expanded_rotation: rotation }, comparison: {} };
const origin = process.env.MARTIN_FORWARD_ORIGIN || "http://127.0.0.1:5173";
const browser = await chromium.launch({ headless: true, channel: "chrome" });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/api/auth/me", (route) => route.fulfill({ json: { authenticated: true, role: "admin", user: null, capabilities: ["view_results", "run_screeners"] } }));
  await page.route("**/api/admin/martin-forward-test", (route) => route.fulfill({ json: payload }));
  await page.goto(`${origin}/backtests/martin-forward`);
  await page.getByRole("button", { name: "Account book", exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "Account book", exact: true }).getAttribute("aria-pressed"), "true", "Account remains the default book");
  await page.getByRole("button", { name: "RS Expanded · 8 + Rotation", exact: true }).click();
  await page.getByRole("heading", { name: "Automatic Rotation v1", exact: true }).waitFor();
  assert.match(page.url(), /book=rs_expanded_rotation/);
  const panel = page.getByRole("region", { name: "Automatic rotation status" });
  assert.match(await panel.innerText(), /\$2,100/);
  assert.match(await panel.innerText(), /\$3,650/);
  assert.match(await panel.innerText(), /OLD[\s\S]*NEW/);
  await page.getByText("Await paired exit, then check the entry limit", { exact: true }).waitFor();
  assert.equal(await panel.getByRole("button").count(), 0, "Paper rotation has no manual broker or approval action");
  await page.screenshot({ path: "/tmp/martin-rotation-desktop.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  const closeDrawer = page.getByRole("button", { name: /close/i });
  if (await closeDrawer.count()) await closeDrawer.first().evaluate((element) => element.click());
  await page.waitForFunction(() => document.querySelector(".sidebar")?.getBoundingClientRect().right <= 1 && Number.parseFloat(getComputedStyle(document.querySelector(".app-shell-backdrop")).opacity) < 0.01);
  assert.equal(await page.evaluate(() => document.body.scrollWidth <= innerWidth), true, "Rotation content fits the 390px viewport");
  await page.screenshot({ path: "/tmp/martin-rotation-mobile.png", fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole("tab", { name: "Performance", exact: true }).click();
  await page.getByRole("heading", { name: "8 + Rotation equity and drawdown", exact: true }).waitFor();
  await page.getByRole("heading", { name: "RS cohort outcomes", exact: true }).waitFor();
  await page.getByRole("heading", { name: "Account vs SPY and QQQ", exact: true }).waitFor();
  assert.equal(await page.locator("path.rs-rotation-series").count(), 1);
  const comparison = page.getByRole("heading", { name: "Book comparison", exact: true }).locator("..");
  assert.match(await comparison.innerText(), /RS Expanded · 8 \+ Rotation/);
  assert.match(await comparison.innerText(), /\$100,500/);
  await page.getByRole("button", { name: "RS Expanded · experimental", exact: true }).click();
  await page.getByRole("heading", { name: "RS Expanded equity and drawdown", exact: true }).waitFor();
  assert.equal(await page.getByRole("heading", { name: "Automatic Rotation v1", exact: true }).count(), 0);
  rotation.runs[0].summary_json.rotation = { status: "pending_replaced", reason: "higher_priority_unfilled_order", cooldown_sessions_remaining: 2, outgoing_ticker: "OLDORDER", incoming_ticker: "NEW" };
  rotation.events.unshift({ event_date: "2026-10-23", ticker: "OLDORDER", event_type: "pending_replaced", payload_json: { incoming_ticker: "NEW", reason: "higher_priority_unfilled_order" } });
  await page.goto(`${origin}/backtests/martin-forward?book=rs_expanded_rotation&view=today`);
  await page.getByText("replace unfilled OLDORDER with NEW.", { exact: false }).waitFor();
  assert.match(await page.getByRole("region", { name: "Automatic rotation status" }).innerText(), /2 exchange sessions/);
  payload.books.rs_expanded_rotation.runs = [];
  await page.goto(`${origin}/backtests/martin-forward?book=rs_expanded_rotation&view=today`);
  await page.getByRole("heading", { name: "No session yet", exact: true }).waitFor();
  await page.getByText("No 8 + Rotation session has been saved yet.", { exact: false }).waitFor();
  delete payload.books.rs_expanded_rotation;
  await page.reload();
  await page.getByRole("heading", { name: "No session yet", exact: true }).waitFor();
  assert.deepEqual(errors, []);
  console.log("Rotation UI probe passed: default and deep-link selection, saved risk/pair evidence, account performance, comparison series, empty/older API state, and 390px overflow. Synthetic screenshots: /tmp/martin-rotation-desktop.png, /tmp/martin-rotation-mobile.png.");
} finally {
  await browser.close();
}
