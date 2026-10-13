import { chromium } from "playwright";
import fs from "node:fs";

const baseUrl = "http://127.0.0.1:5173";
const browserCandidates = [
  process.env.PLAYWRIGHT_EXECUTABLE_PATH,
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].filter(Boolean);
const executablePath = browserCandidates.find((candidate) => fs.existsSync(candidate));
const browser = await chromium.launch({
  headless: true,
  executablePath,
});
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
let scheduleRequests = 0;
let jobRequests = 0;

const schedule = {
  job_id: "weekday_test",
  job_label: "Weekday Test",
  action_id: "weekly_rs",
  cron_expr: "0 18 * * 1-5",
  cron_tz: "America/New_York",
  enabled: true,
  options: {},
  priority: 50,
  depends_on_job_ids: [],
  worker_pool: "any",
  estimated_duration_seconds: 600,
  estimate_sample_count: 1,
};

await page.route("**/api/**", async (route) => {
  const url = new URL(route.request().url());
  if (url.pathname === "/api/auth/me") {
    await route.fulfill({
      json: {
        authenticated: true,
        role: "admin",
        capabilities: ["view_results", "run_screeners", "manage_exclusions"],
        user: { authenticated: true, email: "test@example.com", role: "admin", capabilities: ["view_results", "run_screeners", "manage_exclusions"], is_active: true },
      },
    });
    return;
  }
  if (url.pathname === "/api/admin/schedules") {
    scheduleRequests += 1;
    if (scheduleRequests > 1) await new Promise((resolve) => setTimeout(resolve, 300));
    await route.fulfill({ json: { jobs: [schedule], available_actions: [], common_timezones: ["America/New_York"], scheduler_command: "", max_parallel_jobs: 5 } });
    return;
  }
  if (url.pathname === "/api/admin/scheduled-jobs") {
    jobRequests += 1;
    await route.fulfill({ json: { jobs: [] } });
    return;
  }
  await route.fulfill({ json: {} });
});

await page.goto(`${baseUrl}/screeners/schedules`);
const timeline = page.locator(".schedule-timeline-scroll");
await timeline.waitFor();
const scheduleRequestsBeforeRefresh = scheduleRequests;
const jobRequestsBeforeRefresh = jobRequests;
await timeline.evaluate((element) => { element.scrollLeft = 500; });
const before = await timeline.evaluate((element) => element.scrollLeft);
await page.waitForTimeout(15_700);
const after = await page.locator(".schedule-timeline-scroll").evaluate((element) => element.scrollLeft);

console.log(JSON.stringify({ before, after, scheduleRequestsBeforeRefresh, scheduleRequests, jobRequestsBeforeRefresh, jobRequests }));
await browser.close();

if (after !== before || scheduleRequests !== scheduleRequestsBeforeRefresh || jobRequests <= jobRequestsBeforeRefresh) process.exit(1);
