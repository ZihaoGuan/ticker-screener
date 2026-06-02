import { chromium } from "playwright";

const ticker = (process.argv[2] || "NVDA").trim().toUpperCase();
const chromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

function toNumber(text) {
  if (!text) return null;
  const normalized = String(text).replace(/[$,%]/g, "").replace(/,/g, "").trim();
  const value = Number(normalized);
  return Number.isFinite(value) ? value : null;
}

function normalizeDate(text) {
  const value = String(text || "").trim();
  if (!value) return null;
  const isoMatch = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) return value;
  const slashMatch = value.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!slashMatch) return null;
  const first = Number(slashMatch[1]);
  const second = Number(slashMatch[2]);
  const year = Number(slashMatch[3]);
  if (!Number.isFinite(first) || !Number.isFinite(second) || !Number.isFinite(year)) return null;
  const day = first > 12 ? first : second;
  const month = first > 12 ? second : first;
  return `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function buildEarningsHistoryFromTables(tables) {
  for (const table of tables) {
    const firstRow = table.rows.find((row) => row[0] === "Est. EPS" || row[0] === "EPS Est.");
    const secondRow = table.rows.find((row) => row[0] === "Actual EPS" || row[0] === "EPS Actual");
    const diffRow = table.rows.find((row) => row[0] === "Difference");
    if (!firstRow || !secondRow || !diffRow) continue;
    const dates = table.headers.slice(1).map(normalizeDate);
    const rows = [];
    for (let index = 0; index < dates.length; index += 1) {
      const date = dates[index];
      if (!date) continue;
      const epsEstimate = toNumber(firstRow[index + 1]);
      const reportedEps = toNumber(secondRow[index + 1]);
      const difference = toNumber(diffRow[index + 1]);
      let surprisePct = null;
      if (epsEstimate != null && epsEstimate !== 0 && difference != null) {
        surprisePct = Number(((difference / epsEstimate) * 100).toFixed(4));
      }
      rows.push({
        date,
        eps_estimate: epsEstimate,
        reported_eps: reportedEps,
        surprise_pct: surprisePct,
      });
    }
    rows.sort((a, b) => b.date.localeCompare(a.date));
    return rows;
  }
  return [];
}

function buildHoldersPctFromTables(tables) {
  for (const table of tables) {
    for (const row of table.rows) {
      if (row.length < 2) continue;
      const valueText = String(row[0] || "").trim();
      const labelText = String(row[1] || "").trim().toLowerCase();
      if (!labelText.includes("% of float held by institutions")) continue;
      return toNumber(valueText);
    }
  }
  return null;
}

async function extractTableSummaries(page) {
  return await page.evaluate(() => {
    return Array.from(document.querySelectorAll("table")).slice(0, 6).map((table, index) => {
      const headers = Array.from(table.querySelectorAll("th")).map((node) => node.textContent?.trim() || "").filter(Boolean);
      const rows = Array.from(table.querySelectorAll("tr"))
        .slice(1, 4)
        .map((row) =>
          Array.from(row.querySelectorAll("td"))
            .map((node) => node.textContent?.trim() || "")
            .filter(Boolean),
        )
        .filter((row) => row.length > 0);
      return { index, headers, rows };
    });
  });
}

async function probePage(page, url) {
  const result = { url };
  try {
    const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
    result.status = response?.status() ?? null;
  } catch (error) {
    result.goto_error = String(error);
    return result;
  }

  try {
    await page.waitForLoadState("networkidle", { timeout: 5000 });
  } catch {
    result.networkidle_timeout = true;
  }

  result.final_url = page.url();
  result.title = await page.title();
  result.body_excerpt = (await page.locator("body").innerText().catch(() => "")).slice(0, 600);
  result.tables = await extractTableSummaries(page);

  const institutionText = await page
    .locator("body")
    .innerText()
    .then((text) => {
      const match = text.match(/([0-9]+(?:\.[0-9]+)?)%\s*(?:\||\n|\s)+%\s+of\s+Float\s+Held\s+by\s+Institutions/i);
      return match ? match[1] : null;
    })
    .catch(() => null);
  if (institutionText !== null) {
    result.holders_float_held_by_institutions_pct = Number(institutionText);
  }
  if (result.holders_float_held_by_institutions_pct == null) {
    result.holders_float_held_by_institutions_pct = buildHoldersPctFromTables(result.tables ?? []);
  }

  return result;
}

const browser = await chromium.launch({
  executablePath: chromePath,
  headless: true,
});

const context = await browser.newContext({
  userAgent:
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  viewport: { width: 1440, height: 1200 },
  locale: "en-US",
});

const page = await context.newPage();

const payload = {
  ticker,
  analysis: await probePage(page, `https://finance.yahoo.com/quote/${ticker}/analysis/`),
  analysis_nz: await probePage(page, `https://nz.finance.yahoo.com/quote/${ticker}/analysis/`),
  holders: await probePage(page, `https://finance.yahoo.com/quote/${ticker}/holders/`),
};

const nzEarningsHistory = buildEarningsHistoryFromTables(payload.analysis_nz.tables ?? []);
const usEarningsHistory = buildEarningsHistoryFromTables(payload.analysis.tables ?? []);
payload.earnings_eps_history = nzEarningsHistory.length > 0 ? nzEarningsHistory : usEarningsHistory;
payload.holders_float_held_by_institutions_pct =
  payload.holders?.holders_float_held_by_institutions_pct ??
  payload.analysis_nz?.holders_float_held_by_institutions_pct ??
  payload.analysis?.holders_float_held_by_institutions_pct ??
  null;

console.log(JSON.stringify(payload, null, 2));

await context.close();
await browser.close();
