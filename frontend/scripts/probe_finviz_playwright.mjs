import { chromium } from "playwright";
import fs from "node:fs";

const ticker = (process.argv[2] || "NVDA").trim().toUpperCase();
const browserCandidates = [
  process.env.PLAYWRIGHT_EXECUTABLE_PATH,
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].filter(Boolean);

function resolveExecutablePath() {
  for (const candidate of browserCandidates) {
    if (candidate && fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

const executablePath = resolveExecutablePath();
const browser = await chromium.launch({
  executablePath,
  headless: true,
  args: process.platform === "linux" ? ["--no-sandbox", "--disable-setuid-sandbox"] : [],
});

const context = await browser.newContext({
  userAgent:
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  viewport: { width: 1440, height: 2200 },
  locale: "en-US",
});

const page = await context.newPage();
const url = `https://finviz.com/quote.ashx?t=${ticker}&p=d`;
const result = { ticker, url };

try {
  const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
  result.status = response?.status() ?? null;
} catch (error) {
  result.goto_error = String(error);
  console.log(JSON.stringify(result, null, 2));
  await context.close();
  await browser.close();
  process.exit(0);
}

try {
  await page.waitForLoadState("networkidle", { timeout: 8000 });
} catch {
  result.networkidle_timeout = true;
}

result.final_url = page.url();
result.title = await page.title();
result.body_excerpt = (await page.locator("body").innerText().catch(() => "")).slice(0, 2000);
result.company_header = await page.evaluate(() => {
  const bodyText = document.body.innerText || "";
  const lines = bodyText
    .split("\n")
    .map((value) => value.trim())
    .filter(Boolean);
  const tickerIndex = lines.findIndex((value) => value === (window.location.search.match(/t=([^&]+)/i)?.[1] || "").toUpperCase());
  if (tickerIndex < 0) {
    return {
      company_name: null,
      sector: null,
      industry: null,
      country: null,
      market_cap_class: null,
      exchange: null,
    };
  }
  const companyName = lines[tickerIndex + 1] || null;
  const metaLine = lines[tickerIndex + 8] || "";
  const parts = metaLine
    .split("•")
    .map((value) => value.trim())
    .filter(Boolean);
  return {
    company_name: companyName,
    sector: parts[0] || null,
    industry: parts[1] || null,
    country: parts[2] || null,
    market_cap_class: parts[3] || null,
    exchange: parts[4] || null,
  };
});
result.metric_pairs = await page.evaluate(() => {
  const wantedLabels = new Set([
    "Market Cap",
    "Forward P/E",
    "PEG",
    "P/S",
    "P/B",
    "P/FCF",
    "Profit Marg",
    "Oper. Marg",
    "Gross Marg",
    "ROA",
    "ROE",
    "EPS this Y",
    "EPS next Y",
    "EPS next 5Y",
    "Sales Q/Q",
    "EPS Q/Q",
    "Perf Month",
    "Perf Quart",
    "Perf Half Y",
    "Perf Year",
    "Perf YTD",
    "Volatility",
  ]);

  const cells = Array.from(document.querySelectorAll("td"));
  const pairs = [];
  for (let index = 0; index < cells.length - 1; index += 1) {
    const label = (cells[index].textContent || "").trim();
    const value = (cells[index + 1].textContent || "").trim();
    if (wantedLabels.has(label) && value) {
      pairs.push([label, value]);
    }
  }
  return pairs;
});

console.log(JSON.stringify(result, null, 2));

await context.close();
await browser.close();
