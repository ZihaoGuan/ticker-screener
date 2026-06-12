import { chromium } from "playwright";
import fs from "node:fs";

const browserCandidates = [
  process.env.PLAYWRIGHT_EXECUTABLE_PATH,
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].filter(Boolean);

const SOURCES = [
  {
    id: "investing",
    url: "https://ng.investing.com/indices/s-p-500-stocks-above-50-day-average-scoreboard",
  },
  {
    id: "barchart",
    url: "https://www.barchart.com/stocks/quotes/$S5FI/interactive-chart",
  },
  {
    id: "macromicro",
    url: "https://en.macromicro.me/series/18331/sp500-50ma-breadth",
  },
];

function resolveExecutablePath() {
  for (const candidate of browserCandidates) {
    if (candidate && fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

function normalizePercent(text) {
  if (!text) return null;
  const value = Number(String(text).replace(/[%,\s]/g, "").trim());
  return Number.isFinite(value) ? value : null;
}

function collectPercentCandidates(text) {
  if (!text) return [];
  const results = [];
  const patterns = [
    /([0-9]+(?:\.[0-9]+)?)%\s*(?:of\s+)?s&p\s*500\s*stocks\s*(?:are\s*)?above\s*(?:its\s*)?50\s*day/gi,
    /above\s*(?:its\s*)?50\s*day(?:\s*moving\s*average|\s*average|\s*ma)?[^0-9]{0,30}([0-9]+(?:\.[0-9]+)?)%/gi,
    /([0-9]+(?:\.[0-9]+)?)%\s*above\s*50\s*day/gi,
    /([0-9]+(?:\.[0-9]+)?)%/g,
  ];
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      const raw = match[1];
      const value = normalizePercent(raw);
      if (value == null) continue;
      const snippet = String(match[0]).slice(0, 160);
      results.push({ value, snippet });
    }
    if (results.length > 0) break;
  }
  return results;
}

function extractBestGuessForSource(sourceId, text) {
  if (!text) return null;
  const sourcePatterns = {
    investing: [
      /S&P 500 - 50 Day MA\s+([0-9]+(?:\.[0-9]+)?)/i,
      /S&P 500 Stocks Above 50-Day Average\s*\(S5FI\)[\s\S]{0,200}?([0-9]+(?:\.[0-9]+)?)\s+[+-][0-9]+(?:\.[0-9]+)?/i,
    ],
    barchart: [
      /S&P 500 Stocks Above 50-Day Average\s*\(\$S5FI\)\s*([0-9]+(?:\.[0-9]+)?)/i,
      /\(\+?[0-9]+(?:\.[0-9]+)?%\)\s+[0-9/]+\s+\[INDEX\][\s\S]{0,40}/i,
    ],
    macromicro: [
      /US - S&P 500 Stocks above 50-Day Average[\s\S]{0,80}?(\d{4}-\d{2}-\d{2})[\s\S]{0,40}?([0-9]+(?:\.[0-9]+)?)\s*%/i,
      /Latest Stats[\s\S]{0,120}?([0-9]+(?:\.[0-9]+)?)\s*%/i,
    ],
  };

  for (const pattern of sourcePatterns[sourceId] ?? []) {
    const match = text.match(pattern);
    if (!match) continue;
    const raw = sourceId === "macromicro" && match[2] ? match[2] : match[1];
    const value = normalizePercent(raw);
    if (value != null && value >= 0 && value <= 100) {
      return { value, snippet: match[0].slice(0, 200) };
    }
  }
  return null;
}

async function extractStructuredHints(page) {
  return await page.evaluate(() => {
    const metas = Array.from(document.querySelectorAll('meta[property], meta[name]'))
      .map((node) => ({
        key: node.getAttribute("property") || node.getAttribute("name") || "",
        value: node.getAttribute("content") || "",
      }))
      .filter((item) => item.value);
    const headings = Array.from(document.querySelectorAll("h1, h2, h3"))
      .map((node) => node.textContent?.trim() || "")
      .filter(Boolean)
      .slice(0, 20);
    const bodyText = document.body?.innerText || "";
    return {
      metas,
      headings,
      bodyText,
    };
  });
}

async function probeSource(page, source) {
  const result = { id: source.id, url: source.url };
  try {
    const response = await page.goto(source.url, { waitUntil: "domcontentloaded", timeout: 20000 });
    result.status = response?.status() ?? null;
  } catch (error) {
    result.goto_error = String(error);
    return result;
  }

  try {
    await page.waitForLoadState("networkidle", { timeout: 8000 });
  } catch {
    result.networkidle_timeout = true;
  }

  result.final_url = page.url();
  result.title = await page.title();
  const structured = await extractStructuredHints(page);
  result.headings = structured.headings;
  result.body_excerpt = structured.bodyText.slice(0, 2000);
  const mergedText = `${result.title || ""}\n${structured.headings.join("\n")}\n${structured.bodyText}`;

  const candidates = [
    ...collectPercentCandidates(result.title || ""),
    ...collectPercentCandidates(structured.headings.join("\n")),
    ...collectPercentCandidates(structured.bodyText),
    ...structured.metas.flatMap((item) => collectPercentCandidates(`${item.key}: ${item.value}`)),
  ];

  const deduped = [];
  const seen = new Set();
  for (const candidate of candidates) {
    const key = `${candidate.value}|${candidate.snippet}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(candidate);
  }

  result.percent_candidates = deduped.slice(0, 12);
  result.best_guess = extractBestGuessForSource(source.id, mergedText) ?? deduped.find((item) => item.value >= 0 && item.value <= 100) ?? null;
  return result;
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
  viewport: { width: 1440, height: 1800 },
  locale: "en-US",
});

const page = await context.newPage();
const results = [];
for (const source of SOURCES) {
  results.push(await probeSource(page, source));
}

console.log(JSON.stringify({ scraped_at: new Date().toISOString(), results }, null, 2));

await context.close();
await browser.close();
