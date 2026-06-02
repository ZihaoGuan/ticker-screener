import { chromium } from "playwright";

const ticker = (process.argv[2] || "NVDA").trim().toUpperCase();
const chromePath = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";

function toNumber(text) {
  if (!text) return null;
  const normalized = String(text).replace(/[$,%]/g, "").replace(/,/g, "").trim();
  const value = Number(normalized);
  return Number.isFinite(value) ? value : null;
}

const browser = await chromium.launch({
  executablePath: chromePath,
  headless: true,
});

const context = await browser.newContext({
  userAgent:
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
  viewport: { width: 1440, height: 1400 },
  locale: "en-NZ",
});

const page = await context.newPage();
const url = `https://nz.finance.yahoo.com/quote/${ticker}/options/`;
const result = { ticker, url };

try {
  const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });
  result.status = response?.status() ?? null;
} catch (error) {
  result.goto_error = String(error);
  console.log(JSON.stringify(result, null, 2));
  await context.close();
  await browser.close();
  process.exit(0);
}

try {
  await page.waitForLoadState("networkidle", { timeout: 6000 });
} catch {
  result.networkidle_timeout = true;
}

result.final_url = page.url();
result.title = await page.title();
result.body_excerpt = (await page.locator("body").innerText().catch(() => "")).slice(0, 1200);

result.price = await page.evaluate(() => {
  const bodyText = document.body.innerText;
  const lines = bodyText.split("\n");
  for (let i = 0; i < lines.length - 2; i += 1) {
    if (/\([A-Z.]+\)/.test(lines[i])) {
      const candidate = Number(lines[i + 1].replace(/,/g, "").trim());
      if (Number.isFinite(candidate)) return candidate;
    }
  }
  return null;
});

result.expirations = await page.evaluate(() =>
  Array.from(document.querySelectorAll("option"))
    .map((node) => node.textContent?.trim() || "")
    .filter((text) => text)
    .slice(0, 12),
);

result.tables = await page.evaluate(() => {
  return Array.from(document.querySelectorAll("table")).slice(0, 4).map((table, index) => {
    const headers = Array.from(table.querySelectorAll("th")).map((node) => node.textContent?.trim() || "").filter(Boolean);
    const rows = Array.from(table.querySelectorAll("tr"))
      .slice(1, 60)
      .map((row) =>
        Array.from(row.querySelectorAll("td"))
          .map((node) => node.textContent?.trim() || "")
          .filter((value) => value !== ""),
      )
      .filter((row) => row.length > 0);
    return { index, headers, rows };
  });
});

const firstTable = result.tables.find((table) => table.headers.includes("Strike"));
if (firstTable) {
  const strikeIndex = firstTable.headers.indexOf("Strike");
  const bidIndex = firstTable.headers.indexOf("Bid");
  const askIndex = firstTable.headers.indexOf("Ask");
  const ivIndex = firstTable.headers.findIndex((header) => /Implied Volatility/i.test(header));
  const calls = firstTable.rows
    .map((row) => ({
      strike: toNumber(row[strikeIndex]),
      bid: toNumber(row[bidIndex]),
      ask: toNumber(row[askIndex]),
      iv: row[ivIndex] ?? null,
    }))
    .filter((row) => row.strike !== null)
    .slice(0, 60);
  result.call_rows = calls;
  if (result.price != null) {
    let closest = null;
    for (const row of calls) {
      const mid = row.bid != null && row.ask != null ? (row.bid + row.ask) / 2 : null;
      const distance = row.strike != null ? Math.abs(row.strike - result.price) : Infinity;
      if (!closest || distance < closest.distance) {
        closest = { ...row, mid, distance };
      }
    }
    result.closest_call = closest;
  }
}

const secondTable = result.tables.filter((table) => table.headers.includes("Strike"))[1];
if (secondTable) {
  const strikeIndex = secondTable.headers.indexOf("Strike");
  const bidIndex = secondTable.headers.indexOf("Bid");
  const askIndex = secondTable.headers.indexOf("Ask");
  const ivIndex = secondTable.headers.findIndex((header) => /Implied Volatility/i.test(header));
  const puts = secondTable.rows
    .map((row) => ({
      strike: toNumber(row[strikeIndex]),
      bid: toNumber(row[bidIndex]),
      ask: toNumber(row[askIndex]),
      iv: row[ivIndex] ?? null,
    }))
    .filter((row) => row.strike !== null)
    .slice(0, 60);
  result.put_rows = puts;
  if (result.price != null) {
    let closest = null;
    for (const row of puts) {
      const mid = row.bid != null && row.ask != null ? (row.bid + row.ask) / 2 : null;
      const distance = row.strike != null ? Math.abs(row.strike - result.price) : Infinity;
      if (!closest || distance < closest.distance) {
        closest = { ...row, mid, distance };
      }
    }
    result.closest_put = closest;
  }
}

if (result.price != null && result.closest_call?.mid != null && result.closest_put?.mid != null) {
  const straddle = result.closest_call.mid + result.closest_put.mid;
  result.implied_move = {
    strike: result.closest_call.strike,
    straddle_mid: Number(straddle.toFixed(4)),
    dollar_move: Number(straddle.toFixed(4)),
    percent_move: Number(((straddle / result.price) * 100).toFixed(4)),
  };
}

console.log(JSON.stringify(result, null, 2));

await context.close();
await browser.close();
