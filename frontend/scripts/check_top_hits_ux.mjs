import assert from 'node:assert/strict';
import { chromium } from 'playwright';

// Local UI regression check with a deterministic snapshot; no live API writes.
const browser = await chromium.launch({ headless: true, channel: 'chrome' });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const scanners = Array.from({ length: 12 }, (_, i) => ({ id: `scanner_${i}`, label: `Scanner ${String(i).padStart(2, '0')}` }));
  const row = { ticker: 'TEST', company: 'Example Company', sector: 'Technology', industry: 'Software', scanner_count: scanners.length, scanners, scanner_labels: scanners.map(s => s.label) };
  let admin = true;
  await page.route('**/api/**', async route => {
    const path = new URL(route.request().url()).pathname;
    const body = path === '/api/auth/me' ? { authenticated: admin, role: admin ? 'admin' : 'visitor', user: null, capabilities: admin ? ['view_results', 'manage_exclusions'] : ['view_results'] }
      : path === '/api/scanner-board/top-hits' ? { rows: [row, { ...row, ticker: 'OTHER', scanners: scanners.slice(0, 2), scanner_count: 2 }], total_unique_tickers: 2, total_live_scanners: 12 }
      : { rows: [] };
    await route.fulfill({ json: body });
  });
  await page.goto(process.env.TOP_HITS_URL || 'http://127.0.0.1:5173/scanner/top-hits');
  const cells = page.locator('td[data-label="Scanners"]');
  await cells.first().waitFor();
  assert.equal(await cells.first().locator('a').count(), 3);
  await cells.first().getByRole('button', { name: '+9 more', exact: true }).click();
  assert.equal(await cells.first().locator('a').count(), 12);
  await cells.first().getByRole('button', { name: 'Show fewer' }).click();
  await page.getByLabel('Scanner 11', { exact: true }).check();
  assert.equal(await cells.count(), 1);
  assert.match(await cells.first().locator('a').first().innerText(), /Scanner 11/i);
  assert.match(await cells.first().locator('a').first().getAttribute('class'), /is-selected/);
  for (const label of ['Scanner 00', 'Scanner 01', 'Scanner 02']) await page.getByLabel(label, { exact: true }).check();
  assert.equal(await cells.count(), 1, 'Multiple scanner filters use AND');
  assert.match(await cells.first().getByRole('button').innerText(), /1 selected/);
  await page.getByLabel('Weekly Candidate Pool', { exact: true }).check();
  await page.getByText('No tickers match current filters.', { exact: true }).waitFor();
  await page.getByLabel('Weekly Candidate Pool', { exact: true }).uncheck();
  for (const label of ['Scanner 00', 'Scanner 01', 'Scanner 02']) await page.getByLabel(label, { exact: true }).uncheck();
  await page.getByText('Customize scanner names', { exact: true }).click();
  const name = page.getByRole('textbox', { name: 'Display name for Scanner 11', exact: true });
  await name.fill('My momentum');
  await name.press('Tab');
  assert.match(await cells.first().locator('a').first().innerText(), /My momentum/i);
  await page.getByRole('textbox', { name: 'Find scanners', exact: true }).fill('My momentum');
  assert.equal(await page.locator('.scanner-top-hit-filter-list').first().getByRole('checkbox').count(), 1);
  const pinned = page.locator('tbody .pinned-ticker').first();
  const pick = page.locator('tbody .pinned-pick').first();
  const before = await pinned.boundingBox();
  const pickBefore = await pick.boundingBox();
  await page.locator('.scanner-result-table-wrap').evaluate(el => { el.scrollLeft = 800; });
  assert.ok(Math.abs((await pinned.boundingBox()).x - before.x) < 2, 'Ticker stays pinned');
  assert.ok(Math.abs((await pick.boundingBox()).x - pickBefore.x) < 2, 'My Pick stays pinned');
  assert.ok(before.x >= pickBefore.x + pickBefore.width - 1, 'Pinned columns do not overlap');
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  assert.equal(await cells.count(), 2);
  await page.reload();
  await cells.first().waitFor();
  assert.equal(await page.getByLabel('My momentum', { exact: true }).count(), 1, 'Alias persists');
  await page.getByLabel('My momentum', { exact: true }).check();
  await page.getByRole('button', { name: 'Charts', exact: true }).click();
  assert.match(await page.locator('.scanner-top-hit-chart-card .scanner-card-pill').first().innerText(), /My momentum/i);
  await page.getByRole('button', { name: 'List', exact: true }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Mobile page fits viewport');
  await page.screenshot({ path: '/tmp/top-hits-mobile.png', fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.screenshot({ path: '/tmp/top-hits-desktop.png', fullPage: true });
  admin = false;
  await page.reload();
  await cells.first().waitFor();
  assert.equal(await page.locator('.pinned-pick').count(), 0);
  const visitorBefore = await pinned.boundingBox();
  await page.locator('.scanner-result-table-wrap').evaluate(el => { el.scrollLeft = 800; });
  assert.ok(Math.abs((await pinned.boundingBox()).x - visitorBefore.x) < 2, 'Visitor ticker stays pinned without pick column');
  assert.deepEqual(errors, []);
  console.log('Top Hits UX passed: collapse, selection, AND filtering, rename persistence, search, reset, chart badges, desktop pinning, visitor layout, mobile width.');
} finally { await browser.close(); }
