import assert from 'node:assert/strict';
import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true, channel: 'chrome' });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  let detailRequests = 0;
  let ratingsRequests = 0;
  const entry = {
    ticker: 'TEST', company_name: 'Example Company', sector: 'Technology', industry: 'Software',
    current_volume: 1200000, daily_change_pct: 1.2, ta_rating: 80, fa_rating: 70,
    rs_rating: 90, daily_rs_rating: 95, is_new: true,
    technical_indicator_ratings: { '1d': { rating_label: 'Bullish' }, '1w': { rating_label: 'Strong Bullish' } },
    canslim_score: 10, canslim_max_score: 14,
  };
  page.on('pageerror', error => { throw error; });
  await page.route('**/api/**', async route => {
    const path = new URL(route.request().url()).pathname;
    if (path === '/api/auth/me') return route.fulfill({ json: { authenticated: false, user: null, role: 'visitor', capabilities: ['view_results'] } });
    if (path === '/api/scanner-board') return route.fulfill({ json: { target_trading_date: '2026-10-15', cards: [{ id: 'test', label: 'Test Scanner', stem: 'test_2026-10-15', description: 'Test signals' }] } });
    if (path.startsWith('/api/watchlists/')) {
      detailRequests += 1;
      if (detailRequests === 1) {
        await new Promise(resolve => setTimeout(resolve, 250));
        return route.fulfill({ status: 503, json: { detail: 'Temporary failure' } });
      }
      return route.fulfill({ json: { stem: 'test_2026-10-15', has_previous_scan: true, entry_count: 1, entries: [entry] } });
    }
    if (path.startsWith('/api/ratings/')) ratingsRequests += 1;
    return route.fulfill({ json: { rows: [] } });
  });

  await page.goto(`${process.env.SCANNER_RESULT_URL || 'http://127.0.0.1:5173'}/scanner/test`);
  await page.getByText('Loading signals…', { exact: true }).waitFor();
  await page.getByRole('alert').getByRole('button', { name: 'Retry' }).waitFor();
  assert.equal(await page.getByText('No Active Hits', { exact: true }).count(), 0, 'Loading and errors do not look like empty results');
  await page.getByRole('alert').getByRole('button', { name: 'Retry' }).click();
  await page.getByText('Persisted signals', { exact: true }).waitFor();
  assert.equal(await page.locator('.scanner-result-table tbody tr').count(), 1);
  await page.getByText('Advanced', { exact: true }).click();
  assert.match(await page.locator('.scanner-result-more').innerText(), /Bullish \/ Strong Bullish/, 'Enriched indicators render from scanner detail');
  assert.equal(await page.locator('th[aria-sort]').count(), 7);
  assert.equal(await page.locator('.scanner-result-view-chip[aria-pressed]').count(), 2);
  assert.equal(ratingsRequests, 0, 'Scanner detail makes no separate ratings requests');

  for (const [width, expectedColumns] of [[390, 1], [1024, 2], [1440, 4]]) {
    await page.setViewportSize({ width, height: 900 });
    const metricsColumns = await page.locator('.scanner-result-metrics').evaluate(el => getComputedStyle(el).gridTemplateColumns.split(' ').length);
    const filterColumns = await page.locator('.scanner-result-filter-grid').evaluate(el => getComputedStyle(el).gridTemplateColumns.split(' ').length);
    assert.equal(metricsColumns, expectedColumns, `${width}px metrics grid`);
    assert.equal(filterColumns, expectedColumns, `${width}px filter grid`);
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${width}px viewport has no page-level horizontal scrolling`);
  }
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.getByRole('button', { name: 'Charts', exact: true }).click();
  const chartColumns = await page.locator('.scanner-result-chart-grid').evaluate(el => getComputedStyle(el).gridTemplateColumns.split(' ').length);
  assert.equal(chartColumns, 3, 'Desktop chart view uses three columns');
  assert.deepEqual(await page.locator('html').evaluate(el => [el.scrollWidth, innerWidth]).then(([scrollWidth, width]) => scrollWidth <= width), true);
  console.log('Scanner result UX passed: loading/error/retry, no extra ratings requests, accessibility states, and 390/1024/1440 layouts.');
} finally {
  await browser.close();
}
