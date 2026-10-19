import assert from 'node:assert/strict';
import { chromium } from 'playwright';

// Local UI regression check with a deterministic snapshot; no live API writes.
const browser = await chromium.launch({ headless: true, channel: 'chrome' });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  await page.addInitScript(() => {
    if (!sessionStorage.getItem('top-hits-management-check-initialized')) {
      localStorage.removeItem('top-hits-trade-management-style');
      sessionStorage.setItem('top-hits-management-check-initialized', 'true');
    }
  });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  const scanners = Array.from({ length: 12 }, (_, i) => ({ id: `scanner_${i}`, label: `Scanner ${String(i).padStart(2, '0')}` }));
  const row = { ticker: 'TEST', company: 'Example Company', sector: 'Technology', industry: 'Software', day_close: 104, scanner_count: scanners.length, scanners, scanner_labels: scanners.map(s => s.label) };
  const plan = (risk = 2, overrides = {}) => ({ source_strategy_id: 'ma_pullback_retest', state: 'ready', support_label: 'EMA21', support_price: 98, trigger_label: 'Close', trigger_price: 103, stop_label: 'Low', stop_price: 95, reference_risk_pct: risk, execution_timeframe: 'daily', entry_policy: 'close_confirmed', max_risk_pct: 5, max_entry_price: 105, risk_valid: true, risk_status: risk > 5 ? 'over_limit' : 'within_limit', signal_date: '2026-10-16', ...overrides });
  const readinessRows = [
    { ...row, ticker: 'LEAD', trade_readiness: 'leader', daily_rs_rating: 80 },
    { ...row, ticker: 'TRIG', sector: 'Industrials', trade_readiness: 'reclaim_trigger', daily_rs_rating: 90, trade_plan: plan() },
    { ...row, ticker: 'READY', trade_readiness: 'defined_risk', daily_rs_rating: 95, trade_plan: plan(2, { state: 'active' }) },
    { ...row, ticker: 'BAD3R', trade_readiness: 'defined_risk', daily_rs_rating: 94, trade_plan: plan(2, { state: 'active', trigger_price: 95, stop_price: 95, risk_valid: false, risk_status: 'invalid_levels' }) },
    { ...row, ticker: 'OVER', sector: 'Health Care', day_close: 106, trade_readiness: 'defined_risk', trade_plan: plan(6, { state: 'active' }) },
    { ...row, ticker: 'RISK', trade_readiness: 'pullback_zone', daily_rs_rating: 70, trade_plan: plan(6) },
    { ...row, ticker: 'NOPLAN', sector: null, trade_readiness: 'pullback_zone', trade_plan: null },
    { ...row, ticker: 'AVOID', sector: 'Financials', trade_readiness: 'avoid', trade_plan: plan(), strike_zone: { state: 'avoid', score: 20 }, pullback_quality: { state: 'failed', score: 15 } },
    { ...row, ticker: 'UNKNOWN', sector: null, trade_readiness: undefined, trade_plan: plan() },
    ...Array.from({ length: 9 }, (_, i) => ({ ...row, ticker: `SUP${String(i + 1).padStart(2, '0')}`, trade_readiness: 'pullback_zone', trade_plan: plan(i + 1), daily_rs_rating: 75 + i })),
  ];
  const topHitsRequests = { list: 0, guru: 0 };
  let admin = true;
  await page.route('**/api/**', async route => {
    const path = new URL(route.request().url()).pathname;
    const section = new URL(route.request().url()).searchParams.get('section') || 'all';
    if (path === '/api/scanner-board/top-hits' && section === 'guru') topHitsRequests.guru += 1;
    if (path === '/api/scanner-board/top-hits' && section === 'list') topHitsRequests.list += 1;
    if (path === '/api/admin/my-picks' && route.request().method() === 'POST') {
      await new Promise(resolve => setTimeout(resolve, 250));
      return route.fulfill({ json: { ok: true, pick: { id: 99, ticker: JSON.parse(route.request().postData() || '{}').ticker } } });
    }
    const body = path === '/api/auth/me' ? { authenticated: admin, role: admin ? 'admin' : 'visitor', user: null, capabilities: admin ? ['view_results', 'manage_exclusions'] : ['view_results'] }
      : path === '/api/scanner-board/top-hits' ? (() => {
        if (section === 'guru') {
          return { rows: [], guru_board: { definitions: [], total_unique_tickers: readinessRows.length, total_scanner_matches: 0, confluence_ticker_count: 0, rows: readinessRows } };
        }
        return { rows: [row, { ...row, ticker: 'OTHER', scanners: scanners.slice(0, 2), scanner_count: 2 }], total_unique_tickers: 2, total_live_scanners: 12 };
      })()
      : path === '/api/admin/my-picks' ? { rows: [] }
      : { rows: [] };
    await route.fulfill({ json: body });
  });
  const listResponse = page.waitForResponse(response => new URL(response.url()).pathname === '/api/scanner-board/top-hits' && new URL(response.url()).searchParams.get('section') === 'list');
  await page.goto(process.env.TOP_HITS_URL || 'http://127.0.0.1:5173/scanner/top-hits');
  await listResponse;
  const cells = page.locator('td[data-label="Scanners"]');
  await page.getByText('Loading scanner top hits…', { exact: true }).waitFor({ state: 'hidden' });
  const initialListRequests = topHitsRequests.list;
  assert.ok(initialListRequests >= 1 && initialListRequests <= 2, 'Initial view fetches the compact list section');
  assert.match(
    await page.getByLabel('Scanner filter expression').innerText(),
    /Qullamaggie[\s\S]*Weekly Candidate Pool[\s\S]*kai_s2[\s\S]*AND Group 2[\s\S]*one_year_winners/i,
    'Built-in scanner groups load by default',
  );
  await page.getByLabel('Saved filter preset').selectOption('built-in:martin-focus-list');
  assert.equal(await page.getByLabel('Saved filter preset').inputValue(), 'built-in:martin-focus-list');
  assert.equal(await page.getByLabel('Minimum daily RS').inputValue(), '90');
  assert.match(
    await page.getByLabel('Scanner filter expression').innerText(),
    /Group 1[\s\S]*Weekly Candidate Pool[\s\S]*Best Winners[\s\S]*Qullamaggie[\s\S]*one_year_winners[\s\S]*daily_rs_new_high[\s\S]*rs/i,
    'Martin Focus List keeps one OR leadership group and does not require a pullback signal',
  );
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await cells.first().waitFor();
  assert.equal(await cells.first().locator('a').count(), 3);
  await cells.first().getByRole('button', { name: '+9 more', exact: true }).click();
  assert.equal(await cells.first().locator('a').count(), 12);
  await cells.first().getByRole('button', { name: 'Show fewer' }).click();
  await page.getByLabel('Scanner 11', { exact: true }).check();
  assert.equal(await cells.count(), 1);
  assert.match(await cells.first().locator('a').first().innerText(), /Scanner 11/i);
  assert.match(await cells.first().locator('a').first().getAttribute('class'), /is-selected/);
  await page.getByLabel('Scanner 00', { exact: true }).check();
  assert.equal(await cells.count(), 2, 'Scanners inside one group use OR');
  await page.getByRole('button', { name: '+ AND group', exact: true }).click();
  await page.getByLabel('Scanner 02', { exact: true }).check();
  assert.equal(await cells.count(), 1, 'Separate scanner groups use AND');
  assert.match(await page.getByLabel('Scanner filter expression').innerText(), /Group 1[\s\S]*AND Group 2/i);
  await page.getByRole('button', { name: '+ AND group', exact: true }).click();
  await page.getByLabel('Weekly Candidate Pool', { exact: true }).check();
  await page.getByText('No tickers match current filters.', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Remove scanner group 3', exact: true }).click();
  assert.equal(await cells.count(), 1, 'Removing a group removes its clause');
  page.once('dialog', dialog => dialog.accept('Momentum setup'));
  await page.getByRole('button', { name: 'Save preset', exact: true }).click();
  await page.getByRole('button', { name: 'Set default', exact: true }).click();
  await page.reload();
  await cells.first().waitFor();
  assert.equal(await page.getByLabel('Saved filter preset').inputValue(), 'Momentum setup');
  assert.equal(await cells.count(), 1, 'Default preset restores scanner groups');
  await page.getByRole('button', { name: 'Delete', exact: true }).click();
  assert.equal(await cells.count(), 2, 'Deleting a preset restores unfiltered results');
  await page.getByLabel('Scanner 11', { exact: true }).check();
  await page.getByText('Customize scanner names', { exact: true }).click();
  const name = page.getByRole('textbox', { name: 'Display name for Scanner 11', exact: true });
  await name.fill('My momentum');
  await name.press('Tab');
  assert.match(await cells.first().innerText(), /My momentum/i);
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
  await page.getByText('No tickers match current filters.', { exact: true }).waitFor();
  assert.equal(await page.getByLabel('My momentum', { exact: true }).count(), 1, 'Alias persists');
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await cells.first().waitFor();
  await page.getByLabel('My momentum', { exact: true }).check();
  await page.getByRole('button', { name: 'Charts', exact: true }).click();
  await page.locator('.scanner-top-hit-chart-card .scanner-chart-card-details summary').first().click();
  assert.match(await page.locator('.scanner-top-hit-chart-card .scanner-chart-card-details').first().innerText(), /My momentum/i);
  await page.getByRole('button', { name: 'List', exact: true }).click();
  await page.getByRole('button', { name: 'Trade Readiness', exact: true }).click();
  await page.getByText('Plan Ready uses completed daily bars. Recheck the next-session price against Max Entry before placing an order.', { exact: true }).waitFor();
  assert.equal(await page.locator('.trade-readiness-lane.is-leader .trade-readiness-card').count(), 1, 'Known leader stays in its lane');
  assert.equal(await page.locator('.trade-readiness-lane.is-needs-data .trade-readiness-card').count(), 1, 'Missing readiness is isolated in Needs Data');
  assert.equal(await page.locator('.trade-readiness-lane.is-leader').getByText('UNKNOWN').count(), 0, 'Unknown readiness never defaults to Leader');
  assert.equal(await page.locator('.trade-readiness-lane.is-support .trade-readiness-card').count(), 8, 'Support lane starts with eight cards');
  await page.locator('.trade-readiness-lane.is-support').getByRole('button', { name: 'Show 8 more (3 remaining)' }).click();
  assert.equal(await page.locator('.trade-readiness-lane.is-support .trade-readiness-card').count(), 11, 'Show more reveals all remaining candidates');
  await page.getByLabel('Sort readiness candidates').selectOption('risk');
  assert.match(await page.locator('.trade-readiness-lane.is-support .trade-readiness-card').first().getAttribute('aria-label'), /SUP01/, 'Lowest risk sorts first');
  await page.getByLabel('Sort readiness candidates').selectOption('rs');
  assert.match(await page.locator('.trade-readiness-lane.is-support .trade-readiness-card').first().getAttribute('aria-label'), /SUP09/, 'Highest RS sorts first');
  assert.equal(await page.locator('.trade-readiness-lane.is-support .trade-readiness-card').count(), 8, 'Sorting resets the lane cap');
  await page.getByRole('button', { name: 'My Picks', exact: true }).click();
  assert.equal(await page.locator('.trade-readiness-card').count(), 0, 'My Picks filter shows only saved picks');
  await page.getByRole('heading', { name: 'Plan Ready', exact: true }).waitFor();
  await page.getByText('No leaders are waiting at this stage.', { exact: true }).waitFor();
  await page.getByRole('button', { name: 'Show All', exact: true }).click();
  await page.getByLabel('Sort readiness candidates').selectOption('readiness');
  const leaderCard = page.locator('.trade-readiness-lane.is-leader .trade-readiness-card');
  await leaderCard.getByRole('button', { name: 'Save', exact: true }).click();
  await leaderCard.getByRole('button', { name: 'Saving…', exact: true }).waitFor();
  assert.equal(await leaderCard.getAttribute('aria-busy'), 'true', 'Saving card announces busy state');
  await leaderCard.getByRole('button', { name: 'Saved', exact: true }).waitFor();
  await page.getByRole('button', { name: 'My Picks', exact: true }).click();
  await page.getByLabel('Filter by entry status').selectOption('unavailable');
  assert.equal(await page.locator('.trade-readiness-card').count(), 1, 'My Picks and entry status filters compose');
  assert.match(await page.locator('.trade-readiness-card').first().getAttribute('aria-label'), /LEAD/);
  await page.getByRole('button', { name: 'Show All', exact: true }).click();
  await page.getByLabel('Filter by entry status').selectOption('all');
  const readyCard = page.getByRole('article', { name: 'READY trade candidate' });
  assert.equal(await readyCard.locator('.trade-readiness-card-badges .guru-sector-tag').innerText(), 'Technology', 'Sector is visible without expanding candidate evidence');
  assert.match(await readyCard.locator('.trade-readiness-eligibility').innerText(), /Within max entry · daily trigger confirmed/, 'Eligible label requires active daily confirmation as well as acceptable price');
  assert.match(await page.getByRole('article', { name: 'TRIG trade candidate' }).locator('.trade-readiness-eligibility').innerText(), /Waiting for daily trigger/, 'Acceptable price alone does not imply entry');
  assert.match(await page.getByRole('article', { name: 'OVER trade candidate' }).locator('.trade-readiness-eligibility').innerText(), /Extended · do not chase/);
  const entryStatusFilter = page.getByLabel('Filter by entry status');
  assert.equal(await entryStatusFilter.inputValue(), 'all', 'Entry status defaults to all candidates');
  await entryStatusFilter.selectOption('within_max');
  assert.equal(await page.locator('.trade-readiness-card').count(), 1, 'Within max status keeps only price eligible candidates with confirmed daily triggers');
  await entryStatusFilter.selectOption('extended');
  assert.deepEqual(await page.locator('.trade-readiness-card').allTextContents().then(values => values.map(value => value.match(/^[A-Z0-9]+/)?.[0])), ['OVER'], 'Extended status isolates do-not-chase candidates');
  await entryStatusFilter.selectOption('waiting_trigger');
  assert.ok(await page.locator('.trade-readiness-card').count() > 1, 'Waiting status includes candidates that still need a daily trigger');
  await entryStatusFilter.selectOption('unavailable');
  assert.equal(await page.locator('.trade-readiness-card').count(), 2, 'Unavailable status isolates missing entry plans');
  assert.match(await page.getByRole('article', { name: 'NOPLAN trade candidate' }).locator('.trade-readiness-eligibility').innerText(), /No entry limit \/ plan unavailable/);
  await entryStatusFilter.selectOption('all');
  const readinessFilter = page.getByLabel('Filter by readiness stage');
  await readinessFilter.selectOption('defined_risk');
  assert.equal(await page.locator('.trade-readiness-card').count(), 3, 'Readiness filter isolates Plan Ready candidates');
  await readinessFilter.selectOption('needs-data');
  assert.equal(await page.locator('.trade-readiness-card').count(), 1, 'Readiness filter isolates unrecognized stages as Needs Data');
  await readinessFilter.selectOption('all');
  await page.getByLabel('Group readiness candidates').selectOption('sector');
  const sectorGroups = page.locator('.trade-readiness-sector-group');
  assert.equal(await sectorGroups.count(), 4, 'Each available sector renders once');
  const sectorCandidateCount = await sectorGroups.locator(':scope > header > span').allTextContents().then(values => values.reduce((sum, value) => sum + Number(value), 0));
  assert.equal(sectorCandidateCount, readinessRows.length - 1, 'Every non-avoided candidate belongs to exactly one sector group');
  assert.deepEqual(await sectorGroups.getByRole('heading').allTextContents(), ['Technology', 'Health Care', 'Industrials', 'Unknown sector'], 'Sectors sort by count, then name, with unknown last');
  assert.equal(await page.locator('.trade-readiness-sector-group').getByRole('article', { name: 'AVOID trade candidate' }).count(), 0, 'Avoid stays hidden in sector grouping');
  assert.equal(await readyCard.locator('.trade-readiness-stage').innerText(), 'Plan Ready', 'Sector grouping keeps readiness visible on each card');
  await readinessFilter.selectOption('reclaim_trigger');
  assert.equal(await page.locator('.trade-readiness-card').count(), 1, 'Readiness filter composes with sector grouping');
  assert.equal(await page.locator('.trade-readiness-sector-group').getByRole('heading').innerText(), 'Industrials');
  await readinessFilter.selectOption('all');
  await page.getByLabel('Group readiness candidates').selectOption('stage');
  assert.match(await readyCard.innerText(), /Max Entry[\s\S]*\$105\.00[\s\S]*Stop[\s\S]*\$95\.00[\s\S]*Planned Risk[\s\S]*2\.00%[\s\S]*Support[\s\S]*\$98\.00/);
  const managementStyle = page.getByLabel('Management style');
  assert.equal(await managementStyle.inputValue(), 'martin', 'Martin-like management is the default');
  assert.match(await readyCard.innerText(), /T1 · 3R · sell 10–15%[\s\S]*\$127\.00[\s\S]*trim 10–15% at T1[\s\S]*remainder using daily EMA9[\s\S]*Hypothetical targets[\s\S]*actual fill/i);
  await managementStyle.selectOption('balanced_daily');
  assert.match(await readyCard.innerText(), /T1 · 3R · sell 33%[\s\S]*\$127\.00[\s\S]*T2 · 6R · sell 33%[\s\S]*\$151\.00[\s\S]*34% runner using daily EMA9[\s\S]*Hypothetical targets[\s\S]*actual fill/);
  await page.reload();
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await page.getByRole('button', { name: 'Trade Readiness', exact: true }).click();
  await page.getByRole('article', { name: 'READY trade candidate' }).waitFor();
  assert.equal(await page.getByLabel('Management style').inputValue(), 'balanced_daily', 'Preset persists after reload');
  assert.match(await page.getByRole('article', { name: 'READY trade candidate' }).innerText(), /T2 · 6R · sell 33%[\s\S]*\$151\.00/);
  const readyCardAfterReload = page.getByRole('article', { name: 'READY trade candidate' });
  const sizeLink = readyCardAfterReload.getByRole('link', { name: 'Size READY position', exact: true });
  const handoff = new URL(await sizeLink.getAttribute('href'), 'http://localhost');
  assert.equal(handoff.pathname, '/position-calculator');
  assert.deepEqual(Object.fromEntries(handoff.searchParams), { ticker: 'READY', entry: '103', stop: '95', maxEntry: '105', signalDate: '2026-10-16', management: 'balanced_daily', source: 'top-hits' });
  await sizeLink.click();
  await page.locator('.risk-context').waitFor();
  assert.equal(await page.getByLabel('Entry price').inputValue(), '103');
  assert.equal(await page.getByLabel('Stop price').inputValue(), '95');
  assert.match(await page.locator('.risk-context').innerText(), /READY[\s\S]*\$103\.00[\s\S]*\$95\.00[\s\S]*\$105\.00[\s\S]*2026-10-16[\s\S]*Balanced Daily/);
  assert.match(await page.locator('.risk-result-lead').innerText(), /Buy up to[\s\S]*62 shares[\s\S]*\$6,386\.00/);
  assert.match(await page.locator('.risk-management').innerText(), /T1 · 3R · sell 33%[\s\S]*\$127\.00 · 20 shares[\s\S]*T2 · 6R · sell 33%[\s\S]*\$151\.00 · 20 shares[\s\S]*Runner · daily EMA9[\s\S]*22 shares/);
  await page.setViewportSize({ width: 390, height: 844 });
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Prefilled calculator fits mobile viewport');
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByLabel('Entry price').fill('106');
  await page.getByText('WAIT — ABOVE MAX ENTRY', { exact: true }).waitFor();
  assert.match(await page.locator('.risk-result-lead').innerText(), /Calculated size · plan invalid/);
  assert.match(await page.locator('.risk-result-list').innerText(), /Planned cash loss at stop[\s\S]*Account risk actually used[\s\S]*Limiting factor/);
  await page.getByRole('checkbox', { name: /Remember on this device/ }).check();
  const rememberedDefaults = JSON.parse(await page.evaluate(() => localStorage.getItem('ticker-screener:position-calculator-defaults') || '{}'));
  assert.deepEqual(Object.keys(rememberedDefaults).sort(), ['accountValue', 'maxAllocationPercent', 'riskPercent']);
  const topHitsBase = new URL(process.env.TOP_HITS_URL || 'http://127.0.0.1:5173/scanner/top-hits');
  await page.goto(new URL('/scanner/top-hits', topHitsBase).toString());
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await page.getByRole('button', { name: 'Trade Readiness', exact: true }).click();
  await readyCardAfterReload.waitFor();
  assert.equal(await page.locator('.trade-readiness-lane.is-support .trade-readiness-management').count(), 0, 'Support cards do not show management guidance');
  assert.equal(await page.locator('.trade-readiness-lane.is-support .trade-readiness-size').count(), 0, 'Support cards do not show sizing handoff');
  assert.equal(await page.locator('.trade-readiness-lane.is-trigger .trade-readiness-management').count(), 0, 'Daily Trigger cards do not show management guidance');
  assert.equal(await page.locator('.trade-readiness-lane.is-trigger .trade-readiness-size').count(), 0, 'Daily Trigger cards do not show sizing handoff');
  assert.equal(await page.getByRole('article', { name: 'BAD3R trade candidate' }).locator('.trade-readiness-management').count(), 0, 'Invalid trigger and stop do not produce targets');
  assert.equal(await page.getByRole('article', { name: 'BAD3R trade candidate' }).locator('.trade-readiness-size').count(), 0, 'Invalid plan has no sizing handoff');
  assert.equal(await page.getByRole('article', { name: 'OVER trade candidate' }).locator('.trade-readiness-size').count(), 0, 'Over-limit plan has no sizing handoff');
  assert.equal(await page.locator('.trade-readiness-lane.is-needs-data .trade-readiness-management').count(), 0, 'Needs Data cards do not show management guidance');
  assert.equal(await page.locator('.trade-readiness-lane.is-needs-data .trade-readiness-size').count(), 0, 'Needs Data cards do not show sizing handoff');
  assert.equal(await readyCardAfterReload.locator('details[open]').count(), 0, 'Evidence starts collapsed');
  await readyCardAfterReload.getByText('Candidate evidence').click();
  assert.match(await readyCardAfterReload.innerText(), /MA pullback retest[\s\S]*Signal date 2026-10-16/);
  assert.equal(await readyCardAfterReload.getByText('Technology', { exact: true }).count(), 1, 'Sector is not duplicated inside candidate evidence');
  await page.locator('.trade-readiness-lane.is-support').getByRole('button', { name: 'Show 8 more (3 remaining)' }).click();
  await page.locator('.trade-readiness-lane.is-support').getByRole('article', { name: 'RISK trade candidate' }).getByText(/Risk exceeds 5%/).waitFor();
  await page.locator('.trade-readiness-lane.is-support').getByRole('article', { name: 'NOPLAN trade candidate' }).getByText('Trade plan unavailable for this candidate.').waitFor();
  assert.match(await readyCardAfterReload.getAttribute('aria-label'), /READY trade candidate/);
  await readyCardAfterReload.locator('.trade-readiness-select').click();
  await page.locator('.trade-readiness-lane.is-plan-ready .trade-readiness-card.is-selected').waitFor();
  assert.match(await readyCardAfterReload.getAttribute('class'), /is-selected/, 'Selected ticker has visible card state');
  assert.equal(await page.locator('.trade-readiness-card[aria-busy="true"]').count(), 0, 'Idle cards expose not busy state');
  assert.ok(await page.locator('.trade-readiness-lane.is-leader').getByText(/No leaders are waiting/).count() === 0, 'Non-empty lane hides empty copy');
  await page.getByRole('button', { name: /Show avoided/ }).click();
  assert.equal(await page.locator('.trade-readiness-lane.is-avoided .trade-readiness-card').count(), 1, 'Avoided rows are explicitly shown');
  const avoidedCard = page.getByRole('article', { name: 'AVOID trade candidate' });
  assert.match(await avoidedCard.innerText(), /AVOID NEW ENTRY[\s\S]*Strike Zone says avoid; pullback quality failed/);
  assert.equal(await avoidedCard.getByText('Max Entry').count(), 0, 'Avoided cards suppress the actionable trade ticket');
  assert.equal(await avoidedCard.locator('.trade-readiness-management').count(), 0, 'Avoided cards suppress management guidance');
  assert.equal(await avoidedCard.getByRole('button', { name: /Save|Saved|Saving/ }).count(), 0, 'Avoided cards cannot be saved as actionable picks');
  await page.getByLabel('Group readiness candidates').selectOption('sector');
  for (const width of [390, 768, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    const viewportWidth = await page.evaluate(() => ({ inner: innerWidth, scroll: document.documentElement.scrollWidth }));
    assert.ok(viewportWidth.scroll <= viewportWidth.inner, `Sector-grouped readiness view fits ${width}px viewport (${viewportWidth.scroll}/${viewportWidth.inner})`);
  }
  await page.getByRole('button', { name: 'List', exact: true }).click();
  await page.getByRole('button', { name: 'Guru Board', exact: true }).click();
  await page.getByText('Loading Guru Board data…', { exact: true }).waitFor({ state: 'hidden' });
  const guruRequests = topHitsRequests.guru;
  assert.ok(guruRequests >= 1, 'Readiness data is loaded from the shared lazy Guru endpoint');
  await page.getByRole('button', { name: 'List', exact: true }).click();
  await page.getByRole('button', { name: 'Guru Board', exact: true }).click();
  assert.equal(topHitsRequests.guru, guruRequests, 'Loaded Guru section stays cached when switching views');
  await page.getByRole('button', { name: 'List', exact: true }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Mobile page fits viewport');
  await page.screenshot({ path: '/tmp/top-hits-mobile.png', fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.screenshot({ path: '/tmp/top-hits-desktop.png', fullPage: true });
  admin = false;
  await page.reload();
  await page.getByRole('button', { name: 'Clear filters', exact: true }).click();
  await cells.first().waitFor();
  assert.equal(await page.locator('.pinned-pick').count(), 0);
  const visitorBefore = await pinned.boundingBox();
  await page.locator('.scanner-result-table-wrap').evaluate(el => { el.scrollLeft = 800; });
  assert.ok(Math.abs((await pinned.boundingBox()).x - visitorBefore.x) < 2, 'Visitor ticker stays pinned without pick column');
  await page.getByRole('button', { name: 'Trade Readiness', exact: true }).click();
  await page.getByRole('heading', { name: 'Plan Ready', exact: true }).waitFor();
  assert.equal(await page.locator('.trade-readiness-save').count(), 0, 'Visitor cannot see save controls');
  assert.deepEqual(errors, []);
  console.log('Top Hits UX passed: grouped filters, lazy list/Guru requests, saved default preset, management preset switching/persistence, rename persistence, search, reset, chart badges, desktop pinning, visitor layout, mobile width.');
} finally { await browser.close(); }
