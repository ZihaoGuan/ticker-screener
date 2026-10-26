// Bounded read-only intraday request for one ticker and one US session day.
import { pathToFileURL } from 'node:url';
import path from 'node:path';
const [clientDir, symbol, day, timeframe='15'] = process.argv.slice(2);
if (!clientDir || !/^[A-Z0-9._-]+:[A-Z0-9._-]+$/.test(symbol ?? '') ||
    !/^\d{4}-\d{2}-\d{2}$/.test(day ?? '') || !['15','1'].includes(timeframe)) {
  process.stderr.write('usage: node fetch_tradingview_intraday.mjs CLIENT_DIR EXCHANGE:SYMBOL YYYY-MM-DD [15|1]\n');
  process.exit(64);
}
const load = file => import(pathToFileURL(path.join(clientDir, 'dist', file)).href);
const { TradingViewClient } = await load('client/client.js');
const { runOperation } = await load('data/operation.js');
const { planHistory, openMarket, loadHistory, selectCandles } = await load('data/history.js');
const credentials = process.env.TRADINGVIEW_SESSION && process.env.TRADINGVIEW_SIGNATURE
  ? {session:process.env.TRADINGVIEW_SESSION, signature:process.env.TRADINGVIEW_SIGNATURE} : undefined;
const from = new Date(`${day}T00:00:00Z`), to = new Date(`${day}T23:59:59Z`);
const query = {symbol, timeframe, from, to, maxCount:timeframe === '1' ? 500 : 100,
  session:'regular', adjustment:'splits', timezone:'America/New_York', timeoutMs:45000};
const client = new TradingViewClient({credentials});
try {
  const plan = planHistory(query);
  const result = await runOperation({...query, client}, 'history', ({client,resolve,reject}) => {
    const chart = client.createChart(); let metadata;
    chart.on('error', reject);
    chart.on('symbolLoaded', info => { metadata = info; });
    loadHistory(chart, plan, true, completion => resolve({metadata,completion,candles:selectCandles(chart.candles,plan)}));
    openMarket(chart,plan);
    return () => chart.delete();
  });
  process.stdout.write(JSON.stringify({query:{...query,from:from.toISOString(),to:to.toISOString()},retrieved_at:new Date().toISOString(),...result})+'\n');
} catch (error) {
  process.stdout.write(JSON.stringify({error_code:error.code ?? 'PROVIDER_ERROR'})+'\n');
  process.exitCode = 2;
} finally { await client.close(); }
