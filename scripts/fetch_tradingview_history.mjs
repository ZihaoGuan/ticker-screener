// One bounded historical request. Credentials remain in the environment.
import { pathToFileURL } from 'node:url';
import path from 'node:path';
const [clientDir, symbol, from, to] = process.argv.slice(2);
const load = file => import(pathToFileURL(path.join(clientDir, 'dist', file)).href);
const { TradingViewClient } = await load('client/client.js');
const { runOperation } = await load('data/operation.js');
const { planHistory, openMarket, loadHistory, selectCandles } = await load('data/history.js');
const credentials = process.env.TRADINGVIEW_SESSION && process.env.TRADINGVIEW_SIGNATURE
  ? {session:process.env.TRADINGVIEW_SESSION, signature:process.env.TRADINGVIEW_SIGNATURE} : undefined;
const query = {symbol, timeframe:'D', from:new Date(`${from}T00:00:00Z`),
  to:new Date(`${to}T23:59:59Z`), maxCount:20000, session:'regular',
  adjustment:'splits', timezone:'America/New_York', timeoutMs:45000};
const client = new TradingViewClient({credentials});
try {
  const plan = planHistory(query);
  const result = await runOperation({...query, client}, 'history', ({client,resolve,reject}) => {
    const chart = client.createChart();
    let metadata;
    chart.on('error', reject);
    chart.on('symbolLoaded', info => {metadata = info;});
    loadHistory(chart, plan, true, completion => resolve({metadata,completion,candles:selectCandles(chart.candles,plan)}));
    openMarket(chart,plan);
    return () => chart.delete();
  });
  process.stdout.write(JSON.stringify({query,retrieved_at:new Date().toISOString(),...result})+'\n');
} catch (error) {
  // Raw errors can include account response details; expose only the typed code.
  process.stdout.write(JSON.stringify({error_code:error.code ?? 'PROVIDER_ERROR'})+'\n');
  process.exitCode = 2;
} finally { await client.close(); }
